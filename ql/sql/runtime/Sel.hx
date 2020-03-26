package ql.sql.runtime;

import haxe.Constraints.Function;
import ql.sql.runtime.sel.SelectStmtContext;
import ql.sql.runtime.Stmt.SelectStmt;
import ql.sql.runtime.Stmt.StmtNode;
import ql.sql.grammar.CommonTypes.Contextual;
import ql.sql.grammar.CommonTypes.JoinType;
import pmdb.core.Object;
import pmdb.core.Object.Doc as JsonObject;
import pmdb.core.FrozenStructSchema as JsonSchema;
import ql.sql.common.SqlSchema;
import ql.sql.runtime.TAst;
import ql.sql.grammar.CommonTypes.SqlSymbol;
import ql.sql.ast.Query.SelectStatement;
import ql.sql.runtime.VirtualMachine;
// import ql.sql.runtime.

import pm.Helpers.nor;
import pm.Helpers.nn;

using pm.Functions;

@:allow(ql.sql.runtime)

/**
 * boop
 */
class Sel<Table, Row, ResultRow> extends StmtNode<Array<ResultRow>> {
	private var _attachments:JsonObject;

	public var context:SelectStmtContext<Dynamic, Table, Row>;
	public var evalHead:Array<JITFn<Dynamic>>;
	public var evalTail:Array<JITFn<Dynamic>>;
    // public var driver:ISelDriver<Table, Row, Column>;
    
	public final source:TableSource;
	public final sourceSchema:SqlSchema<Row>;
    public final resultSchema:SqlSchema<ResultRow>;
    
	public final i:SelectImpl;

	/**
	 * construct, boo
	 * @param context
	 * @param source
	 * @param sel
	 * @param driver
	 */
	public function new(context, source, sel:SelectImpl, ?driver:Dynamic) {
		super();

		_attachments = {};
		this.context = context;
		this.source = source;
		// this.driver = driver;
		this.sourceSchema = cast this.source.getSchema(cast context.context);
		this.i = sel;
		this.resultSchema = cast this.i._exporter.schema;
		this.evalHead = new Array();
        this.evalTail = new Array();

        // pruneSources();
    }
    
    public function getSourcesUsedInOutput() {
        if (context.sources.length <= 1)
            return context.sources;

        var used = new pm.map.Set();
        inline function add(n: Null<String>) {
            if (!n.empty())
                used.add(n);
        }

        for (item in i._exporter) {
            switch item {
                case All(table):
                    add(table.label());
                
                case Column(table, column, alias):
                    add(nor(alias, table).label());
                
                case Expression(alias, expr):
                    //
            }
        }

        return new SelectSourceCollection(context.sources.sources.filter(tbl -> used.has(tbl.name)));
    }

	public override function eval():Array<ResultRow> {
		var g = {context: context.context};
		for (f in evalHead)
			f(g);
		var result:Array<ResultRow> = cast i.apply(this);
		for (f in evalTail)
			f(g);
		return result;
	}

	override function bind(params:Dynamic) {
		if (i._predicate != null) i._predicate.bindParameters(cast params);
		//Console.log(params);

		for (itm in i._exporter.items) {
			switch itm {
				case Expression(_, expr):
					expr.bindParameters(params);
				default:
			}
        }
        
        switch source.item {
            case Table(_):
            case Stream(s):
                if (s.term.select != null)
                    s.term.select.bind(params);
        }
    }
     
    private function export():ResultRow {
        final out = i._exporter;
        if (out.mCompiled != null)
            return out.mCompiled(context);
        return cast out.build(context.context);
    }
}

@:access(ql.sql)
class SelectImpl {
    public var _iter: Null<JITFn<Iterator<Dynamic>>> = null;
    public var _stmt: Null<SelectStatement> = null;
    public var _predicate:Null<SelPredicate> = null;
    public var _exporter:Null<Exporter<Doc>> = null;
    public var _traverser:Null<Traverser> = null;
    public var order: Null<Array<{accessor:TExpr, direction:Int}>> = null;

    public function new(sel, ?stmt:SelectStatement, ?p:SelPredicate, ?e:SelOutput) {
        if (stmt != null) _stmt = stmt;
        if (p != null) _predicate = p;
        if (e != null) _exporter = e;
        _traverser = new Traverser();
        _traverser.iter = function(sel, source, f) {
            sel.context.context.glue.tblGetAllRows(source).iter(f);
        };
    }

    function sort_(g:Contextual, arr:Array<Doc>) {
		if (order != null) {
            inline function acc(e:TExpr, o:Dynamic):Dynamic {
                g.context.focus(o, '_');
                return e.eval(g);
            }

			arr.sort(function(x:Doc, y:Doc):Int {
				// var x: = a.asObject(), y = b.asObject();
				var c, cmp;
				for (i in 0...order.length) {
					c = order[i];
					cmp = c.direction * pmdb.core.Arch.compareThings(acc(c.accessor, x), acc(c.accessor, y));
					if (cmp != 0)
						return cmp;
				}
				return 0;
			});
		}
    }

    public function apply(sel:Sel<Dynamic, Dynamic, Dynamic>, ?interp:Interpreter) {
        final isSimpleSelect = true;//placeholder for variable determining whether or not aggregate functions are used in the selection

        var g:SelectStmtContext<Dynamic, Dynamic, Dynamic> = sel.context;
        if (g.sources.length == 0)
            g.addSource(sel.source.getSpec(g.context));
            // g.sources = [sel.source.getSpec(g.context)];

        var acc:Array<Dynamic> = new Array();
        var input:Iterator<Dynamic>;// the iterator object to be walked over

        /**
         * the function that transforms the source row(s) into the result (output) row
         */
        var extract;
        extract = function(g: Contextual):Dynamic {
            // g.context.focus(, sel.source.getName(g.context));
            return _exporter.mCompiled(g);
        };
        if (nn(interp)) {
            extract = o -> interp.buildSelectStmtResultRow(cast sel);
        }
        if ((_exporter.items : pm.ImmutableList.ListRepr<SelItem>).match(Hd(All(null), Tl))) {
            extract = o -> o.context.getCurrentRow();
        }

        /**
         * the function that determines whether a row is processed further
         */
        var test:Null<JITFn<Bool>> = null;
        if (_predicate != null)
            test = nn(interp) ? g->interp.pred(_predicate) : (_predicate.mCompiled !=  null ? g->_predicate.mCompiled(g) : g->_predicate.eval(g));
        else
            test = g->true;
        
        input = _traverser.iterator(sel, g, test, extract, interp);


        // for (src in g.sources) {
            // g.context.use(src);
            // }
        final c = g.context;
        c.beginScope();
        c.pushSourceScope(cast g.sources.sources);
        if (g.sources.length == 1) {
            c.use(g.sources.sources[0]);
        }

        var outputSources = sel.getSourcesUsedInOutput();

        for (item in input) {
            var rows:Array<Dynamic>;
            if ((item is Array<Dynamic>)) {
                rows = cast item;
            }
            else {
                rows = [item];
            }

            #if debug
            if (rows.length != outputSources.length) {
                null;
                //! may indicate a problem
            }
            #end

            for (i in 0...rows.length) {
                var row:Dynamic = rows[i];
                var src:TableSpec = outputSources.sources[i];

                if (src != null) {
                    g.context.focus(row, src.name);
                }
            }

            if (test(g)) {
                acc.push(extract(g));
            }
        }

        c.popSourceScope();
        c.endScope();

        return acc;
    }
}

@:yield
class Traverser {
    public var iter: Null<(sel:Sel<Dynamic, Dynamic, Dynamic>, source:Dynamic, fn:Dynamic->Void)->Void> = null;
    // public var sel: Sel<Dynamic>;
    #if js
    public var generator: haxe.Constraints.Function;
    public var coroutine: js.lib.Iterator<Dynamic>;
    #end
    public function new() {
        // this.sel = sel;
        #if js
        this.iterator = jsGenItrWrap(this.iterator);
        //Console.log(this.iterator);
        #end
    }

    // public dynamic function apply

    /**
     * constructs and returns an `Iterator<?>` over the candidate and/or operand rows of the query in question
     * TODO the primary iterator unit doesn't need to be ()->Iterator<Dynamic>, and this harms performance. Correct this
     * @param sel the `SelectStmt` instance this Traverser belongs to
     * @param g 
     * @param filter (when one exists) the function to be invoked to obtain the boolean value determining whether the currently focused row is operated upon
     * @param extract the function invoked to obtain the output (result) row for a given step of the traversal
     * @param interp (when being called by `Interpreter`) the Interpreter instance executing the statement
     * @return an `Iterator<?>` object over the candidate rows
     */
    public dynamic function iterator(sel:Sel<Dynamic, Dynamic, Dynamic>, g:Contextual, filter:Null<JITFn<Bool>>, extract:JITFn<Dynamic>, ?interp:Interpreter):Iterator<Dynamic> {
        var c = g.context;
        var src = sel.source;

        var itr:()->Iterator<Dynamic> = ()-> src.getSpec(c).open(g);
        var proceed:Bool = true;

        if (proceed && src.joins != null) {
            final join = src.joins[0].unwrap();
            var jsrc = join.mJoinWith;
            if (jsrc == null)
				throw new pm.Error('Missing source item');
            // sel.context.sources.push(jsrc.src);
            sel.context.addSource(jsrc.src);
            var sourceNames:Array<String> = [src.getName(g.context), jsrc.src.name];

            switch join.joinType {
                case Cross:
			    	final joinIter:() -> Iterator<Dynamic> = () -> jsrc.src.open(g);
                    var srcIter:() -> Iterator<Dynamic> = itr;

                    #if (js || neko || py)
                    itr = () -> Traverser.nestedLoopCrossJoinItr(g, sourceNames, srcIter, joinIter);
                    #else
					itr = () -> Traverser.nestedLoopCrossJoin(g, sourceNames, srcIter, joinIter);
                    #end

                case Inner:
                    final joinIter = ()->jsrc.src.open(g);
                    var srcIter:()->Iterator<Dynamic> = itr;
                    var filter:JITFn<Bool> = function(g: Contextual) {
                        return join.on != null ? (interp != null ? interp.pred(join.on) : join.on.eval(g)) : true;
                    };
                    
					#if (js || neko || py)
					itr = () -> Traverser.nestedLoopInnerJoinItr(g, sourceNames, srcIter, joinIter, filter);
					#else
					itr = () -> Traverser.nestedLoopInnerJoin(g, sourceNames, srcIter, joinIter, filter);
					#end
                    // return it;

                default:
                    //Console.error('TODO');
            }
        }

        return itr();
    }

    #if js
    extern inline public static function jsGenItrWrap<T, Fn:Function>(f:Fn):Fn {
        final tmp = f.toGenerator();
        
        return cast Reflect.makeVarArgs(function(args: Array<Dynamic>):js.lib.HaxeIterator<T> {
            
            var coro:js.lib.Iterator<T> = cast Reflect.callMethod(null, tmp, args);
            var iter = new js.lib.HaxeIterator(coro);
            return iter;
        });
    }
    #end

	public static function nestedLoopCrossJoinItr(g:Contextual, sources:Array<String>, a_set:Void->Iterator<Dynamic>, b_set:Void->Iterator<Dynamic>):Iterator<Array<Dynamic>> {
        // var acc:Array<Dynamic> = new Array();
        var rows:Array<Dynamic> = [null, null];

		for (a in a_set()) {
            // g.context.focus(a, sources[0]);
            rows[0] = a;
			for (b in b_set()) {
                // g.context.focus(b, sources[1]);
                rows[1] = b;
                @yield return rows;
			}
        }
        
		// return [].iterator();
		#if macro
		return [].iterator();
		#end
    }
    
    public static function nestedLoopInnerJoinItr(g:Contextual, sources:Array<String>, aSet:Void->Iterator<Dynamic>, bSet:Void->Iterator<Dynamic>, filter:JITFn<Bool>):Iterator<Array<Dynamic>> {
        var rows:Array<Dynamic> = [null, null];
        for (a in aSet()) {
            rows[0] = a;
            for (b in bSet()) {
                g.context.focus(a, sources[0]);
                g.context.focus(b, sources[1]);
                var doJoin:Bool = filter(g);
                if (doJoin) {
                    rows[1] = b;
                    @yield return rows;
                }
            }
        }

		#if macro
		return [].iterator();
		#end
    }

    public static function nestedLoopInnerJoin(g:Contextual, sources:Array<String>, aSet:Void->Iterator<Dynamic>, bSet:Void->Iterator<Dynamic>, filter:JITFn<Bool>):Iterator<Array<Dynamic>> {
        // return aSet().flatMap(function(a: Dynamic):Iterator<Array<Dynamic>> {
        //     return bSet().map(function(b: Dynamic) {
        //         return [a, b];
        //     })
        //     .filter(function(arr: Array<Dynamic>) {
        //         g.context.focus(arr[0], sources[0]);
        //         g.context.focus(arr[1], sources[1]);
        //         return arr[0] != arr[1] && filter(g);
        //     });
        // });
        var aa = aSet().array();
        var ba = bSet().array();
        var res = [];
        for (a in aa) {
            for (b in ba) {
                if (a != b) {
                    g.context.focus(a, sources[0]);
                    g.context.focus(b, sources[1]);
                    if (filter(g)) {
                        res.push([a, b]);
                    }
                }
            }
        }
        return res.iterator();
    }

	public static function nestedLoopCrossJoin(g:Contextual, sources:Array<String>, a_set:Void->Iterator<Dynamic>, b_set:Void->Iterator<Dynamic>):Iterator<Array<Dynamic>> {
        // var rows:Array<Dynamic> = [null, null];
        // return a_set.iterator().flatMap(function(a:Dynamic):Iterator<Array<Dynamic>> {
        //     return b_set.iterator().map(function(b: Dynamic) {
        //         return [a, b];
        //     });
        // });
        var aa = a_set().array();
        var ba = b_set().array();
        var res = [];
        for (a in aa) {
            for (b in ba) {
                res.push([a, b]);
            }
        }
        return res.iterator();
    }
}

typedef DummyTable<Row> = {
    var name: String;
    var data: Array<Row>;
};

private typedef Exp = ql.sql.runtime.TAst.SelOutput;

@:forward
abstract Exporter<To> (Exp) from Exp to Exp {
    public function build(c: Context<Dynamic, Dynamic, Dynamic>):To {
        return cast this.build(cast c);
    }
}

@:generic
class Aliased<T> {
    public var alias: Null<String>;
    public var term: T;

    public function new(alias, term) {
        this.alias = alias;
        this.term = term;
    }
}

enum TableSourceItem {
    Table(table:Aliased<TableSpec>);
    Stream(table: Aliased<TableStream>);
	// Aliased(table: Aliased<TableRef>);
	// Join(source1:TableSource, joinType:JoinType, source2:TableSource);
}

class TableSource {
    // public var type: TableSourceType;
    public var item:TableSourceItem;
    public var joins: Null<Array<TableJoin>> = null;
    // public var name: String;
    public function new(item:TableSourceItem, ?joins) {
        this.item = item;
        this.joins = joins;
    }

    private var _spec:Null<TableSpec> = null;
    public function getSpec(c: Context<Dynamic, Dynamic, Dynamic>):TableSpec {
        if (_spec != null)
            return _spec;

        return _spec = switch item {
            case Table({alias:alias, term:r}):
                assert(c.glue.valIsTable(r.table), new pm.Error('TableRef constructed with non-Table reference'));
                var t = new TableSpec(r.name, getSchema(c), {table:r.table});
                if (alias != null)
                    t = new TableSpec(alias, t.schema, {src:t});
                t;

            case Stream({alias:alias, term:stream}):
                var t = new TableSpec(nor(alias, '_'), stream.schema, {
                    stream: stream,
                    stmt: stream.select
                });
                // t.open = (g -> new pm.iterators.EmptyIterator());
                t;
        };
    }

    public function getSchema(context: Context<Dynamic, Dynamic, Dynamic>):Null<SqlSchema<Dynamic>> {
        // this.
        switch item {
            case Table({term:{table:table}}):
                return context.glue.tblGetSchema(table);

            case Stream({term:{schema:schema}}):
                return schema;
        }
    }

    public function getName(context: Context<Dynamic, Dynamic, Dynamic>):String {
        return switch item {
            case Table({alias:alias, term:{name:name}}): pm.Helpers.nor(alias, name);
            case Stream({alias:name}): nor(name, '_');
        }
    }

    public function getTable():TableSpec {
        return switch item {
            case Table({term:r}): r;
            default: null;
        }
    }

    public function getAllTables():Map<String, TableSpec> {
        var res = new Map<String, TableSpec>();
        switch item {
            case Table({alias:alias, term:table}):
                if (alias != null)
                    res[alias] = table;
                else
                    res[table.name] = table;

            default:
                throw pm.Error.withData(item);
        }
        if (joins != null) for (join in joins) {
            switch join.joinWith {
                case Table({alias:alias, term:table}):
                    res[pm.Helpers.nor(alias, table.name)] = table;

                case other:
                    throw pm.Error.withData(other);
            }
        }
        return res;
    }
}

// class TableRef  {
//     public var table: Dynamic;
//     public var name: String;
//     public var pos: haxe.PosInfos;

//     public function new(name, table, ?pos) {
//         this.name = name;
//         this.table = table;
//         this.pos = pos;
//     }
// }
typedef TableRef = ql.sql.ast.Query.TableSpec;

class TableJoin {
    public var joinWith: TableSourceItem;
	public var mJoinWith:Null<{src:TableSpec, ?cursor:Dynamic}>;
    public var joinType: JoinType;
    public var on: Null<SelPredicate> = null;
    public var used: Null<Array<Dynamic>> = null;

    public function new(type, with) {
        this.joinType = type;
        this.joinWith = with;
    }
}

class TableStream {
    public var schema: SqlSchema<Dynamic>;
    public var open: JITFn<Iterator<Dynamic>>;
    // public var astNode: ql.sql.TsAst.SqlAstNode;

    public var select:Null<SelectStmt> = null;

    public function new(?sel:SelectStmt) {
        if (sel != null) {
            this.select = sel;
        }
    }
}