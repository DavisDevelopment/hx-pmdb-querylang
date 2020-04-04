package ql.sql.runtime;

import ql.sql.runtime.plan.SelectPlan;
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
import ql.sql.runtime.Traverser;

// import pm.Helpers.nor;
import pm.Helpers.*;

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
    
    public var plan:SelectPlan<Table, Row, ResultRow> = null;

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

    public function new(?stmt:SelectStatement, ?p:SelPredicate, ?e:SelOutput) {
        if (stmt != null) _stmt = stmt;
        if (p != null) _predicate = p;
        if (e != null) _exporter = e;
        _traverser = new Traverser();
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

    public function apply(sel:Sel<Dynamic, Dynamic, Dynamic>, ?interp:Interpreter):Array<Dynamic> {
        if (sel.plan != null) {
            var accumulator:Array<Dynamic> = new Array();
            function reduc(a:Array<Dynamic/*OutRow*/>, row:Dynamic/*InRow*/):Array<Dynamic> {
                a.push(row);
                return a;
            }
            final success = sel.plan.candidates(function(current:Dynamic, offset:Int) {
                accumulator = reduc(accumulator, current);
                return true;
            });

            if (!success)
                throw new pm.Error('you\'re a failure, dude.');

            return accumulator;
        }
        else {
            return applyNaive(sel, interp);
        }
    }

#if isnt_awful_code
    private var __planItr:Itr<Dynamic> = null;
    private function naiveCandidatePlan(sel:Sel<Dynamic, Dynamic, Dynamic>, ?interp:Interpreter):Itr<Dynamic> {
        //cache iterator instance
        if (__planItr != null) {
            __planItr.reset();
            return __planItr;
        }

		var g:SelectStmtContext<Dynamic, Dynamic, Dynamic> = sel.context;
		if (g.sources.length == 0)
            g.addSource(sel.source.getSpec(g.context));
        final c = g.context;
        var outputSources = sel.getSourcesUsedInOutput();
        
        /**
         * called before iteration starts
         */
        function begin() {
            Console.debug('candidate-iteration begun');
            c.beginScope();
            c.pushSourceScope(cast g.sources.sources);
            if (g.sources.length == 1)
                c.use(g.sources.sources[0]);
        }

        /**
         * called immediately after iteration completes, 
         * before the first call to `hasNext` for which the result is `false` returns
         */
        function finish() {
            c.popSourceScope();
            c.endScope();
        }

        /**
         * applied to each step, before `next` returns
         * @param step the value from `next`
         */
        
        var middle = function(step: Dynamic) {};
        function middle0(step: Dynamic) {
            if (!nn(step)) {
                throw new pm.Error('null');
            }

            // var rows:Array<Dynamic> = [row];
            if ((step is Array<Dynamic>)) {
                final len:Int = cast(step, Array<Dynamic>).length;
                switch len {
                    case 0, 1: throw new pm.Error('y');
                    /**
                     * some loop-unrolling for speed :D
                     */
                    case 2: 
                        middle = cast function(rows:Array<Dynamic>) {for (i in 0...2) if (nn(outputSources.sources[i])) g.context.focus(rows[i], outputSources.sources[i].name);};
                    case 3: 
                        middle = cast function(rows:Array<Dynamic>) {for (i in 0...3) if (nn(outputSources.sources[i])) g.context.focus(rows[i], outputSources.sources[i].name);};
                    case 4: 
                        middle = cast function(rows:Array<Dynamic>) {for (i in 0...4) if (nn(outputSources.sources[i])) g.context.focus(rows[i], outputSources.sources[i].name);};
                    case 5: 
                        middle = cast function(rows:Array<Dynamic>) {for (i in 0...5) if (nn(outputSources.sources[i])) g.context.focus(rows[i], outputSources.sources[i].name);};
                    default:
                        middle = cast function(rows: Array<Dynamic>) {
                            // var rows:Array<Dynamic> = cast step;
                            for (i in 0...rows.length) {
                                var src:TableSpec = outputSources.sources[i];
                                
                                if (src != null) {
                                    g.context.focus(rows[i], src.name);
                                }
                            }
                        };
                }
            }

            else {
                middle = function(row: Dynamic) {
                    // var row = step;
                    if (outputSources.sources[0] != null)
                        g.context.focus(row, outputSources.sources[0].name);
                    else {
                        #if debug
                        throw new pm.Error('outputSources empty');
                        #end
                        middle = #if js cast Functions.noop #else function(x:Dynamic) {} #end;
                    }
                };
            }

            middle(step);
        }
        middle = middle0; 
       
        /*
        function middle(step: Dynamic) {
            if (Std.is(step, Array)) {
                var rows:Array<Dynamic> = cast step;
                for (i in 0...rows.length) {
                    var src:TableSpec = outputSources.sources[i];
                    
                    if (src != null) {
                        g.context.focus(rows[i], src.name);
                    }
                }
            }
            else {
                var row = step;
                if (outputSources.sources[0] != null)
                    g.context.focus(row, outputSources.sources[0].name);
            }
        }
        */
        
        var itr:Iterator<Dynamic> = null;
        var dyn:Dynamic = {};
        var _Started = false,
            _Finished = false;
        
        dyn.hasNext = function() {
            var r = itr.hasNext();
            if (!_Finished && !r) {
                _Finished = true;
                finish();
            }
            return r;
        };

        dyn.next = function() {
            if (!_Started) {
                begin();
                _Started = true;
            }

            var row:Dynamic = itr.next();
            log(row);
            middle(row);
            return row;
        };

        dyn.reset = function _init() {
            if (_Started && !_Finished) {
                throw new pm.Error('Unterminated iteration. Are you trying to use multiple iterators at once?');
            }

            _Started = false;
            _Finished = false;
            itr = _traverser.iterator(sel, g, null, null, interp);//.map(x -> {middle(x); x;});
        };
        _init();

        var out:Itr<Dynamic> = cast dyn;
        return this.__planItr = out;
    }
#end

    /**
     * get 'r dun
     * @param sel 
     * @param interp 
     */
    public function applyNaive(sel:Sel<Dynamic, Dynamic, Dynamic>, ?interp:Interpreter) {
        var g:SelectStmtContext<Dynamic, Dynamic, Dynamic> = sel.context;
        if (g.sources.length == 0)
            g.addSource(sel.source.getSpec(g.context));

        var output:Array<Dynamic> = new Array();
        
//{region callbacks
        /**
         * the function that transforms the source row(s) into the result (output) row
         */
         var extract;
         extract = nn(interp) ? o -> interp.buildSelectStmtResultRow(cast sel) : o -> _exporter.mCompiled(o);
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
//}endregion
        
        // // var input:Iterator<Dynamic>; // the iterator object to be walked over
        // // input = _traverser.iterator(sel, g, test, extract, interp);
        var input = new Array();//allocate new array to hold candidate rows
        
        // scoping doodads
        final c = g.context;
        c.beginScope();
        c.pushSourceScope(cast g.sources.sources);
        if (g.sources.length == 1) {
            c.use(g.sources.sources[0]);
        }
        
        var outputSources = sel.getSourcesUsedInOutput();
        var sourceNames = outputSources.map(src -> src.name);

        _traverser.computeCandidates(sel, g, input, test, interp);// actually compute the content of `input`
        
        for (item in input) {
            /* // var rows:Array<Dynamic> = (rowsBuf==null?rowsBuf=[]:rowsBuf);
            if ((item is Array<Dynamic>)) {
                final a = cast(item, Array<Dynamic>);
                // if (rows.length == 0) rows.resize(a.length);
                if (rowCount == -1) rowCount = a.length;
                else assert(rowCount == a.length, '${rowCount} != ${a.length}');

                // a.blit(0, rows, 0, a.length);
                for (i in 0...rowCount) {
                    // var row:Dynamic = rows[i];
                    var src:TableSpec = outputSources.sources[i];

                    if (src != null) {
                        g.context.focus(a[i], src.name);
                    }
                }
            }
            else {
                // rows[0] = item;
                // assert(rows.length == 1);
                var row:Dynamic = item;
                var src = outputSources.sources[0];
                if (src != null)
                    g.context.focus(row, src.name);
                else if (g.context.unaryCurrentRow)
                    g.context.focus(row);
            } */

            Traverser.focusRows(g, item, sourceNames);

            if (test(g)) {
                output.push(extract(g));
            }
        }

        c.popSourceScope();
        c.endScope();
        // scope exited

        return output;
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

typedef Itr<T> = Iterator<T> & {
    function reset():Void;
};