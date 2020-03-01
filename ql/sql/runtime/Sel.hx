package ql.sql.runtime;

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
import ql.sql.TsAst.SelectStatement;
import ql.sql.runtime.VirtualMachine;
// import ql.sql.runtime.

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
            sel.context.glue.tblGetAllRows(source).iter(f);
        };
    }

    function sort_(g:Contextual, arr:Array<Doc>) {
		if (order != null) {
            inline function acc(e:TExpr, o:Dynamic):Dynamic {
                g.context.focus(o);
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

    public function apply(sel:Sel<Dynamic, Dynamic, Dynamic>) {
        var c = sel.context;
        // var acc:Array<Dynamic> = [];
        // if (_traverser.iter != null) _traverser.iter(sel, sel.source, function(row: Dynamic) {
        //     _apply(acc, row, sel);
        // });
        // return acc;
        var acc:Array<Dynamic> = new Array();
        var input:Iterator<Dynamic>;
        
        input = _traverser.iterator(sel, cast sel);

        // var test = _predicate != null ? 
        for (row in input) {
            // c.currentRows = ;
            if (_predicate != null) {
                if (_predicate.eval(cast sel)) {
                    acc.push(_exporter.mCompiled(cast sel));
                }
            }
            else {
                acc.push(_exporter.mCompiled(cast sel));
            }
        }

        sort_(cast sel, cast acc);

        return acc;
    }
}

class Traverser {
    public var iter: Null<(sel:Sel<Dynamic, Dynamic, Dynamic>, source:Dynamic, fn:Dynamic->Void)->Void> = null;
    // public var sel: Sel<Dynamic>;
    public function new() {
        // this.sel = sel;
    }

    public dynamic function iterator(sel:Sel<Dynamic, Dynamic, Dynamic>, g:Contextual):Iterator<Dynamic> {
        var c = g.context;
        var src = sel.source;
        // c.currentDefaultTable = src.getName(c);
        var itr:Iterator<Dynamic> = new pm.iterators.EmptyIterator();
        switch src.item {
            case Table({term:ref={table:table, name:name}}):
                var tbl = g.context.resolveTableFrom(table);
                itr = g.context.glue
                    .tblGetAllRows(tbl)
                    .iterator()
                    .map(function(row: Dynamic) {
                        // c.currentRows[name] = row;
                        c.focus(row, name);
                        return row;
                    });

            case Stream({alias:alias, term:stream}):
                itr = stream.open(g);

            case other:
                throw new pm.Error('Unhandled $other');
        }

        if (src.joins != null) {
            Console.error('TODO');
        }

        return itr;
    }
}

@:allow(ql.sql.runtime.Sel.ISelDriver)
/**
 * boop
 */
class Sel<Table, Row, ResultRow> extends StmtNode<Array<ResultRow>> {

    private var _attachments:JsonObject;
    
    public var evalHead:Array<JITFn<Dynamic>>;
    public var evalTail:Array<JITFn<Dynamic>>;
    // public var driver:ISelDriver<Table, Row, Column>;
    public final source: TableSource;
    public final sourceSchema: SqlSchema<Row>;
    public final resultSchema: SqlSchema<ResultRow>;
    public var context: Context<Dynamic, Table, Row>;
    public final i: SelectImpl;

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
        this.sourceSchema = cast this.source.getSchema(context);
        this.i = sel;
        this.resultSchema = cast this.i._exporter.schema;
        this.evalHead = new Array();
        this.evalTail = new Array();
    }

    public override function eval():Array<ResultRow> {
        for (f in evalHead)
            f(this);
        var result:Array<ResultRow> = cast i.apply(this);
        for (f in evalTail)
            f(this);
        return result;
    }

    public function bind(params: Dynamic):Sel<Table, Row, ResultRow> {
        i._predicate.bindParameters(cast params);

        return this;
    }
}

typedef DummyTable<Row> = {
    var name: String;
    var data: Array<Row>;
};

private typedef Exp = ql.sql.runtime.TAst.SelOutput;

@:forward
abstract Exporter<To> (Exp) from Exp to Exp {
    public inline function build(c: Context<Dynamic, Dynamic, Dynamic>):To {
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
    Table(table:Aliased<TableRef>);
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
                assert(c.glue.valIsTable(r.table), new pm.Error('TableRef constructed with non-Table reference', r.pos));
                var t = new TableSpec(r.name, getSchema(c), {table:r.table});
                if (alias != null)
                    t = new TableSpec(alias, t.schema, {src:t});
                t;

            case Stream({alias:alias, term:stream}): 
                var t = new TableSpec(alias, stream.schema, {
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
            case Stream({alias:name}): name;
        }
    }

    public function getTable():TableRef {
        return switch item {
            case Table({term:r}): r;
            default: null;
        }
    }

    public function getAllTables():Map<String, TableRef> {
        var res = new Map<String, TableRef>();
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


class TableRef {
    public var table: Dynamic;
    public var name: String;
    public var pos: haxe.PosInfos;

    public function new(name, table, ?pos) {
        this.name = name;
        this.table = table;
        this.pos = pos;
    }
}

class TableJoin {
    public var joinWith: TableSourceItem;
    public var mJoinWith: Null<{src:TableSpec, ?cursor:Dynamic}>;
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
    public var astNode: ql.sql.TsAst.SqlAstNode;

    public var select:Null<SelectStmt> = null;

    public function new(?sel:SelectStmt) {
        if (sel != null) {
            this.select = sel;
        }
    }
}