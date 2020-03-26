package ql.sql.runtime;

import pmdb.core.Arch;
import ql.sql.Dummy.DummyTable;
import ql.sql.grammar.CommonTypes.TablePath;
import haxe.Constraints.IMap;
import ql.sql.runtime.Sel.TableSource;
import pmdb.core.Object.Doc;
import ql.sql.common.TypedValue;
import ql.sql.runtime.TAst;
import ql.sql.runtime.VirtualMachine;
import ql.sql.runtime.Stmt;

import pm.Helpers.nor;

using ql.sql.common.TypedValue;

@:access(ql.sql)

@:yield
class Interpreter extends SqlRuntime {
    public var context(default, set): Context<Dynamic, Dynamic, Dynamic>;
    public var returnValue: Dynamic = null;

    public function new(ctx) {
        super();

        this.context = ctx;
    }

    function set_context(c: Context<Dynamic, Dynamic, Dynamic>) {
        this.context = this._ctx = c;

        return this.context;
    }

    public function execStmt(stmt: Stmt<Dynamic>) {
        final c = context;

        switch stmt.type {
            case SelectStatement(stmt):
                var sources = stmt.context.sources.array();
                // stmt.context.sources = sources;
                // Console.debug(sources);
                var out:Array<Doc> = cast selectStmtScan(stmt);
                applySelectClauses(stmt, out);
                // stmt.context._sources.resize(0);
                returnValue = out;

            case UpdateStatement(stmt):
                returnValue = null;
                applyUpdate(stmt);

            case InsertStatement(stmt):
                returnValue = null;
                applyInsert(stmt);

            case CreateTable(stmt):
                throw new pm.Error('Unreachable');
        }

        return returnValue;
    }

    function applyInsert(i: InsertStmt) {
        var tbl = context.getSource(i.path.tableName);
        var columns = i.columns;
        if (columns == null) columns = [for (f in tbl.schema.fields) if (!f.ctor_field.omissionMandatory) f.name];
        //Console.examine(columns);
        var output = [];
        for (row in i.rows) {
            assert(row.length == columns.length);
            var b:Doc = new Doc();
            for (i in 0...row.length) {
                b[columns[i]] = expr(row[i]);
            }
            b = tbl.schema.induct(b);
            output.push(b);
        }
        returnValue = output;
    }

    function applyUpdate(u: UpdateStmt) {
        var src = context.getSource(u.path.name);
        var table = context.resolveTableFrom(src);
        context.beginScope();
        context.use(src);
        var updates = [];
        for (row in openTablePath(u.path)) {
            if (false && (table is DummyTable<Dynamic>)) {
                u.context.focus(row);
            }
            else {
                u.context.focus({});
            }
            context.focus(row, u.path.name);
            
            if (u.predicate != null ? pred(u.predicate) : true) {
                for (op in u.operations) {
                    inline applyUpdateOp(u, op);
                }
            }

            updates.push({
                row: row,
                d: u.context.swapObject
            });
        }
        context.endScope();

        var old = [];
        var young = [];

        for (up in updates) {
            old.push(up.row);
            var y:Doc = up.row.clone(CloneMethod.ShallowRecurse);
            for (k=>v in up.d)
                y[k] = v;
            young.push(y);
        }

        context.glue.tblUpdate(table, cast old, cast young);
    }

    function applyUpdateOp(u:UpdateStmt, op:UpdateOperation) {
        final g = u.context;
        g.swapObject[op.columnName] = expr(op.e);
    }

    function openTablePath(path: TablePath):Iterator<Doc> {
        return cast context.getSource(path.name).open(cast this);
    }

    @:access(ql.sql.runtime)
	function applySelectClauses(select:SelectStmt, results:Array<Doc>) {
        var i = select.i;
        i.sort_(cast select.context, results);
	}

    function selectStmtScan(stmt:SelectStmt):Array<Dynamic> {
        var resultRows = stmt.i.apply(cast stmt, this);
        
        return resultRows;
    }

    /**
     *! TODO clone values upon assignment
     * @param stmt 
     * @param tables 
     * @param rows 
     * @return Doc
     */
    function buildSelectStmtResultRow(stmt:SelectStmt):Doc {
        // assert(!tables.empty() && !rows.empty());
        final c = context;
        final sources = stmt.context.sources;

        //? short-circuit
        var items:Array<SelItem> = (stmt.i._exporter : ql.sql.runtime.TAst.SelOutput).items;
        switch items {
            case [All(null)|All(_.label()=>null)] if (sources.length == 1):
                if (stmt.context.sources.length == 1) {
                    return c.getCurrentRow();
                }

            default:
        }

        // inline function getRow(?t: String):Doc {
        //     return rows[t!=null?tables.indexOf(t):0];
        // }
        var cucr = c.unaryCurrentRow;
        c.unaryCurrentRow = false;
        var out:Doc = new Doc();
        for (item in items) {
            switch item {
                case All(null)|All(_.label()=>null):
                    var m = new Map();
                    // var r:Doc = new Doc();
                    for (table => rowObj in c.currentRows) {
                        var row:Doc = rowObj;

                        for (col => v in row) {
                            if (m.exists(col))
                                continue;
                            m[col] = true;
                            out[col] = v;
                        }
                    }
                    m.clear();

                case All(_.label()=>table):
                    var schema = c.getTableSchema(table);
                    var row:Doc = c.getCurrentRow(table);
                    for (f in schema.fields) {
                        out[f.name] = row[f.name];
                    }

                case Column(_.label()=>table, _.label()=>column, _.label()=>alias):
                    var key:String = nor(alias, column);
                    if (table == null) {
                        var row:Doc = c.getCurrentRow();
                        out[key] = row[column];
                    }
                    else {
                        var row:Doc = c.getCurrentRow(table);
                        if (!row.exists(column))
                            throw new pm.Error.ValueError(row, 'No "$column" column found on `$table`');
                        out[key] = row[column];
                    }

                case Expression(_.label()=>alias, e):
                    var key = alias;
                    if (key == null)
                        key = e.print();
                    out[key] = expr(e);
            }
        }
        c.unaryCurrentRow = cucr;

        return out;
    }

    public function expr(e: TExpr):Dynamic {
        final c = this.context;

        if (e.mConstant != null) 
            return e.mConstant;
        
        switch e.expr {
            case TConst({value:null}):
                return null;
            
            case TReference(name):
                return context.get(name);

            case TParam(name):
                throw new pm.Error('Unbound parameter ${name.label.identifier}');
            
                case TTable(name):
                throw new pm.Error('Should not happen');
                
            case TColumn(name, null|{identifier:null}):
                var row:Doc = context.getCurrentRow();
                return row.get(name.identifier);

                for (row in (cast context.currentRows : Map<String, Doc>))
                    if (row.exists(name.identifier))
                        return row[name.identifier];
                throw new pm.Error('Not Found');

            case TColumn(name, table={identifier:tableName}):
                var row:Doc = (context.getCurrentRow(tableName) : Null<Doc>).unwrap();
                return row[name.identifier];
            
            case TField(o, field):
                // throw new pm.Error('TODO');
                return Reflect.field(expr(o), field.identifier);

            case TArray(arrayExpr, indexExpr):
                var array:Dynamic = expr(arrayExpr);
                var index:Dynamic = expr(indexExpr);
                if ((array is IMap<Dynamic, Dynamic>)) {
                    var map:IMap<Dynamic, Dynamic> = cast array;
                    return map.get(index);
                }
                else if ((array is Array<Dynamic>)) {
                    var array:Array<Dynamic> = cast array;
                    return array[cast(index, Int)];
                }
                else if ((array is String)) {
                    var s:String = cast array;
                    return s.charAt(cast(index, Int));
                }

                try {
                    return untyped array[index];
                }
                catch (err: Dynamic) {
                    throw new pm.Error('Invalid access: (${e.expr.print()}) $array[$index]');
                }

            case TFunc(f):
				final fname = f.id;
                if (c.functions.exists(fname)) {
                    var handle = c.functions[fname];
                    
                    return handle;
                }
                else {
                    throw new pm.Error('Function "$fname" not found');
                }

            case TCall(fexpr={expr:TField(o, field)}, params):
                throw new pm.Error('TODO: method-call expression');
            
            case TCall(fexpr, params):
                // throw new pm.Error('TODO: call expression');
                final f:Callable = expr(fexpr);
                return f.call(params.map(e -> expr(e)));
            
            case TArrayDecl(_) if (e.mConstant != null):
                return e.mConstant;

            case TArrayDecl(arr):
                return arr.map(x->expr(x));
            
            case TBinop(op, left, right):
                #if !neko
                return op.apply(expr(left), expr(right));
                #end
                var fh = op.getMethodHandle();
                // Console.debug('${left.print()} + ${right.print()}');
                var ret:Dynamic = fh(expr(left), expr(right));
                if (ret == null)
                    throw new pm.Error('$op, $left, $right');
                return ret;

            case TUnop(op, post, e):
                return op.apply(expr(e));

            case TObjectDecl(fields):
                var o:Doc = new Doc();
                for (f in fields)
                    o[f.key] = expr(f.value);
                return o;

            case TCase(type):
                switch type {
                    case Expr:
                        throw new pm.Error.NotImplementedError();
                    case Standard(branches, elseExpr):
                        for (b in branches) {
                            if (pred(b.e))
                                return expr(b.result);
                        }
                        if (elseExpr != null)
                            return expr(elseExpr);
                        throw new pm.Error('Not Found');
                }

            case other:
                throw new pm.Error('Unhandled $other');
        }
    }

    public function pred(e: SelPredicate):Bool {
        switch e.type {
            case Rel(relation):
                var op = Operators.getMethodHandle(relation.op);
                return op(expr(relation.left), expr(relation.right));

            case And(subs):
                for (p in subs)
                    if (!pred(p))
                        return false;
                return true;

            case Or(subs):
                for (p in subs)
                    if (pred(p))
                        return true;
                return false;

            case Not(negated):
                return !pred(negated);
        }

        throw false;
    }

    private function computeSourcesUsedBySelOutput(select:SelectStmt, out:SelOutput):Array<TableSpec> {
        var specs = new Array();
        
		var sourceList = [
			for (x in out.items)
				switch x {
					case All(_.label() => table) if (table != null):
						context.getSource(table);
					case Column(_.label() => table, _, _) if (table != null):
						context.getSource(table);
					case Expression(_, _), All(_), Column(_):
						null;
				}
        ].filter(x -> x != null);
        return sourceList;
    }
}