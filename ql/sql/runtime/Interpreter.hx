package ql.sql.runtime;

import pmdb.core.Object.Doc;
import ql.sql.common.TypedValue;
import ql.sql.runtime.TAst;
import ql.sql.runtime.VirtualMachine;
import ql.sql.runtime.Stmt;

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

    public function execStmt(stmt: Stmt) {
        final c = context;

        switch stmt.node {
            case SelectStatement(stmt):
                var itr = stmt.i._traverser.iterator(stmt, cast stmt);
                var out = [];

                c.unaryCurrentRow = true;
                for (row in itr) {
                    context.focus(row);
                    if (stmt.i._predicate != null) {
                        if (pred(stmt.i._predicate)) {
                            out.push(row);
                        }
                    }
                    else {
                        out.push(row);
                    }
                }
                
                if (stmt.i._exporter != null) for (i in 0...out.length) {
                    var row:Doc = out[i];
                    context.focus(row);
                    out[i] = stmt.i._exporter.build(context);
                }
				c.unaryCurrentRow = false;

                applySelectClauses(stmt, out);
                returnValue = out;

            case CreateTable(stmt):
                throw new pm.Error('Unreachable');
        }

        return returnValue;
    }

    function applySelectClauses(select:SelectStmt, results:Array<Doc>) {
        var i = select.i;
        
        @:privateAccess i.sort_(cast select, results);
    }

    public function expr(e: TExpr):Dynamic {
        if (e.mConstant != null) 
            return e.mConstant;
        
        switch e.expr {
            // case TConst(value):
            case TReference(name):
                return context.get(name);
            case TParam(name):
                throw new pm.Error('Unbound parameter ${name.label.identifier}');
            case TTable(name):
                throw new pm.Error('Should not happen');
            case TColumn(name, null|{identifier:null}):
                for (row in (cast context.currentRows : Map<String, Doc>))
                    if (row.exists(name.identifier))
                        return row[name.identifier];
                throw new pm.Error('Not Found');
            case TColumn(name, table={identifier:tableName}):
                return (context.currentRows.get(tableName) : Doc).get(name.identifier);
            case TField(o, field):
                throw new pm.Error('TODO');
            case TFunc(f):
                throw new pm.Error('TODO');
            case TCall(fexpr={expr:TField(o, field)}, params):
                throw new pm.Error('TODO: method-call expression');
            case TCall(f, params):
                throw new pm.Error('TODO: call expression');
            case TBinop(op, left, right):
                throw new pm.Error('TODO: ${e.expr}');
            case TUnop(op, post, e):
                throw new pm.Error('TODO: ${e.expr}');
            case TArrayDecl(values):
                throw new pm.Error('TODO: ${e.expr}');
            case TObjectDecl(fields):
                throw new pm.Error('TODO: ${e.expr}');

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
}