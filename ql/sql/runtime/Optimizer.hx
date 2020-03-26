package ql.sql.runtime;

import ql.sql.runtime.sel.SelectStmtContext;
import ql.sql.runtime.Stmt;
import ql.sql.runtime.Stmt.CSTNode;
import ql.sql.runtime.Stmt.SelectStmt;
import ql.sql.runtime.Sel.TableStream;
import ql.sql.runtime.Sel.TableSource as TblSrc;
import ql.sql.runtime.Sel.TableSourceItem as TblSrcItem;
import ql.sql.runtime.Sel.TableRef;
import ql.sql.runtime.Sel.TableJoin;
import ql.sql.runtime.Sel.Aliased;
import haxe.ds.Either;
import ql.sql.runtime.Sel.SelectImpl;
import pmdb.core.Object;
import pmdb.core.Arch;
import ql.sql.TsAst;
import ql.sql.grammar.CommonTypes;
import ql.sql.grammar.expression.Expression;
import ql.sql.runtime.VirtualMachine;
import ql.sql.runtime.TAst;
import ql.sql.common.TypedValue;
import ql.sql.common.SqlSchema;
import ql.sql.common.internal.ImmutableStruct;

using Lambda;
using pm.Arrays;
using StringTools;
using pm.Strings;
using pm.Functions;

class Optimizer {
    public var compiler: Compiler;
    public var context(get, never):Context<Dynamic, Dynamic, Dynamic>;
    private inline function get_context():Context<Dynamic, Dynamic, Dynamic> return compiler.context;

    public function new(c) {
        this.compiler = c;
    }

    public function optimizeStmt(stmt: Stmt<Dynamic>) {
        switch stmt.type {
            case SelectStatement(stmt):
                optimizeSelectStmt(stmt);
            case UpdateStatement(stmt):
            case InsertStatement(stmt):
            case CreateTable(stmt):
        }

        throw new pm.Error();
    }

    function optimizeSelectStmt(stmt: SelectStmt) {
        switch stmt.i._predicate.type {
            case Rel(relation):
            case And(p):
            case Or(p):
            case Not(negated):
        }
    }

    function optimizePredicate(p: SelPredicate):SelPredicate {
        switch p.type {
            case Rel(relation):
                switch [relation.left, relation.op, relation.right] {
                    default:
                }

            case And(subs):
                //TODO
            
            case Or(subs):
                //TODO
            
            case Not(negated):
                var n = optimizePredicate(negated);
                return new SelPredicate(Not(n));
        }

        return p;
    }
}