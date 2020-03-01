package ql.sql.runtime;

import ql.sql.common.SqlSchema;
import ql.sql.TsAst;
import ql.sql.grammar.CommonTypes;
import ql.sql.grammar.expression.Expression;
import ql.sql.common.TypedValue;
import ql.sql.runtime.TAst;
import ql.sql.runtime.VirtualMachine;

class SqlRuntime {
	private var _ctx: Context<Dynamic, Dynamic, Dynamic>;
	// private var querySource:Array<TableSpec>;

    private function new() {
		_ctx = null;
		// querySource = null;
    }
    private inline function runtimeError(code:E, ?msg:String, ?pos:haxe.PosInfos) {
        throw new SqlRuntimeError(code, msg, pos);
    }
    private inline function _contextGuard(?pos:haxe.PosInfos) {
        if (_ctx == null) {
            runtimeError(MissingContext, null, pos);
        }
    }

	private function predicateNodeConvert(predicate:Predicate):SelPredicate {
		if ((predicate is RelationPredicate)) {
			var rp = cast(predicate, RelationPredicate);

			var left = expressionNodeConvert(rp.left);
			var right = expressionNodeConvert(rp.right);
			// expressionNodeConvert2(left);
			// expressionNodeConvert2(right);
			var rpo = RelationPredicateOperator, rel;
			var res = new SelPredicate(SelPredicateType.Rel(rel = new RelationalPredicate(switch rp.op {
				case OpEq: rpo.Equals;
				case OpNEq: rpo.NotEquals;
				case OpGt: rpo.Greater;
				case OpGte: rpo.GreaterEq;
				case OpLt: rpo.Lesser;
				case OpLte: rpo.LesserEq;
			}, left, right)));
			// res.mCompiled = g -> rel.eval(g);
			return res;
		}

		if ((predicate is NotPredicate)) {
			var np = cast(predicate, NotPredicate);
			var p = predicateNodeConvert(np.predicate);
			// var tmp = p.mCompiled;
			// p.mCompiled = g -> !tmp(g);
			return p;
		}

		if ((predicate is ql.sql.grammar.expression.InPredicate)) {
			var p = cast(predicate, InPredicate);
			var left = expressionNodeConvert(p.left);
			switch p.right.data {
				case LExpression(e):
					var right = expressionNodeConvert(e);
					var rel;
					var res = new SelPredicate(SelPredicateType.Rel(rel = new RelationalPredicate(In, left, right)));
					// res.mCompiled = g -> rel.eval(g);
					return res;

				case LSubSelect(select):
					throw new pm.Error('TODO');
			}
		}

		if ((predicate is ql.sql.grammar.expression.AndPredicate)) {
			var p = cast(predicate, AndPredicate);
			var left = predicateNodeConvert(p.left);
			var right = predicateNodeConvert(p.right);
			var res = SelPredicate.And(left, right);
			// res.mCompiled = g -> (left.eval(g) && right.eval(g));
			switch res.type {
				case And(subs):
					// res.mCompiled = function(g) {
					// 	for (predicate in subs)
					// 		if (!predicate.eval(g))
					// 			return false;
					// 	return true;
					// };
				default:
			}
			return res;
		}

		if ((predicate is ql.sql.grammar.expression.OrPredicate)) {
			var p = cast(predicate, OrPredicate);
			var left = predicateNodeConvert(p.left);
			var right = predicateNodeConvert(p.right);
			var res = SelPredicate.Or(left, right);
			switch res.type {
				case Or(subs):
					// res.mCompiled = function(g) {
					// 	for (predicate in subs)
					// 		if (predicate.eval(g))
					// 			return true;
					// 	return false;
					// };
				default:
			}
			return res;
		}

		throw new pm.Error('Unhandled ' + Type.getClassName(Type.getClass(predicate)));
    }
    
    /**
     * you know what it is
     * @param e 
     * @return TExpr
     */
    private function expressionNodeConvert(e: Expression):TExpr {
		if ((e is Predicate))
            throw new pm.Error('Compile predicate nodes with compileSelectPredicate');
        
		if ((e is C<Dynamic>)) {
			var value:Dynamic = (cast e : C<Dynamic>).getConstValue();
			final type = TExprType.TConst(value);
			return new TExpr(type);
		}

		if ((e is ql.sql.grammar.expression.ColumnName)) {
			var c = cast(e, ql.sql.grammar.expression.Expression.ColumnName);
			resolveTableSymbol(c);
			c.name.type = SqlSymbolType.Field;
			return new TExpr(TExprType.TColumn(c.name, c.table));
		}

		if ((e is ArithmeticOperation)) {
			var ao:ArithmeticOperation = cast(e, ArithmeticOperation);
			var left = expressionNodeConvert(ao.left),
				right = expressionNodeConvert(ao.right);
			var type = TExprType.TBinop(ao.op.toBinaryOperator(), left, right);
			return new TExpr(type);
		}

		if ((e is FunctionCallBase<Dynamic>)) {
			if ((e is ql.sql.grammar.expression.SimpleFunctionCall)) {
				var call:SimpleFunctionCall = cast e;
				// return new TExpr(TExprType.TFunc(new TFunction(call.symbol, call.kind, function() {
				//     throw -9;
				// })));
				var f = new TFunction(call.symbol, call.kind);
				var type = TExprType.TFunc(f);
				var fe = new TExpr(type);
				type = TExprType.TCall(fe, call.args.map(x -> expressionNodeConvert(x)));
				return new TExpr(type);
			}

			throw new pm.Error('Invalid FunctionCallBase<?> instance ${Type.getClassName(Type.getClass(e))}');
		}

		if ((e is ql.sql.grammar.expression.ParameterExpression)) {
			var param:ParameterExpression = cast e;
			var type = TExprType.TParam(param);
			return new TExpr(type);
		}

		throw new pm.Error(Type.getClassName(Type.getClass(e)));
	}

	function resolveTableSymbol(el:{table:Null<SqlSymbol>}) {
		return ;
		
		// _contextGuard();
		// var context = _ctx;

		// if (el.table == null) {
		// 	el.table = new SqlSymbol(context.currentDefaultTable);
		// 	el.table.table = loadTable(el.table.identifier);
		// }

		// assert(el.table != null, new pm.Error('table symbol must be defined'));
	}

	public function loadTable(name:String):Null<Dynamic> {
		_contextGuard();
		// var c = _ctx;
		return _ctx.getSource(name).table;
	}
}

private enum E {
    MissingContext;
}
class SqlRuntimeError extends pm.Error {
    public final code: E;
    public function new(code:E, ?msg, ?pos) {
        this.code = code;
        super(msg, code.getName(), pos);
    }
}
