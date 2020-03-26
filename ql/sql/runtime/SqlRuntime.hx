package ql.sql.runtime;

import ql.sql.runtime.Stmt.UpdateOperation;
import ql.sql.common.SqlSchema;
// import ql.sql.TsAst;
import ql.sql.common.TypedValue;
import ql.sql.grammar.CommonTypes;
// import ql.sql.grammar.expression.Expression;
// import ql.sql.grammar.expression.Expression.ArrayAccess as ArrayAccessExpression;
import ql.sql.runtime.TAst;
import ql.sql.runtime.VirtualMachine;
import ql.sql.ast.Query;

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

#if poop
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
    private function expressionNodeConvert(e:Expression, ?onConvert:TExpr->Void):TExpr {
		if (e == null) return null;

		if ((e is Predicate))
			throw new pm.Error('Compile predicate nodes with compileSelectPredicate');
		
		if (onConvert == null) onConvert = function(e) return ;
		inline function h(te: TExpr):TExpr {
			return te.tap(onConvert);
		}
        
		if ((e is C<Dynamic>)) {
			var value:Dynamic = (cast e : C<Dynamic>).getConstValue();
			final type = TExprType.TConst(value);
			return h(new TExpr(type));
		}

		if ((e is ql.sql.grammar.expression.ColumnName)) {
			var c = cast(e, ql.sql.grammar.expression.Expression.ColumnName);
			resolveTableSymbol(c);
			c.name.type = SqlSymbolType.Field;
			return h(new TExpr(TExprType.TColumn(c.name, c.table)));
		}

		if ((e is ArithmeticOperation)) {
			var ao:ArithmeticOperation = cast(e, ArithmeticOperation);
			var left = expressionNodeConvert(ao.left, onConvert),
				right = expressionNodeConvert(ao.right, onConvert);
			var type = TExprType.TBinop(ao.op.toBinaryOperator(), left, right);
			return h(new TExpr(type));
		}

		if ((e is FunctionCallBase<Dynamic>)) {
			if ((e is ql.sql.grammar.expression.SimpleFunctionCall)) {
				var call:SimpleFunctionCall = cast e;
				// return h(new TExpr(TExprType.TFunc(new TFunction(call.symbol, call.kind, function()) {
				//     throw -9;
				// })));
				var f = new TFunction(call.symbol, call.kind);
				var type = TExprType.TFunc(f);
				var fe = new TExpr(type);
				type = TExprType.TCall(fe, call.args.map(x -> expressionNodeConvert(x, onConvert)));
				return h(new TExpr(type));
			}

			throw new pm.Error('Invalid FunctionCallBase<?> instance ${Type.getClassName(Type.getClass(e))}');
		}

		if ((e is ql.sql.grammar.expression.ParameterExpression)) {
			var param:ParameterExpression = cast e;
			var type = TExprType.TParam(param);
			return h(new TExpr(type));
		}

		if ((e is ListExpression)) {
			var le:ListExpression = cast e;
			var array = [for (item in le.values) expressionNodeConvert(item, onConvert)];
			return h(new TExpr(TArrayDecl(array)));
		}

		if ((e is ql.sql.grammar.expression.ArrayAccess)) {
			var aa:ArrayAccess = cast e;
			// var arr:TExpr = expressionNodeConvert()
			// throw new pm.Error('$aa');
			var arr = expressionNodeConvert(aa.e);
			var idx = expressionNodeConvert(aa.index);
			if (arr == null || idx == null) {
				throw new pm.Error('array or index expressions are null');
			}
			// Console.examine(arr, idx);
			// throw 0;
			return h(new TExpr(TArray(arr, idx)));
		}

		if ((e is ql.sql.grammar.expression.CaseExpression)) {
			var ce:CaseExpression = cast e;
			var tcase:CaseType;
			switch ce.expression {
				case null:
					var branches = [];
					for (b in ce.branches) {
						var br = new CaseBranchStd(predicateNodeConvert(b.expression), expressionNodeConvert(b.result));
						// Console.debug(br);
						branches.push(br);
					}
					tcase = CaseType.Standard(branches, ce.elseBranch != null ? expressionNodeConvert(ce.elseBranch) : null);

				case cee:
					throw new pm.Error('$cee');
			}
			var r:TExpr = new TExpr(TCase(tcase));
			return h(r);
		}

		//Console.debug(e);
		throw new pm.Error(Type.getClassName(Type.getClass(e)));
	}
#end

	public function predicateNodeConvert(e: Expr):SelPredicate {
		switch e {
			case EBinop(Binop.LogAnd, l, r):
				var left = predicateNodeConvert(l), right = predicateNodeConvert(r);
				var terms = (switch left.type {
					case And(lh): lh;
					default: [left];
				}).concat(switch right.type {
					case And(rh): rh;
					default: [right];
				});
				return new SelPredicate(And(terms));
			case EBinop(LogOr, l, r):
				var left = predicateNodeConvert(l),
					right = predicateNodeConvert(r);
				var terms = (switch left.type {
					case Or(lh): lh;
					default: [left];
				})
				.concat(switch right.type {
					case Or(rh): rh;
					default: [right];
				});
				return new SelPredicate(Or(terms));
			
			case EBinop(op, l, r):
				var rop = RelationPredicateOperator;
				return new SelPredicate(SelPredicateType.Rel(new RelationalPredicate(switch op {
					case Eq: rop.Equals;
					case NEq: rop.NotEquals;
					case Gt: rop.Greater;
					case Gte: rop.GreaterEq;
					case Lt: rop.Lesser;
					case Lte: rop.LesserEq;
					case In: rop.In;
					case Like: throw new pm.Error('TODO');
					default:
						throw new pm.Error('Unexpected $op');
				}, expressionNodeConvert(l), expressionNodeConvert(r))));

			case EUnop(Unop.Not, false, e):
				return new SelPredicate(Not(predicateNodeConvert(e)));

			case EParent(e):
				return predicateNodeConvert(e);

			default:
				throw new pm.Error('Unhandled $e');
		}
	}

	public function expressionNodeConvert(e: Expr):TExpr {
		inline function te(t: TExprType)
			return new TExpr(t);
		inline function sym(s: String)
			return new SqlSymbol(s);

		switch e {
			case CTrue:
				return te(TConst(true));
			case CFalse:
				return te(TConst(false));
			case CNull:
				return te(TConst(TypedValue.NULL));
			case CUnderscore:
				throw new pm.Error('Unexpected $e');
			case CInt(i):
				return te(TConst(i));
			case CFloat(n):
				return te(TConst(n));
			case CString(v):
				return te(TConst(v));
			case CParam(parameter):
				return te(TParam({label:sym(parameter), offset:0}));
			case EParent(e):
				return enc(e);
			case EList(arr):
				return enc(ql.sql.ast.Query.Expr.EArrayDecl(arr));
			case EId(id):
				return te(TReference(sym(id)));
			case EField(_, All):
				throw new pm.Error('Unreachable');
			case EField(e, Name(field)):
				return te(TField(enc(e), sym(field)));
			case ECall(e, args):
				return te(TCall(enc(e), [for (x in args) enc(x)]));
			case ECase(branches, def, e):
				var branchesOut = [for (b in branches) caseBranchNodeConvert(b)];
				var defaultCase = def != null ? expressionNodeConvert(def) : null;
				var subject = e != null ? expressionNodeConvert(e) : null;
				return te(TCase(Standard(branchesOut, defaultCase)));

			case EBinop(op, l, r):
				var left = enc(l), right = enc(r);
				switch op {
					case _:
						return te(TBinop(BinaryOperator.createByName('Op${op.getName()}'), left, right));
				}
			case EUnop(op, postfix, e):
				var unop = UnaryOperator.createByName('Op${op.getName()}');
				return te(TUnop(unop, postfix, enc(e)));
			case ETernary(cond, t, f):
				throw new pm.Error.NotImplementedError();
			case EArray(a, idx):
				return te(TArray(enc(a), enc(idx)));
			case EArrayDecl(arr):
				return te(TArrayDecl([for (x in arr) enc(x)]));
			case EComprehension(expr, iterWhat, iterOf, iterPredicate):
				throw new pm.Error.NotImplementedError();
			case NestedSelectStmt(select):
				throw new pm.Error.NotImplementedError();
		}

		throw new pm.Error('Unhandled $e');
	}

	function caseBranchNodeConvert(b: CaseBranch):CaseBranchStd {
		return new CaseBranchStd(
			predicateNodeConvert(b.e),
			expressionNodeConvert(b.body)
		);
	}

	inline function enc(x: Expr):TExpr return expressionNodeConvert(x);

	public function updateOpConvert(op: UpdateOp):UpdateOperation {
		final columnName = op.column;
		final e = expressionNodeConvert(op.e);
		return new UpdateOperation(op.type, columnName, e);
	}

	function resolveTableSymbol(el:{table:Null<SqlSymbol>}) {
		return ;
	}

	public function loadTable(name:String):Null<Dynamic> {
		_contextGuard();
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
