package ql.sql.runtime;

import pm.OneOrMany;
import haxe.extern.EitherType;
import ql.sql.runtime.Callable.F;
import haxe.macro.Expr as HaxeExpr;
import haxe.macro.Expr.ExprOf as HaxeExprOf;
import pmdb.ql.ts.DataType;
import pmdb.core.FrozenStructSchema;
import pm.ImmutableList;
import pmdb.core.Object;
import pmdb.core.Arch;
import haxe.ds.Option;
using pm.Options;

import ql.sql.grammar.CommonTypes.SqlSymbol;
// import ql.sql.grammar.expression.Expression.Function;
// import ql.sql.grammar.expression.Expression.FunctionKind;

import ql.sql.grammar.CommonTypes.BinaryOperator;
import ql.sql.grammar.CommonTypes.UnaryOperator;
import ql.sql.grammar.CommonTypes.Contextual;

import ql.sql.runtime.VirtualMachine;
import haxe.Constraints.IMap;
import pm.Helpers.nn;
import pm.Helpers.nor;

import ql.sql.runtime.SType;
import ql.sql.runtime.DType;
import ql.sql.common.SqlSchema;
import ql.sql.common.TypedValue;

enum SelItem {
    All(table:SqlSymbol);
    Column(table:SqlSymbol, column:SqlSymbol, ?alias:SqlSymbol);
    Expression(?alias:Null<SqlSymbol>, expr:TExpr);
}

class CSelOut {
    public var items: ImmutableList<SelItem>;
    public var mCompiled:Null<JITFn<Dynamic>> = null;
    public var schema:Null<SqlSchema<Dynamic>> = null;

    public function new(l:ImmutableList<SelItem>) {
        this.items = l;
    }

    public inline function hasSchema():Bool {
        return schema != null;
    }

	/**
	 * given a `Context<?,?,?>` instance, build the output row
	 * @param c
	 * @return Doc
	 */
	public function build<Row>(c:Context<Dynamic, Dynamic, Row>):Doc {
		var output:Doc = new Doc();

		for (item in this.items) {
			switch item {
				case Column(table, column, alias):
					var value = c.glue.rowGetColumnByName(c.getCurrentRow(table.label()), column.identifier);
					if (alias == null) {
						output[column.identifier] = value;
					} else {
						output[alias.identifier] = value;
					}

				case All(null | {identifier: null}):
					var input:Doc = Doc.unsafe(c.getCurrentRow());
					for (f => value in input) {
						output[f] = value;
					}

				case All(table):
					var n = table.label();
					var schema = c.getTableSchema(n);
					for (col in schema.fields) {
						var value = Doc.unsafe(c.getCurrentRow(n)).get(col.name);
						output[col.name] = value;
					}

				case Expression(alias, e):
					var value = e.eval({context: c});
					if (alias == null) {
						throw new pm.Error('TODO: AST printing');
					} else {
						output[alias.identifier] = value;
					}
			}
		}

		return output;
	}
}

/**
 * SelOutput - collection of SelectElement derivatives which are used to build output rows
 */
@:forward
abstract SelOutput (CSelOut) from CSelOut to CSelOut {
    /* Constructor */
    public function new(items) {
        this = new CSelOut(items);
    }

    public function iterator():Iterator<SelItem> {
        return this.items.iterator();
    }

    @:from 
    public static inline function ofList(l: ImmutableList<SelItem>):SelOutput {
        return new SelOutput(l);
    }

    @:from 
    public static inline function create(a: Iterable<SelItem>):SelOutput {
        return ImmutableList.fromArray(a.array());
    }
}

/**
 * SelPredicate class
 * TODO rename to Term
 *  @see https://www.sqlite.org/optoverview.html
 */
class SelPredicate {
    public var type: SelPredicateType;
    public var mCompiled:Null<JITFn<Bool>> = null;

    public function new(predicate:SelPredicateType) {
        this.type = predicate;
    }

    public function eval(g:{context:Context<Dynamic, Dynamic, Dynamic>}):Bool {
        if (mCompiled == null) {
            throw new pm.Error('No method handle');
        }
        return mCompiled(g);
    }

	public function bindParameters(params:EitherType<Array<Dynamic>, Map<String, Dynamic>>) {
        var t = SelPredicateType;
        switch type {
            case t.Rel(relation):
                relation.bindParameters(params);
            case t.And(p):
                for (x in p)
                    x.bindParameters(params);
            case t.Or(p):
                for (x in p)
                    x.bindParameters(params);
            case t.Not(p):
                p.bindParameters(params);
        }
    }

    public function clone():SelPredicate {
        return new SelPredicate(type.clone());
    }

    public static function And(l:SelPredicate, r:SelPredicate):SelPredicate {
        var t = SelPredicateType;
        var type = switch l.type {
            case t.And(larr): switch r.type {
                case t.And(rarr): t.And(larr.concat(rarr));
                default: t.And(larr.concat([r]));
            }
            default: t.And([l, r]);
        }
        return new SelPredicate(type);
    }

	public static function Or(l:SelPredicate, r:SelPredicate):SelPredicate {
		var t = SelPredicateType;
		var type = switch l.type {
			case t.Or(larr): switch r.type {
					case t.Or(rarr): t.Or(larr.concat(rarr));
					default: t.Or(larr.concat([r]));
				}
			default: t.Or([l, r]);
		}
		return new SelPredicate(type);
	}
}

@:using(ql.sql.runtime.TAst.TermTypes)
enum SelPredicateType {
    Rel(relation: RelationalPredicate);
    // And(left:SelPredicate, right:SelPredicate);
    // Or(left:SelPredicate, right:SelPredicate);
    And(p: Array<SelPredicate>);
    Or(p: Array<SelPredicate>);
    Not(negated: SelPredicate);
}
class TermTypes {
    public static inline function clone(t: SelPredicateType):SelPredicateType {
        return switch t {
            case Rel(relation): Rel(relation.clone());
            case And(p): And([for (sub in p) sub.clone()]);
            case Or(p): Or([for (sub in p) sub.clone()]);
            case Not(negated): Not(negated.clone());
        }
    }
}

typedef IRNode<F:haxe.Constraints.Function> = {
    var mCompiled:Null<F>;
};
typedef IEvalable<R> = {
	function eval(g:{context:Context<Dynamic, Dynamic, Dynamic>}):R;
}

@:generic
class ARel<L, R> {
    public final left: L;
    public final right: R;
    public final op: RelationPredicateOperator;
    public var mOpCompiled:Null<(Dynamic, Dynamic)->Bool> = null;

    public function new(op, left, right) {
        this.op = op;
        this.left = left;
        this.right = right;
    }

    function computeLeft(left: L):Dynamic {
        throw 'Not Implemented';
    }
    function computeRight(right: R):Dynamic {
        throw 'Not Implemented';
    }
}
class RelationalPredicate extends ARel<TExpr, TExpr> {
    @:noCompletion 
    public var mCompiled:Null<JITFn<Bool>> = null;

    public function new(op, l, r) {
        super(op, l, r);

        // this.mOpCompiled = this.op.getMethodHandle(left.type, right.type);
    }

    public function eval(g:{context:Context<Dynamic, Dynamic, Dynamic>}):Bool {
        if (this.mOpCompiled == null) {
            this.mOpCompiled = this.op.getMethodHandle(left.type, right.type);
        }

        #if pmdb.use_typedvalue
        var l:TypedValue = left.eval(g);
        var r:TypedValue = right.eval(g);
        #else
        var l = left.eval(g);
        var r = right.eval(g);
        #end

        return mOpCompiled(l, r);
    }

    public function clone():RelationalPredicate {
        return new RelationalPredicate(op, left.clone(), right.clone());
    }

	public function bindParameters(params:EitherType<Array<Dynamic>, Map<String, Dynamic>>) {
        left.bindParameters(params);
        right.bindParameters(params);
    }
}

/**
 * TExpr - 
 * TODO refactor Compiler to change all assignments of `type` based on the types of other expressions assignments to `typeHint` instead
 */
class TExpr {
    public var mCompiled:Null<JITFn<Dynamic>> = null;
    public var mConstant:Null<Dynamic> = null;
    public final expr: TExprType;
    public var type:SType;
    public var typeHint:Null<ImmutableList<SType>> = null;

    public var extra(default, null): Doc;

    public function new(e:TExprType) {
        this.expr = e;
        this.type = TUnknown;

        switch expr {
            case TConst(tv={type:t}):
                this.mConstant = tv.export();
                this.type = t;

            default:
        }

        this.extra = new Doc();
    }

    public function isTyped():Bool {
        return type.match(TUnknown);
    }
    
    public function suggestType(t: SType) {
        if (t.match(TUnknown)) throw new pm.Error('y tho');
        if (typeHint != null) {
            if (!typeHint.has(t, (a, b)->a.eq(b)))
                typeHint = t & typeHint;
        }
        else {
            typeHint = ImmutableList.Hd(t, Tl);
        }
    }

    public dynamic function eval(g: Contextual):Dynamic {
        if (mConstant != null) return mConstant;
        if (mCompiled != null) return mCompiled(g);

        //TODO type
        
        throw new pm.Error('No evaluation method provided for $expr');
    }

    public inline function print():String {
        return expr.print();
    }

    public function bindParameter(name:EitherType<Int, String>, value:Dynamic):TExpr {
        switch this.expr {
            case TParam({label:_.identifier=>label, offset:offset}):
                if ((name is String) && label == name) {
                    this.mConstant = value;
                }
                else if ((name is Int) && offset == name) {
                    this.mConstant = value;
                }

            case TField(o, _):
                o.bindParameter(name, value);

            case TCall(f, params):
                f.bindParameter(name, value);
                for (p in params)
                    p.bindParameter(name, value);

            case TBinop(_, l, r):
                l.bindParameter(name, value);
                r.bindParameter(name, value);

            case TUnop(_, _, e):
                e.bindParameter(name, value);

            case TArrayDecl(values):
                for (v in values)
                    v.bindParameter(name, value);

            case _:
        }

        return this;
    }

	public function bindParameters(params:EitherType<Array<Dynamic>, Map<String, Dynamic>>, raw=false):TExpr {
        var mapping:Array<{key:Dynamic, value:Dynamic}> = new Array();
        if (raw) {
            mapping = cast params;
        }
        else {
            if ((params is Array<Dynamic>)) {
                var arr = (cast params : Array<Dynamic>);
                for (k in 0...arr.length) {
                    mapping.push({key:k, value:arr[k]});
                }
            }
            else if ((params is IMap<Dynamic, Dynamic>)) {
                var m = (cast params : IMap<Dynamic, Dynamic>);
                for (pair in m.keyValueIterator()) {
                    mapping.push(pair);
                }
            }
            else {
                throw new pm.Error('Unhandled ${Type.typeof(params)}');
            }
        }

		switch this.expr {
			case TParam({label: _.identifier => label, offset: offset}):
				for (pair in mapping) {
                    bindParameterOnto(label, offset, pair.key, pair.value);
                }

			case TField(o, _):
                o.bindParameters(mapping, true);
                
            case TArray(a, idx):
                a.bindParameters(mapping);
                idx.bindParameters(mapping);

			case TCall(f, params):
				f.bindParameters(mapping, true);
				for (p in params)
					p.bindParameters(mapping, true);

			case TBinop(_, l, r):
				l.bindParameters(mapping, true);
				r.bindParameters(mapping, true);

			case TUnop(_, _, e):
				e.bindParameters(mapping, true);

			case TArrayDecl(values):
				for (v in values)
					v.bindParameters(mapping, true);

			case _:
                //
		}

		return this;
	}

	function bindParameterOnto(label:String, offset:Int, key:EitherType<Int, String>, value:Dynamic) {
		if ((key is String) && label == key) {
            //Console.examine(label, value);
			this.mConstant = value;
        } 
        else if ((key is Int) && offset == key) {
			this.mConstant = value;
		}
    }

    public function getChildNodes():Array<TExpr> {
        var res = [];
        inline function add(e: OneOrMany<TExpr>) {
            for (e in e.asMany())
                res.push(e);
        }

        switch expr {
            case TConst(value):
            case TReference(name):
            case TParam(name):
            case TTable(name):
            case TColumn(name, table):
            case TField(o, field):
                add(o);
            case TArray(arr, index):
                add([arr, index]);
            case TFunc(f):
            case TCall(f, params):
                add(f);
                add(params);
            case TBinop(op, left, right):
                add([left, right]);
            case TUnop(_, _, e):
                add(e);
            case TArrayDecl(values):
                add(values);
            case TObjectDecl(fields):
                for (f in fields)
                    add(f.value);
            case TCase(type):
                switch type {
                    case Expr:
                        //
                    case Standard(branches, defaultExpr):
                        for (b in branches) {
                            // add(b.e);
                            add(b.result);
                        }
                        if (defaultExpr != null)
                            add(defaultExpr);
                }
        }

        return res;
    }

    public function clone():TExpr {
        return new TExpr(expr.clone());
    }
}

@:using(ql.sql.runtime.TAst.TExprTypeTools)
enum TExprType {
    TConst(value: TypedValue);
    /**
     * a reference to some named value, to be transformed into a typed reference later
     */
    TReference(name: Sym);
    
    /**
     * a parameter expression, i.e. `?`, `:param_name`, and with our syntax extensions, also `${param_name}`
     */
    TParam(name: {label:Sym, offset:Int});
    TTable(name: Sym);
    TColumn(name:Sym, table:Sym);
    TField(o:TExpr, field:Sym);
    TArray(arr:TExpr, index:TExpr);

    /**
      an expression which references a built-in function by name, complete with type information
     **/
    // TFunc(f: TFunction);
    TFunc(f: {id:String, f:Callable});
    TCall(f:TExpr, params:Array<TExpr>);
    TBinop(op:BinaryOperator, left:TExpr, right:TExpr);
    TUnop(op:UnaryOperator, post:Bool, e:TExpr);

    TArrayDecl(values: Array<TExpr>);//TODO: allow array comprehensions of subqueries
    TObjectDecl(fields: Array<{key:String, value:TExpr}>);
    TCase(type: CaseType);
}

enum CaseType {
    Expr;
    Standard(branches:Array<CaseBranchStd>, ?defaultExpr:TExpr);
}

class CaseBranchStd {
    public final e: SelPredicate;
    public final result: TExpr;

    public function new(e, result) {
        this.e = e;
        this.result = result;
    }
}

class TExprTypeTools {
    public static function clone(e: TExprType):TExprType {
        return switch e {
            case TConst(value): TConst(value);
            case TReference(name): TReference(name);
            case TParam(name): TParam(name);
            case TTable(name): TTable(name);
            case TColumn(name, table): TColumn(name, table);
            case TField(o, field): TField(o.clone(), field);
            case TArray(arr, index): TArray(arr.clone(), index.clone());
            case TFunc(f): TFunc(f);
            case TCall(f, params): TCall(f.clone(), [for (p in params) p.clone()]);
            case TBinop(op, left, right): TBinop(op, left.clone(), right.clone());
            case TUnop(op, post, e): TUnop(op, post, e.clone());
            case TArrayDecl(values): TArrayDecl([for (v in values) v.clone()]);
            case TObjectDecl(fields): TObjectDecl([for (f in fields) {key:f.key, value:f.value.clone()}]);
            case TCase(type): TCase(switch type {
                case Expr: Expr;
                case Standard(branches, defaultExpr): Standard([for (b in branches) new CaseBranchStd(b.e.clone(), b.result.clone())], if (defaultExpr != null) defaultExpr.clone() else null);
            });
        }
    }

    public static function print(e:TExprType):String {
		var b = new StringBuf();
        printExpr(e, b);
        return b.toString();
    }

    /**
     * prints the string representation of `e` to the given `StringBuf`
     * @param e 
     * @param out the output buffer
     */
    public static function printExpr(e:TExprType, out:StringBuf) {
        final len1 = out.length;
        inline function add(s:String) {
            out.addSub(s, 0);
        }

        switch e {
            case TConst(value): 
                add(tvprint(value));
            
            case TParam(name): 
                var s = ':${name.label}';
                add(s);
            
            case TTable(name), TReference(name):
                var s = name.identifier;
                add(s);
            
            case TColumn(_.label()=>name, _.label()=>table):
                if (table.empty()) {
                    add(name);
                }
                else {
                    add(table);
                    add('.');
                    add(name);
                }
            
            case TField(o, field):
                printExpr(o.expr, out);
                add('.$field');
            
            case TArray(arr, idx):
                printExpr(arr.expr, out);
                add('[');
                printExpr(idx.expr, out);
                add(']');
            
            case TFunc(f):
                var s = f.id;
                add(s);
            
            case TCall(f, params):
                printExpr(f.expr, out);
                add('(');
                if (params.length > 1) {
                    var last = params.pop();
                    for (p in params) {
                        printExpr(p.expr, out);
                        add(',');
                    }
                    printExpr(last.expr, out);
                    add(')');
                }
                else if (params.length == 1) {
                    printExpr(params[0].expr, out);
                }
                add(')');
            
			case TBinop(op, _.expr => left, _.expr =>right):
                printExpr(left, out);
                add(printbinop(op));
                printExpr(right, out);
            
            case TUnop(op, post, e):
                // post ? print(e.expr)+printunop(op) : printunop(op)+print(e.expr);
                if (post) {
                    add(printunop(op));
                    printExpr(e.expr, out);
                }
                else {
                    printExpr(e.expr, out);
                    add(printunop(op));
                }
            
            case TArrayDecl(values):
                // '(${values.map(e -> print(e.expr)).join(',')})';
                var last = values[values.length - 1];
                add('[');
                for (i in 0...(values.length - 1)) {
                    printExpr(values[i].expr, out);
                    add(',');
                }
                printExpr(last.expr, out);
                add(']');
            
            case TObjectDecl(fields):
                // "{" + fields.map(function(_) {
                //     return print(TConst(_.key)) + ': ' + print(_.value.expr);
                // }).join(',\n') + "}";

            case TCase(type):
                switch type {
                    case Expr:
                    case Standard(branches, elseExpr):
                        add('CASE ');
                        throw new pm.Error('CASE');
                }
        }
        // return ;

        if (out.length > len1)
            return ;

        throw new pm.Error('Unhandled $e');
    }

    static function printbinop(op: BinaryOperator):String {
        return switch op {
            case OpEq:'=';
            case OpGt:'>';
            case OpGte:'>=';
            case OpLt:'<';
            case OpLte:'<=';
            case OpNEq:'!=';
            case OpIn: 'IN';
            case OpMult:'*';
            case OpDiv:'/';
            case OpMod:'%';
            case OpAdd:'+';
            case OpSubt:'-';
            case OpBoolAnd:'&&';
            case OpBoolOr:'||';
            case OpBoolXor:'??';
        }
    }
    static function printunop(op: UnaryOperator):String {
        return switch op {
            case OpNot: '!';
            case OpNegBits: '~';
            case OpPositive: '+';
            case OpNegative: '-';
        }
    }

    static function tvprint(value: TypedValue):String {
        inline function sv(x: Dynamic):String return haxe.Json.stringify(x);

        return switch value.type {
            case TUnknown: sv(value.value);
            case TBool: sv(value.boolValue);
            case TInt: sv(value.intValue);
            case TFloat: sv(value.floatValue);
            case TString: sv(value.stringValue);
            case TDate: 'new DateTime(${sv(value.dateValue.getTime())})';
            case TArray(_): '['+value.arrayAnyValue.map(tvprint).join(",")+']';
            case TMap(key, value): throw new pm.Error('TODO');
            case TStruct(schema): sv(value.value);
        }
    }
}

// class TFunction {
//     public var kind:FunctionKind;
//     public var symbol: Sym;
//     public var f: Null<Funct> = null;
    
//     // public var parameters:Array<Array<SType>>;
//     // public var returnType:Array<SType>;

//     public function new(id, kind, ?f) {
//         this.kind = kind;
//         this.symbol = id;
//         this.f = f;

//         // this.parameters = new Array();
//         // this.returnType = new Array();
//     }
// }

private typedef Sym = SqlSymbol;
typedef JITFn<Out> = (g:{context:Context<Dynamic, Dynamic, Dynamic>}) -> Out;

// enum Funct {
//     FSimple(f: F);
//     FAggregate(agg: Dynamic);
// }