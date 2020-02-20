package ql.sql.runtime;

import haxe.extern.EitherType;
import ql.sql.runtime.VirtualMachine.F;
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
import ql.sql.grammar.expression.Expression.Function;
import ql.sql.grammar.expression.Expression.FunctionKind;
import ql.sql.grammar.CommonTypes.BinaryOperator;
import ql.sql.grammar.CommonTypes.UnaryOperator;

import ql.sql.runtime.VirtualMachine.Context;
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

    /**
     * given a `Context<?,?,?>` instance, build the output row
     * @param c 
     * @return Doc
     */
    public function build<Row>(c: Context<Dynamic, Dynamic, Row>):Doc {
        var output:Doc = new Doc();

        for (item in this.items) {
            switch item {
                case Column(_, column, alias):
                    var value = c.glue.rowGetColumnByName(c.currentRow, column.identifier);
                    if (alias == null) {
                        output[column.identifier] = value;
                    }
                    else {
                        output[alias.identifier] = value;
                    }

                case All(table):
                    for (col in c.glue.tblListColumns(table.table)) {
                        var value = c.glue.rowGetColumnByName(c.currentRow, col.name);
                        output[col.name] = value;
                    }

                case Expression(alias, e):
                    var value = e.eval({context:c});
                    if (alias == null) {
                        throw new pm.Error('TODO: AST printing');
                    }
                    else {
                        output[alias.identifier] = value;
                    }
            }
        }

        return output;
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
        }
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

enum SelPredicateType {
    Rel(relation: RelationalPredicate);
    // And(left:SelPredicate, right:SelPredicate);
    // Or(left:SelPredicate, right:SelPredicate);
    And(p: Array<SelPredicate>);
    Or(p: Array<SelPredicate>);
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

        this.mOpCompiled = this.op.getMethodHandle(left.type, right.type);
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

	public function bindParameters(params:EitherType<Array<Dynamic>, Map<String, Dynamic>>) {
        left.bindParameters(params);
        right.bindParameters(params);
    }
}

/**
 * TExpr - 
 */
class TExpr {
    public var mCompiled:Null<JITFn<Dynamic>> = null;
    public var mConstant:Null<Dynamic> = null;
    public final expr: TExprType;
    public var type:SType;

    private var _meta:Doc;

    public function new(e:TExprType) {
        this.expr = e;
        this.type = TUnknown;

        switch expr {
            case TConst({value:v, type:t}):
                this.mConstant = v;
                this.type = t;

            default:
        }
        this._meta = {};
    }

    public function isTyped():Bool {
        return type.match(TUnknown);
    }

    public function eval(g:{context:Context<Dynamic, Dynamic, Dynamic>}):Dynamic {
        if (mConstant != null) {
            return mConstant;
        }
        if (mCompiled != null) {
            return mCompiled(g);
        }
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
		}

		return this;
	}

	function bindParameterOnto(label:String, offset:Int, key:EitherType<Int, String>, value:Dynamic) {
		if ((key is String) && label == key) {
			this.mConstant = value;
        } 
        else if ((key is Int) && offset == key) {
			this.mConstant = value;
		}
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

    /**
      an expression which references a built-in function by name, complete with type information
     **/
    TFunc(f:TFunction);
    TCall(f:TExpr, params:Array<TExpr>);
    TBinop(op:BinaryOperator, left:TExpr, right:TExpr);
    TUnop(op:UnaryOperator, post:Bool, e:TExpr);

    TArrayDecl(values: Array<TExpr>);//TODO: allow array comprehensions of subqueries
    TObjectDecl(fields: Array<{key:String, value:TExpr}>);
}

class TExprTypeTools {
    public static function print(e: TExprType):String {
        switch e {
            case TConst(value): 
                return tvprint(value);
            case TParam(name): 
                return ':${name.label}';
            case TTable(name):
                return name.identifier;
            case TColumn(name, table):
                return table.identifier + '.' + name.identifier;
            case TField(o, field):
                return print(o.expr) + '.$field';
            case TFunc(f):
                return f.symbol.identifier;
            case TCall(f, params):
                return print(f.expr) + '('+params.map(e -> print(e.expr)).join(',')+')';
			case TBinop(op, print(_.expr) => left, print(_.expr) =>right):
                return left + printbinop(op) + right;
            case TUnop(op, post, e):
                return post ? print(e.expr)+printunop(op) : printunop(op)+print(e.expr);
            case TArrayDecl(values):
                return '(${values.map(e -> print(e.expr)).join(',')})';
            case TObjectDecl(fields):
                return "{" + fields.map(function(_) {
                    return print(TConst(_.key)) + ': ' + print(_.value.expr);
                }).join(',\n') + "}";
        }

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

typedef ATFunction = Function & {
    var f: haxe.Constraints.Function;
};
class TFunction {
    public var kind:FunctionKind;
    public var symbol: Sym;
    public var f: Null<Funct> = null;
    // public var parameters:Array<Array<SType>>;
    // public var returnType:Array<SType>;

    public function new(id, kind, ?f) {
        this.kind = kind;
        this.symbol = id;
        this.f = f;

        // this.parameters = new Array();
        // this.returnType = new Array();
    }
}

private typedef Sym = SqlSymbol;
typedef JITFn<Out> = (g:{context:Context<Dynamic, Dynamic, Dynamic>}) -> Out;

enum Funct {
    FSimple(f: F);
    FAggregate(agg: Dynamic);
}