package ql.sql.grammar.expression;

import ql.sql.TsAst.SelectStatement;
import ql.sql.TsAst.FunctionArgument;
import ql.sql.TsAst.SqlAstNode;
import ql.sql.grammar.CommonTypes;

class Expression extends SqlAstNode implements FunctionArgument {
    final expr_key:Int = HashKey.next();
    public function new() {
        _init_();
    }

    public function _betty() {
        haxe.Serializer.USE_CACHE = true;
        Console.println(haxe.Serializer.run(this));
    }

    public function cloneExpression():E {
        return Type.createEmptyInstance(Type.getClass(this));
    }
}

private typedef E = ql.sql.grammar.expression.Expression;

interface BinaryExpression<L, R> {
    var left: L;
    var right: R;
}

interface C<T> {
    function getConstValue():T;
}

// @:generic 
class ConstantValue<T> extends E implements C<T> {
    public var value(default, null): T;
    public function new(value) {
        super();
        this.value = value;
    }

    public inline function getConstValue():T {
        return value;
    }
}

class BoolValue extends ConstantValue<Bool> {
    public static var CTRUE:BoolValue = new BoolValue(true);
    public static var CFALSE:BoolValue = new BoolValue(false);
}

class StringValue extends ConstantValue<String> {}
// class UStringValue extends ConstantValue<UnicodeString> {}
class FloatValue extends ConstantValue<Float> {}
class IntValue extends ConstantValue<Int> {}
class NullValue extends ConstantValue<pm.Noise> {
    // public static var instance = new NullValue();
    public function new() {
        super(Noise);
    }
}

class ListExpression extends Expression implements FunctionArgument {
    public var values: Array<Expression>;
    public function new(array: Array<Expression>) {
        super();
        this.values = array.copy();
    }

    public function isConstantList():Bool {
        return values.all(function(e: E) {
            return ((e is C<Dynamic>) || ((e is ListExpression) && cast(e, ListExpression).isConstantList()));
        });
    }

    /**
      TODO: refactor to return `Array<Variant>`
      @returns physical array of values, when `this` is a list of constants
     **/
    public function getConstantList():Array<Dynamic> {
        return [];//values.map(function(e: Expression) {

        // })
    }
}

class Predicate extends E {
    //
}
class RelationPredicate extends Predicate implements BinaryExpression<E, E> {
    public var op:ComparisonOperator;
    public var left: Expression;
    public var right: Expression;

    public function new(op, l, r) {
        super();
        this.op = op;
        this.left = l;
        this.right = r;
    }
}
class NotPredicate extends Predicate {
    public var predicate: Predicate;
    public function new(e: Predicate) {
        super();
        predicate = e;
    }
}

class InPredicate extends Predicate implements BinaryExpression<Expression, PredicateListExpression> {
	public var left:Expression;
    public var right:PredicateListExpression;
    
    public function new(l:Expression, r:PredicateListExpression) {
        super();
        this.left = l;
        this.right = r;
    }
}

enum ParamBinding {
    PIndex(i: Int);
    PNamed(name: SqlSymbol);
}
enum ListExprData {
    LParam(p: ParamBinding);
    LExpr(e: ListExpression);
    LSubSelect(select: SelectStatement);
}
enum ListType {
    LTList;
    LTSet;
}
class PredicateListExpression extends Expression {
    public var data:ListExprData;
    public var type:ListType;

    public function new(list: ListExprData) {
        super();
        this.type = LTList;
        this.data = list;
    }
}

class CompoundPredicate extends Predicate implements BinaryExpression<Predicate, Predicate> {
    public var left: Predicate;
    public var right: Predicate;
    public var op: LogicalOperator;
    public function new(op, l, r) {
        super();
        this.op = op;
        this.left = l;
        this.right = r;
    }
}
class AndPredicate extends CompoundPredicate {
    public function new(l, r) {
        super(ELogicalOperator.OpBoolAnd, l, r);
    }
}
class OrPredicate extends CompoundPredicate {public function new(l, r){super(ELogicalOperator.OpBoolOr, l, r);}}

class ColumnName extends Expression {
    public var name:SqlSymbol;
    public var table:Null<SqlSymbol> = null;
    public function new(name, ?table) {
        super();
        this.name = name;
        this.table = table;
    }
}
private class BinopExprBase<@:followWithAbstracts Op:EnumValue, Left, Right> extends Expression implements BinaryExpression<Left, Right> {
    public var left: Left;
    public var right: Right;
    public var op: Op;

    public function new(op, left, right) {
        super();
        this.op = op;
        this.left = left;
        this.right = right;
    }
}
class ArithmeticOperation extends BinopExprBase<CommonTypes.MathOperator, Expression, Expression> {

}
class BinaryPredicateOperation extends BinopExprBase<CommonTypes.ELogicalOperator, Predicate, Predicate> {
    //
}
class PredicateExpression extends Expression {
    public var predicate:Predicate;
    public function new(predicate) {
        super();
        this.predicate = predicate;
        assert(this.predicate != null);
    }
}
typedef Function = {
    symbol:SqlSymbol, 
    kind:FunctionKind
};
enum FunctionKind {Simple;}
class FunctionCallBase<Arg> extends Expression implements FunctionArgument {
    public var func: Function;
    public var args:Array<Arg>;

    public function new(f:Function, args:Array<Arg>) {
        super();

        this.func = f;
        this.args = args;
    }
}
class SimpleFunctionCall extends FunctionCallBase<Expression> {
    public function new(f:SqlSymbol, args) {
        super({symbol:f, kind:Simple}, args);
    }
}