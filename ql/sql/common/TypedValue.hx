package ql.sql.common;

// import ql.sql.runtime.TAst.BinaryOperators;
import ql.sql.runtime.SType;
import pm.Helpers.nn;
import pm.Helpers.nor;
import haxe.Constraints.IMap;
import haxe.ds.Option;
using pm.Options;

import ql.sql.grammar.CommonTypes.BinaryOperator;
import ql.sql.grammar.CommonTypes.UnaryOperator;
import pmdb.core.Arch;

@:access(ql.sql.common)
@:allow(ql.sql.common)
class CTypedValue {
    public var value(default, null):Dynamic;

    public var type(default, null):SType;

    private var _validated:Null<Bool> = null;

    /**
     * construct new CTypedValue instance
     * @param value the `Dynamic` value to be represented
     * @param type the `SType` value which represents `value`'s expected type
     * @param validate =`false` whether or not to verify that `value` and `type` are compatible
     */
    public function new(value, type, validate:Validate = Lazy) {
        this.value = value;
        this.type = type;

        switch validate {
            case Never:
                _validated = true;
            case Eager if (!this.validate()):
                throw new pm.Error('Expected ${this.type}, got ${this.value}', 'TypeMismatchError');
            case Lazy, Eager:
                //
        }
        
        isNull = value == null;
        stringValue = (value is String) ? cast(value, String) : null;
        intValue = (value is Int) ? cast(value, Int) : null;
        floatValue = (value is Float) ? cast(value, Float) : null;
        numValue = intValue != null ? intValue + 0.0 : (floatValue != null ? floatValue : null);
		boolValue = (value is Bool) ? cast(value, Bool) : null;
		dateValue = try SType.TDate.importValue(value) catch (e: Dynamic) null;
        arrayAnyValue = ((value is Array<Dynamic>) ? cast(value, Array<Dynamic>) : null);
        mapValue = ((value is haxe.IMap<Dynamic, Dynamic>) ? cast(value, haxe.Constraints.IMap<Dynamic, Dynamic>) : null);
    }
    
    // @:property((value == null)) 
    public var isNull:Bool;

    // @:property((value is String) ? cast(value, String) : null)
    public var stringValue:Null<String>;

    // @:property((value is Int) ? cast(value, Int) : null)
    public var intValue:Null<Int>;

    // @:property((value is Float) ? cast(value, Float) : null)
	public var floatValue:Null<Float>;
	
	public var dateValue:Null<DateTime>;

    // @:property(intValue != null ? intValue + 0.0 : (floatValue != null ? floatValue : null))
    public var numValue:Null<Float>;

    // @:property((value is Bool) ? cast(value, Bool) : null)
    public var boolValue:Null<Bool>;

    // @:property((value is Array<Dynamic>) ? cast(value, Array<Dynamic>) : null)
    public var arrayAnyValue:Null<Array<Dynamic>>;

    // @:property((value is haxe.IMap<Dynamic, Dynamic>) ? cast(value, haxe.Constraints.IMap<Dynamic, Dynamic>) : null)
    public var mapValue:Null<haxe.Constraints.IMap<Dynamic, Dynamic>>;

    public inline function validate():Bool {
        if (_validated == null) {
            _validated = inline this.type.validateValue(this.value);
        }
        return _validated;
    }

    @:pure
    public function update(?newValue:Dynamic, ?newType:SType):TypedValue {
        return switch [newValue, newType] {
            case [null, null]: new TypedValue(this.value, this.type, Eager);
            case [_, null]: new TypedValue(newValue, this.type, Eager);
            case [null, _]: new TypedValue(this.value, newType, Eager);
            case [_, _]:
                throw new pm.Error('Cannot supply newValue AND newType as arguments to TypedValue.update');
        }
    }

    @:pure
    public function clone():TypedValue {
        return new TypedValue(this.value, this.type, switch _validated {
            case null | false: Lazy;
            case true: Never;
        });
    }

    /**
     * TODO
     * @return Dynamic
     */
    public inline function export():Dynamic {
        return this.value;
    }

    extern public inline function unsafely<TIn, TOut>(f:TIn->TOut):TOut {
        if (_validated == true || validate()) {
            return f(this.value);
        } else {
            throw new pm.Error('Expected ${this.type}, got ${this.value}', 'TypeMismatchError');
        }
    }

    public function safely<TIn, TOut>(f:TIn->TOut):pm.Outcome<TOut, pm.Error> {
        return if (_validated == true || validate())
            Success(f(this.value));
        else
            Failure(new pm.Error('Expected ${this.type}, got ${this.value}', 'TypeMismatchError'));
    }

    public function isOfType(t:SType):Bool {
        return this.type.eq(t) || t.validateValue(this.value);
    }
}

@:access(ql.sql.common)
@:allow(ql.sql.common)
@:allow(ql.sql.runtime)
@:forward

abstract TypedValue(CTypedValue) from CTypedValue to CTypedValue {
	public function new(value:Dynamic, type:SType, ?validate:Validate) {
		this = new CTypedValue(value, type, validate);
	}

	public static function is(x: Dynamic):Bool {return (x is CTypedValue);}

	@:arrayAccess
	public inline function arrayGet(index:TypedValue):TypedValue {
		return _arrayGet(index);
	}

	@:arrayAccess
	public inline function arraySet(index:TypedValue, value:TypedValue):TypedValue {
		return _arraySet(index, value);
    }

	public function equals(other:TypedValue):Bool {
		return this == (other : CTypedValue) || pmdb.core.Arch.areThingsEqual(this.value, other.value);
	}

	function _arrayGet(index:TypedValue):TypedValue {
		return switch this.type {
			case TString:
				if (index.isOfType(TInt))
					this.stringValue.charAt(cast(index.value, Int));
				else
					throw new pm.Error('expected Int, got ${index.type.print()}', 'TypeError');
			case TArray(type):
				if (index.isOfType(TInt))
					this.arrayAnyValue[cast(index.value, Int)];
				else
					throw new pm.Error('expected Int, got ${index.type.print()}', 'TypeError');
			case TMap(key, value):
				if (key.validateValue(index.value))
					this.mapValue.get(index.value);
				else
					throw new pm.Error('expected ${key.print()}, got ${index.type.print()}', 'TypeError');
			// case TStruct(schema): //TODO
			case _:
				throw new pm.Error('Array-Access on ${this.type.print()} not allowed');
		}
	}

	function _arraySet(index:TypedValue, itemValue:TypedValue):TypedValue {
		switch this.type {
			case TArray(itemType):
			// TODO

			case TMap(keyType, valueType):
			// TODO

			default:
		}

		throw new pm.Error('Array-Access on ${this.type.print()} not allowed');
	}

	@:op(A.b)
	public function __getattr__(name:String):TypedValue {
		return arrayGet(name);
	}

	@:op(A == B)
	static inline function _eq_typed_typed_(l:TypedValue, r:TypedValue):Bool {
		return l.equals(r);
	}

	@:op(A == B)
	@:commutative
	static inline function _eq_typed_untyped_(l:TypedValue, r:Dynamic):Bool {
		return l.value == r;
	}

	
	// {region cast_from
	static inline function make(v:Dynamic, t:SType, validate:Validate = Never):TypedValue
		return new TypedValue(v, t, validate);
	
	@:from public static function ofBool(v: Bool)
		return make(v, TBool);
	
	@:from public static function ofFloat(v:Float)
		return make(v, TFloat);
	
	@:from public static function ofInt(v:Int)
		return make(v, TInt);
	
	@:from public static function ofString(v:String):TypedValue
		return make(v, TString);
	
	@:from public static function ofDate(v:Date)
		return make(v, TDate, Lazy);
	@:from public static function ofDateTime(v: DateTime) return make(v, TDate, Lazy);
	
	@:from public static function ofStringArray(v:Array<String>)
		return make(v, TArray(TString));
	
	@:from public static function ofIntArray(v:Array<Int>)
		return make(v, TArray(TInt));
	
	@:from public static function ofFloatArray(v:Array<Float>)
		return make(v, TArray(TFloat));
	
	@:from public static function ofBoolArray(v:Array<Bool>)
		return make(v, TArray(TBool));
	
	@:from public static function ofDateArray(v:Array<Date>)
		return make(v, TArray(TDate));
	
	// @:from public static function ofIntIntMap(m: IMap<Int,Int>) return make(m, TMap(TInt, TInt));
	// @:from public static function ofStringVMap(v: IMap<String, Dynamic>) return make(v, TMap(TString, TUnknown));
	
	@:from public static function ofIntBoolMap(m:IMap<Int, Bool>)
		return make(m, TMap(TInt, TBool));
	
	@:from public static function ofIntIntMap(m:IMap<Int, Int>)
		return make(m, TMap(TInt, TInt));
	
	@:from public static function ofIntFloatMap(m:IMap<Int, Float>)
		return make(m, TMap(TInt, TFloat));
	
	@:from public static function ofIntStringMap(m:IMap<Int, String>)
		return make(m, TMap(TInt, TString));
	
	@:from public static function ofIntDateMap(m:IMap<Int, Date>)
		return make(m, TMap(TInt, TDate));
	
	@:from public static function ofStringBoolMap(m:IMap<String, Bool>)
		return make(m, TMap(TString, TBool));
	
	@:from public static function ofStringIntMap(m:IMap<String, Int>)
		return make(m, TMap(TString, TInt));
	
	@:from public static function ofStringFloatMap(m:IMap<String, Float>)
		return make(m, TMap(TString, TFloat));
	
	@:from public static function ofStringStringMap(m:IMap<String, String>)
		return make(m, TMap(TString, TString));
	
	@:from public static function ofStringDateMap(m:IMap<String, Date>)
		return make(m, TMap(TString, TDate));

	// @:op(A + B) public static function binop_add(a:TypedValue, b:TypedValue):TypedValue {return BinaryOperators.op_add(a, b);}

	// @:op(A - B) public static function binop_subt(a:TypedValue, b:TypedValue):TypedValue {return BinaryOperators.op_subt(a, b);}

	// @:op(A * B) public static function binop_mult(a:TypedValue, b:TypedValue):TypedValue {return BinaryOperators.op_mult(a, b);}

	// @:op(A / B) public static function binop_div(a:TypedValue, b:TypedValue):TypedValue {return BinaryOperators.op_div(a, b);}

	// @:op(A % B) public static function binop_mod(a:TypedValue, b:TypedValue):TypedValue {
	// 	return BinaryOperators.op_mod(a, b);
	// }

	@:from
	public static function ofAny(value:Dynamic):TypedValue {
		var type:SType = switch Type.typeof(value) {
			case TNull, TUnknown: SType.TUnknown;
			case TBool: SType.TBool;
			case TFloat: SType.TFloat;
			case TInt: SType.TInt;
			case TClass(ql.sql.common.CTypedValue):
				return cast(value, CTypedValue).clone();
			case TClass(String): SType.TString;
			case TClass(Date), TClass(ql.sql.common.DateTime.DateTimeContainer): SType.TDate;
			case TClass(Array): SType.TArray(TUnknown);
			case TClass(haxe.ds.StringMap): SType.TMap(TString, TUnknown);
			case TClass(haxe.ds.IntMap): SType.TMap(TInt, TUnknown);
			case TClass(c):
				throw new pm.Error('No conversion from Class<${Type.getClassName(c)}> to SType');
			case TEnum(e):
				throw new pm.Error('No conversion from Enum<${Type.getEnumName(e)}> to SType');
			case TObject:
				throw new pm.Error('TODO');
			case TFunction:
				throw new pm.Error('No conversion from Function type to SType');
		};
		
		return new TypedValue(value, type, Never);
	}

	// }endregion
	// {region cast_to

	public inline function castTo(t:SType):TypedValue {
		return _castTo(t);
    }
    
	function _castTo(t:SType):TypedValue {
		if (this.isOfType(t)) {
			return this.clone();
		} else {
			if (this.isNull)
				return new TypedValue(null, t);
			switch t {
				case TInt:
					if (nn(this.boolValue))
						return (this.boolValue ? 1 : 0:TypedValue);
					if (nn(this.floatValue))
						return Std.int(this.floatValue);
					if (nn(this.stringValue))
						return Std.parseInt(this.stringValue);

				default:
			}

			throw new pm.Error('Cannot cast ${this.type.print()} to ${t.print()}');
		}
	}

	// }endregion
}

enum abstract Validate(Int) {
	var Never;
	var Lazy;
	var Eager;
}

@:using(ql.sql.common.TypedValue.Operators)
enum RelationPredicateOperator {
	Equals;
	NotEquals;
	Greater;
	Lesser;
	GreaterEq;
	LesserEq;
	In;
}

// typedef Document = Object<Variant>;

class Operators {
	public static function getMethodHandle(op:RelationPredicateOperator, leftType:SType=TUnknown, rightType:SType=TUnknown):(Dynamic, Dynamic) -> Bool {
		switch [op, leftType, rightType] {
			case [In, t1, TArray(t2)] if (t1.eq(t2)): 
				return function(l:Dynamic, r:Array<Dynamic>):Bool {
					return r.search(x -> x == l);
				}
			case [In, TString, TString]:
				return (l:String, r:String) -> r.has(l);
			case [_, TBool | TInt | TFloat | TString, _] | [_, _, TBool | TInt | TFloat | TString]:
				return switch op {
					case Equals: (l, r) -> try l == r catch (e: Dynamic) false;
					case NotEquals: (l, r) -> try l != r catch (e:Dynamic) true;
					case Greater: utop_gt;
					case Lesser: utop_lt;
					case GreaterEq: utop_gte;
					case LesserEq: utop_lte;
					case In:
						throw new pm.Error('Unreachable');
				}

			default:
				return switch op {
					case Equals: utop_eq;
					case NotEquals: utop_neq;
					case Greater: utop_gt;
					case Lesser: utop_lt;
					case GreaterEq: utop_gte;
					case LesserEq: utop_lte;
					case In: utop_in;
				}
		}

		throw new pm.Error('Unhandled $op, $leftType, $rightType');
	}

	static function utop_eq(l:Dynamic, r:Dynamic):Bool {
		return Arch.areThingsEqual(l, r);
	}

	static function utop_neq(l:Dynamic, r:Dynamic):Bool {
		return !utop_eq(l, r);
	}

	static function cmp(l:Dynamic, r:Dynamic):Int {
		return Arch.compareThings(l, r);
	}

	static function utop_gt(l:Dynamic, r:Dynamic):Bool {
		// throw new pm.Error.NotImplementedError();
		return cmp(l, r) > 0;
	}

	static function utop_gte(l:Dynamic, r:Dynamic):Bool {
		return cmp(l, r) >= 0;
	}

	static function utop_lt(l:Dynamic, r:Dynamic):Bool {
		return cmp(l, r) < 0;
	}

	static function utop_lte(l:Dynamic, r:Dynamic):Bool {
		// throw new pm.Error.NotImplementedError();
		return cmp(l, r) <= 0;
	}

	static function utop_in_(l:TypedValue, r:TypedValue):Bool {
		// trace(l, r);
		// throw new pm.Error.NotImplementedError();
		switch r.type {
			case TArray(itemType) if (l.isOfType(itemType)):
				for (_x in r.arrayAnyValue) {
					var x:TypedValue = _x;
					if (x.equals(l))
						return true;
				}
				return false;

			case TString if (l.isOfType(TString)):
				return r.stringValue.has(l.stringValue);

			default:
				throw new pm.Error('Invalid operation ${l.type.print()} in ${r.type.print()}');
		}

		return false;
	}

	static function utop_in(left:Dynamic, right:Dynamic):Bool {
		if (right == null) return false;
		if (TypedValue.is(left))
			left = (left : TypedValue).value;
		if (TypedValue.is(right))
			right = (right : TypedValue).value;

		if ((right is String)) {
			return (right : String).has(Std.string(left));
		}

		if ((right is Array<Dynamic>)) {
			var a:Array<Dynamic> = cast right;
			if (a.length == 0) return false;
			if (a.has(left)) return true;
			for (item in a) {
				if (left == item) return true;
				if (utop_eq(left, item)) return true;
			}
			return false;
		}

		throw new pm.Error('Invalid operation ${Type.typeof(left)} in ${Type.typeof(right)}');
	}
}

@:access(ql.sql.common.TypedValue)
class BinaryOperators {
	public static function getMethodHandle(op:BinaryOperator):(l:TypedValue, r:TypedValue) -> TypedValue {
		return switch op {
			case OpEq: op_eq;
			case OpGt: op_gt;
			case OpGte: op_gte;
			case OpLt: op_lt;
			case OpLte: op_lte;
			case OpNEq: op_neq;
			case OpMult: op_mult;
			case OpDiv: op_div;
			case OpMod: op_mod;
			case OpAdd: op_add;
			case OpSubt: op_subt;
			case OpBoolAnd: op_bool_and;
			case OpBoolOr: op_bool_or;
			case OpBoolXor: op_bool_xor;
		}
	}

	public static function op_err(l:TypedValue, r:TypedValue):TypedValue {
		throw new pm.Error();
	}

	public static function op_add(l:TypedValue, r:TypedValue):TypedValue {
		return switch [l, r] {
			case [{type: TString}, _] | [_, {type: TString}]: TypedValue.ofString('${l.value}${r.value}'); // : TypedValue);
			case [{type: TFloat | TInt}, {type: TFloat | TInt}]: TypedValue.ofAny(l.value + r.value);
			case [_, _]:
				throw new pm.Error('Invalid operation: $l + $r');
		}
	}

	public static function op_mult(l:TypedValue, r:TypedValue):TypedValue {
		var n:Int = -1, v:TypedValue = r;
		switch [l.type, r.type] {
			case [TInt, _]:
				n = l.intValue;
				v = r;

			case [_, TInt]:
				n = r.intValue;
				v = l;

			default:
				throw new pm.Error('Invalid operation: $l * $r');
		}

		return switch v.type {
			case TInt: v.intValue * n;
			case TFloat: v.floatValue * n;
			case TString: v.stringValue.repeat(n);
			case TArray(_): v.arrayAnyValue.repeat(n);
			case other:
				throw new pm.Error('Invalid operation: ${v.value}:${v.type.print()} * $n');
		}
	}

	public static function op_div(l:TypedValue, r:TypedValue):TypedValue {
		switch [l, r] {
			case [{type: TInt | TFloat}, {type: TInt | TFloat}]:
				return (l.intValue != null ? 0.0 + l.intValue : l.floatValue) / (r.intValue != null ? 0.0 + r.intValue : r.floatValue);
			default:
				throw new pm.Error('Invalid operation: $l / $r');
		}
	}

	public static function op_mod(l:TypedValue, r:TypedValue):TypedValue {
		throw new pm.Error.NotImplementedError();
	}

	public static function op_subt(l:TypedValue, r:TypedValue):TypedValue {
		switch [l, r] {
			case [{type: TInt | TFloat}, {type: TInt | TFloat}]:
				return l.numValue - r.numValue;

			default:
		}
		throw new pm.Error('Invalid operation: $l - $r');
	}

	public static function op_eq(l:TypedValue, r:TypedValue):TypedValue {
		if (l.value == r.value)
			return true;
		if (pmdb.core.Arch.areThingsEqual(l.value, r.value))
			return true;

		return false;
	}

	public static function op_neq(l:TypedValue, r:TypedValue):TypedValue {
		return !op_eq(l, r).boolValue;
	}

	public static function op_cmp(l:TypedValue, r:TypedValue):Int {
		return pmdb.core.Arch.compareThings(l.value, r.value);
	}

	public static function op_gt(l:TypedValue, r:TypedValue):TypedValue {
		return op_cmp(l, r) > 0;
	}

	public static function op_gte(l:TypedValue, r:TypedValue):TypedValue {
		return op_cmp(l, r) >= 0;
	}

	public static function op_lt(l:TypedValue, r:TypedValue):TypedValue {
		return op_cmp(l, r) < 0;
	}

	public static function op_lte(l:TypedValue, r:TypedValue):TypedValue {
		return op_cmp(l, r) <= 0;
	}

	public static function op_bool_and(l:TypedValue, r:TypedValue):TypedValue {
		return switch [l.type, r.type] {
			case [TBool, TBool]: l.boolValue && r.boolValue;
			case [lt, rt]:
				throw new pm.Error('Invalid operation: ${l.value}:${lt.print()} - ${r.value}:${rt.print()}');
		}
	}

	public static function op_bool_or(l:TypedValue, r:TypedValue):TypedValue {
		return switch [l.type, r.type] {
			case [TBool, TBool]: l.boolValue || r.boolValue;
			case [_, _]: pmdb.core.Arch.isTruthy(l.value) ? l : r;
		}
	}

	public static function op_bool_xor(l:TypedValue, r:TypedValue):TypedValue {
		throw new pm.Error.NotImplementedError();
	}
}

class UnaryOperators {
	public static function getMethodHandle(op:UnaryOperator):(value:TypedValue) -> TypedValue {
		return switch op {
			case OpNot: op_not;
			case OpNegBits: throw new pm.Error.NotImplementedError();
			case OpPositive: op_positive;
			case OpNegative: op_negative;
		}
	}

	public static function op_not(value:TypedValue):TypedValue {
		return switch value.boolValue {
			case null: !pmdb.core.Arch.isTruthy(value.value);
			case b: !b;
		}
	}

	public static function op_negative(value:TypedValue):TypedValue {
		return switch value.type {
			case TInt: -(value.intValue);
			case TFloat: -(value.floatValue);
			default:
				throw new pm.Error('Invalid operation: -(${value.value}:${value.type.print()})');
		}
	}

	public static function op_positive(value:TypedValue):TypedValue {
		return switch value.type {
			case TInt: pm.Numbers.Ints.abs(value.intValue);
			case TFloat: Math.abs(value.floatValue);
			case TString: Std.parseFloat(value.stringValue);
			default:
				throw new pm.Error('Invalid operation: -(${value.value}:${value.type.print()})');
		}
	}
}