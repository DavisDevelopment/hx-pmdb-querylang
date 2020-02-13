package ql.sql.runtime;

import pmdb.ql.ts.DataType;
import haxe.Constraints.IMap;
import ql.sql.common.SqlSchema;

@:using(ql.sql.runtime.DType.DTypes)
enum DType {
	/* === [Atomic Types] === */
	TUnknown;
	TBool;
	TInt;
	TFloat;
	TString;
	TDate;
	TArray(type:DType);
	TMap(key:DType, value:DType);
	TStruct(schema:SqlSchema<Dynamic>);

	/* === [Descriptive "Types"] === */
	TAny;
	TNull(type:DType);
	TEither(left:DType, right:DType);
}

class DTypes {
	public static function toDataType(type:DType):DataType {
		return switch type {
			case TUnknown: DataType.TUnknown;
			case TBool: DataType.TScalar(ScalarDataType.TBoolean);
			case TInt: DataType.TScalar(ScalarDataType.TInteger);
			case TFloat: DataType.TScalar(ScalarDataType.TDouble);
			case TString: DataType.TScalar(ScalarDataType.TString);
			case TDate: DataType.TScalar(ScalarDataType.TDate);
			case TArray(t): DataType.TArray(toDataType(t));
			case TMap(key, value):
				throw new pm.Error('TODO');
			case TStruct(schema):
				throw new pm.Error('TODO');

			case TAny: DataType.TAny;
			case TNull(t): DataType.TNull(toDataType(t));
			case TEither(l, r): DataType.TUnion(toDataType(l), toDataType(r));
		}
	}

	public static function fromDataType(type:DataType):DType {
		return switch type {
			case TVoid, TMono(_):
				throw new pm.Error('Unsupported');
			case TUndefined | TUnknown: DType.TUnknown;
			case TAny: DType.TAny;
			case TArray(type): DType.TArray(fromDataType(type));
			case TTuple(types): throw new pm.Error('TODO');
			case TStruct(schema): throw new pm.Error('TODO');
			case TAnon(type): throw new pm.Error('TODO');
			case TClass(c): throw new pm.Error('TODO');
			// case TMono(type):
			case TNull(type): DType.TNull(fromDataType(type));
			case TUnion(left, right): TEither(fromDataType(left), fromDataType(right));
			case TScalar(type): switch type {
					case TBoolean: DType.TBool;
					case TDouble: DType.TFloat;
					case TInteger: DType.TInt;
					case TString: DType.TString;
					case TBytes: throw new pm.Error('TODO');
					case TDate: DType.TDate;
				}
		}
	}

	public static function validateValue(type:DType, value:Dynamic):Bool {
		return switch type {
			case TUnknown: true;
			case TBool: (value is Bool);
			case TInt: (value is Int);
			case TFloat: (value is Float);
			case TString: (value is String);
			case TDate: ql.sql.common.DateTime.is(value);
			case TArray(itemType): (value is Array<Dynamic>) && cast(value, Array<Dynamic>).all(validateValue.bind(itemType, _));
			case TMap(keyType, valueType): (value is haxe.IMap<Dynamic, Dynamic>) && cast(value, haxe.Constraints.IMap<Dynamic, Dynamic>).keyValueIterator()
					.all(kv -> keyType.validateValue(kv.key) && valueType.validateValue(kv.value));
			case TStruct(schema):
				// throw new pm.Error('TODO');
				try schema.validate(value) catch (e:Dynamic) false;

			case TAny: true;
			case TNull(t): value == null || validateValue(t, value);
			case TEither(l, r): validateValue(l, value) || validateValue(r, value);
		}
	}

	public static inline function eq(a:DType, b:DType):Bool {
		return a.equals(b);
	}

	public static function print(type:DType) {
		return switch type {
			case TUnknown: 'Unknown';
			case TBool: 'Bool';
			case TInt: 'Int';
			case TFloat: 'Float';
			case TString: 'String';
			case TDate: 'Date';
			case TArray(type): 'Array<${print(type)}>';
			case TMap(key, value): 'Map<${print(key)}, ${print(value)}>';
			case TStruct(schema): throw new pm.Error('TODO');
			case TAny: 'Any';
			case TNull(type): 'Null<${print(type)}>';
			case TEither(left, right): 'Either<${print(left)}, ${print(right)}>';
		}
	}

	public static function toSType(t:DType):SType {
		return switch t {
			case TUnknown: SType.TUnknown;
			case TBool: SType.TBool;
			case TInt: SType.TInt;
			case TFloat: SType.TFloat;
			case TString: SType.TString;
			case TDate: SType.TDate;
			case TArray(type): SType.TArray(toSType(type));
			case TMap(key, value): SType.TMap(toSType(key), toSType(value));
			case TStruct(schema): SType.TStruct(schema);
			case TAny: SType.TUnknown;
			case TNull(type): toSType(type);
			case TEither(_, _):
				throw new pm.Error('${print(t)} cannot be converted to SType');
		}
	}
}