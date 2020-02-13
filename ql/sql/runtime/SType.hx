package ql.sql.runtime;

import pmdb.ql.ts.DataType;
import haxe.Constraints.IMap;
import ql.sql.common.SqlSchema;
import ql.sql.common.DateTime;

@:using(ql.sql.runtime.SType.STypes)
enum SType {
	TUnknown;
	TBool;
	TInt;
	TFloat;
	TString;
	TDate;
	TArray(type:SType);
	TMap(key:SType, value:SType);
	TStruct(schema:SqlSchema<Dynamic>);
}

class STypes {
	public static function toDataType(type:SType):DataType {
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
		}
	}

	public static function validateValue(type:SType, value:Dynamic):Bool {
		return switch type {
			case TUnknown: true;
			case TBool: (value is Bool);
			case TInt: (value is Int);
			case TFloat: (value is Float);
			case TString: (value is String);
			case TDate: DateTime.is(value);
			case TArray(_): (value is Array<Dynamic>);
			case TMap(key, value): (value is haxe.IMap<Dynamic, Dynamic>);
			case TStruct(schema):
				throw new pm.Error('TODO');
		}
	}

	public static function validateInput(type:SType, value:Dynamic):Bool {
		if (validateValue(type, value)) return true;
		else {
			switch type {
				case TStruct(struct_type):
					return struct_type.validateInput(value).status;
				case TMap(k, v):
					// try {
					// 	for (key => value in (cast value.keyValueIterator() : KeyValueIterator<Dynamic,Dynamic>)) {
					// 		k.validateInput()
					// 	}
					// }
					throw new pm.Error('TODO');
				case TDate:
					return DateTime.is(value)||(value is Date)||(value is Float);
				default:
			}
		}
		return false;
	}

	public static function validateRepr(type:SType, value:Dynamic):Bool {
		return switch type {
			case TUnknown: true;
			case TBool: (value is Bool);
			case TInt: (value is Int);
			case TFloat: (value is Float);
			case TString: (value is String);
			case TDate: DateTime.is(value);
			case TArray(_): (value is Array<Dynamic>);
			case TMap(key, value): (value is haxe.IMap<Dynamic, Dynamic>);
			case TStruct(schema): schema.validate(value);
		}
	}

	public static function importValue(type:SType, value:Dynamic, ?pos:haxe.PosInfos):Dynamic {
		inline function tn():String return Std.string(Type.typeof(value));
		return switch type {
			case TUnknown: value;
			case TBool: value == true;
			case TInt:
				if ((value is Int)) value;
				else if ((value is Float)) Std.int(value);
				else throw new pm.Error('Expected Int|Float, got ${tn()}', pos);
			case TFloat:
				if ((value is Float)) value;
				else throw new pm.Error('Expected Float, got ${tn()}', pos);
			case TString:
				if ((value is String)) value;
				else if ((value is haxe.io.Bytes)) cast(value, haxe.io.Bytes).toString();
				else throw new pm.Error('Expected String|Bytes, got ${tn()}', pos);
			case TDate:
				if (DateTime.is(value)) value;
				else DateTime.fromAny(value);
			case TArray(type):
				if ((value is Array<Dynamic>)) {
					var a:Array<Dynamic> = cast value;
					a.map(v -> importValue(type, v));
				}
				else {
					pmdb.core.Arch.makeIterator(value).map(v -> importValue(type, v)).array();
				}
			case TMap(key, valueType):
				trace('TODO: TMap(${key.print()}, ${valueType.print()})');
				value;
			case TStruct(schema):
				schema.induct(value);
		}
	}

	/**
	 * obtain a pointer to a function which will check the given value is of `type`
	 * TODO: return simple references to static methods of a private class
	 * @param type 
	 * @return (value: Dynamic)->Bool
	 */
	public static function valueCheckerFn(type: SType):(value: Dynamic)->Bool {
		return x->false;
	}

	public static inline function eq(a:SType, b:SType):Bool {
		return a.equals(b);
	}

	public static function print(type:SType):String {
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
		}
	}

	public static function toDType(type:SType):DType {
		return switch type {
			case TUnknown: DType.TUnknown;
			case TBool: DType.TBool;
			case TInt: DType.TInt;
			case TFloat: DType.TFloat;
			case TString: DType.TString;
			case TDate: DType.TDate;
			case TArray(type): DType.TArray(toDType(type));
			case TMap(key, value): DType.TMap(toDType(key), toDType(value));
			case TStruct(schema): DType.TStruct(schema);
		}
	}
}