package ql.sql.runtime;

import pmdb.ql.ts.DataType;
import haxe.Constraints.IMap;
import ql.sql.common.SqlSchema;
import ql.sql.common.DateTime;

import haxe.rtti.CType;
import haxe.macro.Expr;

using pm.Functions;

using haxe.macro.ComplexTypeTools;

@:using(ql.sql.runtime.SType.STypes)
enum SType {
	//? TAny;
	TUnknown;

	TBool;
	TInt;
	TFloat;
	TString;
	TDate;
	TArray(type:SType);
	TMap(key:SType, value:SType);
	TStruct(schema:SqlSchema<Dynamic>);

	/*
	TBytes;
	TMono(t: Null<SType>);
	TSet(t: SType);
	*/
}

class STypes {
	public static function fromCType(type: CType):SType {
		return switch type {
			case CUnknown: TUnknown;
			case CEnum(name, params)|CClass(name, params)|CTypedef(name, params)|CAbstract(name, params):
				switch name {
					case 'Bool': TBool;
					case 'Int': TInt;
					case 'Float': TFloat;
					case 'String': TString;
					case 'Date': TDate;
					case 'Array': TArray(fromCType(params[0]));
					case 'Map': TMap(fromCType(params[0]), fromCType(params[1]));
					case 'Null': fromCType(params[0]);
					case other:
						throw new pm.Error('Unhandled $other');
				}

			case CFunction(args, ret):
				throw new pm.Error('Unhandled $args->$ret');

			case CAnonymous(fields):
				throw new pm.Error('Unhandled {$fields}');

			case CDynamic(t):
				throw new pm.Error('Unhandled Dynamic<$t>');

			case other:
				throw new pm.Error('Unhandled $other');
		}
	}

	@:from
	public static function ofComplexType(ctype: haxe.macro.ComplexType):SType {
		switch ctype {
			case ComplexType.TAnonymous(fields):
				var schema = SqlSchema.fromHaxeMacroFieldArray(fields);
				return SType.TStruct(schema);

			case ComplexType.TParent(ctype), ComplexType.TNamed(_, ctype):
				return ofComplexType(ctype);

			case ComplexType.TOptional(ctype):
				return ofComplexType(ctype);

			case ComplexType.TExtend(parent_paths, own_fields):
				// TODO
				throw new pm.Error('On hold until TStruct is the default object checker');

			case ComplexType.TIntersection(types):
				throw new pm.Error('On hold until TStruct is the default object checker');

			case ComplexType.TFunction(_, _):
				throw new pm.Error('Sorry. Function types are not implemented yet, but are very much on the roadmap');

			/** [RealType - referenced by its fully qualified name] **/
			case ComplexType.TPath(path):
				return ofTypePath(path);

			case other:
				throw 'Unhandled ${ctype.toString()}';
		}
	}

	public static function ofTypePath(path: haxe.macro.Expr.TypePath):SType {
		final t = SType;

		var isSimple = (path.pack.length == 0 && ((path.name == 'StdTypes' && path.sub != null) || (path.sub == null && !path.name.empty())));
		if (isSimple) {
			var typeName   = pm.Helpers.nor(pm.Strings.nullEmpty(path.sub), path.name);
			var typeParams = path.params.empty() ? [] : path.params.map(function(tp:haxe.macro.Expr.TypeParam) {
				return switch tp {
					case TPType(c): ofComplexType(c);
					case TPExpr(_): 
						throw 'Expression type parameters not supported yet';
				}
			});
			// typeName;

			switch typeName {
				case 'Bool':
					return t.TBool;
				case 'Float':
					return t.TFloat;
				case 'Int':
					return t.TInt;
				case 'Date':
					return t.TDate;
				case 'String':
					return t.TString;
				case 'Bytes':
					throw new pm.Error('TODO: Bytes');
				
				// case ('Array' | 'Null') if (typeParams.length == 1):
				// 	return Type.createEnum(t, 'T$typeName', [typeParams[0]]);
				case other:
					// throw other;
			}

			return switch [typeName, typeParams] {
				case ['Array', [itm]]: t.TArray(itm);
				case ['Map'|'Hash', [keyType, valType]]: t.TMap(keyType, valType);
				default:
					throw new pm.Error('Unhandled ${path}');
			}
		}

		throw new pm.Error('Unhandled ${path}');
	}

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
	public static function opt_validateRepr(type:SType):Dynamic->Bool {
		return switch type {
			case TUnknown://?should this just throw an error?
				(_ -> true);
			case TBool: (value:Dynamic) -> (value is Bool);
			case TInt: (value:Dynamic) -> (value is Int);
			case TFloat: (value:Dynamic) -> (value is Float);
			case TString: (value:Dynamic) -> (value is String);
			case TDate: (value:Dynamic) -> DateTime.is(value);
			case TArray(_): (value:Dynamic) -> (value is Array<Dynamic>);
			case TMap(key, value): (value:Dynamic) -> (value is haxe.IMap<Dynamic, Dynamic>);
			case TStruct(schema): (value:Dynamic) -> schema.validate(value);
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

	public static function opt_importValue(type:SType, ?pos:haxe.PosInfos):Dynamic->Dynamic {
		inline function tn(value:Dynamic):String
			return Std.string(Type.typeof(value));
		return switch type {
			case TUnknown: Functions.identity;
			case TBool: (value:Dynamic) -> value == true;
			case TInt:
				function(value: Dynamic):Dynamic {
					return if ((value is Int)) value; else if ((value is Float)) Std.int(value); else throw new pm.Error('Expected Int|Float, got ${tn(value)}', pos);
				};
			case TFloat:
				function(value:Dynamic):Dynamic return {
					if ((value is Float)) value; else throw new pm.Error('Expected Float, got ${tn(value)}', pos);
				};
			case TString:
				function(value:Dynamic):Dynamic return {
					if ((value is String)) value; else if ((value is haxe.io.Bytes)) cast(value, haxe.io.Bytes).toString(); 
					else throw new pm.Error('Expected String|Bytes, got ${tn(value)}', pos);
				};
			case TDate:
				function(value:Dynamic):Dynamic return {
					if (DateTime.is(value)) value; else DateTime.fromAny(value);
				};
			case TArray(type):
				var import_item = opt_importValue(type);
				function(value:Dynamic):Dynamic return {
					if ((value is Array<Dynamic>)) {
						var a:Array<Dynamic> = cast value;
						a.map(import_item);
					} 
					else {
						var itr = pmdb.core.Arch.makeIterator(value);
						// .map(v -> importValue(type, v)).array();
						[for (item in itr) import_item(item)];
					}
				};
			case TMap(key, valueType):
				function(value:Dynamic):Dynamic {
					trace('TODO: TMap(${key.print()}, ${valueType.print()})');
					return value;
				}
			
			case TStruct(schema):
				function(value: Dynamic) {
					//Console.error('use optimized schema.induct function');
					return schema.induct(value);
				}
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