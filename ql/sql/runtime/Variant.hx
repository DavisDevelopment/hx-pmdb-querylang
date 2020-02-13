package ql.sql.runtime;

enum VariantType {
    VT_Null;
    VT_Bool(b: Bool);
    VT_Int(n: Int);
    VT_Float(n: Float);
    VT_DateTime(n: Date);
    VT_String(s: String);
    // VT_Array(a: Array<Variant>);
}

@:notNull
abstract Variant (VariantType) from VariantType to VariantType {
    public var type(get, never):VariantType;
    private inline function get_type():VariantType return this;

    public function data():Dynamic {
        return switch type {
            case VT_Null: null;
            case VT_Bool(b): b;
            case VT_Int(n): n;
            case VT_Float(n): n;
            case VT_DateTime(n): n;
            case VT_String(s): s;
        }
    }

    @:to static function toBool(v:Variant):Bool {
        return switch v.type {
            case VT_Bool(b): b;
            case other: throw new pm.Error('Invalid variant $other');
        }
    }

//{region operations

//{region arithmetic
    @:op(A + B)
    @:commutative
    public static function addVariantFloat(v:Variant, n:Float):Variant {
        return switch v.type {
            case VT_Float(n1): n + n1;
            case VT_Int(n1): n + n1;
            default:
                throw new pm.Error('Invalid argument');
        }
    }

    @:op(A + B) static function addVariantString(v:Variant, s:String):Variant {
        return v.data() + s;
    }
    @:op(A + B) static function addStringVariant(s:String, v:Variant):Variant {
        return s + v.data();
    }
//}endregion
//}endregion

//{region factories

	@:from static inline function ofBool(b:Bool):Variant
		return VT_Bool(b);

	@:from static inline function ofInt(i:Int):Variant
		return VT_Int(i);

	@:from static inline function ofFloat(n:Float):Variant
		return VT_Float(n);

	@:from static inline function ofDate(d:Date):Variant
		return VT_DateTime(d);

	@:from static inline function ofString(s:String):Variant
		return VT_String(s);

	@:from public static function of(x:Dynamic):Variant {
		return switch Type.typeof(x) {
			case TEnum(VariantType): cast(x, VariantType);
			case TNull: VT_Null;
			case TInt: VT_Int(cast(x, Int));
			case TFloat: VT_Float(cast(x, Float));
			case TBool: VT_Bool(cast(x, Bool));
			// case TObject:
			case TFunction: throw new pm.Error('disallowed');
			case TClass(String): VT_String(cast(x, String));
			case TClass(Date): VT_DateTime(cast(x, Date));
			case t=(TEnum(_)|TUnknown|TClass(_)|TObject): 
                throw new pm.Error('Unsupported $t');
		}
	}

//}endregion
}