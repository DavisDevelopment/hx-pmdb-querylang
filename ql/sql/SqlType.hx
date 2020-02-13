package ql.sql;

@:using(ql.sql.SqlType.SqlTypes)
enum SqlType {
    SAny;
	SInt;
	SFloat;
	// SBigInt;
	// SDouble;

	SText;
	SBlob;
	// SDate;
    SDateTime;
    SList;
    SExt(type: SqlTypeExt);
}
enum SqlTypeExt {

}

class SqlTypes {
    public static function isScalarType(t: SqlType):Bool {
        return switch t {
            case SAny: false;
            case SList: false;
            case SInt: true;
            case SFloat: true;
            case SText: true;
            case SBlob: true;
            case SDateTime: true;
            case SExt(type): false;
        }
    }

    public static function validateValue(t:SqlType, value:Dynamic, ?pos:haxe.PosInfos):Bool {
        if (value == null) throw new pm.Error('null not accepted', 'InvalidArgument', pos);
        return switch t {
            case SAny: true;
            case SInt: (value is Int);
            case SFloat: (value is Float);
            case SText: (value is String);
            case SBlob: (value is String);//FIXME
            case SDateTime: (value is Date);//TODO
            case SList: (value is Array<Dynamic>);
            case SExt(type):
                throw 'TODO';
        }
    }
}