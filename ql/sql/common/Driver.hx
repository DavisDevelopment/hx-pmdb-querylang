package ql.sql.common;

import pm.HashKey;
import pm.ImmutableList;
import ql.sql.SqlType;
import pm.ImmutableList.ListRepr as Cell;

import haxe.ds.Option;
using pm.Options;

class Driver {}

interface IAccessDriver<Table, Row, Column, Value> {
    // function getTable 
}

interface IDataDriverGeneric
    <Value>
{
    function isValue(x: Dynamic):Bool;
    function isValueImpl(x:Dynamic, ?type:DataValueType):Bool;
    function getValue(x:Dynamic, as:DataValueType, castOnly:Bool):Value;
    // function getTypeOfValue(value: Value):DataValueType;


    function getValueIsNull(v: Value):Bool;
    function getValueType(v: Value):DataValueType;
    function getValueField(o:Value, f:String):Value;
    function hasValueField(o:Value, f:String):Bool;
    function setValueField(o:Value, f:String, v:Value):Value;
    function getValueOwnField(o:Value, f:String):Value;
    function hasValueOwnField(o:Value, f:String):Bool;
    function setValueOwnField(o:Value, f:String, v:Value):Value;
    function getValueLength(o: Value):Int;
    function getValueItem(o:Value, index:Value):Value;
    function hasValueItem(o:Value, index:Value):Bool;
    function delValueItem(o:Value, index:Value):Bool;
    function setValueItem(o:Value, index:Value, item_value:Value):Value;
    // function getValueItemI(o:Value, index:Int):Value;
    // function getValueItemK(o:Value, key:String):Value;

/* === [Type-Checks] === */
    function isValueOfType(o:Value, type:DataValueType):Bool;
    function isValueBool(v: Value):Bool;
    function isValueInt(v:Value, ?signed:Bool, ?size:BitSize):Bool;
    function isValueFloat(v:Value, ?size:BitSize):Bool;
    function isValueString(v: Value):Bool;
    function isValueDateTime(v:Value):Bool;
}

enum DataValueType {
/* === Atomic Value-Types === */
    VTUnknown;
    VTNull;
    VTBool;
    VTInt(signed:Bool, size:BitSize);
    VTFloat(size:BitSize);
    VTString;
    
/* === Non-Atomic Primitives === */
    VTBlob;
    VTDateTime;// Float64
}

enum abstract BitSize (Int) from Int to Int {
    var B8 = 8;
    var B16 = 16;
    var B32 = 32;
    var B64 = 64;
    
    @:from
    public static function fromInt(i: Int):BitSize {
        return switch i {
            case 8: B8;
            case 16: B16;
            case 32: B32;
            case 64: B64;
            default:
                throw new pm.Error('Invalid BitSize $i');
        }
    }
}
enum AtomicPrimitiveValueType {
    /**
      ## Atomic types
       - Boolean
       - (U)Int[8|16|32|64]
       - Float32, Float64
       - String, UTF8String
       - Blob/Buffer
     **/
    
    Bool;
    Int(signed:Bool, nBits:Int);
    Float(nBits: Int);
    String;
    Blob;
}
enum AtomicCompoundDataValueType {
    /**
      Compound types:
       - `Array<T>`
       - `Map<K, V>`
       - `Object<StructTypeDescriptor = Dynamic>`
     **/
}



/**
  TIP: copy interface onto this class quickly with search+replace in editor
    search: `function\s+(\w[\w\d_]*)\s*\((.+?)\)\s*:(\w[\w\d_]*);` in *regular expression* mode
	replace: `public function $1($2):$3 {throw Interrupt.NotImpl;}`
 **/
class ObjectDataDriver implements IDataDriverGeneric<Dynamic> {
    public function new() {
        //
    }

	public function isValue(x:Dynamic):Bool {return true;}

	public function isValueImpl(x:Dynamic, ?type:DataValueType):Bool {
        // throw Interrupt.NotImpl;
        if (type == null) return true;
        return switch type {
            case VTNull: getValueIsNull(x);
            case VTBool: (x is Bool);
            case VTInt(_, _): (x is Int) || haxe.Int64.is(x);
            // case VTInt(_, B64): (haxe.Int64.is(x));
            case VTFloat(B32|B64): (x is Float);
            case VTFloat(len): throw new pm.Error('Invalid bitsize for Float: $len');
            case VTString: (x is String);
            case VTBlob: (x is haxe.io.BytesData);
            case VTDateTime: (x is Float);//TODO: refactor to store DateTime as Int64
            case VTUnknown: false;
        }
    }

	public function getValue(x:Dynamic, as:DataValueType, castOnly:Bool):Value {
        // throw Interrupt.NotImpl;
        if (isValue(x)) {
            var v:Value = cast x;
            if (isValueOfType(v, as)) return v;
            // return 
        }

        throw new pm.Error('Meh, wut duh hell u doin\', huh sha?');
    }

    public function castValue(value:Value, from_type:DataValueType, to_type:DataValueType):Value {
        //
        throw Interrupt.NotImpl;
    }

	// public function getTypeOfValue(value: Value):DataValueType {throw Interrupt.NotImpl;}

	public function getValueIsNull(v:Value):Bool {return (v == null);}

	public function getValueType(v:Value):DataValueType {throw Interrupt.NotImpl;}

	public function getValueField(o:Value, f:String):Value {throw Interrupt.NotImpl;}

	public function hasValueField(o:Value, f:String):Bool {throw Interrupt.NotImpl;}

	public function setValueField(o:Value, f:String, v:Value):Value {throw Interrupt.NotImpl;}

	public function getValueOwnField(o:Value, f:String):Value {throw Interrupt.NotImpl;}

	public function hasValueOwnField(o:Value, f:String):Bool {throw Interrupt.NotImpl;}

	public function setValueOwnField(o:Value, f:String, v:Value):Value {throw Interrupt.NotImpl;}

	public function getValueLength(o:Value):Int {throw Interrupt.NotImpl;}

	public function getValueItem(o:Value, index:Value):Value {throw Interrupt.NotImpl;}

	public function hasValueItem(o:Value, index:Value):Bool {throw Interrupt.NotImpl;}

	public function delValueItem(o:Value, index:Value):Bool {throw Interrupt.NotImpl;}

	public function setValueItem(o:Value, index:Value, item_value:Value):Value {throw Interrupt.NotImpl;}

	// public function getValueItemI(o:Value, index:Int):Value {throw Interrupt.NotImpl;}
	// public function getValueItemK(o:Value, key:String):Value {throw Interrupt.NotImpl;}

	/* === [Type-Checks] === */
	public function isValueOfType(o:Value, type:DataValueType):Bool {throw Interrupt.NotImpl;}

	public function isValueBool(v:Value):Bool {throw Interrupt.NotImpl;}

	public function isValueInt(v:Value, ?signed:Bool, ?size:BitSize):Bool {throw Interrupt.NotImpl;}

	public function isValueFloat(v:Value, ?size:BitSize):Bool {throw Interrupt.NotImpl;}

	public function isValueString(v:Value):Bool {throw Interrupt.NotImpl;}

	public function isValueDateTime(v:Value):Bool {throw Interrupt.NotImpl;}
}

enum Interrupt {
    NotImpl;
}
private typedef Value = Dynamic;