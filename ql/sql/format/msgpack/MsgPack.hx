package ql.sql.format.msgpack;

import haxe.Int64;
import haxe.io.Eof;
import haxe.io.Bytes;
import haxe.io.BytesOutput;
import haxe.io.BytesInput;
import haxe.ds.IntMap;
import haxe.ds.StringMap;

using Reflect;

class Encoder {
//{region constants
	static private inline var FLOAT_SINGLE_MIN:Float = 1.40129846432481707e-45;
	static private inline var FLOAT_SINGLE_MAX:Float = 3.40282346638528860e+38;
	static private inline var FLOAT_DOUBLE_MIN:Float = 4.94065645841246544e-324;
	static private inline var FLOAT_DOUBLE_MAX:Float = 1.79769313486231570e+308;
//}endregion

	var o: BytesOutput;

	public function new(d: Dynamic) {
		o = new BytesOutput();
		o.bigEndian = true;

		encode(d);
	}

	function encode(d:Dynamic) {
		
		switch (Type.typeof(d)) {
			case TNull    : o.writeByte(0xc0);
			case TBool    : o.writeByte(d ? 0xc3 : 0xc2);
			case TInt     : writeInt(d);
			case TFloat   : writeFloat(d);
			
			case TClass(Type.getClassName(_)=>c): 
				switch c {
					case "haxe._Int64.___Int64" : writeInt64(d);
					case "haxe.io.Bytes": writeBinary(d);
					case "String": writeString(d);
					case "Array": writeArray(d);
					case "haxe.ds.IntMap" | "haxe.ds.StringMap" | "haxe.ds.UnsafeStringMap":
						writeMap(d);
					default: throw 'Error: ${c} not supported';
				}

			case TObject  : writeObject(d);
			case TEnum(e) : throw new pm.Error("Enum not supported");
			case TFunction: throw new pm.Error("Function not supported");
			case TUnknown : throw new pm.Error("Unknown Data Type");
		}
	}

	inline function writeInt64(d:Int64) {
		o.writeByte(0xd3);
		o.writeInt32(d.high);
		o.writeInt32(d.low);
	}

	inline function writeInt(d:Int) {
		if (d < -(1 << 5)) {
			// less than negative fixnum ?
			if (d < -(1 << 15)) {
				// signed int 32
				o.writeByte(0xd2);
				o.writeInt32(d);
			} 
			else if (d < -(1 << 7)) {
				// signed int 16
				o.writeByte(0xd1);
				o.writeInt16(d);
			} 
			else {
				// signed int 8
				o.writeByte(0xd0);
				o.writeInt8(d);
			}
		} 
		else if (d < (1 << 7)) {
			// negative fixnum < d < positive fixnum [fixnum]
			o.writeByte(d & 0x000000ff);
		} 
		else {
			// unsigned land
			if (d < (1 << 8)) {
				// unsigned int 8
				o.writeByte(0xcc);
				o.writeByte(d);
			} 
			else if (d < (1 << 16)) {
				// unsigned int 16
				o.writeByte(0xcd);
				o.writeUInt16(d);
			} 
			else {
				// unsigned int 32 
				// TODO: HaXe writeUInt32 ?
				o.writeByte(0xce);
				o.writeInt32(d);
			}
		}
	}

	inline function writeFloat(d:Float) {		
		var a = Math.abs(d);
		if (a > FLOAT_SINGLE_MIN && a < FLOAT_SINGLE_MAX) {
			// Single Precision Floating
			o.writeByte(0xca);
			o.writeFloat(d);
		} 
		else {
			// Double Precision Floating
			o.writeByte(0xcb);
			o.writeDouble(d);
		}
	}

	inline function writeBinary(b: Bytes) {
		var length = b.length;
		if (length < 0x100) {
			// binary 8
			o.writeByte(0xc4);
			o.writeByte(length);
		} 
		else if (length < 0x10000) {
			// binary 16
			o.writeByte(0xc5);
			o.writeUInt16(length);
		} 
		else {
			// binary 32
			o.writeByte(0xc6);
			o.writeInt32(length);
		}

		o.write(b);
	}

	inline function writeString(b: String) {
		var length = b.length;
		if (length < 0x20) {
			// fix string
			o.writeByte(0xa0 | length);
		} 
		else if (length < 0x100) {
			// string 8
			o.writeByte(0xd9);
			o.writeByte(length);
		} 
		else if (length < 0x10000) {
			// string 16
			o.writeByte(0xda);
			o.writeUInt16(length);
		} 
		else {
			// string 32
			o.writeByte(0xdb);
			o.writeInt32(length);
		}
		o.writeString(b);
	}

	inline function writeArray(d:Array<Dynamic>) {
		var length = d.length;
		if (length < 0x10) {
			// fix array
			o.writeByte(0x90 | length);
		} 
		else if (length < 0x10000) {
			// array 16
			o.writeByte(0xdc);
			o.writeUInt16(length);
		} 
		else {
			// array 32
			o.writeByte(0xdd);
			o.writeInt32(length);
		}

		for (e in d)
			encode(e);
	}

	inline function writeMapLength(length:Int) {
		if (length < 0x10) {
			// fix map
			o.writeByte(0x80 | length);
		} 
		else if (length < 0x10000) {
			// map 16
			o.writeByte(0xde);
			o.writeUInt16(length);
		} 
		else {
			// map 32
			o.writeByte(0xdf);
			o.writeInt32(length);
		}		
	}

	inline function writeMap<K, V>(d:Map<K, V>) {
		var length = 0;
		for (k in d.keys()) 
			length++;

		writeMapLength(length);
		for (k in d.keys()) { 
			encode(k);
			encode(d.get(k));
		}
	}

	inline function writeObject(d:Dynamic) {
		var f = d.fields();

		writeMapLength(Lambda.count(f));
		for (k in f) {
			encode(k);
			encode(d.field(k));
		}
	}

	public inline function getBytes():Bytes {
		return o.getBytes();
	}
}

/*
  Decoder.hx
*/
// package org.msgpack;

enum DecodeOption {
	AsMap;
	AsObject;
}

private class Pair {

	public var k (default, null) : Dynamic;
	public var v (default, null) : Dynamic;

	public function new(k, v)
	{
		this.k = k;
		this.v = v;
	}
}

class Decoder {
	var o:Dynamic;

	public function new(b:Bytes, option:DecodeOption) {
		var i       = new BytesInput(b);
		i.bigEndian = true;
		o           = decode(i, option);
	}

	function decode(i:BytesInput, option:DecodeOption):Dynamic {
		try {
			var b = i.readByte();
			switch (b) {
				// null
				case 0xc0: return null;

				// boolean
				case 0xc2: return false;
				case 0xc3: return true;

				// binary
				case 0xc4: return i.read(i.readByte  ());
				case 0xc5: return i.read(i.readUInt16());
				case 0xc6: return i.read(i.readInt32 ());

				// floating point
				case 0xca: return i.readFloat ();
				case 0xcb: return i.readDouble();
				
				// unsigned int
				case 0xcc: return i.readByte  ();
				case 0xcd: return i.readUInt16();
				case 0xce: return i.readInt32 ();
				case 0xcf: throw "UInt64 not supported";

				// signed int
				case 0xd0: return i.readInt8 ();
				case 0xd1: return i.readInt16();
				case 0xd2: return i.readInt32();
				case 0xd3: return readInt64(i);

				// string
				case 0xd9: return i.readString(i.readByte  ());
				case 0xda: return i.readString(i.readUInt16());
				case 0xdb: return i.readString(i.readInt32 ());

				// array 16, 32
				case 0xdc: return readArray(i, i.readUInt16(), option);
				case 0xdd: return readArray(i, i.readInt32 (), option);

				// map 16, 32
				case 0xde: return readMap(i, i.readUInt16(), option);
				case 0xdf: return readMap(i, i.readInt32 (), option);

				default  : {
					if (b < 0x80) {	return b;                               } else // positive fix num
					if (b < 0x90) { return readMap  (i, (0xf & b), option); } else // fix map
					if (b < 0xa0) { return readArray(i, (0xf & b), option); } else // fix array
					if (b < 0xc0) { return i.readString(0x1f & b);          } else // fix string
					if (b > 0xdf) { return 0xffffff00 | b;                  }      // negative fix num
				}
			}
		}
		catch (e: Eof) {}
		return null;
	}

	inline function readInt64(i: BytesInput){
		var high = inline i.readInt32();
		var low =  inline i.readInt32();
		return Int64.make(high, low);
	}

	inline function readArray(i:BytesInput, length:Int, option:DecodeOption) {
		var a = [];
		a.resize(length);
		for (x in 0...length)
			a[x] = decode(i, option);
		return a;
	}

	function readMap(i:BytesInput, length:Int, option:DecodeOption):Dynamic {
		switch (option) {
			case DecodeOption.AsObject:
				var out = {};
				for (n in 0...length) {
					var k = decode(i, option);
					var v = decode(i, option);
					Reflect.setField(out, Std.string(k), v);
				}

				return out;

			case DecodeOption.AsMap:
				var pairs = [];
				for (n in 0...length) {
					var k = decode(i, option);
					var v = decode(i, option);
					pairs.push(new Pair(k, v));
				}

				if (pairs.length == 0)
					return new StringMap();

				switch (Type.typeof(pairs[0].k)) {
					case TInt:
						var out = new IntMap();
						for (p in pairs){
							switch(Type.typeof(p.k)){
								case TInt:
								default:  
									throw "Error: Mixed key type when decoding IntMap";
							}
							
							if (out.exists(p.k)) 
								throw 'Error: Duplicate keys found => ${p.k}';

							out.set(p.k, p.v);
						}

						return out;

					case TClass(c) if (Type.getClassName(c) == "String"):
						var out = new StringMap();
						for (p in pairs){
							switch(Type.typeof(p.k)){
								case TClass(c) if (Type.getClassName(c) == "String"):
								default: 
									throw "Error: Mixed key type when decoding StringMap";
							}

							if (out.exists(p.k)) 
								throw 'Error: Duplicate keys found => ${p.k}';
							
							out.set(p.k, p.v);
						}

						return out;

					default:
						throw "Error: Unsupported key Type";
				}
		}

		throw "Should not get here";
	}

	public inline function getResult() {
		return o;
	}
}

class MsgPack {
	public static inline function encode(d:Dynamic):Bytes { 
		return new Encoder(d).getBytes(); 
	}

	public static inline function decode(b:Bytes, ?option:DecodeOption):Dynamic {
		if (option == null) 
            option = DecodeOption.AsObject;

		return new Decoder(b, option).getResult();
	}
}
