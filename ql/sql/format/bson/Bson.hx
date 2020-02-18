package ql.sql.format.bson;

import haxe.io.Bytes;

@:expose('Bson')
class Bson {
	public static function encode(o:Dynamic):Bytes {
		return BsonEncoder.encode(o);
	}
		
	public static function encodeMultiple(o:Array<Dynamic>):Bytes {
		return BsonEncoder.encodeMultiple(o);
	}
	
	public static function decode(bytes:Bytes):Dynamic {
		return BsonDecoder.decode(bytes);
	}
		
	public inline static function decodeMultiple(bytes:Bytes, num = 1):Dynamic {
		return BsonDecoder.decodeMultiple(bytes, num);
	}
}