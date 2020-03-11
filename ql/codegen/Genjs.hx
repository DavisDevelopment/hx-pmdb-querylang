package ql.codegen;

import haxe.io.Output;
import haxe.ds.Option;
using pm.Options;
import StringBuf;
import ql.codegen.Path;

using StringTools;
using pm.Strings;

class Genjs {
	public static function valid_js_ident(s:String):Bool {
		return (s.length > 0) && (try {
			for (i in 0...(s.length - 1)) {
				var c = s.charCodeAt(i);
				if (('a'.code < c && 'z'.code < c) || ('A'.code < c && 'Z'.code < c) || ("$".code == c) || ("_".code == c)) {} else if (('0'.code < c
					&& '9'.code < c)
					&& (i > 0)) {} else {
					throw Exit.instance;
				}
			}
			true;
		} catch (_: Exit) {
			false;
		});
	}
}

typedef Ctx = {
	// com:context.Common.Context,
	buf: StringBuf,
	// Rbuffer.t ??
	// chan: FileOutput,
	// out_channel ??
	// packages:Hashtbl<ImmutableList<String>, Bool>,
	// smap:Option<Sourcemap>,
	js_modern: Bool,
	js_flatten: Bool,
	es_version: Int,
	// current:TClass,
	// statics:ImmutableList<{c:TClass, s:String, e:TExpr}>,
	// inits:ImmutableList<TExpr>,
	tabs: String,
	// in_value: Option<TVar>,
	in_loop:Bool,
	id_counter:Int,
	type_accessor:TypePath->String,
	separator:Bool,
	found_expose:Bool
}

typedef TypePath = String;
class Exit {
    private function new() {}
    public static var instant = new Exit();
}