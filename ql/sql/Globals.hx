package ql.sql;

import pm.Error;
import haxe.PosInfos;

#if macro
import haxe.macro.Expr;
import haxe.macro.Context;
// using haxe.macro.TypeTools;
// using haxe.macro.ExprTools;
#end

class Globals {
    extern public static inline function unwrap<T>(value:Null<T>, ?error:Dynamic #if !macro , ?pos:PosInfos #end):T {
        #if macro var pos:PosInfos = null; #end
		if (#if js js.Syntax.code('typeof {0} === \'undefined\' || {0} === null', value) #else value == null #end)
            throw (error != null ? error : new Error('Null unwrap failed', 'NullIsBadError', pos));
        return value;
    }
}