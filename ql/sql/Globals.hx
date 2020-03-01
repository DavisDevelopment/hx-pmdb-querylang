package ql.sql;

import pm.Error;
import haxe.PosInfos;

class Globals {
    extern public static inline function unwrap<T>(value:Null<T>, ?error:Dynamic #if !macro , ?pos:PosInfos #end):T {
        #if macro var pos:PosInfos = null; #end
        if (value == null)
            throw (error != null ? error : new Error('Null unwrap failed', 'NullIsBadError', pos));
        return value;
    }
}