package ql.sql.ast;

import pmdb.ql.ts.TypeChecks;
import pm.async.DynamicFunction;
import pmdb.core.Arch;

import ql.sql.ast.InterpOperators.DTools.*;

class InterpFunctions {
    public static function ql_min(a:Dynamic, b:Dynamic):Dynamic {
        if (a == null) {
            if (b == null) return null;
            return b;
        }
        if (b == null) return a;

        // try {
            var cmp:Int = Arch.compareThings(a, b);
            return if (cmp < 0) a else b;
        // }
    }
    public static function ql_max(a:Dynamic, b:Dynamic):Dynamic {
        if (a == null) {
            if (b == null) return null;
            return b;
        }
        if (b == null) return a;
        var cmp = Arch.compareThings(a, b);
        return if (cmp > 0) a else b;
    }

    public static function ql_len(v: Dynamic):Int {
        if (Reflect.hasField(v, 'length')) {
            var pv = Reflect.field(v, 'length');
            if ((TypeChecks.is_integer(pv))) return asInt(pv);
            if (Reflect.isFunction(pv)) {
                var prv = try pv() catch(e:Dynamic) null;
                if (TypeChecks.is_integer(prv)) return prv;
            }
        }
        throw new pm.Error('Invalid argument for len');
    }
}