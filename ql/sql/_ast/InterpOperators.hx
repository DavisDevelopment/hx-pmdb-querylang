package ql.sql.ast;

import haxe.Int64;
import pm.BigInt;
import datetime.DateTime;

import pmdb.ql.ts.TypeChecks;
import pmdb.core.Arch;

using Lambda;
using pm.Arrays;

import ql.sql.ast.InterpOperators.DTools.*;
using ql.sql.ast.InterpOperators.DTools;

class InterpOperators {
//{region binary_ops

    /**
      [TODO]: 
       - [] Int64 support
       - [] BigInt support
       - [] Array/Set support
       - [] Dict support
     **/
    public static function add(a:Dynamic, b:Dynamic):Dynamic {
        var c = TypeChecks;
        
        if ((a is String) || (b is String)) return Std.string(a) + Std.string(b);
        
        if (c.is_integer(a)) {
            if (c.is_integer(b)) return (a : Int)+(b : Int);
            if (c.is_number(b)) return (a : Int)+(b : Float);

            // if (Int64.is(b)) return cast(a, Float) + ((b : Int) : Int64).toInt();
        }

        if ((a is Float)) {
            if ((b is Float)) return (a:Float)+(b:Float);

            throw new pm.Error('Invalid right-hand operand $b');
        }

        //TODO
        
        throw new pm.Error('Invalid operands $a, $b');
    }

    public static function subt(a:Dynamic, b:Dynamic):Dynamic {
        var c = TypeChecks;
        
        if ((a is Float)) {
            if ((b is Float)) return (a:Float)-(b:Float);
            //TODO Int64, BigInt, Decimal
            throw new pm.Error('Invalid right-hand operand $b');
        }

        throw new pm.Error('Invalid operands $a, $b');
    }

    public static function div(a:Dynamic, b:Dynamic):Dynamic {
        return null;
    }

    /**
      multiply `a` by `b`
       - [] String repetition
       - [] Array repetition
       - [] Int64,BigInt support
     **/
    public static function mult(a:Dynamic, b:Dynamic):Dynamic {
        // return null;
        if (TypeChecks.is_integer(a)) {
            if (TypeChecks.is_integer(b)) return cast(a, Int)*cast(b, Int);
            if ((b is Float)) return (cast(a, Int)+1.2-0.2-1)*cast(b, Float);
            if ((b is String)) return '$b'.repeat(cast(a, Int));
        }

        if ((a is Float)) {
            if (TypeChecks.is_integer(b)) return cast(a, Float)*cast(b, Int);
            if ((b is Float)) return a.asFloat()*b.asFloat();
        }

        if ((TypeChecks.is_integer(b))) {
            try {
                return mult(b, a);
            }
            catch (e: Dynamic) {}
        }

        throw pm.Error.withData([a, b], 'Invalid operands');
    }

    public static function idiv(a:Dynamic, b:Dynamic):Dynamic {
        return null;
    }

    public static function mod(a:Dynamic, b:Dynamic):Dynamic {
        return null;
    }

    public static function isin(a:Dynamic, b:Dynamic):Bool {
        var rwtf = Error.withData(b, 'Invalid right-hand operand');
        var lwtf = Error.withData(a, 'Invalid left-hand operand');
        if (b == null) throw rwtf;
        if ((b is String)) return '$b'.indexOf('$a') != -1;
        if ((b is Array<Dynamic>)) return if (Arch.isAtomic(a)) cast(b, Array<Dynamic>).has(a) else cast(b, Array<Dynamic>).search(item -> Arch.areThingsEqual(a, item));
        if (Arch.isIterable(b)) {
            var itr = Arch.makeIterator(b);
            for (item in itr) {
                if (Arch.areThingsEqual(a, item)) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    public static function boolean_or(a:Dynamic, b:Dynamic):Dynamic {
        var left:Bool = Arch.isTruthy(a);
        return left ? a : b;
    }

    public static function cmp_eq(a:Dynamic, b:Dynamic):Bool {
        return Arch.areThingsEqual(a, b);
    }
    public static function cmp_gt(a:Dynamic, b:Dynamic):Bool {
        return Arch.compareThings(a, b) > 0;
    }
	public static function cmp_lt(a:Dynamic, b:Dynamic):Bool {
		return Arch.compareThings(a, b) < 0;
	}

//}endregion

//{region pseudo_ops
/**
 - _*Internal Language Operations - "Pseudo Ops"*_
*/

    public static function existsConst(c:Dynamic):Bool return c != null;

//}endregion
}

class DTools {
    public static inline function asFloat(v: Dynamic):Float {
        return (1 + (v : Float) - 1);
    }
    public static function asString(v:Dynamic):String return Std.string(v);
    public static function asInt(v: Dynamic):Int {
        return if ((TypeChecks.is_integer(v))) (v : Int) else Std.int(asFloat(v));
    }
}