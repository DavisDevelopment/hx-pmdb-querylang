package ql.sql.ast;

import haxe.Int64;
import pm.async.DynamicFunction;
import ql.sql.ast.Expr;
import ql.sql.ast.Variant;
using ql.sql.ast.Exprs;
import haxe.ds.Option;
using pm.Options;
import pm.Error;
import pm.map.Dictionary;

using pmdb.ql.ts.TypeChecks;
import pm.Helpers.*;

// @:tink
class Interp {
    public var parameters:Dictionary<Ref>;
    public var functions:Dictionary<pm.async.DynamicFunction>;
    public var locals:Map<String, Ref>;
    public var declared:Array<Decl>;
    public var depth:Int = 0;
    public var returnValue:Null<Dynamic> = null;
    public var binops:Map<String, Expr->Expr->Dynamic>;

    public function new() {
        _init_();
    }

    private function _init_() {
		parameters = new Dictionary();
		functions = new Dictionary();
		locals = new Map();
		declared = new Array();
		depth = 0;
		binops = new Map();

		initInterp();
    }

    private function initInterp() {
        function todo0() {
            throw new pm.Error('TODO');
        }

        inline function putf(n:String, f:DynamicFunction) {
            functions[n] = f;
        }

        function d_abs(n: Dynamic):Dynamic {
            if (n.is_number()) {
                if (n == Math.POSITIVE_INFINITY || n == Math.NEGATIVE_INFINITY || n == Math.NaN) return n;
                if (n.is_integer()) return pm.Numbers.Ints.abs(cast(n, Int));
            
                return Math.abs(cast(n, Float));
            }

            if (Int64.is(n)) {
                var i:Int64 = n;
                return i.isNeg() ? -i : i;
            }

            if (BigInt.is(n)) {
                var big:BigInt = n;
                return big.abs();
            }

            throw new pm.Error('Invalid argument $n');
        }

        putf('abs', d_abs);

        binops['=='] = function(l:Expr, r:Expr):Dynamic {
            var lv = expr(l), rv = expr(r);
            // Console.examine(lv, rv);
            return (pmdb.core.Arch.areThingsEqual(lv, rv));
        };


        function wrapBinFn(f:(Dynamic,Dynamic)->Dynamic):Expr->Expr->Dynamic {
            return function(l:Expr, r:Expr):Dynamic {
                return f(expr(l), expr(r));
            }
        }

        var ops = InterpOperators;
        inline function binop(s, f) {
            binops[s] = wrapBinFn(f);
        }
        binop('+', ops.add);
        binop('-', ops.subt);
        binop('*', ops.mult);
        binop('/', ops.div);
        binop('//', ops.idiv);
        binop('%', ops.mod);
        binop('in', ops.isin);

        binop('||', ops.boolean_or);

        binop('>', ops.cmp_eq);

        var pkg = InterpFunctions;
        functions['min'] = pkg.ql_min;
		functions['max'] = pkg.ql_max;
        functions['len'] = pkg.ql_len;
    }

    public function defineParam(sym:String, v:Dynamic):Ref {
        if ((v is IRef<Dynamic>)) {
            parameters[sym] = cast cast(v, IRef<Dynamic>);
        }
        else {
            parameters[sym] = new Ref(v);
        }
        return parameters[sym];
    }

    public function resolveRef(id):Option<Ref> {
        if (locals.exists(id)) return Some(locals.get(id));
        if (parameters.exists(id)) {return Some(parameters[id]);}
        // if (functions.exists(id)) return Some()
        return None;
    }

    function resolveFunctionInner(id):Option<DynamicFunction> {
        if (functions.exists(id)) return Some(functions.get(id));
        if (locals.exists(id)) {
            var l = locals.get(id).get();
            if (Reflect.isFunction(l)) {
                var f:DynamicFunction = l;
                return Some(f);
            }
        }
        return None;
    }

    public function resolveFunction(id):DynamicFunction {
        return switch resolveFunctionInner(id) {
            case Some(f): f;
            case None:
                throw new pm.Error('Unresolved identifier $id');
        }
    }

    public function expr(e: Expr):Dynamic {
        switch e.data {
            case EId(id):
                switch resolveRef(id) {
                    case Some(r): 
                        return r.get();
                    case None:
                        throw new pm.Error('Unresolved identifier $id');
                }
            
            // cast($e, $type)
            // ($e : $type)
            case ECast(e, type, safe):
                throw new pm.Error('TODO');
            
            // $o.$f
            case EField(eo, field):
				var o:Dynamic = expr(eo);
                return get(o, field);

            // $ef($a{eargs})
            case ECall(ef, eargs):
                var f:DynamicFunction;
                f = function(args:Arguments) throw 'newp';

                switch ef.data {
                    case EId(id):
                        switch resolveFunctionInner(id) {
                            case Some(func):
                                f = func;
                            
                            case None:
                                throw new pm.Error('Unresolved identifier $id');
                        }

                    default:
                        throw new pm.Error('Expected identifier');
                }

                var args:Array<Dynamic> = eargs.map(e -> expr(e));

                return f._call(args);
            
            case EUnOp(op, e, postfix):
				// throw new pm.Error('TODO');
                switch op {
                    case Not:
                        return !Arch.isTruthy(expr(e));

                    case IsNull:
						return !nnSlow(expr(e));

                    case Neg:
                        throw new pm.Error('TODO: @:operator(-A)');
                }
            
            case EBinOp(op, a, b):
				var key:String = op.print();
                var opFn = binops[key];
                if (opFn == null) throw new pm.Error('BinOp.$op not yet implemented');

                Console.examine(op, a.data, b.data);
                Console.examine(Type.typeof(a), Type.typeof(b));
                
                // return opFn(expr(a), expr(b));
                return opFn(a, b);

            // if ($econd) $eif
            // if ($econd) $eif else $eelse
            case EIf(econd, eif, eelse):
				return if (Arch.isTruthy(expr(econd))) expr(eif) else if (eelse == null) null else expr(eelse);
            
            case EValue(value, type):
                return e.data.getValue();

            case EParent(e):
                return expr(e);

            case other:
                throw new pm.Error('Unhandled ${other}');
        }
    }

    function get(o:Dynamic, field:String):Dynamic {
        return Reflect.getProperty(o, field);
    }

    function set(o:Dynamic, field:String, value:Dynamic) {
        Reflect.setProperty(o, field, value);
        return value;
    }

    function restore(old: Int) {
        while (declared.length > old) {
            var d = declared.pop();
            locals.set(d.n, d.old);
        }
    }
}

interface IRef<T> {
    function get():T;
    function set(v: T):Void;
    final hashCode: UInt;
}

class TRef<T> implements IRef<T> {
    var r:T;
    public final hashCode:UInt;

    public function new(?init: T) {
        this.r = init;
        this.hashCode = nextHashKey();
    }

    public inline function get():T return this.r;
    public inline function set(v: T):Void {
        this.r = v;
    }

    private static var _hashCounter:UInt = (1 : UInt);
    public static inline function nextHashKey():UInt {
        return _hashCounter++;
    }
}

class OFRef {
    public final hashCode:UInt;
    public var isProperty:Bool;
    public var name:String;
    public var o:R<Dynamic>;

    public function new(o:R<Dynamic>, n, prop=false) {
        this.o = o;
        this.name = n;
        this.isProperty = prop;
        this.hashCode = TRef.nextHashKey();
    }

    public function get():Dynamic {
        var o:Dynamic = this.o.get();
        if (o == null) throw new pm.Error('Cannot get field "$name" of null');
        
        var ret:Dynamic = if (isProperty) Reflect.getProperty(o, name) else Reflect.field(o, name);
        return ret;
    }

    public function set(v: Dynamic):Void {
        var o:Dynamic = this.o.get();
		if (o == null)
			throw new pm.Error('Cannot set field "$name" of null');
        try {
            if (isProperty) 
                Reflect.setProperty(o, name, v);
            else
                Reflect.setField(o, name, v);
        }
        catch (e: Dynamic) {
            var exception = pm.Error.withData(e, 'assignment of field "$name" to $o failed');
            throw exception;
        }
        Console.success('assigned reference');
    }
}

class TDecl<T> {
    public var n:String;
    public var old:TRef<T>;

    public function new(n, old) {
        this.n = n;
        this.old = old;
    }
}

typedef Ref = TRef<Dynamic>;
typedef Decl = TDecl<Dynamic>;

private enum ER<T> {
    R_Const(v: T);
    R_Ref(r: TRef<T>);
}

abstract R<T> (ER<T>) from ER<T> to ER<T> {
    @:from static function ofRef<T>(r: TRef<T>):R<T> return R_Ref(r);
    @:from static function ofConst<T>(v: T):R<T> return R_Const(v);
    
    @:to
    public function get():T {
        return switch this {
            case R_Const(v): v;
            case R_Ref(r): r.get();
        }
    }

    public function set(v: T):T {
        switch this {
            case R_Ref(r):
                r.set(v);
                return v;
            case e:
                return v;
        }
    }
}