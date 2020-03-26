package ql.sql.runtime;

import ql.sql.runtime.DType.DTypes;
import pmdb.core.Object.Doc;
import pmdb.core.ValType;
import haxe.ds.ReadOnlyArray;
import haxe.Constraints.Function;

import pm.Helpers.nor;

class FOverload {
	public final signature:haxe.ds.ReadOnlyArray<DType>;
	public final returnType:DType;
	public var f:Null<haxe.Constraints.Function> = null;

	public function new(signature:Array<DType>, ret:DType, ?fn:haxe.Constraints.Function) {
		this.signature = signature;
		this.returnType = ret;
		this.f = fn;
	}

	public function call(args:Array<Dynamic>):Dynamic {
		if (f == null) {
			throw new pm.Error('Cannot call function, no implementation defined');
		} else {
			try {
				var arity = pm.Functions.getNumberOfParameters(f);
				// Console.examine(arity);
				if (arity != args.length) {
					//
					#if (debug && !cpp)
					//Console.warn('Invalid number of arguments. Accepts $arity positional arguments, but ${args.length} were given');
					#end
				}
			} catch (e:String) {}

			return Reflect.callMethod(null, f, args);
		}
	}

	public function match(args:Array<Dynamic>):Bool {
		if (args.length != signature.length)
			return false;
		for (i in 0...args.length) {
			if (!signature[i].validateValue(args[i]))
				return false;
		}
		return true;
	}
}

interface Callable {
	public function call(args:Array<Dynamic>):Dynamic;
}

class NF implements Callable {
	public final f:Function;
	public final signature:ReadOnlyArray<DType>;
	public final returnType:SType;

	public function new(f, signature, returnType) {
		this.f = f;
		this.signature = signature;
		this.returnType = returnType;
	}

	public function call(args:Array<Dynamic>):Dynamic {
		return switch args.length {
			case 0: f();
			case 1: f(args[0]);
			case 2: f(args[0], args[1]);
			default:
				Reflect.callMethod(null, f, args);
		}
	}

	public static function declare(sign:String, ?ret:String, f:Function):NF {
		var stringRet = sign.afterLast('->').trim() + nor(ret, '');
		var stringTypes = ~/\s*,\s*/g.split(sign.beforeLast('->'));
		//Console.examine(stringRet, stringTypes);
		var types = [for (s in stringTypes) ValType.ofString(s.trim())];
		var ret = DTypes.fromDataType(ValType.ofString(stringRet)).toSType();
		var argTypes = types.map(v -> DTypes.fromDataType(v));

		return new NF(f, argTypes, ret);
	}
}

enum FArg {
    Normal(t: DType);
    Rest(t: DType);
}

class F implements Callable {
	public final overloads:Array<FOverload>;
	public var proxy:Null<haxe.Constraints.Function> = null;

	public function new(?o:Array<FOverload>, ?f:Function) {
		this.overloads = o.nor([]);
		this.proxy = f;
		// this.proxy = Reflect.makeVarArgs(this.call.bind(null, _));
	}

	public function add(ret:DType = DType.TUnknown, signature:Array<DType>, f:Function):F {
		overloads.push(new FOverload(signature, ret, f));
		return this;
	}

	public function match(args:Array<Dynamic>):Null<FOverload> {
		for (o in overloads) {
			if (o.match(args)) {
				return o;
			}
		}
		return null;

		var candidates = overloads.copy();

		for (argIdx in 0...args.length) {
			var arg = args[argIdx];

			var fnIdx = 0;
			while (fnIdx < candidates.length) {
				var i = fnIdx;
				var f = candidates[fnIdx++];
				var candidacy = true;

				if (argIdx >= f.signature.length) {
					candidacy = false;
				} else if (f.signature[argIdx].validateValue(arg)) {
					// Console.examine(f.signature[argIdx], arg, candidates);
				} else {
					candidacy = false;
				}

				if (!candidacy) {
					candidates.remove(f);

					if (candidates.length == 0)
						break;
					else
						continue;
				}
			}

			if (candidates.length == 0)
				return null;
		}

		for (c in candidates)
			assert(c.match(args));

		return candidates[0];
	}

	public function call(args:Array<Dynamic>):Dynamic {
		// if (overloadIdx != null && overloadIdx >= 0 && overloadIdx < overloads.length)
		// 	return overloads[overloadIdx].call(args);

		if (proxy != null)
			return Reflect.callMethod(null, proxy, args);

		return switch match(args) {
			case null:
				throw new pm.Error('Invalid call, no overload matched (${args.join(',')})');

			case f: f.call(args);
		}
	}

	public static function declare(cfg:Doc):F {
		var kvi = cfg.keyValueIterator();
		var overloads = [];

		for (signatureString => method in kvi) {
			if ((method is F)) {
				method = Reflect.makeVarArgs(a -> (cast method : F).call(a));
			} else if (!Reflect.isFunction(method)) {
				throw new pm.Error('Invalid overload function');
			}

			var stringRet = signatureString.afterLast('->').trim();
			var stringTypes = ~/\s*,\s*/g.split(signatureString.beforeLast('->'));
			var types = [for (s in stringTypes) ValType.ofString(s.trim())];
			var ret = ValType.ofString(stringRet);
			// Console.examine(stringTypes, types);
			var types = types.map(v -> DTypes.fromDataType(v));
			// Console.examine(types);

			overloads.push(new FOverload(types, DTypes.fromDataType(ret), method));
		}

		return new F(overloads);
	}

	public static function native<Fun:Function>(f:Fun):F {
		var ret = new F([]);
		ret.proxy = cast f;
		return ret;
	}
}