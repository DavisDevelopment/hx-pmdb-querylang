package ql.sql.ast;

import haxe.ds.Option;
using pm.Options;

import haxe.io.Bytes;

using ql.sql.ast.Exprs;

@:notNull
@:forward
abstract Expr(ExprInst) from ExprInst {
	private inline function new(e:ExprInst) {
		this = e;
	}

	public var _(get, never):ExprInst;
	@:to private inline function get__():ExprInst return this;

	public var meta(get, never):Metadata;
	private inline function get_meta():Metadata	return this.metadata;

	public var data(get, never):ExprData;
	@:to private inline function get_data()	return this.expr;

	@:from public static function ofInst(o: ExprInst):Expr {return new Expr(o);}
	@:from public static function ofData(d: ExprData):Expr {return new ExprInst(d);}

// {region relations

	@:op(a == b) public inline static function eq(a:Expr, b:Expr):Condition {
		return EBinOp(Equals, a, b);
	}

	@:op(a != b) public inline static function neq<T>(a:Expr, b:Expr):Condition {
		return not(a == b);
	}

	@:op(a > b)
	public inline static function gt(a:Expr, b:Expr):Condition {
		return EBinOp(Greater, a, b);
	}

	@:op(a < b)
	public inline static function lt(a:Expr, b:Expr):Condition {
		return EBinOp(Greater, b, a);
	}

	@:op(a >= b)
	public inline static function gte(a:Expr, b:Expr):Condition {
		return not(EBinOp(Greater, b, a));
	}

	@:op(a <= b)
	public inline static function lte(a:Expr, b:Expr):Condition {
		return not(EBinOp(Greater, a, b));
	}

// } endregion

// { region logic

	@:op(!a) 
	public inline static function not(c: Expr):Condition {
		return EUnOp(Not, c, false);
	}

	@:op(a && b) 
	public inline static function and(a:Expr, b:Expr):Condition {
		return switch [a.data, b.data] {
			case [null, _]: b;
			case [_, null]: a;
			default: EBinOp(And, a, b);
		}
	}

	@:op(a || b) 
	public inline static function or(a:Expr, b:Expr):Condition {
		return EBinOp(Or, a, b);
	}

	@:op(a || b) 
	public inline static function constOr(a:Bool, b:Expr):Condition {
		return EBinOp(Or, EValue(a, VBool), b);
	}

	@:op(a || b) 
	public inline static function orConst(a:Expr, b:Bool):Condition {
		return EBinOp(Or, a, EValue(b, VBool));
	}

// } endregion

//{region arithmetic_ops

	@:op(a + b) public static function add(a:Expr, b:Expr):Expr return EBinOp(BinOp.Add, a, b);
	@:op(a - b) public static function subtract(a:Expr, b:Expr):Expr return EBinOp(BinOp.Subt, a, b);

	@:op(a+b) @:commutative static inline function addConstInt(e:Expr, i:Int):Expr return add(e, i);
	@:op(a + b) @:commutative static inline function addConstFloat(e:Expr, i:Float):Expr return add(e, i);
	@:op(a + b) @:commutative static inline function addConstString(e:Expr, i:String):Expr return add(e, i);

	@:extern
	public static inline function addConst<I>(a:Expr, b:I):Expr return add(a, ofAny(b));

//} endregion

// { region stuff

	public function toString():String {
		return data.print();
	}

	public function isNull<T>():Condition
		return EUnOp(IsNull, this, true);

	public inline function castTo<T>(type:ValueType<T>, safe:Bool=true):ExprOf<T> {
		return (ofData(ECast(this, type, safe)) : ExprOf<T>);
	}

	public function toBoolExpr():ExprOf<Bool> {
		return castTo(ValueType.VBool);
	}
	public function toIntExpr():ExprOf<Int> return castTo(ValueType.VInt);
	public function toFloatExpr():ExprOf<Float> return castTo(ValueType.VFloat);
	public function toStringExpr():ExprOf<String> return castTo(ValueType.VString);
	public function toBytesExpr():ExprOf<Bytes> return castTo(ValueType.VBytes);
	public function toDateExpr():ExprOf<Date> return castTo(ValueType.VDate);

	public inline function push(tail: Expr):Expr {
		this = ejoin(this, tail);
		return this;
	}

	public function call(args: Array<Expr>):Expr {
		return ecall(this, args);
	}

	public macro function fcall(f:haxe.macro.Expr.ExprOf<Expr>, args:Array<haxe.macro.Expr>) {
		var eargs = macro $a{args};
		return macro $f.call($eargs);
	}

	public static macro function make(e: haxe.macro.Expr) {
		var expr:Expr = e.convert();
		// trace(expr);
		return ExprMacros.getHaxeExprFor(expr);
	}

	// @:op(a in b) // https://github.com/HaxeFoundation/haxe/issues/6224
	public function inArray<T>(b : Expr):Condition
		return EBinOp(In, this, b);

	public function like(b:Expr):Condition
		return EBinOp(Like, (this : Expr), b);

	@:from static function ofArray<T:Int>(v: Array<T>):ExprOf<Array<T>>
		return EValue(v, VArray(cast VInt));

	@:from static function ofFloatArray<T:Float>(v:Array<T>):ExprOf<Array<T>>
		return EValue(v, VArray(cast VFloat));

	@:from static function ofStringArray<T:String>(v:Array<T>):ExprOf<Array<T>>
		return EValue(v, VArray(cast VString));

	@:from static function ofBool<S:Bool>(b:S):Expr
		return cast EValue(b, cast VBool);

	@:from static function ofDate<S:Date>(s:S):Expr
		return EValue(s, cast VDate);

	@:from static function ofString<S:String>(s:S):Expr
		return EValue(s, cast VString);

	@:from static function ofInt(s:Int):Expr
		return EValue(s, VInt);

	@:from static function ofFloat(s:Float):Expr
		return EValue(s, VFloat);

	@:from static function ofBytes(b:Bytes):Expr
		return EValue(b, VBytes);

	@:from static function ofAny(v: Any):Expr {
		return ExprValues.toEValue(v);
	}

	// } endregion

//{region ctors

	@:noUsing public static function ident(n: String):Expr return EId(n);
	@:noUsing public static function parent(e: Expr):Expr return EParent(e);
	@:noUsing public static function efield(e:Expr, field:String):Expr {
		return EField(e, field);
	}
	public static function ecall(f:Expr, args:Array<Expr>):Expr return ECall(f, args);
	public static function eif(condition:Expr, eif:Expr, ?eelse:Expr):Expr return EIf(condition, eif, eelse);
	@:from public static function eblock(e: Array<Expr>):Expr return EBlock(e);
	public static function evars(vars: Array<Var>):Expr return EVars(vars);

	public static function ethrow(errorExpr:Expr):Expr return EThrow(errorExpr);

	public static function ejoin(l:Expr, r:Expr):Expr {
		switch [l.data, r.data] {
			case [EBlock(l), EBlock(r)]: 
				return EBlock(l.concat(r));

			case [EVars(l), EVars(r)]:
				return EVars(l.concat(r));

			// case [EBlock(block), expr]:
				// return EBlock(block.withAppend(expr));

			case [_, _]:
				var blk:Array<Expr> = [];
				blockAppend(blk, l);
				blockAppend(blk, r);
				return eblock(blk);
		}
	}

	static function blockAppend(block:Array<Expr>, e:Expr) {
		while (true) {
			if (e == null) throw new pm.Error('expr cannot be null');
			switch e.data {
				case EParent(pe)|EBlock([pe]):
					e = pe;
					continue;

				case EBlock(subblock):
					for (e in subblock)
						blockAppend(block, e);
					return ;


				default:
					block.push(e);
					return ;
			}
		}
	}

//}endregion

	public static var g:ExprGlobal = (ofData(EValue(0, ValueType.VInt)) : ExprGlobal);
}

class ExprInst {
	public final expr:ExprData;
	public var metadata(default, null):Metadata = null;

	public function new(e:ExprData, ?meta:Map<String, Dynamic>) {
		this.expr = e;
		assert(this.expr != null);
		if (meta != null) {
			this.metadata = new Metadata();
		}
	}

	public function withExpr(e: ExprData):ExprInst {
		return new ExprInst(e, if (metadata == null) null else metadata);
	}

	public function equals(e: ExprInst):Bool {
		return expr.equals(e.expr);
	}
}

typedef ExprOf<T> = Expr;

typedef Condition = ExprOf<Bool>;

@:using(ql.sql.ast.Exprs.ExprEnums)
enum ExprData {
	EId(id:String);
	EValue<T>(value:T, type:ValueType<T>);
	EParent(e: Expr);
	ECast<T>(e:Expr, type:ValueType<T>, safe:Bool);
	EField(o:Expr, f:String);
	ECall(f:Expr, args:Array<Expr>);
	EUnOp(op:UnOp, a:Expr, postfix:Bool);
	EBinOp(op:BinOp, a:Expr, b:Expr);

	// EQuery<T, Db, Result>(query : Query<Db, Result>) : ExprData;

//{region structures
	
	EBreak;
	EContinue;
	EBlock(e: Array<Expr>);
	EThrow(e: Expr);
	EVars(vars : Array<Var>);
	EFunction(kind:Null<FunctionKind>, f:Function);
	EIf(econd:Expr, eif:Expr, eelse:Null<Expr>);

//}endregion
}

//{region internal_types

/**
	Represents a function in the AST.
**/
typedef Function = {
	/**
		A list of function arguments.
	**/
	var args:Array<FunctionArg>;

	/**
		The return type-hint of the function, if available.
	**/
	var ret:Null<ValueType<Dynamic>>;

	/**
		The expression of the function body, if available.
	**/
	var expr:Null<Expr>;
}

enum FunctionKind {
	FAnonymous;
	FNamed(name:String, ?inlined:Bool);
	FArrow;
}

/**
	Represents a function argument in the AST.
**/
typedef FunctionArg = {
	/**
		The name of the function argument.
	**/
	var name:String;

	/**
		Whether or not the function argument is optional.
	**/
	var ?opt:Bool;

	/**
		The type-hint of the function argument, if available.
	**/
	var type:Null<ValueType<Dynamic>>;

	/**
		The optional value of the function argument, if available.
	**/
	var ?value:Null<Expr>;

	/**
		The metadata of the function argument.
	**/
	var ?meta:Metadata;
}

typedef Var = {
	var name:String;
	var type:Null<ValueType<Dynamic>>;
	var expr:Null<Expr>;
	var ?isFinal:Bool;
}

@:using(ql.sql.ast.Exprs)
enum ValueType<T> {
	VAny:ValueType<Any>;
	VString:ValueType<String>;
	VBool:ValueType<Bool>;
	VFloat:ValueType<Float>;
	VInt:ValueType<Int>;
	VArray<T>(type : ValueType<T>) : ValueType<Array<T>>;
	VBytes:ValueType<Bytes>;
	VDate:ValueType<Date>;
}

@:using(ql.sql.ast.Exprs.ExprBinops)
enum BinOp/*<A, B, Ret>*/ {
    Add;
    Subt;
    Mult;
    Mod;
    Div;
    Greater;
    // Lt;
    Equals;
    And;
    Or;
    Like;
    In;
}

@:using(ql.sql.ast.Exprs.ExprUnops)
enum UnOp/*<A, Ret>*/ {
	Not;//:UnOp<Bool, Bool>;
	IsNull;//<T>:UnOp<T, Bool>;
	Neg;//<T:Float>:UnOp<T, T>;
}

//}endregion

class ArrayItr<T> implements Itr<T> {
	public var array:Array<T>;
	public var index:Int;

	// public var delta: Int;

	public function new(a, i:Int = 0 /*, d*/) {
		this.array = a;
		this.index = i;
		// this.delta = d;
	}

	public inline function hasNext():Bool {
		return array != null && (array.length - index) > 0;
	}

	public inline function next():T {
		return array[index++];
	}

	public inline function remove() {
		array.splice(index - 1, 1);
	}

	public inline function reset() {
		index = 0;
		return this;
	}
}

//{region metatypes
/**
 # Metadata.hx
 */
class MetaEntry {
	public final name:String;
	public var value:Null<Dynamic> = null;
	public var params:Null<Array<{expr:Null<Expr>, value:Null<Dynamic>}>> = null;
	public var expr:Null<Expr> = null;

	public function new(name:String, ?params) {
		this.name = name;
		this.params = params;
	}

	public function valueEquals(v:Dynamic):Bool
		return pm.Arch.areThingsEqual(value, v);

	public function exprEquals(e:Expr):Bool {
		return (expr._ == e._
			|| (expr != null && (expr.data == e.data || expr.data.equals(e.data)))
			|| (expr != null && pm.Arch.areThingsEqual(expr.data, e.data)));
	}

	public function pull(entry:MetaEntry) {
		value = entry.value;
		params = entry.params != null ? entry.params.map(x -> {expr: x.expr, value: x.value}) : null;
		expr = entry.expr;
	}
}

@:yield
abstract Metadata(Array<MetaEntry>) from Array<MetaEntry> to Array<MetaEntry> {
	public function new() {
		this = new Array();
	}

	public function insert(entry:MetaEntry, overwrite:Bool = false):MetaEntry {
		if (overwrite) {
			var i = this.length;
			while (i-- > 0) {
				if (this[i].name == entry.name) {
					this[i].pull(entry);
					return this[i];
				}
			}
		}
		this.push(entry);
		return entry;
	}

	function find(predicate:MetaEntry->Bool, reverse:Bool = true):Null<MetaEntry> {
		final backward = reverse;
		if (!backward) {
			var i = 0;
			while (i < this.length) {
				if (predicate(this[i]))
					return this[i];
				i++;
			}
		} else {
			var i = this.length;
			while (i-- > 0) {
				if (predicate(this[i]))
					return this[i];
			}
		}
		return null;
	}

	public function add(name:String, ?expr:Expr):MetaEntry {
		var entry = new MetaEntry(name);

		if (expr == null)
			entry.value = true;
		else
			entry.expr = expr;

		return insert(entry);
	}

	public function fromStringMap(map:haxe.ds.StringMap<Dynamic>) {
		for (key => value in map) {
			var val:Option<Dynamic> = None;
			var e:Option<Expr> = None;
			if ((value is ExprInst)) {
				var e2:Expr = (value : ExprInst);
				e = Some(e2);
			} else if ((value is ExprData)) {
				e = Some(cast(value, ExprData));
			}

			switch e {
				case Some({data: EValue(v, _)}):
					val = Some(v);
				case None:
					val = Some(value);
				case Some(expr):
					add(key, expr);
					continue;
			}

			var entry = add(key);
			entry.value = switch val {
				case None: true;
				case Some(v): v;
			};
			continue;
		}
		return this;
	}

	public function toStringMap(map:haxe.ds.StringMap<Dynamic>) {
		for (entry in entries()) {
			map.set(entry.name, entry.value);
		}
	}

	@:to
	public function asStringMap():Map<String, Dynamic> {
		var m:Map<String, Dynamic> = new Map();
		toStringMap(m);
		return m;
	}

	public function entries():ArrayItr<MetaEntry> {
		return new ArrayItr(this);
	}

	public function cull(predicate:MetaEntry->Bool) {
		var it = entries();
		while (it.hasNext()) {
			var entry = it.next();
			if (!predicate(entry)) {
				it.remove();
			}
		}
	}

	public function removeEntriesNamed(name:String) {
		return cull(e -> e.name == name);
	}

	public function getNamed(name:String):OneOrMany<MetaEntry> {
		var nodes = named(name);
		return OneOrMany.many(nodes);
	}

	public function extract(name:String):Dynamic {
		var node = find(e -> e.name == name);
		return switch node {
			case null: null;
			case {value: null, expr: null}: null;
			case {expr: {data: ExprData.EValue(v, _)}}: cast v;
			case {value: v}: v;
			default: throw new pm.Error.WTFError();
		}
	}

	public function removeEntry(e:MetaEntry):Bool {
		return this.remove(e);
		// named()
	}

	public function named(name:String) {
		return this.filter(e -> e.name == name);
	}
}
//}endregion

abstract ExprGlobal (Expr) from Expr {
	@:op(a.b)
	public function ident(id: String):Expr {
		return Expr.ofData(EId(id));
	}
}