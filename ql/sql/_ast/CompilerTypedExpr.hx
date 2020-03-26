package ql.sql.ast;

import haxe.ds.Option;
using pm.Options;

import haxe.io.Bytes;

#if !undefined
    #error
#end

@:notNull
abstract Expr<T> (ExprInst<T>) from ExprInst<T> {
	inline function new(e: ExprInst<T>) {
		this = e;
    }

    public var _(get, never):ExprInst<T>;
    @:to
    private inline function get__():ExprInst<T> return this;

    public var meta(get, never):Metadata;
    private inline function get_meta():Metadata return this.metadata;

    public inline function print():String {
        return data.print();
    }

    @:from
    public static function ofInst<T>(o: ExprInst<T>):Expr<T> {
        return new Expr(o);
    }

	@:from 
    public static function ofData<T>(d: ExprData<T>):Expr<T> {
		return new ExprInst(d);
    }

	public var data(get, never):ExprData<T>;
	@:to private inline function get_data() return this.expr;

    //{region relations
	@:op(a == b) 
    public inline static function eq<T>(a:Expr<T>, b:Expr<T>):Condition {
		return EBinOp(Equals, a, b);
    }

	@:op(a != b) 
    public inline static function neq<T>(a:Expr<T>, b:Expr<T>):Condition {
		return not(a == b);
    }

	@:op(a > b) 
    public inline static function gt<T:Float>(a:Expr<T>, b:Expr<T>):Condition {
		return EBinOp(Greater, a, b);
    }

	@:op(a < b) 
    public inline static function lt<T:Float>(a:Expr<T>, b:Expr<T>):Condition {
		return EBinOp(Greater, b, a);
    }

	@:op(a >= b) 
    public inline static function gte<T:Float>(a:Expr<T>, b:Expr<T>):Condition {
		return not(EBinOp(Greater, b, a));
    }

	@:op(a <= b) 
    public inline static function lte<T:Float>(a:Expr<T>, b:Expr<T>):Condition {
		return not(EBinOp(Greater, a, b));
    }

	@:op(a > b) 
    public inline static function gtDate(a:Expr<Date>, b:Expr<Date>):Condition {
		return EBinOp(Greater, a, b);
    }

	@:op(a < b) 
    public inline static function ltDate(a:Expr<Date>, b:Expr<Date>):Condition {
		return EBinOp(Greater, b, a);
    }

	@:op(a >= b) 
    public inline static function gteDate(a:Expr<Date>, b:Expr<Date>):Condition {
		return not(EBinOp(Greater, b, a));
    }

	@:op(a <= b) 
    public inline static function lteDate(a:Expr<Date>, b:Expr<Date>):Condition {
		return not(EBinOp(Greater, a, b));
    }
    //} endregion

	// { region logic
	@:op(!a) 
    public inline static function not(c:Condition):Condition {
		return EUnOp(Not, c, false);
    }

	@:op(a && b) 
    public inline static function and(a:Condition, b:Condition):Condition {
		return switch [a.data, b.data] {
			case [null, _]: b;
			case [_, null]: a;
			default: EBinOp(And, a, b);
		}
    }

	@:op(a || b) 
    public inline static function or(a:Condition, b:Condition):Condition {
		return EBinOp(Or, a, b);
    }

	@:op(a || b) 
    public inline static function constOr(a:Bool, b:Condition):Condition {
		return EBinOp(Or, EValue(a, VBool), b);
    }

	@:op(a || b) 
    public inline static function orConst(a:Condition, b:Bool):Condition {
		return EBinOp(Or, a, EValue(b, VBool));
    }

	// } endregion

    //{ region stuff
	public function isNull<T>():Condition return EUnOp(IsNull, this, true);
	// @:op(a in b) // https://github.com/HaxeFoundation/haxe/issues/6224
	public function inArray<T>(b:Expr<Array<T>>):Condition	return EBinOp(In, this, b);
	public function like(b:Expr<String>):Condition	return EBinOp(Like, (this : Expr<String>), b);
	// @:from static function ofIdArray<T>(v:Array<Id<T>>):Expr<Array<Id<T>>> return EValue(v, cast VArray(VInt));
	@:from static function ofIntArray<T:Int>(v:Array<T>):Expr<Array<T>> return EValue(v, VArray(cast VInt));
	@:from static function ofFloatArray<T:Float>(v:Array<T>):Expr<Array<T>> return EValue(v, VArray(cast VFloat));
	@:from static function ofStringArray<T:String>(v:Array<T>):Expr<Array<T>> return EValue(v, VArray(cast VString));
	@:from static function ofBool<S:Bool>(b:S):Expr<S> return cast EValue(b, cast VBool);
	@:from static function ofDate<S:Date>(s:S):Expr<S> return EValue(s, cast VDate);
	@:from static function ofString<S:String>(s:S):Expr<S> return EValue(s, cast VString);
	@:from static function ofInt(s:Int):Expr<Int> return EValue(s, VInt);
	@:from static function ofFloat(s:Float):Expr<Float>	return EValue(s, VFloat);
	@:from static function ofBytes(b:Bytes):Expr<Bytes> return EValue(b, VBytes);
    //} endregion
}

class ExprInst<T> {
    public final expr : ExprData<T>;
    public var metadata(default, null): Metadata = null;

    public function new(e:ExprData<T>, ?meta:Map<String, Dynamic>) {
        this.expr = e;
        if (meta != null) {
            this.metadata = new Metadata();

        }
    }
    public function withExpr(e: ExprData<T>):ExprInst<T> {
        return new ExprInst(e, if (metadata == null) null else metadata);
    }
}

typedef Condition = Expr<Bool>;

@:using(ql.sql.ast.Exprs.ExprEnums)
enum ExprData<T> {
	EUnOp<A, Ret>(op : UnOp<A, Ret>, a : Expr<A>, postfix : Bool) : ExprData<Ret>;
	EBinOp<A, B, Ret>(op : BinOp<A, B, Ret>, a : Expr<A>, b : Expr<B>) : ExprData<Ret>;
    EId(id:String):ExprData<T>;
    EField(o:ExprData<Any>, f:String):ExprData<T>;
    ECall(f:Expr<Any>, args:Array<Expr<Any>>):ExprData<T>;
	EValue<T>(value : T, type : ValueType<T>) : ExprData<T>;
	// EQuery<T, Db, Result>(query : Query<Db, Result>) : ExprData<T>;
}

@:using(ql.sql.ast.Exprs)
enum ValueType<T> {
	VString:ValueType<String>;
	VBool:ValueType<Bool>;
	VFloat:ValueType<Float>;
	VInt:ValueType<Int>;
	VArray<T>(type:ValueType<T>):ValueType<Array<T>>;
	VBytes:ValueType<Bytes>;
	VDate:ValueType<Date>;
	// VGeometry<T>(type : geojson.GeometryType<T>) : ValueType<T>;
}

@:using(ql.sql.ast.Exprs.ExprBinops)
enum BinOp<A, B, Ret> {
	Add<T:Float>:BinOp<T, T, T>;
	Subt<T:Float>:BinOp<T, T, T>;
	Mult<T:Float>:BinOp<T, T, T>;
	Mod<T:Float>:BinOp<T, T, T>;
	Div<T:Float>:BinOp<T, T, Float>;

	Greater<T>:BinOp<T, T, Bool>;
	Equals<T>:BinOp<T, T, Bool>;
	And:BinOp<Bool, Bool, Bool>;
	Or:BinOp<Bool, Bool, Bool>;
	Like<T:String>:BinOp<T, T, Bool>;
	In<T>:BinOp<T, Array<T>, Bool>;
}

@:using(ql.sql.ast.Exprs.ExprUnops)
enum UnOp<A, Ret> {
	Not:UnOp<Bool, Bool>;
	IsNull<T>:UnOp<T, Bool>;
	Neg<T:Float>:UnOp<T, T>;
}

class MetaEntry {
    public final name: String;
    public var value:Null<Dynamic> = null;
    public var params:Null<Array<{expr:Null<Expr<Dynamic>>, value:Null<Dynamic>}>> = null;
    public var expr:Null<Expr<Dynamic>> = null;

    public function new(name:String, ?params) {
        this.name = name;
        this.params = params;
    }
    public function valueEquals(v: Dynamic):Bool return pm.Arch.areThingsEqual(value, v);
    public function exprEquals(e: Expr<Dynamic>):Bool {
        return (expr._ == e._ || (expr != null && (expr.data == e.data || expr.data.equals(e.data))) || (expr != null && pm.Arch.areThingsEqual(expr.data, e.data)));
    }
    public function pull(entry: MetaEntry) {
        value = entry.value;
        params = entry.params != null ? entry.params.map(x -> {expr:x.expr, value:x.value}) : null;
        expr = entry.expr;
    }
}

@:yield
abstract Metadata (Array<MetaEntry>) from Array<MetaEntry> to Array<MetaEntry> {
    public function new() {
        this = new Array();
    }

    public function insert(entry:MetaEntry, overwrite:Bool=false):MetaEntry {
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

    function find(predicate:MetaEntry->Bool, reverse:Bool=true):Null<MetaEntry> {
        final backward = reverse;
        if (!backward) {
            var i = 0;
            while (i < this.length) {
                if (predicate(this[i]))
                    return this[i];
                i++;
            }
        }
        else {
            var i = this.length;
            while (i-- > 0) {
                if (predicate(this[i]))
                    return this[i];
            }
        }
        return null;
    }

    public function add(name:String, ?expr:Expr<Dynamic>):MetaEntry {
        var entry = new MetaEntry(name);
        
        if (expr == null) entry.value = true;
        else entry.expr = expr;

        return insert(entry);
    }

    public function fromStringMap(map: haxe.ds.StringMap<Dynamic>) {
        for (key=>value in map) {
            var val:Option<Dynamic> = None;
            var e:Option<Expr<Dynamic>> = None;
            if ((value is ExprInst<Dynamic>)) {
                var e2:Expr<Dynamic> = (value : ExprInst<Dynamic>);
                e = Some(e2);
            }
            else if ((value is ExprData<Dynamic>)) {
                e = Some(cast(value, ExprData<Dynamic>));
            }

            switch e {
                case Some({data:EValue(v,_)}):
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

    public function toStringMap(map: haxe.ds.StringMap<Dynamic>) {
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

    public function removeEntriesNamed(name: String) {
        return cull(e -> e.name == name);
    }

    public function getNamed(name: String):OneOrMany<MetaEntry> {
        var nodes = named(name);
        return OneOrMany.many(nodes);
    }
    public function extract(name:String):Dynamic {
        var node = find(e->e.name==name);
        return switch node {
            case null: null;
            case {value:null, expr:null}: null;
            case {expr:{data:ExprData.EValue(v,_)}}: cast v;
            case {value: v}: v;
            default: throw new pm.Error.WTFError();
        }
    }

    public function removeEntry(e: MetaEntry):Bool {
        return this.remove(e);
        // named()
    }

    public function named(name: String) {
        return this.filter(e->e.name==name);
    }
}

class ArrayItr<T> implements Itr<T> {
    public var array:Array<T>;
    public var index: Int;
    // public var delta: Int;
    
    public function new(a, i:Int=0/*, d*/) {
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