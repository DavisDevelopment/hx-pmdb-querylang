package ql.sql.common.index;

import pmdb.core.ds.Itr;
import ql.sql.runtime.DType;
import ql.sql.runtime.SType;
import pmdb.ql.ast.BoundingValue;
// import pmdb.core.schema.Types.IndexType;
import pmdb.core.schema.Types.IndexAlgo;
import ql.sql.runtime.TAst;

import pm.Helpers.nor;

interface IIndex<Key, Item> {
    var sparse(default, null): Bool;
    var unique(default, null): Bool;
    var keyType(default, null): SType;
    var itemType(default, null): Null<SqlSchema<Item>>;
    var indexType(default, null): IndexType;
    var options(default, null): IndexOptions<Key, Item>;

    private function compareKeys(a:Key, b:Key):Int;
    private function compareItems(a:Item, b:Item):Int;
    private function getItemKey(item: Item):Key;
    private function itemsEq(a:Item, b:Item):Bool;

    function insertOne(item: Item):Void;
    function removeOne(item: Item):Bool;
    function updateOne(oldDoc:Item, newDoc:Item):Void;

    function insertMany(items: Array<Item>):Void;
    function removeMany(items: Array<Item>):Void;
    function updateMany(updates: Array<{oldDoc:Item, newDoc:Item}>):Void;

    function getByKey(key: Key):Null<Array<Item>>;
    function getByKeys(keys: Array<Key>):Array<Item>;
    function getBetweenBounds(?min:KeyBoundary<Key>, ?max:KeyBoundary<Key>):Array<Item>;
    function getAll():Array<Item>;

	// function itrByKey(key:Key):Itr<Item>;
	// function itrByKeys(keys:Array<Key>):Itr<Item>;
	// function itrBetweenBounds(?min:KeyBoundary<Key>, ?max:KeyBoundary<Key>):Itr<Item>;
	// function itrAll():Itr<Item>;
	function iterator():Iterator<Item>;
    function allKeys():Array<Key>;
    function keys():Iterator<Key>;
    function keyValueIterator():Iterator<IdxKvPair<Key, Item>>;

    function size():Int;
}

enum IndexType {
    Simple(path: String);
    Expression(expr: TExpr);
}

typedef IndexOptions<Key, Item> = {
    ?type: IndexType,
    //* ?fieldName: String,
    ?getItemKey: Item -> Key,
	?keyType: SType,
    // ?keyComparator: pmdb.core.Comparator<Key>,
    // ?itemEq: pmdb.core.Equator<Item>,
    ?itemEq: (x:Item, y:Item)->Bool,
    ?keyCmp: pm.Ord<Key>,
    ?unique: Bool,
    ?sparse: Bool
};

@:structInit
class IdxKvPair<K, V> {
    public final key: K;
    public final value: V;

    public function new(k:K, v:V) {
        key = k;
        value = v;
    }
}

class KeyBoundary<Key> {
    public final key: Key;
    public final inclusive: Bool;

    public inline function new(key:Key, inclusive) {
        this.key = key;
        this.inclusive = inclusive;
    }
}

class IndexRollback<Error> {
	public inline function new(e, i) {
		this.error = e;
		this.failingIndex = i;
	}

	public var error:Error;
	public var failingIndex:Int;
}

class IndexError<K, V> extends pm.Error {
    public var type(default, null): IndexErrorCode<K, V>;
    public function new(?type:IndexErrorCode<K, V>, ?msg, ?name, ?pos) {
        super(msg, name, pos);
        this.type = nor(type, cast AssertionFailed);
    }
}

enum IndexErrorCode<K, V> {
	AssertionFailed;
	UniqueConstraintViolated;
	MissingProperty(name:String);
	Custom(error: pm.Error);
}