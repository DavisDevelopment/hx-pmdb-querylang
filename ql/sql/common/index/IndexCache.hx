package ql.sql.common.index;

import pmdb.core.Object.Doc;
import ql.sql.common.index.IIndex;
import ql.sql.common.SqlSchema;

import pm.Helpers.nn;
import pm.map.Set;
import pm.map.OrderedSet;
import pm.map.AnySet;

using Lambda;
using pm.Functions;

class IndexCache<T> {
    public final m: Map<String, AIndex<T>>;
    public final schema: SqlSchema<T>;

    public function new(schema) {
        this.schema = schema;
        this.m = new Map();

        for (i in schema.indexing.indexes) {
            var idx = new AVLIndex({
                schema: schema,
                type: IndexType.Simple(i.label),
                getItemKey: cast i.extractor,
                keyType: i.keyType,
                itemEq: (a, b) -> a == b,
                keyCmp: (a:Dynamic, b:Dynamic) -> pmdb.core.Arch.compareThings(a, b),
                unique: i.unique,
                sparse: i.sparse
            });

            m[i.label] = idx;
        }
    }

    public inline function size():Int {
        return index(schema.primaryKey.name).size();
    }

    public function insert(item: T) {
        for (idx in m)
            idx.insertOne(item);
    }

    public function insertAll(items: Array<T>) {
        for (idx in m) { 
            idx.insertMany(items);
        }
    }

    public function update(oldItem:T, newItem:T) {
        for (idx in m)
            idx.updateOne(oldItem, newItem);
    }

	public function replace(oldItem:T, newItem:T) {
        for (idx in m)
            idx.updateOne(oldItem, newItem);
    }

    public function delete(item: T) {
        for (idx in m)
            idx.removeOne(item);
    }

    public inline function index(label: String):Null<AIndex<T>> {
        return m[label];
    }

    public var primary(get, never):AIndex<T>;
    inline function get_primary() return index(schema.primaryKey.name).unwrap();

    /**
     * create a new Set<?>
     * @param rows 
     * @return OrderedAnySet<Dynamic, T>
     */
    public function newRowSet(?rows: Iterable<T>):OrderedAnySet<Dynamic, T> {
        final pk = schema.primaryKey;
        final pkidx = schema.indexing.index(pk.name);
        final extractor = cast pkidx.extractor;
        final compare = switch pk.type {
            case TInt: (x:Dynamic, y:Dynamic) -> Reflect.compare(x, y);
            case TString: (x:String, y:String) -> Reflect.compare(x, y);
            case TDate: (x:DateTime, y:DateTime) -> 0;
            case TArray(type): (x:Dynamic, y:Dynamic) -> pmdb.core.Arch.compareArrays(x, y);
            case TStruct(schema):throw new pm.Error('TODO');
            default: (x:Dynamic, y:Dynamic) -> pmdb.core.Arch.compareThings(x, y);
        }
        return new OrderedAnySet<Dynamic, T>(extractor, compare, rows);
    }

    public inline function getAll():Array<T> {
        return primary.getAll();
    }

    public function where(?name:String, value:Dynamic):Array<T> {
        if (name == null)
            name = schema.primaryKey.name;
        var i = index(name);
        assert(nn(i), new pm.Error('No index labeled "$name"'));
        if ((value is ValueConstraint))
            return constrainedWhere(name, cast(value, ValueConstraint));
        return i.getByKey(value);
    }

    public function constrainedWhere(name:String, c:ValueConstraint):Array<T> {
        var i = index(name);
        assert(nn(i), new pm.Error('No index labeled "$name"'));
        return switch c {
            case Between(min, max): i.getBetweenBounds(min, max);
            case Lt(k): i.getBetweenBounds(k, null);
            case Gt(k): i.getBetweenBounds(null, k);
            case In(list): i.getByKeys(list);
        }
    }
}

private typedef AIndex<T> = IIndex<Dynamic, T>;

enum ValueConstraint {
    Between(min:KeyBoundary<Dynamic>, max:KeyBoundary<Dynamic>);
    Lt(k: KeyBoundary<Dynamic>);
    Gt(k: KeyBoundary<Dynamic>);
    In(list: Array<Dynamic>);
}