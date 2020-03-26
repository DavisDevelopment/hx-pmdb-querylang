package ql.sql;

import ql.sql.common.index.IndexCache;
import ql.sql.common.index.IIndex;

import pmdb.core.Object;
import pmdb.core.Arch;
import pmdb.core.Store;
import pm.map.Dictionary;
import ql.sql.common.SqlSchema;

// @:generic(Table)
class AbstractDummyDatabase<Table, Row> {
    public var tables: pm.map.Dictionary<Table>;

    public function new(tables: KeyValueIterable<String, Table>) {
        var m = this.tables = new pm.map.Dictionary();
        for (k=>v in tables) {
            m.set(k, v);
        }
    }

    public inline function table(name: String):Table {
        return tables[name];
    }
}

class AbstractDummyTable<Row> {
    public var name: String;
    // public var data: Array<Row>;
    // public var row_keys: Array<Dynamic>;
    // public var row_map: pm.map.OrderedDictionary<Row>;
    public var schema: SqlSchema<Row>;
    public var cache: IndexCache<Row>;

    public function new(name, schema, ?data:Array<Row>) {
        this.name = name;
        this.schema = schema;
        // this.data = data.nor([]);
        // /**
        //   [TODO] sanitize `data` here
        //  **/
        // this.row_keys = pm.Arrays.alloc(this.data.length);
        // for (i in 0...this.data.length) {
        //     row_keys[i] = rowKey(this.data[i]);
        // }

        buildCache();
        if (data != null)
            for (x in data)
                insert(x);
    }

    function buildCache() {
        this.cache = new IndexCache(this.schema);
    }

    public function getAllData():Array<Row> {
        return cache.getAll();
    }

    public function insert(row: Row) {
        row = schema.prepareForInsertion(row);
        // var i = this.data.push(row) - 1;
        // this.row_keys[i] = rowKey(row);
        cache.insert(row);
        return row;
    }

    public function remove(row: Row):Bool {
        // final key = rowKey(row);
        // final i = row_keys.indexOf(key);
        // if (!i.strictEq(-1)) {
        //     row_keys.splice(i, 1);
        //     data.splice(i, 1);
        //     return true;
        // }
        // return false;
        cache.delete(row);
        return true;
    }

    public function replace(oldRow:Row, newRow:Row):Bool {
		// final key = rowKey(oldRow);
        // final i = row_keys.indexOf(key);
        // if (i != -1) {
        //     data[i] = newRow;
        //     return true;
        // }
        // return false;
        cache.replace(oldRow, newRow);
        return true;
    }

    public function update(oldRows:Array<Row>, newRows:Array<Row>) {
        assert(oldRows.length == newRows.length);
        var failureIndex = -1, error = null;
        for (i in 0...oldRows.length) {
            var r = inline replace(oldRows[i], newRows[i]);
            if (!r) {
                // throw new pm.Error('Update failed! TODO: Rollback');
                failureIndex = i;
                error = new pm.Error('Update failed! TODO: Rollback');
                break;
            }
        }

        if (failureIndex != -1 && error != null) {
            for (i in 0...failureIndex) {
                inline replace(newRows[i], oldRows[i]);
            }
            throw error;
        }

        return true;
    }

    public function getIndex(label: String) {
        return cache.m[label].unwrap(new pm.Error('Index("$label")', 'NotFound'));
    }
    public function getIndexes():Iterator<IIndex<Dynamic, Row>> {
        return cache.m.iterator();
    }

    extern inline function rowKey(row: Row):Dynamic {
        return Doc.unsafe(row).get(schema.primaryKey.name);
    }

    public var data(get, set):Array<Row>;
    inline function get_data() return getAllData();
    inline function set_data(a: Array<Row>) {
        buildCache();
        for (x in a)
            insert(x);
        return a;
    }
}

@:access(pmdb.core)
class TableHandle<Row> {
    public final h: Store<Row>;
    public final name: String;
    public final schema: SqlSchema<Row>;

    public function new(store, name, schema) {
        this.h = store;
        this.name = name;
        this.schema = schema;
    }

    public function getAllData():Array<Row> {
        return h.getAllData();
    }

    public function getWhere(a:Dynamic, ?b:Dynamic) {
        return h.get(a, b);
    }

    public function getSize():Int {
        return h.size();
    }

    public function insert(row: Row) {
        var status = h.insert(row);
        if (false)
            throw new pm.Error('INSERT failed');
    }
}

@:keep
class DummyTable<Row> extends AbstractDummyTable<Row> {

}