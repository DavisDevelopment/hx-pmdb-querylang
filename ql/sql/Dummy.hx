package ql.sql;

import pmdb.core.FrozenStructSchema;
import pmdb.core.Object;
import pmdb.core.Arch;
import pm.map.Dictionary;
import haxe.Constraints.Constructible;
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
    public var name:String;
    public var data:Array<Row>;
    public var schema: SqlSchema<Row>;

    public function new(name, schema, ?data:Array<Row>) {
        this.name = name;
        this.schema = schema;
        this.data = data.nor([]);
        /**
          [TODO] sanitize `data` here
         **/
    }

    public function getAllData():Array<Row> {
        return this.data;
    }

    public function insert(row: Row) {
        row = schema.prepareForInsertion(row);
        this.data.push(row);
        return row;
    }
}

class DummyTable<Row> extends AbstractDummyTable<Row> {

}