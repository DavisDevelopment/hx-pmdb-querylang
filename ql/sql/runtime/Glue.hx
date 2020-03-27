package ql.sql.runtime;

import ql.sql.Dummy;
import ql.sql.common.index.IndexCache;
import ql.sql.Dummy.TableHandle;
import pmdb.core.Store;
import ql.sql.Dummy.AbstractDummyDatabase;
import pmdb.core.Object;
import ql.sql.common.SqlSchema;

/**
	  method naming pattern:
	<domain><method name, capitalized>(<subject>, ...<args>)
	  example:
	   - dbDoSomething(db:Db)
	   - tblGetSchema(table:Table)
	   - rowGetColumnByName(row:Row, name:String)
**/
class Glue<TDb, Table, Row> {
	public var database:TDb;

	var table:Null<Table> = null;

	public function new() {}

	public function dbListTables(db:TDb):Array<String> {
		throw new pm.Error.ValueError(Glue.NotFound, 'error msg');
	}

	public function dbLoadTable(db:TDb, table:String):Table {
		throw new pm.Error.ValueError(Glue.NotFound, 'error msg');
	}

	public function tblGetAllRows(table:Table):Array<Row> {
		throw new pm.Error.ValueError(Glue.NotFound, 'error msg');
	}

	public function tblGetWhere(table:Table, column:String, value:Dynamic):Array<Row> {
		throw new pm.Error.ValueError(Glue.NotFound, 'error msg');
	}

	public function tblGetSchema(table:Table):SqlSchema<Row> {
		throw new pm.Error.ValueError(Glue.NotFound, 'error msg');
	}
	
	public function tblGetIndexCache(table: Table):IndexCache<Row> {
		throw new pm.Error.ValueError(Glue.NotFound, 'error msg');
	}
    
    public function tblUpdate(tbl:Table, oldRows:Array<Row>, newRows:Array<Row>):Void {
		throw new pm.Error.ValueError(Glue.NotFound, 'error msg');
    }

	public function rowGetColumnByName(row:Row, name:String):Dynamic {
		throw new pm.Error.ValueError(Glue.NotFound, 'error msg');
	}

	public function tblListColumns(table:Table):Array<SchemaField> {
		return tblGetSchema(table).fields.fields.copy();
	}

	public function valIsTable(v:Dynamic):Bool {
		throw new pm.Error.NotImplementedError();
	}

	public function valGetField(o:Dynamic, f:String):Dynamic
		return Reflect.field(o, f);

	public function valSetField(o:Dynamic, f:String, v:Dynamic):Void
		return Reflect.setField(o, f, v);

	public function valHasField(o:Dynamic, f:String):Bool
		return Reflect.hasField(o, f);

	public function valRemoveField(o:Dynamic, f:String):Bool
		return Reflect.deleteField(o, f);

	public function valFields(o:Dynamic):Array<String>
		return Reflect.fields(o);

	public function valKeyValueIterator(o:Dynamic):KeyValueIterator<String, Dynamic>
		return Doc.unsafe(o).keyValueIterator();

	public function valCopy(o:Dynamic):Dynamic
		return pmdb.core.Arch.clone(o);

	private static inline final NotFound = -999;
}

class PmdbGlue extends Glue<AbstractDummyDatabase<TableHandle<Dynamic>, Dynamic>, TableHandle<Dynamic>, Dynamic> {
    private var tableSchemas: Map<Int, SqlSchema<Dynamic>>;

	public function new(db) {
		super();
        this.database = db;
        this.tableSchemas = new Map();
	}

	@:extern
	private inline function tfocus(t) {
		return this.table = t;
	}

	override function dbListTables(db) {
		return database.tables.keyArray();
	}

	override function dbLoadTable(db, table) {
		return tfocus(db.table(table));
    }
    
    override function tblListColumns(table):Array<SchemaField> {
        return tblGetSchema(table).fields.array();
    }

    override function tblGetSchema(table: TableHandle<Dynamic>):SqlSchema<Dynamic> {
        final id:Int = table.h._id;
        if (tableSchemas.exists(id))
            return tableSchemas.get(id);
        throw new pm.Error('TODO: SqlSchema => StructSchema conversion');
    }

	override function tblGetAllRows(table):Array<Dynamic> {
		return table.getAllData();
	}

	override function rowGetColumnByName(row:Dynamic, name:String) {
        var row:Doc = Doc.unsafe(row);
		return row.get(name);
    }
    
    override function valIsTable(v:Dynamic):Bool {
        return (v is Store<Dynamic>);
    }
}

class DummyGlue<Row> extends Glue<ql.sql.Dummy.AbstractDummyDatabase<ql.sql.Dummy.DummyTable<Row>, Row>, ql.sql.Dummy.DummyTable<Row>, Row> {
	public function new(db) {
		super();
		this.database = db;
	}

	extern inline function doc(row:Row):Doc {
		return Doc.unsafe(row);
	}

	override function dbLoadTable(db:ql.sql.Dummy.AbstractDummyDatabase<ql.sql.Dummy.DummyTable<Row>, Row>, table:String) {
		return this.table = (db.nor(this.database).table(table));
	}

	override function dbListTables(db:AbstractDummyDatabase<DummyTable<Row>, Row>):Array<String> {
		return db.tables.keyArray();
	}

	override function rowGetColumnByName(row:Row, name:String):Dynamic {
		return doc(row).get(name);
	}

	override function tblGetAllRows(table:ql.sql.Dummy.DummyTable<Row>):Array<Row> {
		return table.getAllData();
	}

	override function tblGetSchema(table:ql.sql.Dummy.DummyTable<Row>):SqlSchema<Row> {
		return table.schema;
	}

	override function tblGetIndexCache(table:DummyTable<Row>):IndexCache<Row> {
		// return super.tblGetIndexCache(table);
		return table.cache;
	}

	override function tblUpdate(tbl:DummyTable<Row>, oldRows:Array<Row>, newRows:Array<Row>) {
		tbl.update(oldRows, newRows);
	}

	override function valIsTable(v:Dynamic):Bool {
		return (v is ql.sql.DummyTable<Dynamic>);
	}
}