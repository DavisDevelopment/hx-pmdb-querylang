package ql.sql.runtime.sel;

import haxe.ds.ReadOnlyArray;
import pm.map.Dictionary;
import ql.sql.runtime.VirtualMachine;

using Lambda;

class SelectStmtContext<Db, Table, Row> {
// {region fields
    public var context: Context<Db, Table, Row>;
    private var sourceMap: Dictionary<TableSpec>;
    // public var _sources(): SelectSourceCollection;
    public var sources: SelectSourceCollection;
    public var inputRow: Null<Row> = null;
    public var outputRow: Null<Row> = null;
// }endregion

	public function new(ctx, sources) {
        this.context = ctx;
        // this._sources = new Array();
        this.sources = new SelectSourceCollection(sources);
        this.sourceMap = new Dictionary();
    }
    
    public function addSource(spec: TableSpec) {
        if (sourceMap.exists(spec.name))
            return ;

        context.addQuerySource(spec);
        // _sources.push(spec);
        sourceMap[spec.name] = spec;
    }

    public function popSource(spec: TableSpec):Bool {
        final key = spec.name;
        sourceMap.remove(key);
        context.removeQuerySource(spec);
        // return _sources.remove(_sources.find(x -> x.strictEq(spec)));
        return true;
    }
}