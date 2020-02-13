package ql.sql.runtime;

import pmdb.core.Object;
import pmdb.core.Object.Doc as JsonObject;
import pmdb.core.FrozenStructSchema as JsonSchema;
import ql.sql.common.SqlSchema;
import ql.sql.runtime.TAst;
import ql.sql.grammar.CommonTypes.SqlSymbol;
import ql.sql.TsAst.SelectStatement;
import ql.sql.runtime.VirtualMachine;

class SelectImpl {
    public var _stmt: Null<SelectStatement> = null;
    public var _predicate:Null<SelPredicate> = null;
    public var _exporter:Null<Exporter<Doc>> = null;

    public function new(?stmt:SelectStatement, ?p:SelPredicate, ?e:SelOutput) {
        if (stmt != null) _stmt = stmt;
        if (p != null) _predicate = p;
        if (e != null) _exporter = e;
    }

    public function apply(acc:Array<Dynamic>, row:Dynamic, sel:Sel<Dynamic, Dynamic, Dynamic>) {
        var c = sel.context;
        var map = _exporter != null ? _exporter.mCompiled : null;
        switch [_predicate, map] {
            case [null, null]:
                acc.push(row);

            case [null, _]:
                c.focus(row);
                acc.push(map(cast sel));

            case [p, null]:
                c.focus(row);
                if (p.eval(cast sel)) {
                    acc.push(row);
                }

            case [p, _]:
                c.focus(row);
                if (p.eval(cast sel)) {
                    acc.push(map(cast sel));
                }
        }
    }
}

@:allow(ql.sql.runtime.Sel.ISelDriver)
/**
 * boop
 */
class Sel<Table, Row, ResultRow> {
    private var _attachments:JsonObject;
    // public var driver:ISelDriver<Table, Row, Column>;
    public final source: Table;
    public final sourceSchema: SqlSchema<Row>;
    public final resultSchema: SqlSchema<ResultRow>;
    public final context: Context<Dynamic, Table, Row>;
    public final i: SelectImpl;

    public function new(context, source, sel:SelectImpl, ?driver:Dynamic) {
        _attachments = {};
        this.context = context;
        this.source = source;
        // this.driver = driver;
        this.sourceSchema = this.context.glue.tblGetSchema(source);
        this.i = sel;
        this.resultSchema = null;
    }

    public function eval():Array<ResultRow> {
        var input = this.context.glue.tblGetAllRows(this.source);
        var output = new Array();

        for (row in input) {
            this.i.apply(output, row, this);
        }

        return output;
    }
}

typedef DummyTable<Row> = {
    var name: String;
    var data: Array<Row>;
};

private typedef Exp = ql.sql.runtime.TAst.SelOutput;

@:forward
abstract Exporter<To> (Exp) from Exp to Exp {
    public inline function build(c: Context<Dynamic, Dynamic, Dynamic>):To {
        return cast this.build(cast c);
    }
}