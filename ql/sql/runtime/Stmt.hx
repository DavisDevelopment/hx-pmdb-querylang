package ql.sql.runtime;

// import ql.sql.grammar.CommonTypes.UpdateOpType;
import ql.sql.ast.Query.TableSpec;
import pmdb.core.Object.Doc;
import ql.sql.runtime.VirtualMachine.Context;
// import ql.sql.grammar.CommonTypes.UpdateOp;
import ql.sql.ast.Query.UpdateOpType;
import ql.sql.ast.Query.UpdateOp;
import ql.sql.runtime.TAst;
import ql.sql.runtime.TAst.SelPredicate;
import ql.sql.grammar.CommonTypes.TablePath;
import ql.sql.common.SqlSchema;

import pm.ImmutableList;

class Stmt<T> extends CSTNode {
    public final node: StmtNode<T>;
    public final type: StmtType;

    public function new(node, type) {
        super();
        
        this.node = node;
        this.type = type;
    }

    public function eval():T {
        return @:privateAccess node.eval();
    }

    /**
     * bind values to parameter expressions
     * @param parameters 
     */
    override function bind(parameters: Dynamic) {
        switch type {
            case SelectStatement(stmt):
                stmt.bind(parameters);

            case UpdateStatement(stmt):
                stmt.bind(parameters);

            case InsertStatement(stmt):
                stmt.bind(parameters);

            case CreateTable(_):
        }
    }
}

enum StmtType {
    SelectStatement(stmt: SelectStmt);
    UpdateStatement(stmt: UpdateStmt);
    InsertStatement(stmt: InsertStmt);
    CreateTable(stmt: CreateTableStmt);
}

/**
 * CSTNode - A Concrete Syntax Tree Node
 */
class CSTNode {
    public function new() {
        //
    }

    public function bind(parameters: Dynamic) {
        //
    }
}

class StmtNode<Out> extends CSTNode {
    public function new() {
        super();
    }

    function eval():Out {
        throw new pm.Error.NotImplementedError();
    }
}

class SelectStmt extends Sel<Dynamic, Dynamic, Dynamic> {
    // public final select: Sel<Dynamic, Dynamic, Dynamic>;
    // public function new(s) {
    //     super();
    //     this.select = s;
    // }
}

class CreateTableStmt extends StmtNode<Dynamic> {
    public final name: String;
    public final spec: SqlSchema<Dynamic>;

    public function new(name, schema) {
        super();

        this.name = name;
        this.spec = schema;
    }
}

class UpdateStmt extends StmtNode<Dynamic> {
    public final path: TablePath;
    public final operations: ImmutableList<UpdateOperation>;
    public final predicate: Null<SelPredicate> = null;
    public var context: UpdateStmtContext;

    public function new(path, operations, predicate) {
        super();

        this.path = path;
        this.operations = operations;
        this.predicate = predicate;
    }

    override function eval():Dynamic {
        throw new pm.Error('TODO');
    }
}

class UpdateStmtContext {
    public var context: Context<Dynamic, Dynamic, Dynamic>;
    public var swapObject: Doc;

    public function new(ctx) {
        this.context = ctx;
    }

    public function focus(row: Dynamic) {
        this.swapObject = Doc.unsafe(pmdb.core.Arch.clone(row));
    }
}

class UpdateOperation {
    public final columnName: String;
    public final type: UpdateOpType;
    public final e: TExpr;

    public var mCompiled:Null<(g: UpdateStmtContext)->Void> = null;

    public function new(type, column, e) {
        this.type = type;
        this.columnName = column;
        this.e = e;
    }
}

class InsertStmt extends StmtNode<Dynamic> {
    public final path: TableSpec;
    public final columns: Null<Array<String>> = null;
    public final rows: Array<Array<TExpr>>;

    public function new(path, columns, rows) {
        super();

        this.path = path;
        this.columns = columns;
        this.rows = rows;
    }
}