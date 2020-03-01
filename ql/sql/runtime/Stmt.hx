package ql.sql.runtime;

import ql.sql.common.SqlSchema;

class Stmt extends CSTNode {
    public final cstNode: StmtNode<Dynamic>;
    public final node: StmtType;

    public function new(node, type) {
        super();
        
        this.cstNode = node;
        this.node = type;
    }

    public function eval() {
        return @:privateAccess cast(cstNode, StmtNode<Dynamic>).eval();
    }
}

enum StmtType {
    SelectStatement(stmt: SelectStmt);
    CreateTable(stmt: CreateTableStmt);
}

/**
 * CSTNode - A Concrete Syntax Tree Node
 */
class CSTNode {
    public function new() {
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