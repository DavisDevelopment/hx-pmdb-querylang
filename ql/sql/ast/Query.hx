package ql.sql.ast;

import ql.sql.grammar.CommonTypes;
import ql.sql.grammar.CommonTypes.JoinType;
import ql.sql.grammar.CommonTypes.SortType;
import ql.sql.grammar.CommonTypes.SelectSpec;

import pm.Helpers.*;
import pm.Assert.*;

/**
  ADT for representing SQL query-statement
 **/
enum Query {
/* 	Select(fields:Array<Field>, table:QuerySrc, cond:Expr);
	Insert(target:String, ?columns:Array<String>, values:Array<Expr>);
	CreateTable(table:String, fields : Array<FieldDesc>, props : Array<TableProp> );
	AlterTable(table:String, alters : Array<AlterCommand> ); */
	Select(select: SelectStatement);
	Insert(insert: Dynamic);
	CreateTable(createTable: CreateTableStatement);
}

class Stmt {
	public function new() {
		//
	}
}

class SelectStatement extends Stmt {
	public var query: QueryIntoExpression;

	public function new(query) {
		super();

		this.query = query;
	}
}

class QueryIntoExpression {
	public var query: Null<QueryExpression> = null;
	public var into: Null<SelectIntoExpression> = null;

	public function new(?query, ?into) {
		this.query = query;
		this.into = into;
	}
}

class SelectIntoExpression {
	public var table: String;
}

class QueryExpression {
	public var selectSpec:Null<Array<SelectSpec>> = null; // ?
	public var elements:Array<SelectElement>;
	public var from:Null<FromClause> = null;
	public var orderBy:Null<OrderByClause> = null;
	public var limit:Null<LimitClause> = null;

	public function new(?specs:Array<SelectSpec>, ?elements:Array<SelectElement>, ?from:FromClause, ?orderBy:OrderByClause, ?limit:LimitClause) {
		this.selectSpec = specs;
		this.elements = elements;
		this.from = from;
		this.orderBy = orderBy;
		this.limit = limit;
	}
}

class FromClause {
	public var where: Null<WhereClause>;
	public var groupBy: Null<GroupByClause>;
	public var having: Null<Expr>;

	public var tables: Array<SelectSource>;

	public function new(tables) {
		this.tables = tables;
	}
}

class WhereClause {
	public final term: Expr;

	public function new(e) {
		this.term = e;
	}
}

// @SqlNodeMarker(SqlNodeType.GroupByClause)
class GroupByClause {
	public var items:  Array<GroupByItem>;
	public var rollup: Bool;

	public function new(items, rollup = false) {
		this.items = items;
		this.rollup = rollup;
	}
}

// @SqlNodeMarker(SqlNodeType.GroupByItem)
class GroupByItem {
	public var expression: Expr;
	public var descending: Bool;

	public function new(expression, descending:Bool = false) {
		this.expression = expression;
		this.descending = descending;
	}
}

class JoinClause {
	public var joinWith: SelectSourceItem;
	public var joinType: Null<JoinType> = null;
	public var on: Null<Expr> = null;
	public var usingColumns: Null<Array<String>>; // Using a list of column names within the scope of the tables

	public function new(joinWith:SelectSourceItem, ?joinType:JoinType, ?joinOn:Expr) {
		this.joinWith = joinWith;
		this.joinType = joinType;
		this.on = joinOn;
	}
}

class OrderByClause {
	public var expressions: Array<OrderByExpression>;

	public function new(expressions) {
		this.expressions = expressions;
	}
}

class OrderByExpression {
	public var expression: Expr;
	public var descending: Bool;

	public function new(expression:Expr, descending:Bool = false, ?type:SortType) {
		this.expression = expression;
		this.descending = descending;
		if (type != null)
			this.descending = switch type {
				case Desc: true;
				case Asc: false;
			};
	}

	public var sortType(get, never):SortType;
	private inline function get_sortType():SortType	return descending ? SortType.Desc : SortType.Asc;
}

class LimitClause {
	public var limit: Int;
	public var offset: Null<Int> = null;

	public function new(limit:Int, ?offset:Int) {
		this.limit = limit;
		this.offset = offset;
	}

	public inline function slice() {
		var idx = nor(offset, 0);
		return {startIndex:idx, length:limit, endIndex:idx + limit};
	}
}

enum SelectSourceItem {
    Table(table: TableSpec);
    Aliased(src:SelectSourceItem, alias:String);
    // Join(kind:JoinType, left:SelectSourceItem, right:SelectSourceItem, predicate:Expr);
	Subquery(query: SelectStatement);
	Expr(e: Expr);
}

enum SelectElement {
	AllColumns(?table: String);
	Column(table:Null<String>, column:String, alias:Null<String>);
	Expr(e:Expr, alias:Null<String>);
}

class TableSpec {
	public var tableName: String;
	public var partitions: Null<Array<TablePartitionExpression>> = null;
	public var indexHints: Null<Array<IndexHint>> = null;

	public var tableRef: Null<Dynamic> = null;
	public final pos: haxe.PosInfos;

	public function new(tableName, ?pos:haxe.PosInfos) {
		this.tableName = tableName;
		this.pos = pos;
	}

	public function toString()
		return tableName;
}

class SelectSource {
	public var sourceItem: SelectSourceItem;
	public var joins: Null<Array<JoinClause>> = null;

	public function new(item) {
		this.sourceItem = item;
	}
}

class TablePartitionExpression {}
class IndexHint {
	public var action: IndexHintAction;
	public var keyFormat: KeyFormat;
	public var type: IndexHintType;

	public function new() {
		//
	}
}

/**
  ADT for SQL statement-expression
 **/
@:using(ql.sql.QueryTools)
enum Expr {
	CTrue;
	CFalse;
	CNull;
	CUnderscore;
	CInt(i: Int);
	CFloat(n: Float);
	CString(v: String);
	CParam(?parameter: String);
	
	EParent(e: Expr);
	EId(id: String);
	EField(e:Expr, field:FieldAccess);
	ECall(e:Expr, args:Array<Expr>);
	ECase(branches:Array<CaseBranch>, ?def:Expr, ?e:Expr);
	EBinop(op:Binop, l:Expr, r:Expr);
	EUnop(op:Unop, postfix:Bool, e:Expr);
	ETernary(cond:Expr, t:Expr, f:Expr);//ext
	
	// EQuery(q: Query);
	NestedSelectStmt(select: Dynamic);
	
	EArray(a:Expr, idx:Expr);
	EArrayDecl(arr: Array<Expr>);
	EList(arr: Array<Expr>);//probably superfluous

	/**
	 `${e} FOR ($iterWhat in $iterOf) [if ($iterPredicate)]` syntax
	 */
	EComprehension(expr:Expr, iterWhat:Expr, iterOf:Expr, iterPredicate:Null<Expr>);//ext
}

enum FieldAccess {
	Name(name: String);
	All;
}

enum Binop {
	Eq;
	NEq;
	In;
	Like;
	Match;//ext

	Add;
	Sub;
	Div;
	Mult;
	Mod;

	Gt;
	Gte;
	Lt;
	Lte;

	LogAnd;
	LogOr;
}

enum Unop {
    Not;
}

typedef CaseBranch = {
	var e: Expr;
	var body: Expr;
};

typedef Field = {
	?table: String,
	?field: String,
	?alias: String,
	?all: Bool
};

// typedef SqlType = ql.sql.SqlType;

typedef FieldDesc = {
    name: String,
    ?type: SqlType,
    ?notNull: Bool,
    ?autoIncrement: Bool,
    ?unique: Bool,
    ?primaryKey: Bool,
    ?digits: Int
};

enum TableProp {
	PrimaryKey(field : Array<String>);
	Engine(name : String);
}

enum AlterCommand {
	AddConstraintFK(name:String, field:String, table:String, targetField:String, ?onDelete:FKDelete);
}

enum CreateTableEntry {
    TableField(field: FieldDesc);
    TableProp(prop: TableProp);
}

enum FKDelete {
	FKDSetNull;
	FKDCascade;
}

class UpdateStatement extends Stmt {
	public var target:TableSpec;
	public var operations:Array<UpdateOp>;
	public var where:Null<Expr> = null;

	public function new(target, ops, where) {
		super();

		this.target = target;
		this.operations = ops;
		this.where = where;
	}
}

class InsertStatement extends Stmt {
	public var target: TableSpec;
	public var columns: Null<Array<String>> = null;
	public var values: Array<Array<Expr>>;

	public function new(target, columns, values) {
		super();

		this.target = target;
		this.columns = columns;
		this.values = values;
	}
}

class CreateTableStatement extends Stmt {
	public var ifNotExists:Bool = false;
	public var columns: Array<CreateTableColumn>;
}

class CreateTableColumn {
	public var name: String;
	public var type: SqlType;

	public var primaryKey:Bool = false;
	public var autoIncrement:Bool = false;
	public var notNull:Bool = false;
	public var unique:Bool = false;
}

/**
```g4
	columnConstraint
	: nullNotnull                                                   #nullColumnConstraint
	| DEFAULT defaultValue                                          #defaultColumnConstraint
	| AUTO_INCREMENT                                                #autoIncrementColumnConstraint
	| PRIMARY? KEY                                                  #primaryKeyColumnConstraint
	| UNIQUE KEY?                                                   #uniqueKeyColumnConstraint
	| COMMENT STRING_LITERAL                                        #commentColumnConstraint
	| COLUMN_FORMAT colformat=(FIXED | DYNAMIC | DEFAULT)           #formatColumnConstraint
	| STORAGE storageval=(DISK | MEMORY | DEFAULT)                  #storageColumnConstraint
	| referenceDefinition                                           #referenceColumnConstraint
	;

	tableConstraint
	: (CONSTRAINT name=uid?)?
	  PRIMARY KEY indexType? indexColumnNames indexOption*          #primaryKeyTableConstraint
	| (CONSTRAINT name=uid?)?
	  UNIQUE indexFormat=(INDEX | KEY)? index=uid?
	  indexType? indexColumnNames indexOption*                      #uniqueKeyTableConstraint
	| (CONSTRAINT name=uid?)?
	  FOREIGN KEY index=uid? indexColumnNames
	  referenceDefinition                                           #foreignKeyTableConstraint
	| CHECK '(' expression ')'                                      #checkTableConstraint
	;
```
 */

enum abstract ColumnFormat (Int) {
	var Fixed;
	var Dynamic;
	var Default;
}
enum abstract ColumnStorage (Int) {
	var Disk;
	var Memory;
	var Default;
}

enum ColumnConstraint {
	NotNull;
	Default(defaultValue: Expr);
	AutoIncrement;
	PrimaryKey;
	Unique(?key:Bool);
	Key;
	Comment(comment: String);
	ColumnFormat(f: ColumnFormat);
	Storage(s: ColumnStorage);
	References(r: ReferenceDefinition);
}

enum TableConstraint {
	//
}

class ReferenceDefinition {
	public var tableName: String;
	public var indexColumnNames: Array<String>;

	public function new(tableName, cols) {
		this.tableName = tableName;
		this.indexColumnNames = cols;
	}
}

enum UpdateOpType {
	SET;
}

class UpdateOp {
	public final type:UpdateOpType;
	public final column:String;
	public final e:Expr;

	public function new(type, column, e) {
		this.type = type;
		this.column = column;
		this.e = e;
	}
}