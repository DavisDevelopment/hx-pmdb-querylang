package ql.sql;

import haxe.extern.EitherType as Or;
import ql.sql.SymbolTable.AstSymbol;

// import ql.sql.G.*;

#if macro
import haxe.macro.Expr;
import haxe.macro.Context;
using haxe.macro.ExprTools;
using tink.MacroApi;
#end

import ql.sql.grammar.CommonTypes;
import ql.sql.grammar.expression.Expression;

/* === [Utility/Shorthand Typedefs] === */
typedef Or3<A,B,C> = Or<A, Or<B, C>>;
typedef Or4<A,B,C,D> = Or<A, Or3<B, C, D>>;

/*=  "MySQL" | "Custom" */
typedef SqlDialect = Dynamic;

/*=  "*" */
typedef SelectAll = Dynamic;

class SqlAstNode {

    function _init_():Void {
        
    }

    public var nodeType(get, never):SqlNodeType;
    private function get_nodeType():SqlNodeType {
	    if (mNodeType == null) {
            return mNodeType = _get_nodeType();
        }
        else 
            return mNodeType;
    }
    private var mNodeType:Null<SqlNodeType> = null;

    @:keep
    function _get_nodeType():SqlNodeType {
        var cls = Type.getClass(this);
        // if (Reflect.hasField(cls, 'nodeType')) {
        //     var v = Reflect.field(cls, 'nodeType');
        //     return cast(v, SqlNodeType);
        // }

        var thisNodeType:Dynamic = try Reflect.getProperty(this, '_nodeType') catch (e:Dynamic) null;
        if (thisNodeType != null && (thisNodeType is SqlNodeType))
            return cast(thisNodeType, SqlNodeType);

        var constructName:String = Type.getClassName(cls).afterLast('.');
        // trace(constructName);
        switch haxe.rtti.Meta.getType(cls) {
            case null:
            case(_ : haxe.DynamicAccess<Array<Dynamic>>) => m if (m.exists('nodeType')):
                constructName = Std.string(m['nodeType']);
        }
        var enumVal = try Type.createEnum(SqlNodeType, constructName) catch (e:Dynamic) null;
        if (enumVal != null)
            return enumVal;

        throw Err.AbstractMethod();
    }
}

class NotImplemented extends SqlAstNode {
    public function new() {
        this._init_();
    }

    public static var instance = new NotImplemented();
}


class SqlRoot extends SqlAstNode {
    public var dialect:String;
    public var statements:Array<SqlStatement>;

	public function new(dialect, ?statements) {
		if (statements == null)
			statements = [];
        this.statements = statements;
        #if !macro inline this._init_(); #end
    }
}

/*=  DdlStatement | DmlStatement | TransactionStatement | ReplicationStatement | PreparedStatement | AdministrationStatement | UtilityStatement */
// typedef SqlStatement = ESqlStatement;
abstract SqlStatement (ESqlStatement) from ESqlStatement to ESqlStatement {
    @:from public static inline function Dml(stmt: DmlStatement):SqlStatement {
        return ESqlStatement.DmlStatement(stmt);
    }
    @:from public static inline function Select(stmt: SelectStatement):SqlStatement {
        return Dml(SelectStatement(stmt));
    }

    @:to
    public function toSelect():SelectStatement {
        return switch this {
            case DmlStatement(SelectStatement(s)): s;
            default: throw new pm.Error('Disallowed');
        }
    }
}
enum ESqlStatement {
    DmlStatement(stmt: DmlStatement);
}

enum DmlStatement {
    SelectStatement(stmt: SelectStatement);
    // InsertStatement;
    // ...etc
}

/*=  NotImplemented */
typedef TransactionStatement = Dynamic;
/*=  NotImplemented */
typedef ReplicationStatement = Dynamic;
/*=  NotImplemented */
typedef PreparedStatement = Dynamic;
/*=  NotImplemented */
typedef AdministrationStatement = Dynamic;
/*=  NotImplemented */
typedef UtilityStatement = Dynamic;

/*=  NotImplemented */
typedef InsertStatement = NotImplemented;

/*=  NotImplemented */
typedef UpdateStatement = Dynamic;
/*=  NotImplemented */
typedef DeleteStatement = Dynamic;
/*=  NotImplemented */
typedef ReplaceStatement = Dynamic;
/*=  NotImplemented */
typedef CallStatement = Dynamic;
/*=  NotImplemented */
typedef LoadDataStatement = Dynamic;
/*=  NotImplemented */
typedef LoadXmlStatement = Dynamic;
/*=  NotImplemented */
typedef DoStatement = Dynamic;
/*=  NotImplemented */
typedef HandlerStatement = Dynamic;


// @SqlNodeMarker(SqlNodeType.SelectStatement)
class SelectStatement extends SqlAstNode {
    public var query:QueryIntoExpression;
    public var lock:Null<LockClause> = null;

    public function new(query) {
        this._init_();
        this.query = query;
    }
}

class QueryExpression extends SqlAstNode {
    public var selectSpec:Null<Array<SelectSpec>> = null; // ?
    // public var selectAll:Null<SelectAll>;
    public var elements:Array<SelectElement>;
    public var from:Null<FromClause> = null;
    public var orderBy:Null<OrderByClause> = null;
    public var limit:Null<LimitClause> = null;

    public function new(?elements: Array<SelectElement>) {
        this._init_();
        if (elements == null)
            elements = [];
        this.elements = elements;
    }

    public function setClause(?from, ?orderBy, ?limit):QueryExpression {
        if (from != null) this.from = from;
        if (orderBy != null) this.orderBy = orderBy;
        if (limit != null) this.limit = limit;
        return this;
    }
}

class QueryIntoExpression extends SqlAstNode {
    public var query:QueryExpression;
    public var into:Null<SelectIntoExpression>=null;

    public function new(query:QueryExpression) {
        this._init_();
        this.query = query;
    }
}

enum UnionType {
    All;
    Distinct;
}

class UnionStatement extends SqlAstNode {
    public var unionWith:QueryExpression;
    public var unionType:Null<UnionType>=null;

    public function new(unionWith, ?unionType) {
        this._init_();
        this.unionWith = unionWith;
        this.unionType = if (unionType != null) unionType else UnionType.All;
    }
}

class UnionGroupStatement extends SqlAstNode {
    public var query:QueryExpression;
    public var unions:Null<Array<UnionStatement>>=null;
    public var unionType:Null<UnionType> = null;
    public var unionLast:Null<QueryIntoExpression>=null;
    public var orderBy:Null<OrderByClause>=null;
    public var limit:Null<LimitClause>=null;

    public function new(query) {
        this._init_();
        
        this.query = query;
    }
}

/*=  
AllColumns
ColumnName
FunctionCall
Expression
AssignedTerm < Expression >
AliasedTerm < ColumnName >
AliasedTerm < FunctionCall >
AliasedTerm < AssignedTerm < Expression >>
*/
// typedef SelectElement = Dynamic;
abstract SelectElement (ESelectElement) from ESelectElement to ESelectElement {
    private static var _all_ = ['AllColumns', 'ColumnName', 'FunctionCall', 'Expression', 'AssignedTerm', 'AliasedTerm'];

#if !macro

    @:from
    public static function fromExpression(e: Expression):SelectElement {
        return ESelectElement.Expression(e);
    }
    
    @:from 
    public static function fromSqlAstNode(node: SqlAstNode):SelectElement {
        var e = ESelectElement, ea = EAliasedSelectElement;
        // return ESelectElement.createByName(node.nodeType.getName(), [node]);
        if ((node is ql.sql.ColumnName)) {
            return e.ColumnName(cast(node, ColumnName));
        }

        if ((node is ql.sql.AllColumns)) {
            return e.AllColumns(cast node);
        }

        if ((node is ql.sql.FunctionCall)) {
            return e.FunctionCall(cast(node, ql.sql.FunctionCall));
        }

        if ((node is ql.sql.grammar.expression.Expression)) {
            return e.Expression(cast node);
        }

        if ((node is ql.sql.AssignedTerm<Dynamic>)) {
            return ESelectElement.AssignedTerm(cast node);
        }

        if ((node is ql.sql.AliasedTerm<SqlAstNode>)) {
            var node:AliasedTerm<SqlAstNode> = cast node;
            if ((node.term is ql.sql.ColumnName)) {
                return ESelectElement.AliasedTerm(ColumnName(cast node));
            }
            if ((node.term is ql.sql.FunctionCall)) {
                return ESelectElement.AliasedTerm(FunctionCall(cast node));
            }
            if ((node.term is ql.sql.grammar.expression.Expression)) {
                return ESelectElement.AliasedTerm(EAliasedSelectElement.Expression(cast node));
            }

			throw new pm.Error('Unexpected ${node.term.nodeType}, ${Type.getClassName(Type.getClass(node.term))}');
        }

        throw new pm.Error('Unexpected ${node.nodeType}, ${Type.getClassName(Type.getClass(node))}');
    }

    @:to
    public inline function toSqlAstNode():SqlAstNode {
        return cast(this.getParameters()[0], SqlAstNode);
    }

    public var nodeType(get, never):SqlNodeType;
    private inline function get_nodeType():SqlNodeType {
        return ((this : SelectElement) : SqlAstNode).nodeType;
    }

#end


    public var data(get, never):ESelectElement;
    private inline function get_data():ESelectElement return this;
}

enum ESelectElement {
    AllColumns(el: AllColumns);
    ColumnName(el: ColumnName);
    FunctionCall(el: FunctionCall);
    Expression(el: Expression);
    AssignedTerm(el: AssignedTerm<Expression>);
    AliasedTerm(el: EAliasedSelectElement);//Or3<AliasedTerm<ColumnName>, AliasedTerm<FunctionCall>, AliasedTerm<AssignedTerm<Expression>>>);
}
enum EAliasedSelectElement {
    ColumnName(el: AliasedTerm<ColumnName>);
    FunctionCall(el: AliasedTerm<FunctionCall>);
    Expression(el: AliasedTerm<Expression>);
    AssignedTerm(el: AliasedTerm<AssignedTerm<Expression>>);
}
/*
class AliasedSelectElement extends SqlAstNode {
    public var alias:SqlSymbol;
    public var data: EAliasedSelectElement;

    public function new(alias, data) {
        this.alias = alias;
        this.data = data;
    }
}
*/

// @SqlNodeMarker(SqlNodeType.AllColumns)
class AllColumns extends SqlAstNode {
    public var table:Null<SqlSymbol>;
    public function new(?table:SqlSymbol) {
        this.table = table;
        this._init_();
    }
}

// @SqlNodeMarker(SqlNodeType.ColumnName)
class ColumnName extends SqlAstNode implements FunctionArgument {
    public var name:SqlSymbol;
    public var table:SqlSymbol;

    public function new(name:SqlSymbol, ?table:SqlSymbol) {
        this.name = name;
        this.table = table;
        this._init_();
    }
}

/*=  
SpecificFunction
AggregatedWindowFunction
SimpleFunctionCall
PasswordFunction 
*/
/**
  [TODO] consider removing these types, in favor of the FunctionCall types defined in the Expression module
 **/
typedef FunctionCall = IFunctionCall;
class IFunctionCall extends SqlAstNode implements FunctionArgument {}

/*=  CaseExpression | NotImplemented */
typedef SpecificFunction = Dynamic;

/*=  NotImplemented */
typedef AggregatedWindowFunction = Dynamic;

/*=  NotImplemented */
typedef PasswordFunction = Dynamic;

// @SqlNodeMarker(SqlNodeType.SimpleFunctionCall)
class SimpleFunctionCall extends IFunctionCall {
    public var name:SqlSymbol;
    public var args:Array<FunctionArgument> = null;

    public function new(name, ?args) {
        this.name = name;
        this.args = args;
        this._init_();
    }
}

// @SqlNodeMarker(SqlNodeType.CaseExpression)
class CaseExpression extends SqlAstNode {
    public var branches:Array<CaseBranchExpression>;
    public var elseBranch:Null<FunctionArgument>=null;
    public var expression:Expression;

    public function new(?expression:Expression) {
        this.expression = expression;
        this._init_();
        this.branches = [];
    }
}

// @SqlNodeMarker(SqlNodeType.CaseBranchExpression)
class CaseBranchExpression extends SqlAstNode {
    public var expression:FunctionArgument;
    public var result:FunctionArgument;

    public function new(expression:FunctionArgument, result:FunctionArgument) {
        this.expression = expression;
        this.result = result;
        this._init_();
    }
}

/*=  Constant | ColumnName | FunctionCall | Expression */
// typedef FunctionArgument = Dynamic;
interface FunctionArgument {}

class Constant extends SqlAstNode implements FunctionArgument {
    public var value: ConstantType;

    public function new(value: ConstantType) {
        this.value = value;
        this._init_();
    }
}

// @SqlNodeMarker(SqlNodeType.AliasedTerm)
// @:generic
@nodeType('AliasedTerm')
class AliasedTerm<TTerm:SqlAstNode> extends SqlAstNode {
    public var term:TTerm;
    public var alias:SqlSymbol;
    final _nodeType:SqlNodeType = SqlNodeType.AliasedTerm;

    public function new(term:TTerm, ?alias) {
        this.term = term;
        this.alias = alias;
        this._init_();
    }
}

interface IPredicate {}

// @SqlNodeMarker(SqlNodeType.AssignedTerm)
// @:generic
class AssignedTerm<TTerm:SqlAstNode> extends SqlAstNode {
    public var value:TTerm;
    public var variable:SqlSymbol;

    public function new(value:TTerm, ?variable) {
        this.value = value;
        this.variable = variable;
        this._init_();
    }
}

class QuantifiedSelectStatement extends SqlAstNode implements IPredicate {
    public var quantifier:Quantifier;
    public var statement:SelectStatement;

    public function new(quantifier:Quantifier, statement:SelectStatement) {
        this.quantifier = quantifier;
        this.statement = statement;
        this._init_();
    }
}

class AssignedExpressionAtom extends SqlAstNode {
    public var expression: ExpressionAtom;
    public var variable: Null<SqlSymbol>;
    final _nodeType:SqlNodeType = SqlNodeType.AssignedExpressionAtom;

    public function new(expression:ExpressionAtom, ?variable) {
        this.expression = expression;
        this.variable = variable;
        this._init_();
    }
}

/*
Constant | ColumnName | FunctionCall
CollatedExpression | Variable | UnaryExpressionAtom | BinaryModifiedExpression
Expression | RowExpression | ExistsSelectStatement
NestedSelectStatement | BinaryExpressionAtom < BitOperator | MathOperator >  
*/
// typedef ExpressionAtom = Dynamic;
abstract ExpressionAtom (SqlAstNode) to SqlAstNode {
    function new(node) {
        this = node;
    }

    @:from
    public static function ofAstNode(node: SqlAstNode):ExpressionAtom {
        // var u:Dynamic = node;
        switch node.nodeType {
            case Constant, ColumnName, Variable, UnaryExpressionAtom, BinaryModifiedExpression, RowExpression, ExistsSelectStatement, NestedSelectStatement, BinaryExpressionAtom:
                return new ExpressionAtom(node);
            
            default:
                if ((node is Expression)) return new ExpressionAtom(node);
                if ((node is FunctionCall)) return new ExpressionAtom(node);
        }

        throw new pm.Error('Unhandled $node');
    }

    public static function is(node: SqlAstNode):Bool {
        return try {ofAstNode(node);true;} catch (e: Dynamic) false;
    }
}

/*=  NotImplemented */
typedef CollatedExpression = Dynamic;

class Variable extends SqlAstNode {
    public var name: SqlSymbol;

    public function new(name) {
        this.name = name;
        this._init_();
    }
}

class UnaryExpressionAtom extends SqlAstNode {
    public var op:UnaryOperator;
    public var expression:ExpressionAtom;

    public function new(op:UnaryOperator, expression:ExpressionAtom) {
        this.op = op;
        this.expression = expression;
        this._init_();
    }
}

class BinaryModifiedExpression extends SqlAstNode {
    public var expression:ExpressionAtom;

    public function new(expression:ExpressionAtom) {
        this.expression = expression;
        this._init_();
    }
}

class RowExpression extends SqlAstNode {
    public var expressions:Dynamic;

    public function new(expressions:Dynamic) {
        this.expressions = expressions;
        this._init_();
    }
}

class IntervalExpression extends SqlAstNode {
    public var expression:Expression;
    public var intervalType:IntervalType;

    public function new(expression:Expression, intervalType:IntervalType) {
        this.expression = expression;
        this.intervalType = intervalType;
        this._init_();
    }
}

// @SqlNodeMarker(SqlNodeType.BinaryExpressionAtom)
// @:generic
class BinaryExpressionAtom<TOperator> extends SqlAstNode {
    public var left:ExpressionAtom;
    public var op:TOperator;
    public var right:ExpressionAtom;

    public function new(left:ExpressionAtom, op:TOperator, right:ExpressionAtom) {
        this.left = left;
        this.op = op;
        this.right = right;
        this._init_();
    }
}

class NestedSelectStatement extends SqlAstNode {
    public var statement:SelectStatement;

    public function new(statement:SelectStatement) {
        this.statement = statement;
        this._init_();
    }
}

class ExistsSelectStatement extends SqlAstNode {
    public var statement:SelectStatement;

    public function new(statement:SelectStatement) {
        this.statement = statement;
        this._init_();
    }
}

/*=  SelectIntoFieldsExpression | SelectIntoDumpFileExpression | SelectIntoOutFileExpression */
typedef SelectIntoExpression = Dynamic;

/*=  NotImplemented */
typedef SelectIntoDumpFileExpression = Dynamic;

/*=  NotImplemented */
typedef SelectIntoOutFileExpression = Dynamic;

// @SqlNodeMarker(SqlNodeType.FromClause)
class FromClause extends SqlAstNode {
    public var where:Null<WhereClause>;
    public var groupBy:Null<GroupByClause>;
    public var having:Null<Expression>;

    public var tables: Array<TableSource>;

    public function new(tables) {
        this.tables = tables;
        this._init_();
    }

    public function addSource(src: Or<TableSource, TableSourceItem>):TableSource {
        if ((src is TableSource)) {
            tables.push(src);
            return cast(src, TableSource);
        }
        else {
            var item:TableSourceItem = cast(src, SqlAstNode);
            var source:TableSource = new TableSource(item);
            tables.push(source);
            return source;
        }
    }

    @:keep
    public function toString():String {
        var srcs = tables.map(t -> '$t');
        var out = srcs.join(',');
        out = 'From($out)';
        if (where != null) {
            out += '.Where(${where})';
        }
        return out;
    }
}

// @SqlNodeMarker(SqlNodeType.TableSource)
class TableSource extends SqlAstNode {
    
    public var joins:Null<Array<JoinClause>>;
    public var tableSourceItem:TableSourceItem;

    public function new(tableSourceItem:TableSourceItem) {
        this.tableSourceItem = tableSourceItem;
        this._init_();
    }

    @:keep function toString() return Std.string(tableSourceItem);
}

/*=  TableSpec | AliasedTerm < TableSpec | NestedSelectStatement >  */
typedef TTableSourceItem = Dynamic;
@:forward
abstract TableSourceItem (SqlAstNode) to SqlAstNode
// from AliasedTerm<NestedSelectStatement>
{
    function new(n: SqlAstNode) {
        this = n;
    }

    @:to 
    public inline function toTableSpec():TableSpec {
        return cast(this, TableSpec);
    }

    @:from
    public static function ofAstNode(node: SqlAstNode):TableSourceItem {
        if ((node is ql.sql.TableSpec)) return new TableSourceItem(node);
        if ((node is ql.sql.AliasedTerm<Dynamic>)) {
            var al:AliasedTerm<SqlAstNode> = cast node;
            if ((al.term is TableSpec) || (al.term is NestedSelectStatement)) return new TableSourceItem(node);
        }

        throw new pm.Error('Unexpected ${Type.getClassName(Type.getClass(node))}');
    }
}

class TableSpec extends SqlAstNode {
    public var partitions:Null<Array<SqlSymbol>>;
    public var indexHints:Null<Array<IndexHint>>;
    public var tableName:SqlSymbol;

    public function new(tableName) {
        this.tableName = tableName;
        this._init_();
    }
    @:keep
    public function toString() return tableName.identifier;

    extern inline public function tableInstance():Null<Dynamic> {
        return tableName.table;
    }
}

/*=  NotImplemented */
typedef IndexHint = NotImplemented;

class WhereClause extends SqlAstNode {
    public var predicate:Predicate;

    public function new(predicate) {
        _init_();
        this.predicate = predicate;
    }
    
    @:keep 
    public function toString() return Std.string(predicate);
}

// @SqlNodeMarker(SqlNodeType.GroupByClause)
class GroupByClause extends SqlAstNode {
    public var items:Array<GroupByItem>;
    public var rollup:Bool;
    public function new(items, rollup = false) {
        this._init_();
        this.items = items;
        this.rollup = rollup;
    }
}

// @SqlNodeMarker(SqlNodeType.GroupByItem)
class GroupByItem extends SqlAstNode {
    public var expression:Expression;
    public var descending:Bool;
    public function new(expression:Expression, descending:Bool = false) {
        this._init_();
        this.expression = expression;
        this.descending = descending;
    }
}

class JoinClause extends SqlAstNode {
    public var joinWith: TableSourceItem;
    public var joinType: Null<JoinType> = null;
    public var on: Null<Expression> = null;

    @:native('_using_')
    public var used:Null<Array<SqlSymbol>>;          // Using a list of column names within the scope of the tables

    public function new(joinWith:TableSourceItem, ?joinType:JoinType, ?joinOn:Expression) {
        this.joinWith = joinWith;
        this.joinType = joinType;
        this.on = joinOn;
        this._init_();
    }
}

class OrderByClause extends SqlAstNode {
    public var expressions:Array<OrderByExpression>;

    public function new(expressions) {
        this.expressions = expressions;
        this._init_();
    }
    public function addExpr(expression:Expression, descending=false):OrderByClause {
        expressions.push(new OrderByExpression(expression, descending));
        return this;
    }
}

class OrderByExpression extends SqlAstNode {
    public var expression:Expression;
    public var descending:Bool;

    public function new(expression:Expression, descending:Bool = false) {
        this._init_();
        this.expression = expression;
        this.descending = descending;
    }

    public var sortType(get, never):SortType;
    private inline function get_sortType():SortType return descending ? SortType.Desc : SortType.Asc;
}

class LimitClause extends SqlAstNode {
    public var limit:Int;
    public var offset:Null<Int> = null;

    public function new(limit:Int, ?offset:Int) {
        this._init_();
        this.limit = limit;
        this.offset = offset;
    }
}

class SelectIntoFieldsExpression extends SqlAstNode {
    public var fields:Array<SqlSymbol>;

    public function new(fields) {
        this.fields = fields;
        this._init_();
    }
}

//# lineMapping=3,1,4,1,5,1,6,1,7,1,8,2,9,4,10,4,11,5,12,5,13,6,14,6,15,7,16,7,17,8,18,8,19,9,20,9,21,10,22,10,23,11,24,11,25,12,26,12,27,13,28,13,29,14,30,14,31,15,32,15,33,16,34,16,35,17,36,17,37,18,38,18,39,20,40,20,41,21,42,21,43,22,44,22,45,23,46,23,47,37,48,37,49,38,50,39,51,40,52,41,53,42,54,43,57,45,58,46,59,47,60,48,61,49,62,50,63,51,64,52,65,53,66,54,67,55,68,56,69,57,70,58,71,59,72,60,73,61,74,62,75,63,76,64,77,65,78,66,79,67,80,68,81,69,82,70,83,71,84,72,85,73,86,74,87,75,88,76,89,77,90,78,91,79,92,80,93,81,94,82,95,83,96,84,97,85,98,86,99,87,100,88,101,89,102,90,103,91,106,93,107,94,108,95,111,96,112,97,113,99,114,99,115,100,116,102,117,103,118,104,119,105,120,107,121,108,122,109,124,110,125,111,126,112,128,113,130,115,132,117,133,117,134,117,135,117,136,117,137,119,138,119,140,120,141,121,142,122,143,123,145,125,146,126,148,128,149,130,152,132,154,134,155,135,157,136,159,139,160,140,162,141,164,141,166,141,169,142,172,144,174,146,175,146,176,156,177,156,178,157,179,157,180,158,181,158,182,159,183,159,184,160,185,160,186,161,187,161,188,164,189,164,190,176,191,176,192,177,193,177,194,178,195,178,196,179,197,179,198,180,199,180,200,181,201,181,202,182,203,182,204,183,205,183,206,184,207,184,208,186,209,187,211,188,213,188,215,189,217,189,219,190,220,192,221,193,224,196,226,198,227,199,229,200,231,200,233,201,235,201,237,202,239,203,241,203,243,204,245,204,247,205,249,205,251,206,252,208,253,209,256,212,258,214,259,215,261,216,263,217,265,217,267,218,268,220,269,221,272,224,274,226,275,227,277,228,279,229,281,229,283,229,285,230,286,232,287,233,288,235,291,237,293,239,294,240,296,241,298,241,300,242,302,242,304,242,306,243,308,243,310,244,312,244,314,245,316,245,318,247,320,246,322,248,325,250,327,252,328,252,329,263,330,264,332,265,334,265,336,266,339,268,341,270,342,271,344,272,346,272,348,272,351,273,354,275,356,277,357,277,358,283,359,283,360,284,361,284,362,285,363,285,364,287,365,288,367,289,369,289,371,289,374,290,377,292,379,294,380,295,382,296,384,297,386,297,388,299,390,298,392,300,393,301,396,304,398,306,399,307,401,308,403,308,405,308,408,309,411,311,413,313,414,313,415,319,416,320,418,321,420,321,422,322,425,324,427,326,429,327,431,328,433,328,435,328,438,329,441,331,443,333,445,334,447,335,449,335,451,335,454,336,457,338,459,340,460,340,461,346,462,347,464,348,466,348,468,349,471,351,473,353,475,354,477,355,479,355,481,355,483,355,487,356,490,358,492,360,493,361,495,364,497,364,499,364,501,362,505,365,508,367,510,369,511,369,512,379,513,380,515,382,517,382,519,382,521,381,525,383,528,385,530,387,531,388,533,389,535,389,537,389,540,390,543,392,545,394,546,395,548,396,550,396,552,396,554,396,558,397,561,399,563,401,564,402,566,403,568,403,570,403,573,404,576,406,578,408,579,409,581,410,583,410,585,410,587,410,589,410,594,411,597,413,599,415,600,416,602,417,604,417,606,417,609,418,612,420,614,422,615,423,617,424,619,424,621,424,623,424,627,425,630,427,632,429,633,429,634,431,635,432,637,433,639,433,641,433,644,434,647,436,649,456,650,456,651,470,652,470,653,472,654,473,656,474,658,474,660,475,663,477,665,479,666,480,668,481,670,481,672,481,675,482,678,484,680,486,681,487,683,488,685,488,687,489,690,491,692,493,693,494,695,495,697,495,699,496,702,498,704,500,705,501,707,502,709,502,711,502,714,503,717,505,719,507,721,508,723,509,725,509,727,509,729,509,733,510,736,512,738,514,739,515,741,516,743,516,745,517,748,519,750,522,751,523,753,524,755,524,757,525,760,527,762,529,763,529,764,530,765,530,766,531,767,531,768,533,769,534,771,535,773,535,775,536,777,536,779,537,781,537,783,539,785,538,787,540,790,542,792,544,793,545,795,546,797,546,799,548,801,547,803,549,806,551,808,553,809,553,810,557,811,558,813,559,815,559,817,560,819,560,821,562,823,561,825,563,828,565,830,567,831,567,832,569,833,570,835,571,837,571,839,572,842,574,844,576,845,577,847,578,849,578,851,578,854,579,857,581,859,583,860,584,862,586,864,586,866,585,869,587,872,589,874,591,875,592,877,593,879,593,881,594,883,594,885,595,887,595,889,597,891,596,893,598,896,600,898,602,899,603,901,604,903,604,905,605,908,607,910,609,911,610,913,611,915,611,917,611,920,612,923,614,925,616,926,617,928,618,930,618,932,618,935,619,938,621,940,623,941,624,943,625,945,625,947,626,950,628
