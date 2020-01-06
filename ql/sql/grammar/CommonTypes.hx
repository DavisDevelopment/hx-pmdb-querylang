package ql.sql.grammar;

import haxe.extern.EitherType as Or;
import ql.sql.SymbolTable.AstSymbol;

/*=  NotOperator | "~" | "+" | "-" */
// typedef UnaryOperator = Dynamic;
enum UnaryOperator {
	OpNot;
	OpNegBits;
	OpPositive;
	OpNegative;
}

/*=  "=" | ">" | "<" | ">=" | "<=" | "!=" | "<=>" */
enum ComparisonOperator {
	OpEq;
	OpGt;
	OpGte;
	OpLt;
	OpLte;
	OpNEq;
	// OpCmp;//?
}

/*=  "AND" | "OR" | "XOR" | "&&" | "||" */
enum ELogicalOperator {
	OpBoolAnd;
	OpBoolOr;
	OpBoolXor;
}

@:forward
@:forwardStatics(OpBoolAnd, OpBoolOr, OpBoolXor)
abstract LogicalOperator(ELogicalOperator) from ELogicalOperator to ELogicalOperator {
	@:from
	public static function ofString(s:String):LogicalOperator {
		return switch s {
			case _.toUpperCase() => 'AND', '&&': ELogicalOperator.OpBoolAnd;
			case _.toUpperCase() => 'OR', '||': ELogicalOperator.OpBoolOr;
			case _.toUpperCase() => 'XOR': ELogicalOperator.OpBoolXor;
			default:
				throw new pm.Error('Unexpected $s');
		}
	}

	@:from
	public static function ofBinaryOperator(op:BinaryOperator):LogicalOperator {
		return switch op {
			case OpBoolAnd: ELogicalOperator.OpBoolAnd;
			case OpBoolOr: ELogicalOperator.OpBoolOr;
			case OpBoolXor: ELogicalOperator.OpBoolXor;
			default:
				throw new pm.Error('Invalid argument: $op');
		}
	}

	@:to
	public function toBinaryOperator():BinaryOperator {
		return switch this {
			case OpBoolAnd: BinaryOperator.OpBoolAnd;
			case OpBoolOr: BinaryOperator.OpBoolOr;
			case OpBoolXor: BinaryOperator.OpBoolXor;
		}
	}
}

/*=  "<<" | ">>" | "&" | "|" | "^" */
typedef BitOperator = Dynamic;

/*=  "*" | "/" | "DIV" | "MOD" | "%" | "+" | "-" */
@:forward
@:forwardStatics(OpMult, OpDiv, OpMod, OpAdd, OpSubt)
abstract MathOperator(EMathOperator) from EMathOperator to EMathOperator {
	@:from
	public static function ofString(s:String):MathOperator {
		return switch s {
			case '+': EMathOperator.OpAdd;
			case '-': EMathOperator.OpSubt;
			case '%', _.toUpperCase() => 'MOD': EMathOperator.OpMod;
			case '/', _.toUpperCase() => 'DIV': EMathOperator.OpDiv;
			case '*': EMathOperator.OpMult;
			default:
				throw new pm.Error('Unexpected $s');
		}
	}
}

enum EMathOperator {
	OpMult;
	OpDiv;
	OpMod;
	OpAdd;
	OpSubt;
}

/*=  "NULL" | "NOT NULL" */
// typedef NullLiteral = Dynamic;
enum NullLiteral {
	NULL;
	NOT_NULL;
}

/*=  string | number | boolean | NullLiteral */
// typedef ConstantType = Or4<String, Or<Int, Float>, Bool, NullLiteral>;
enum ConstantType {
	CNull(value:NullLiteral);
	CBool(value:Bool);
	CInt(value:Int);
	CFloat(value:Float);
	CString(value:String);
}

/*=  ComparisonOperator | LogicalOperator | BitOperator | MathOperator */
// typedef BinaryOperator = Dynamic;
enum BinaryOperator {
	OpEq;
	OpGt;
	OpGte;
	OpLt;
	OpLte;
	OpNEq;
	// OpCmp;//?
	OpMult;
	OpDiv;
	OpMod;
	OpAdd;
	OpSubt;
	OpBoolAnd;
	OpBoolOr;
	OpBoolXor;
}

/*=  "ASC" | "DESC" */
// typedef SortType = Dynamic;
enum SortType {
	Asc;
	Desc;
}

/*=  All | Distinct | "DISTINCTROW" | "HIGH_PRIORITY" | "STRAIGHT_JOIN" | "SQL_SMALL_RESULT" 
	| "SQL_BIG_RESULT" | "SQL_BUFFER_RESULT" | "SQL_CACHE" | "SQL_NO_CACHE" | "SQL_CALC_FOUND_ROWS" */
// typedef SelectSpec = Dynamic;
enum SelectSpec {
	All;
	Distinct;
	DistinctRow;
	HighPriority;
	StraightJoin;
	SqlSmallResult;
	SqlBigResult;
	SqlBufferResult;
	SqlCache;
	SqlNoCache;
	SqlCalcFoundRows;
}

/*=  "FOR UPDATE" | "LOCK IN SHARE MODE" */
enum LockClause {
	ForUpdate;
	LockInShareMode;
}

/*=  "ALL" | "SOME" | "ANY" */
enum Quantifier {
	All;
	Some;
	Any;
}

/*
	IntervalTypeBase | "YEAR" | "YEAR_MONTH" | "DAY_HOUR" | "DAY_MINUTE" | "DAY_SECOND" | "HOUR_MINUTE" |
	"HOUR_SECOND" | "MINUTE_SECOND" | "SECOND_MICROSECOND" | "MINUTE_MICROSECOND" | "HOUR_MICROSECOND" | "DAY_MICROSECOND"
 */
enum IntervalType {
	Quarter;
	Month;
	Day;
	Hour;
	Minute;
	Week;
	Second;
	Microsecond;

	Year;
	YearMonth;
	DayHour;
	DayMinute;
	DaySecond;
	HourMinute;
	HourSecond;
	MinuteSecond;
	SecondMicrosecond;
	MinuteMicrosecond;
	HourMicroSecond;
	DayMicrosecond;
}

/*=  "INNER" | "CROSS" | "LEFT OUTER" | "RIGHT OUTER" */
enum JoinType {
	Inner;
	Cross;
	OuterLeft;
	OuterRight;
}

/**
 * enumeration describing all canonical ast-node types
 */
@:keep
enum SqlNodeType {
	Undefined;
	NotImplemented;

	SqlRoot;
	SelectStatement;
	QueryExpression;
	QueryIntoExpression;
	UnionGroupStatement;
	UnionStatement;
	AllColumns;
	ColumnName;
	SimpleFunctionCall;
	CaseExpression;
	CaseBranchExpression;
	Constant;
	AliasedTerm;
	AssignedTerm;
	NotExpression;
	BinaryExpression;
	TruthyPredicate;
	InPredicate;
	IsNullNotNullPredicate;
	BinaryPredicate;
	QuantifiedSelectStatement;
	BetweenPredicate;
	SoundsLikePredicate;
	LikePredicate;
	RegexPredicate;
	AssignedExpressionAtom;
	Variable;
	UnaryExpressionAtom;
	BinaryModifiedExpression;
	RowExpression;
	IntervalExpression;
	BinaryExpressionAtom;
	NestedSelectStatement;
	ExistsSelectStatement;
	FromClause;
	TableSource;
	TableSpec;
	WhereClause;
	GroupByClause;
	GroupByItem;
	JoinClause;
	OrderByClause;
	OrderByExpression;
	LimitClause;
	SelectIntoFieldsExpression;
}

class AbstractMethodCallError {
	public var position:haxe.PosInfos;
	public var methodPath:String;
	public var message:String;

	public function new(?msg:String, ?pos:haxe.PosInfos) {
		var path:String = if (pos == null) '' else '${pos.className}.${pos.methodName}';
		position = pos;
		methodPath = path;
		message = if (msg != null) msg else 'AbstractMethodError: $methodPath should be overridden by extending classes';
	}
}

class Err {
	public static function AbstractMethod(?pos:haxe.PosInfos)
		return new AbstractMethodCallError(null, pos);
}

enum SqlSymbolType {
	Unknown;
	Table;
	Field;
	Variable;
	Alias;
	Function;
}

// typedef SqlAstSymbol = AstSymbol<SqlSymbolType>;
class SqlSymbol {
	public var identifier:String;
	public var type:SqlSymbolType;
	public var parent : AstSymbol<SqlSymbolType> = null;

	public function new(id:String, ?type:SqlSymbolType, ?parent:SqlSymbol) {
		this.identifier = id;
		this.parent = parent;
		this.type = switch type {
			case null: Unknown;
			case t: t;
		}
	}
}
