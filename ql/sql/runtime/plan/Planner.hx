package ql.sql.runtime.plan;

import ql.sql.common.index.IIndex;
import ql.sql.common.index.IndexCache;
// import pmdb.core.ds.index.IIndex;
import ql.sql.grammar.CommonTypes.BinaryOperator;
import ql.sql.common.TypedValue;
import ql.sql.runtime.Compiler;
import ql.sql.runtime.TAst;
import ql.sql.runtime.plan.QueryPlan;
import ql.sql.runtime.Stmt;

import haxe.ds.Option;
using pm.Options;

#if Console.hx
import Console.*;
#else
import pm.utils.LazyConsole.*;
#end

@:access(ql.sql.runtime)
class Planner {
   public var compiler: Compiler;

   public function new(c) {
      this.compiler = c;
   }

   public function plan(q: Stmt<Dynamic>) {
      //
      switch q.type {
         case SelectStatement(stmt):
            return planSelect(stmt);

         case UpdateStatement(stmt):
            throw new pm.Error('Unhandled ${q.type}');

         case InsertStatement(stmt):
            throw new pm.Error('Unhandled ${q.type}');

         case CreateTable(stmt):
            throw new pm.Error('Unhandled ${q.type}');
      }
   }

   /**
    * computes a SelectPlan which can be used to plot the function which carries out the query
    * @param stmt 
    */
   function planSelect(stmt: SelectStmt) {
      var isSimpleSelect = false,
          isJoinSelect = false;
      
      var tableHandle = stmt.context.sources.sources[0];
      var table = try compiler.context.resolveTableFrom(tableHandle) catch (e: Dynamic) null;
      var cache = table != null ? compiler.context.glue.tblGetIndexCache(table) : null;

      var result = {
         scan: Scan.STable(TableScan.FullTableScan),
         fn: function(row: Dynamic):Void {return ;}
      };

		if (cache == null) {
         return result;
      }
      else for (idx in cache.m) {
         Console.examine(idx.indexType, idx.keyType);
      }

      var predicate = stmt.i._predicate;
      if (predicate != null) {
         var tmp = predicate;
         predicate = simplifyPredicate(predicate);
         if (tmp != predicate) {
            // success('recompiling predicate');
            examine(tmp, predicate);
            compiler.compileTPred(predicate);
         }
      }

      if (predicate != null) switch predicate.type {
         case Rel(relation):
            var cc = getColumnAndConst(relation.left, relation.right);
            var index:Null<IIndex<Dynamic, Dynamic>> = getIndexHandleFromBinaryPredicate(cache, relation);
            if (cc != null && index != null) {
               var queryTypeCtor = switch relation.op {
                  case Equals: IndexQueryType.Equals;
                  case NotEquals: IndexQueryType.NotEquals;
                  case Greater: IndexQueryType.Greater;
                  case Lesser: IndexQueryType.Lesser;
                  case GreaterEq: IndexQueryType.GreaterEq;
                  case LesserEq: IndexQueryType.LesserEq;
                  case In: IndexQueryType.In;
               };
               result.scan = STable(TableScan.Indexed(index.unwrap(), queryTypeCtor(IndexQueryOperand.Const(cc.constValue))));
            }

         default:
      }

      debug(result);

      return result;
   }

   /**
    * attempts to simplify the given predicate expression
    * @param p 
    */
   function simplifyPredicate(p: SelPredicate) {
      switch p.type {
         case Rel(rel):
            var newRel = simplifyBinaryPredicate(rel);
            if (newRel != rel)
               return new SelPredicate(Rel(newRel));

         default:
      }
      return p;
   }

   /**
    * attempts to simplify the given `RelationalPredicate` instance, returning once which has the column isolated to the left-hand expression, in the simplest possible form
    * @param p 
    * @return RelationalPredicate
    */
   function simplifyBinaryPredicate(p: RelationalPredicate):RelationalPredicate {
      var bin = getBinaryOperationFromRel(p);
      var s1 = printBinaryOperation(bin);

      var tmp = simplifyBinop(bin);
      if (tmp != bin) {
         // examine(bin, tmp);
         var s2 = printBinaryOperation(tmp);
         examine(s1, s2);
         bin = tmp;
      }

      var tmp = getRelFromBinaryOperation(bin);

      return tmp;
   }

   /**
    * attempts to simplify the given binary expression
    * @param bin 
    * @return BinaryOperation
    */
   function simplifyBinop(bin: BinaryOperation):BinaryOperation {
      if (bin.op.match(OpEq|OpNEq|OpGt|OpGte|OpLt|OpLte)) {
         switch [bin.left.expr, bin.right.expr] {
            case [TBinop(_,_,_), TConst(rightValue)]:
               var leftOp = getBinaryOperation(bin.left);
               switch leftOp {
                  case {op:OpAdd, left:{expr:TColumn(_.label()=>column, _)}, right:{expr:TConst(leftValue)}}:
                     var newRhv:TypedValue = BinaryOperators.top_subt(rightValue, leftValue);
                     examine(newRhv);
                     return {op:bin.op, left:leftOp.left, right:te(TConst(newRhv))};

                  default:
                     error(bin);
               }

            case [TConst(_), TColumn(_, _)]:
               return simplifyBinop({op:bin.op, left:bin.right, right:bin.left});

            case [TColumn(_, _), TConst(_)]:
               success('Already in simplest form: ', printBinaryOperation(bin));

            default:
               error('Did not simplify: ', printBinaryOperation(bin));
         }
      }
      else {
         switch bin.op {
            case OpMult:
               debug(bin);
            case OpDiv:
               debug(bin);
            case OpMod:
               debug(bin);
            case OpAdd:
               debug(bin);
            case OpSubt:
               debug(bin);
            default:
               log(bin);
         }
      }

      return bin;
   }

   function getIndexHandleFromBinaryPredicate(cache:IndexCache<Dynamic>, predicate:RelationalPredicate):IIndex<Dynamic, Dynamic> {
      var test:IIndex<Dynamic, Dynamic> -> Bool = (idx -> false);
      switch predicate.left.expr {
         case TColumn(_.label()=>columnName, _):
            return cast cache.index(columnName);

         default:
            var bin = getBinaryOperationFromRel(predicate);
            var e = new TExpr(TBinop(bin.op, bin.left, bin.right));
            throw new pm.Error('Unexpected ${e.print()}');
      }

      for (idx in cache.m) {
         if (test(cast idx))
            return cast idx;
      }

      return null;
   }

   inline function getBinaryOperation(e: TExpr):BinaryOperation {
      return switch e.expr {
         case TBinop(op, l, r): {op:op, left:l, right:r};

         default: null;
      }
   }

   inline function printBinaryOperation(o: BinaryOperation):String {
      return te(TBinop(o.op, o.left, o.right)).print();
   }

   inline function getRelFromBinaryOperation(b: BinaryOperation):RelationalPredicate {
      final op:RelationPredicateOperator = switch b.op {
         case OpEq: Equals;
         case OpNEq: NotEquals;
         case OpGt: Greater;
         case OpGte: GreaterEq;
         case OpLt: Lesser;
         case OpLte: LesserEq;
         case OpIn: In;
         default:
            throw new pm.Error('Unexpected ${b.op}');
      };
      return new RelationalPredicate(op, b.left, b.right);
   }

   inline function getBinaryOperationFromRel(p: RelationalPredicate):BinaryOperation {
      return {
			op: switch p.op {
				case Equals: BinaryOperator.OpEq;
				case NotEquals: OpNEq;
				case Greater: OpGt;
				case Lesser: OpLt;
				case GreaterEq: OpGte;
				case LesserEq: OpLte;
				case In: OpIn;
         },
         left: p.left,
         right: p.right
      };
   }

   inline function getColumnAndConst(l:TExpr, r:TExpr, ?test:TypedValue->Bool) {
      if (test == null)
         test = v -> true;
      return switch [l.expr, r.expr] {
         case [TColumn(_.label()=>columnName, _), TConst(typed)]:
            {
               columnExpr: l,
               constExpr: r,
               constValue: typed,
               columnName: columnName
            };

         case [TConst(_), TColumn(_, _)]: getColumnAndConst(r, l, test);
         default: null;
      }
   }

   function getBinopConst(binop: BinaryOperation):Option<TypedValue> {
      return switch [binop.left.expr, binop.right.expr] {
         case [TConst(value), _]: Some(value);
         case [_, TConst(value)]: Some(value);
         default: None;
      }
   }

   /**
    * checks if the given expression refers to a column
    * @param e 
    * @return Bool
    */
   inline function isColumn(e: TExpr):Bool {
      return e.expr.match(TColumn(_,  _));
   }

   static inline function te(e: TExprType):TExpr {
      return new TExpr(e);
   }
}

@:generic
class Bin<L, R> {
   public var left: L;
   public var right: R;
   public function new(left, right) {
      this.left = left;
      this.right = right;
   }
}

typedef BinaryOperation = {
   public var op: BinaryOperator;
   public var left: TExpr;
   public var right: TExpr;
};

typedef ConstBinaryOperation = {
   public final op: BinaryOperator;
   public final left: TypedValue;
   public final right: TypedValue;
   // @:optional public var result: TypedValue;
};