package ql.sql.runtime.plan;

import ql.sql.runtime.plan.SelectPlan;
import ql.sql.runtime.Sel.SelectImpl;
import pm.ImmutableList;
import pm.iterators.*;
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

import tink.Anon.splat;
import tink.Anon.merge;

import pm.Helpers.*;

using Lambda;
using pm.Arrays;
using pm.Iterators;
using pm.Functions;

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

   var selectStmt:Null<SelectStmt> = null;

   public function plan(q: Stmt<Dynamic>) {
      //
      switch q.type {
         case SelectStatement(stmt):
            selectStmt = stmt;
            stmt = planSelectStmt(stmt);
            //
            return new Stmt(stmt, SelectStatement(stmt));

         case UpdateStatement(stmt):
            throw new pm.Error('Unhandled ${q.type}');

         case InsertStatement(stmt):
            throw new pm.Error('Unhandled ${q.type}');

         case CreateTable(stmt):
            throw new pm.Error('Unhandled ${q.type}');
      }
   }

   function compileItrPlan(plan: {?fn:Void->Iterator<Dynamic>, scan:Scan}) {
      plan.fn = switch plan.scan {
         case STable(n): switch n {
            case FullTableScan:
               () -> new EmptyIterator();
            case Indexed(index, type):
               function() {
                  return new EmptyIterator();
               }
         }
         case SResultSet(n):
            var sel = this.selectStmt.unwrap();
            switch n {
               case ComputeArray:
                  //TODO
               case Coroutine:
                  //TODO
            }
            ()->new EmptyIterator();
      }
   }

   inline static function overwrite<T>(a:T, f:T->T, updated:(T, T)->Void, ?equality:T->T->Bool) {
      var tmp = a;
      a = f(tmp);
      if (equality==null?a!=tmp:!equality(a, tmp))
         updated(tmp, a);
      return a;
   }

   /**
    * computes a SelectPlan which can be used to plot the function which carries out the query
    * @param stmt 
    */
   function planSelectStmt(stmt: SelectStmt):SelectStmt {//Plan<Dynamic, Dynamic, Dynamic> {
      var isSimpleSelect = false,
          isJoinSelect = false;
      
      var tableHandle = stmt.context.sources.sources[0];
      var table = try compiler.context.resolveTableFrom(tableHandle) catch (e: Dynamic) null;
      var cache = table != null ? compiler.context.glue.tblGetIndexCache(table) : null;

      var result = {
         scan: Scan.STable(TableScan.FullTableScan),
         fn: function():Iterator<Dynamic> {
            return new EmptyIterator();
         }
      };

		if (cache == null) {
         return stmt;
      }
      else for (idx in cache.m) {
         Console.examine(idx.indexType, idx.keyType);
      }

      // compute the predicate expression
      var predicate:SelPredicate = stmt.i._predicate;
      if (predicate != null) {
         var tmp = predicate;
         predicate = simplifyPredicate(predicate);
         if (tmp != predicate) {
            examine(tmp, predicate);
            compiler.compileTPred(predicate);
         }
      }

      if (predicate != null) switch predicate.type {
         case Rel(relation):
            var cc = getColumnAndConst(relation.left, relation.right);
            var indexes:List<IIndex<Dynamic, Dynamic>> = getIndexHandleFromBinaryPredicate(cache, relation);
            if (indexes != null) {
               var index = null;
               switch indexes.length {
                  case 0:
                     throw new pm.Error('Empty index-list');
                  case 1:
                     index = indexes.first();
                  default:
                     throw new pm.Error('Unhandled multi-index candidacy');
                     index = indexes.last();
               }

               if (cc != null && index != null) {
                  result.scan = STable(TableScan.Indexed(index.unwrap(), {
                     type: relation.op,
                     value: IndexQueryOperand.Const(cc.constValue)
                  }));
               }
            }

         default:
      }

      // debug(result);
      examine(result.scan);
      // compileItrPlan(result);

      var plan = switch result.scan {
         case STable(Indexed(index, query)):
            new IndexedPlan(stmt, index, query);
         // case SResultSet(n):
         default:
            new DefaultPlan(stmt);
      };

      var i = new SelectImpl(null, predicate, stmt.i._exporter);
      var newStmt = new SelectStmt(stmt.context, stmt.source, i);
      newStmt.plan = plan;
      newStmt.plan.planner = this;
      examine(newStmt.plan);

      // return result;
      return newStmt;
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
                  // column + leftValue = rightValue
                  case {op:OpAdd, left:{expr:TColumn(_.label()=>column, _)}, right:{expr:TConst(leftValue)}}:
                     var newRhv:TypedValue = BinaryOperators.top_subt(rightValue, leftValue);
                     examine(newRhv);
                     return {op:bin.op, left:leftOp.left, right:te(TConst(newRhv))};

                  // column - leftValue = rightValue
                  case {op:OpSubt, left:{expr:TColumn(_.label()=>column, _)}, right:{expr:TConst(leftValue)}}:
                     // 
                     var newRhv:TypedValue = BinaryOperators.top_add(rightValue, leftValue);
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
               error('Did not simplify: ' + printBinaryOperation(bin));
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

   private var simplifier: Null<Simplifier> = null;
   function simplifyExpressionViaRuleset(e: TExpr):TExpr {
      if (simplifier == null)
         simplifier = new Simplifier(this);

      return simplifier.simplify(e);
   }

   /**
    * attempts to look up a list of the `IIndex<K, Row>` objects suitable for use in executing the filter-operation represented by `predicate`
    * @param cache the `IndexCache<?>` in use by the medium from which the queries is reading
    * @param predicate the filter-op being applied to the RowSet represented in `cache`
    * @return List<IIndex<Dynamic, Dynamic>> of candidate indices
    */
   private function getIndexHandleFromBinaryPredicate(cache:IndexCache<Dynamic>, predicate:RelationalPredicate):List<IIndex<Dynamic, Dynamic>> {
      var results = new List();
      // var rules:Array<{
      //    id: Int,
      //    match: (idx:IIndex<Dynamic, Dynamic>)->Bool
      // }> = new Array();

      // var ruleIdCnt:Int = 0;
		// function rule(?id:Int, r:(index:IIndex<Dynamic, Dynamic>)->Bool){
      //    final o = {id:-1, match:r};
      //    if (id != null) o.id = id;
      //    else o.id = ruleIdCnt++;

      //    rules.push(o);
      // }

      // var test:IIndex<Dynamic, Dynamic> -> Bool = (idx -> false);

      switch [predicate.left.expr, predicate.right.expr] {
         case [TColumn(_.label()=>columnName, _), _]:
            results.add(cache.index(columnName));
            // rule((idx:IIndex<Dynamic,Dynamic>) -> idx.)

         // case [TCall({expr:TFunc({id:'int'})}, [{expr:TBinop(op, left, right)}])]:
            //TODO

         default:
            var bin = getBinaryOperationFromRel(predicate);
            var e   = new TExpr(TBinop(bin.op, bin.left, bin.right));
            
            Console.error('Unexpected ${e.print()}');
      }

      // for (idx in cache.m) {
      //    // if (test(cast idx))
      //    //    return cast idx;
      //    for (rule in rules) {
      //       if (rule.match(idx)) {
      //          results.add({
      //             rule: rule,
      //             index: idx
      //          });
      //       }
      //    }
      // }

      return results.length == 0 ? null : results;
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

class Simplifier {
   public final planner: Planner;
   public var rules: Array<ExprRule>;

   public function new(planner) {
      this.planner = planner;

      // initRules();
   }

   function initRules() {
      inline function add(rule: ExprRule) {
         rules.push(rule);
      }

      function rule(f: TExpr->Option<TExpr>) {
         for (rule in rules) {
            if (Reflect.compareMethods(rule.apply, f))
               throw new pm.Error();
         }
         return new ExprRule(f);
      }

      function safe_rule(f: TExpr->TExpr) {
         return rule(function(e: TExpr):Option<TExpr> {
            try {
               return Some(f(e));
            }
            catch (code: Int) {
               switch code {
                  case 0:
                     return None;

                  default:
                     throw new pm.Error('Error code = $code');
               }
            }
            catch (err: Dynamic) {
               error(err);
               return None;
            }
         });
      }

      function rrule<Data>(test:TExpr->Bool, extract:TExpr->Data, export:Data->TExpr) {
         return safe_rule(function(e) {
            if (test(e)) {
               return export(extract(e));
            }
            throw 0;
         });
      }

      // rrule(e -> e.expr.match(TBinop(OpEq|OpNEq, _, _)), e -> e.matchFor({expr:TBinop(OpEq|OpNEq, {expr:TBinop(innerOp, lhv, rhv)}, outerRhv)}, {}), )
      add(rule(function(e: TExpr) {
         switch e.expr {
            case TBinop(OpEq, lhv, rhv={expr:TColumn(_, _)}):
               return Some(new TExpr(TBinop(OpEq, rhv, lhv)));

            default:
         }
         return None;
      }));
   }

   public function simplify(e: TExpr):TExpr {
      /* var candidates = applyRules(e);
      if (candidates.length > 1) {
         throw new pm.Error('Multiple candidate expressions');
      }
      return candidates[0]; */
      switch e.expr {
         case TConst(_):
         case TReference(_):
         case TParam(_):
         case TTable(_):
         case TColumn(_, _):
         case TField(o, field):
            var o2 = simplify(o);
            if (o2 != o)
               return te(TField(o2, field));
         case TArray(arr, index):
            var arr2 = simplify(arr), idx2 = simplify(index);
            if (arr != arr2 || index != idx2)
               return te(TArray(arr2, idx2));
         case TFunc(_):
         case TCall(f, params):
            var f2 = simplify(f);
            var params2 = [for (p in params) simplify(p)];
            var replace = f != f2;
            if (!replace) for (i in 0...params2.length) {
               if (params[i] != params2[i]) {
                  replace = true;
                  break;
               }
            }
            if (replace)
               return te(TCall(f2, params2));

         case TBinop(op, left, right):
            switch op {
               case OpEq:
               case OpGt:
               case OpGte:
               case OpLt:
               case OpLte:
               case OpNEq:
               case OpIn:
               case OpMult:
               case OpDiv:
               case OpMod:
               case OpAdd:
               case OpSubt:
               case OpBoolAnd:
               case OpBoolOr:
               case OpBoolXor:
            }

         case TUnop(op, post, e):
            //TODO
         case TArrayDecl(values):
            //TODO
         case TObjectDecl(fields):
            //TODO
         case TCase(type):
            //TODO
      }

      return e;
   }

   function applyRules(e: TExpr):Array<TExpr> {
      var result:ImmutableList<TExpr> = ImmutableList.Tl;
      for (rule in rules) {
         switch rule.apply(e) {
            case None:
               continue;
            case Some(v):
               result = v & result;
         }
      }
      return result;
   }

   function te(e: TExprType):TExpr {
      return new TExpr(e);
   }
}

class ExprRule extends AbstractRule<TExpr, TExpr> {}

@:generic
class AbstractRule<In, Out> {
   public var apply: In->Option<Out>;
   public function new(f) {
      this.apply = f;
   }
}
