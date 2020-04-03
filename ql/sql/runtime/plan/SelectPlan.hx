package ql.sql.runtime.plan;

import pm.map.*;
// import pm.map.AnySet.IMapSet;
// import pm.map.AnySet.OrderedAnySet;
// import pm.map.ISet;
import ql.sql.runtime.Sel;
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
import ql.sql.common.SqlSchema;

import tink.Anon.*;

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
class SelectPlan<Tbl, InRow, OutRow> {
   public final stmt:SelectStmt;
   public var planner: Planner = null;

	public function new(stmt:SelectStmt) {
		this.stmt = stmt;
	}

	/**
	 * get an iterator of all rows which are to be considered for yielding
	 * @return Iterator<InRow>
	 */
	public function candidates():Itr<InRow> {
		throw new pm.Error.NotImplementedError();
   }

   @:keep
   public function toString():String {
      return '<plan tracing TBI>';
   }
}

/**
 * 
 * (default) select plan which uses the completely naive execution strategy
 */
@:access(ql.sql.runtime)
class DefaultPlan<Tbl, In, Out> extends SelectPlan<Tbl, In, Out> {
   public function new(stmt) {
      super(stmt);
   }
   override function candidates():Itr<In> {
      // var arr:Array<In> = cast stmt.i.applyNaive(stmt);
      // return arr.iterator();
      return stmt.i.naiveCandidatePlan(stmt, null);
   }
}

@:yield
class IndexedPlan<Tbl, In, Out> extends SelectPlan<Tbl, In, Out> {
	public var index:IIndex<Dynamic, In>;
	public var query:IndexQuery;

	public function new(stmt, idx, q) {
		super(stmt);
		this.index = idx;
		this.query = q;
   }
   
   /**
    * applies the provided `IndexQuery` to the actual Index data structure
    * @return `Itr<In>` resettable iterator over results
    */
   override function candidates():Itr<In> {
      final idx:IIndex<Dynamic, In> = index;
      switch query.type {
         case Equals:
            throw new pm.Error.NotImplementedError();
         case NotEquals:
            throw new pm.Error.NotImplementedError();
         case Greater:
            throw new pm.Error.NotImplementedError();
         case Lesser:
            throw new pm.Error.NotImplementedError();
         case GreaterEq:
            throw new pm.Error.NotImplementedError();
         case LesserEq:
            throw new pm.Error.NotImplementedError();
         case In:
            throw new pm.Error.NotImplementedError();
      }
   }
}

/**
  static functional utilities for working with `Itr<?>` objects
 */
@:yield
private class Tools {
   /**
    * joins the two given iterators, ensuring that the returned iterator won't yield the same document more than once
    * @param schema 
    * @param a 
    * @param b 
    * @return Iterator<Row>
    */
   public static function combine<Row>(schema:SqlSchema<Row>, a:Iterator<Row>, b:Iterator<Row>):Iterator<Row> {
      return a.append(b);
   }

   public static function union<Row>(schema:SqlSchema<Row>, a:Iterator<Row>, b:Iterator<Row>):Iterator<Row> {
      return a.append(b);
   }

   /**
    * performs slicing, if necessary, on the given iterator
    * @param schema 
    * @param itr 
    * @param limit 
    * @param offset 
    * @return Iterator<Row>
    */
   public static function limit<Row>(schema:SqlSchema<Row>, itr:Iterator<Row>, limit:Int=-1, offset:Int=0):Iterator<Row> {
      return itr;
   }

   // public static function createSet<Row:{}>(schema:SqlSchema<Row>, t:Int=0):ISet<Row> {
   //    var prim = schema.fields.primary;
   //    if (prim == null) 
   //       return new pm.map.ObjectSet<Row>();
   //    return IMapSet(row->Reflect.field(row, prim.field.name), new AnyMap());
   // }
}

class AnyMap<T> extends OrderedMap<Dynamic, T> {
   public function new() {
      super(pmdb.core.Arch.compareThings);
   }
}