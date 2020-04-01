package ql.sql.runtime.plan;

import ql.sql.runtime.TAst;
import pm.LinkedStack;
import ql.sql.common.TypedValue;
import ql.sql.common.index.IIndex;

interface QueryPlan {}

enum TableScan {
   FullTableScan/*(table)*/;
   Indexed(index:IIndex<Dynamic, Dynamic>, type:IndexQueryType);
   // IndexedEq(column:String, constant:Dynamic);
   // IndexedIn(column:String, constant:Dynamic, rtl:Bool);
}

enum IndexQueryType {
   Equals(value: IndexQueryOperand);
	NotEquals(value: IndexQueryOperand);
	Greater(value: IndexQueryOperand);
	Lesser(value: IndexQueryOperand);
	GreaterEq(value: IndexQueryOperand);
	LesserEq(value: IndexQueryOperand);
	In(value: IndexQueryOperand);
}

enum IndexQueryOperand {
   Const(value: TypedValue);
   Expr(e: TExpr);
}

enum ResultSetScan {
   ComputeArray;
   Coroutine;
}

enum Scan {
   STable(n: TableScan);
   SResultSet(n: ResultSetScan);
}

class Strategy {
   public var scan: Scan;

   public function new() {
      //TODO
   }
}