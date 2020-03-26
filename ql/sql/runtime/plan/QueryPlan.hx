package ql.sql.runtime.plan;

interface QueryPlan {}

enum TableScanPlan {
   FullTableScan(?filter: Dynamic);
}