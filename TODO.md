
## Roadmap

 - Integrate ql.sql.runtime.plan.Planner with SQL runtime
 - Complete TExpr typing in Compiler
   - Consider moving typing system from Compiler into its own Typer module
 - Fix parameter binding
   - Implement "reification" system
 - Use IMap-based IIndex implementation for unique indexes
 - Index by expression
 - Computed columns (Generated Columns)
   - Virtual (default)
   - Stored
 - Stored query (SQL View)
   - Computed
   - Live (Subscribed to changes to the database which would alter the content of the query result)
 - Generation of JavaScript functions at runtime for expression computation

## Syntax / Specification
 - BETWEEN operator
 - CREATE TABLE stmt
 - DELETE stmt
 - ALTER TABLE stmt