
import ql.sql.runtime.Sel.TableSourceItem;
import haxe.ds.Either;
import pmdb.core.Object;
import pmdb.core.Arch;

import ql.sql.TsAst;
import ql.sql.grammar.CommonTypes;
import ql.sql.grammar.expression.Expression;
import ql.sql.ast.Query;
import ql.sql.ast.Query.TableSpec as TableRef;

import ql.sql.runtime.VirtualMachine;
import ql.sql.runtime.TAst;
import ql.sql.common.TypedValue;
import ql.sql.common.SqlSchema;
import ql.sql.common.internal.ImmutableStruct;

import ql.sql.runtime.sel.SelectStmtContext;
import ql.sql.runtime.Stmt;
import ql.sql.runtime.Stmt.CSTNode;
import ql.sql.runtime.Stmt.SelectStmt;
import ql.sql.runtime.Sel.TableStream;
import ql.sql.runtime.Sel.TableSource as TblSrc;
import ql.sql.runtime.Sel.TableSourceItem as TblSrcItem;
// import ql.sql.runtime.Sel.TableRef;
import ql.sql.runtime.Sel.TableJoin;
import ql.sql.runtime.Sel.Aliased;
import ql.sql.runtime.Sel.SelectImpl;

using Lambda;
using pm.Arrays;
using StringTools;
using pm.Strings;

using pm.Functions;

class Compiler extends SqlRuntime {
    public var context(default, set): Null<Context<Dynamic, Dynamic, Dynamic>> = null;

    public function new(?ctx) {
        // this.context = new Context();
        super();
        this._ctx = ctx;
        this.context = ctx;
    }

    public var glue(get, never):Glue<Dynamic, Dynamic, Dynamic>;
    private inline function get_glue():Glue<Dynamic, Dynamic, Dynamic> return this.context.glue;

	private function set_context(c:Null<Context<Dynamic, Dynamic, Dynamic>>) {
        this._ctx = c;
        this.context = c;

        if (this.context != null) {
            for (n in this.context.glue.dbListTables(this.context.database)) {
                loadTable(n);
            }
            // for (tbl in tables) {
            //     var schema = context.glue.tblGetSchema(tbl);

            // }
        }
        return c;
    }

    override function loadTable(name:String):Null<Dynamic> {
        var table = super.loadTable(name);
        if (table != null) {
            var schema = context.glue.tblGetSchema(table);
            visitSqlSchema(schema);
        }
        return table;
    }

    public function compile(stmt: Query):Stmt<Dynamic> {
        switch stmt {
            case Select(select):
                var sel = compileSelectStmt(select);
                return new Stmt(sel, SelectStatement(sel));
            case Insert(insert):
                throw new pm.Error('TODO');
            case CreateTable(createTable):
                throw new pm.Error('TODO');
        }
    }

    function visitSqlSchema(schema: SqlSchema<Dynamic>) {
        schema.context = this.context;
        final processSchema = schema.mode.match(RowMode);

		if (processSchema) for (field in schema.fields) {
			if (field.defaultValueString != null && field.defaultValueExpr == null) {
				var e = VMParser.readExpression(field.defaultValueString);
				var e = this.compileExpression(e);
				field.defaultValueExpr = e;
			}
        }

        if (processSchema && schema.indexing != null) for (idx in schema.indexing.indexes) {
            if (idx.extractor == null && idx.extractorSql != null) {
                if (idx.extractorExpr == null) {
					var e = VMParser.readExpression(idx.extractorSql);
                    var e = this.compileExpression(e);
                    idx.extractorExpr = e;
                    var c = new Context(context.glue);
                    c.unaryCurrentRow = true;
                    var self = {context:c};
                    idx.extractor = function(row: Doc):Dynamic {
                        c.focus(row);
                        return e.eval(self);
                    };
                }
            }
        }
    }

    /**
     * compiles/preprocesses SelectStmt instances
     * @param select 
     * @return SelectStmt
     */
    function compileSelectStmt(select : SelectStatement):SelectStmt {
        var query = select.query.query;
        if (query.from == null)
            throw new pm.Error.WTFError();

        var csrcList:Array<TblSrc> = [for (tbl in query.from.tables) tableSourceConvert(tbl)];
        var csrc:TblSrc = null;
        switch csrcList {
            case [one]:
                csrc = one;

            default:
                throw new pm.Error('Unhandled $csrcList');
        }
        
        var querySource = [];
        var thisSource = csrc.getSpec(context);
        // trace(thisSource);
        querySource.push(thisSource);

        if (csrc.joins != null) {
            for (j in csrc.joins) {
                var qs = j.mJoinWith.unwrap().src.unwrap();
                if (qs.src != null)
                    querySource.push(qs.src);
                querySource.push(qs);
            }
        }

		// for (x in querySource)
            // context.addQuerySource(x);
        context.pushSourceScope(querySource);
        
        var predicate = query.from.where != null ? compileSelectPredicate(query.from.where.term) : null;
        
        var items:Array<SelItem> = [for (el in query.elements) compileSelectElement(el)];
        var out:SelOutput = items;
        // var sourceList:Array<TableSpec> = computeReferencedSources()
        compileSelOutput(out, querySource);
        
        var selectCtx = new SelectStmtContext(this.context, querySource);
        // selectCtx.sources = querySource.copy();

        var result = new SelectStmt(
            selectCtx,
            csrc,
            new SelectImpl(select, predicate, out)
        );
        var pre = result.evalHead;
		
        pre.push(g -> {
            g.context.pushSourceScope(cast result.context.sources.sources);
            g.context.beginScope();
            g.context.use(thisSource);
        }); 
        result.evalTail.push(g -> {
            g.context.popSourceScope();
            g.context.endScope();
        });

        if (query.orderBy != null) {
            context.pushSourceScope([new TableSpec('_', result.resultSchema, {stmt:result})]);
            result.i.order = query.orderBy.expressions.map(function(o) {
                return {
                    accessor: {
                        // var tmp = context.unaryCurrentRow;
                        // context.unaryCurrentRow = true;
                        context.use(context.querySources[0]);
                        final e = compileExpression(o.expression);
                        // context.unaryCurrentRow = tmp;
                        e;
                    },
                    direction: switch o.sortType {
                        case Asc: 1;
                        case Desc: -1;
                    }
                };
            });
            context.popSourceScope();
        }

        context.popSourceScope();

        return result;
    }

//{region tablesourceitem

	inline function toTblSrcItem(i:SelectSourceItem):TblSrcItem {
		var res = sourceItemConvert(i);
		compileTblSrcItem(res);
		return res;
	}

    function tableSourceConvert(src: SelectSource):TblSrc {
        var item = toTblSrcItem(src.sourceItem);

        // var csrc:TblSrc = new TblSrc(Table(new Aliased(alias, new TableRef(tableSpec.tableName.identifier, table))));
        var csrc = new TblSrc(item);
		var joins = src.joins;
		if (joins != null) {
			var cjoins:Array<TableJoin> = new Array();
			for (join in joins) {
                var cjoin = new TableJoin(join.joinType, toTblSrcItem(join.joinWith));
                var tmp = new TblSrc(cjoin.joinWith);
                var joinSrc = tmp.getSpec(context);
                context.resolveTableFrom(joinSrc);
                cjoin.mJoinWith = {src:joinSrc};
                
                if (join.on != null) {
                    context.pushSourceScope([csrc.getSpec(context), joinSrc]);
                    cjoin.on = compileSelectPredicate(cast join.on);
                    context.popSourceScope();
                }
                
				if (join.usingColumns != null) {
					throw new pm.Error('TODO');
				}
				cjoins.push(cjoin);
			}
			csrc.joins = cjoins;
        }
        
        return csrc;
    }

    function sourceConvert(src: SelectSource) {
        throw src;
    }

    function sourceItemConvert(item:SelectSourceItem):TblSrcItem {
        var ssi = TblSrcItem;
        switch item {
            case Aliased(src, alias):
                var inner = sourceItemConvert(src);
                switch inner {
                    case Table({term:v}):
                        return ssi.Table(new Aliased(alias, v));
                    case Stream({term:v}):
                        return ssi.Stream(new Aliased(alias, v));
                }

            case Table(table): 
                return ssi.Table(new Aliased(null, context.getSource(table.tableName)));

            case Subquery(query):
                var stream = new TableStream(compileSelectStmt(query));
                stream.schema = stream.select.resultSchema;
                stream.open = function(g: Contextual):Iterator<Dynamic> {
                    return stream.select.eval().iterator();
                };
                return ssi.Stream(new Aliased(null, stream));
            
            case Expr(e):
                throw new pm.Error('TODO: Expr as select source');
        }

        throw new pm.Error('Conversion failed: $item');
    }

    private var targetQuerySource:Null<TableSpec> = null;
    
#if brozen
    function tableSourceItemConvert(item: SelectSourceItem):TblSrcItem {
        var alias:Null<String> = null;
        var tableSpec:Null<TableSpec> = null;
        final node:SqlAstNode = item;
        var tableStream:Null<TableStream> = null;
        
        if ((node is ql.sql.TableSpec)) {
            tableSpec = cast node;
        }

        if ((node is ql.sql.AliasedTerm<Dynamic>)) {
            var aliased:AliasedTerm<Dynamic> = cast node;
            var term:SqlAstNode = aliased.term;
            alias = aliased.alias.identifier;

            if ((term is ql.sql.TableSpec)) {
                tableSpec = (term : SelectSourceItem).toTableSpec();
            }

            if ((term is ql.sql.NestedSelectStatement)) {
                var subStmt:NestedSelectStatement = cast term;
                var stmt = compileSelectStmt(subStmt.statement);

                tableStream = new TableStream();
                tableStream.astNode = term;
                tableStream.schema = stmt.resultSchema;
                tableStream.open = function(g) {
                    return stmt.eval().iterator().map(function(row: Dynamic) {
                        g.context.focus(row, alias);
                        return row;
                    });
                };
            }
        }

        if ((node is ql.sql.NestedSelectStatement)) {
            var subStmt:NestedSelectStatement = cast node;
            var stmt = compileSelectStmt(subStmt.statement);
			tableStream = new TableStream();
			tableStream.astNode = subStmt;
			tableStream.schema = stmt.resultSchema;
			tableStream.open = function(g) {
				return stmt.eval().iterator().map(function(row:Dynamic) {
                    g.context.focus(row, '_');
					return row;
				});
			};
        }

        if (tableSpec != null) {
            var table = context.resolveTableFrom(context.getSource(tableSpec.tableName.identifier));

            if (table == null) throw new pm.Error();

            return TblSrcItem.Table(new Aliased(alias, new TableRef(tableSpec.tableName.identifier, table)));
        }

        if (tableStream != null) {
            return TblSrcItem.Stream(new Aliased(alias, (tableStream : TableStream)));
        }

        throw new pm.Error.ValueError(item);
    }
#end

    function compileTblSrcItem(item: TblSrcItem) {
        // Console.log(item);
        switch item {
            case Table({term:{table:table}}):
                var schema = glue.tblGetSchema(table);
                visitSqlSchema(schema);

            case Stream(a={term:{schema:schema}}):
                visitSqlSchema(schema);
        }
    }
//}endregion

//{region select_element
    function compileSelectElement(element: SelectElement):SelItem {
        return switch element {
            case AllColumns(table): SelItem.All(symbol(table));
            case Column(table, column, alias): 
                SelItem.Column(symbol(table), symbol(column), if (alias == null) null else symbol(alias));
            case Expr(e, null): SelItem.Expression(null, compileExpression(e));
            case Expr(e, alias): SelItem.Expression(symbol(alias), compileExpression(e));
        }
        /*
        switch element {
            case AllColumns(el):
                // resolveTableSymbol(el);
                return SelItem.All(el.table);
            
            case Column(table, el, null):
                // resolveTableSymbol(el);
                return SelItem.Column(el.table, el.name);

            case FunctionCall(el):
                if ((el is ql.sql.SimpleFunctionCall)) {
                    var fc:ql.sql.TsAst.SimpleFunctionCall = cast el;
                    
                    // return SelItem.
                    throw new pm.Error('TODO');
                }
                throw new pm.Error('TODO');
            
            case Expression(el):
                var expr = compileExpression(el);
                return SelItem.Expression(null, expr);
            
            case AssignedTerm(el):
                throw new pm.Error('TODO');
            
            case AliasedTerm(el):
                switch el {
                    case ColumnName(el):
                        resolveTableSymbol(el.term);
                        return SelItem.Column(el.term.table, el.term.name, el.alias);
                    
                    case Expression(el):
                        // throw new pm.Error('TODO');
                        var expr = compileExpression(el.term);
                        return SelItem.Expression(el.alias, expr);

                    case FunctionCall(el):
                        throw new pm.Error('TODO');

                    case AssignedTerm(el):
                        throw new pm.Error('TODO');

                }
            
            default:
                throw new pm.Error('Unhandled $element');
        }
        */
    }

    /**
     * converts a `SelItem` value into a lambda function
     * @param item 
     * @return f (g:{context, out:`ImmutableStruct<TypedValue>`})->`ImmutableStruct<TypedValue>`
     */
    function compileSelItem(item: SelItem):(g:{context:Context<Dynamic, Dynamic, Dynamic>}, out:Doc)->Doc {
        inline function s(sym: SqlSymbol) return if (sym != null) sym.identifier else null;

        return switch item {
            case All(table):
                // resolveTableToSymbol(table);
                var schema = getTableSchema(table.identifier);

                function(g, out:Doc):Doc {
                    var row:Doc = g.context.getCurrentRow(s(table));
                    for (field in schema.fields) {
                        // out = out.append(field.name, (row[field.name] : TypedValue));
                        out[field.name] = row[field.name];
                    }
                    return out;
                }
            
            case Column(table, column, alias):
                var outKey = alias.nor(column);
                function(g, out:Doc):Doc {
                    var row:Null<Doc> = g.context.getCurrentRow(s(table));
                    final row:Doc = row.unwrap();
                    final value:Dynamic = exportValue(row.get(s(column)));

                    out[outKey.identifier] = value;
                    return out;
                }
            
            case Expression(alias, expr):
                var key:String;
                if (alias == null) {
                    key = expr.print();
                }
                else {
                    key = alias.identifier;
                }

                function(g, out:Doc):Doc {
                    // return out.append(key, TypedValue.ofAny(expr.eval(g)));
                    out[key] = exportValue(expr.eval(g));
                    return out;
                }
        }
    }
//}endregion

    inline function exportValue(value: Dynamic):Dynamic {
        if (TypedValue.is(value)) {
            return (untyped value : TypedValue).export();
        }
        else {
            return value;
        }
    }

	function compileSelOutSchema(out:SelOutput, sourcesList:Array<TableSpec>) {
        var sourceList = [for (x in out.items) switch x {
            case All(_.label()=>table) if (table != null): context.getSource(table);
            case Column(_.label() => table, _, _) if (table != null): context.getSource(table);
            case Expression(_, _), All(_), Column(_): null;
        }].filter(x -> x != null);
        var sources = [for (src in sourcesList) src.name => src];
        var columns = new Array();

        for (item in out.items) {
            switch item {
                case All(null):
                    for (src in sources) {
                        for (c in src.schema.fields) {
                            columns.push(c.getInit());
                        }
                    }
                
                case All(table):
					var source = sources[table.label().unwrap()].unwrap(new pm.Error('`${table.label()}` not found'));
                    var schema = source.schema;
                    for (c in schema.fields) {
                        columns.push(c.getInit());
                    }

                case Column(table, column, alias):
                    // final col = context.glue.tblGetSchema(table.table).column(column.identifier).getInit();
                    var ckey = column.identifier;
                    inline function cinit(c: SchemaField) {
                        var r = c.getInit();
                        if (alias.label() != null)
                            r.name = alias.identifier;
                        return r;
                    }

                    if (table.label() == null) {
                        for (src in sources) {
                            var c = src.schema.column(ckey);
                            if (c == null) continue;

                            columns.push(cinit(c));
                        }
                    }
                    else {
                        var src = sources[table.label()].unwrap(new pm.Error('`${table.label()}` not found'));
                        var c = src.schema.column(column.identifier).unwrap(new pm.Error('`${table.label()}`.`${column.identifier}` not found'));
                        columns.push(cinit(c));                        
                    }

                case Expression(alias, expr):
                    var name:String = alias.label();
                    if (alias == null) {
                        // throw new pm.Error('Alias must be defined');
                        name = expr.print();
                    }

                    columns.push({
                        name: name,
                        type: expr.type,
                        notNull: false,
                        unique: false,
                        autoIncrement: false,
                        primaryKey: false
                    });
            }
        }

        var schema = new SqlSchema({
            mode: 'object',
            fields: columns
        });

        out.schema = schema;
        visitSqlSchema(out.schema);

        return schema;
    }

    function compileSelOutput(exporter:SelOutput, sources:Array<TableSpec>) {
        var schema = compileSelOutSchema(exporter, sources);
        switch exporter.items.toArray() {
            case [All(table)]:
                exporter.mCompiled = function(g) {
                    // g.context.currentDefaultTable = table.identifier;
                    return g.context.getCurrentRow(table.label());
                };
                return ;

            default:
        }
        
        var items = exporter.items.map(item -> compileSelItem(item)).toArray();
        items.reverse();

        var f = function(g) {
            var out:Doc = new Doc();
            for (mut in items)
                out = mut(g, out);
            out = schema.induct(out);
            return out;
        };
        exporter.mCompiled = f;
    }

    function compileSelectPredicate(predicate: Predicate):SelPredicate {
        var pred = this.predicateNodeConvert(predicate);
        compileTPred(pred);
        return pred;
    }

    function compileTPred(pred: SelPredicate) {
		switch pred.type {
			case Rel(relation):
				// TODO
				switch relation.op {
					case Equals:
					// TODO
					case NotEquals:
					// TODO
					case Greater:
					// TODO
					case Lesser:
					// TODO
					case GreaterEq:
					// TODO
					case LesserEq:
					// TODO
					case In:
						// TODO
				}
				compileTExpr(relation.left);
				compileTExpr(relation.right);
				pred.mCompiled = g -> relation.eval(g);

			case And(subs):
				for (p in subs)
                    compileTPred(p);
                    pred.mCompiled = function(g) {
					for (predicate in subs)
						if (!predicate.eval(g))
							return false;
					return true;
				};

			case Or(subs):
				for (p in subs)
					compileTPred(p);
				pred.mCompiled = function(g) {
					for (predicate in subs)
						if (predicate.eval(g))
							return true;
					return false;
                };
                
            case Not(sub):
                compileTPred(sub);
                var tmp = sub.mCompiled;
                pred.mCompiled = g -> !tmp(g);
		}
    }

    /**
      compiles the given expression into some shit
     **/
    extern inline private function compileExpression(e: Expr) {
        var expr = expressionNodeConvert(e);
        compileTExpr(expr);
        return expr;
    }

    /**
     [TODO]
     - `TArray(arr:TExpr, index:TExpr)`
     - `TArrayDecl(values: Array<TExpr>)`
     - `TObjectDecl(fields: Array<{field:String, value:TExpr}>)`
   **/
    var parameters:Map<String, Array<TExpr>> = new Map();
    private function compileTExpr(e: TExpr) {
        switch e.expr {
            case TConst(value):
                e.mConstant = value.value;
                e.mCompiled = exprc(e);
            
            // case TTable(name):
            //     e.mCompiled = exprc(e);
            
            case TColumn(name, table):
                e.mCompiled = exprc(e);

            case TField(o, _):
                compileTExpr(o);
                e.mCompiled = exprc(e);

            case TArray(array, index):
                //Console.examine(array, index);
                compileTExpr(array);
                compileTExpr(index);
                e.mCompiled = exprc(e);

            case TCall(f, args):
                compileTExpr(f);
                for (x in args)
                    compileTExpr(x);
                e.mCompiled = exprc(e);

            case TBinop(op, left, right):
                compileTExpr(left);
                compileTExpr(right);
                e.mCompiled = exprc(e);
                
            case TUnop(_, _, value):
                compileTExpr(value);
                e.mCompiled = exprc(e);

            case TArrayDecl(values):
                for (valueExpr in values)
                    compileTExpr(valueExpr);
                e.mCompiled = exprc(e);

            case TCase(caseType):
                switch caseType {
                    case Expr:
                        //TODO

                    case Standard(branches, defaultExpr):
                        for (b in branches) {
                            compileTPred(b.e);
                            compileTExpr(b.result);
                        }
                        if (defaultExpr != null)
                            compileTExpr(defaultExpr);
                        e.mCompiled = exprc(e);
                }

            case most:
                e.mCompiled = exprc(e);
        }

        //*[Probably Useless(?)]
        e.extra.set('compilationLevel', 1);

        typeExpr(e);
        Console.examine(e.type);
    }

    /**
     * returns a lambda function for performing an optimized version of the expression's computation
     * @param expr 
     * @return JITFn<Dynamic>
     */
    private function exprc(expr: TExpr):JITFn<Dynamic> {
        if (expr.extra.exists('compilationLevel')) {
            return expr.mCompiled;
        }

        var unary:Bool = context.unaryCurrentRow;
        inline function wrap(f:JITFn<Dynamic>) {
            return f.wrap(function(_, g:Contextual) {
                final tmp = g.context.unaryCurrentRow;
                g.context.unaryCurrentRow = unary;
                var ret = _(g);
                g.context.unaryCurrentRow = tmp;
                return ret;
            });
        }

        return switch expr.expr {
            case TConst(value):
                // var v = value.clone();
                g -> value.value;

            case TReference(name):
                g -> g.context.get(name);
                // function(g: Contextual) {
                //     try {
                //         return g.context.get(name);
                //     }
                //     catch (error: Dynamic) {
                //         final c = g.context;
                //         var i = c.querySources.length;
                //         while (i-- > 0) {
                //             var src = c.querySources[i];
                //             if (src.name == name.label()) {

                //             }
                //         }
                //     }
                // }

			case TParam({label: label, offset: offset}):
				if (!parameters.exists(label.identifier)) {
					parameters[label.identifier] = [expr];
                } 
                else if (!parameters[label.identifier].has(expr)) {
					parameters[label.identifier].push(expr);
                }

                function(g: {context:Context<Dynamic, Dynamic, Dynamic>}):Dynamic {
                    // return expr.mConstant;
                    throw new pm.Error('No value bound to ${label.identifier}');
                }
                
            case TTable(name):
                g -> {
                    throw new pm.Error.NotImplementedError();
                };

            case TColumn(name, table):
                final name = name.identifier;
                final tableName = table.label().unwrap(new pm.Error('table-name undefined'));


                // assert(true, 'bitch');
                function(g: Contextual) {
                    final c = g.context;
                    if (c.unaryCurrentRow)
                        throw new pm.Error('unary row');
                    final row:Doc = Doc.unsafe(c.getCurrentRow(tableName));
                    if (row == null) {
                        // Console.error(c.currentRows, '$tableName.$name');
                    }

                    if (!row.exists(name)) {
                        throw new pm.Error('$row has no property named "$name"');
                    }

                    return row.get(name);
                }

                
            case TField(o, field):
                function(g) {
                    final obj = o.eval(g);
                    
                    var res:Dynamic = g.context.glue.valGetField(obj, field.identifier);
                    return res;
                };

            case TArray(array, index):
                function(g: Contextual) {
                    var a:Dynamic = array.eval(g);
                    var i:Dynamic = index.eval(g);
                    return a[i];
                }
                    
            case TFunc(f):
                var fname = f.id;
                function(g: Contextual):Dynamic {
                    // return g.context.get(fname);
                    return g.context.functions[fname];
                };
            
            //* Method calls
            case TCall(m={expr:TField(object, method)}, args):
                g -> throw new pm.Error.NotImplementedError();
                
            case TCall(f, params):
                final allConst = params.every(e -> e.mConstant != null);
                final constantParameters = allConst ? [for (p in params) p.mConstant] : null;

                if (allConst) {
                    switch f.expr {
                        case TReference(_.identifier=>fname), TFunc(_.id=>fname):
                            if (context.scope.isDeclared(fname)) {
                                var fptr = cast(context.scope.lookup(fname), Callable);
                                return function(g) {
                                    return fptr.call(constantParameters);
                                };
                            }

                        default:
                    }
                }

                function(g: Contextual) {
                    var fn:Dynamic = f.eval( g );
                    var args = allConst ? constantParameters : [for (p in params) p.eval(g)];

                    if ((fn is Callable)) {
                        var ret = cast(fn, Callable).call(args);
                        return ret;
                    }
                    else {
                        throw new pm.Error('${f} is not callable');
                    }
                };
            
            case TBinop(op, left, right):
                var opf = BinaryOperators.getMethodHandle(op);
                
                //TODO optimize
                function(g) {
                    return opf(left.eval(g), right.eval(g));
                };

            case TUnop(op, _, e):
               var opf = UnaryOperators.getMethodHandle(op);
               function(g) {
                   return opf(e.eval(g));
               };

            case TArrayDecl(values):
                var allConst = true;
                for (value in values) {
                    if (allConst && value.mConstant == null)
                        allConst = false;
                }

                var constValues = allConst ? [for (v in values) v.mConstant] : null;
                
                function(g):Dynamic {
                    if (allConst)
                        return constValues;
                    else
                        return [for (v in values) v.eval(g)];
                };

            case TObjectDecl(fields):
                function(g):Dynamic {
                    throw new pm.Error.NotImplementedError();
                }

            case TCase(caseType):
                switch caseType {
                    case Expr:
                        function(g: Contextual):Dynamic {
							throw new pm.Error.NotImplementedError();
                        }

                    case Standard(branches, defaultExpr):
                        function(g: Contextual):Dynamic {
                            for (branch in branches) {
                                if (branch.e.eval(g)) {
                                    return branch.result.eval(g);
                                }
                            }
                            return defaultExpr.eval(g);
                        }
                }
        }
    }

    /** 
     * @inheritDoc 
     */
    override function expressionNodeConvert(e:Expr):TExpr {
        var tblNames = context.glue.dbListTables(context.database);
        switch e {
            case EField(EId(tableName), Name(columnName)):
                if (tblNames.has(tableName)) {
                    return new TExpr(TColumn(symbol(columnName), symbol(tableName)));
                }
                else {
                    var tbl = context.getSource(tableName);
                    if (tbl != null) {
                        return new TExpr(TColumn(symbol(columnName), symbol(tbl.name)));
                    }
                }

            case EId(ident):
                var i = context.querySources.length;
                while (i-- > 0) {
                    var src = context.querySources[i];
                    var c = src.schema.column(ident);
                    if (c != null)
                        return new TExpr(TColumn(symbol(ident), symbol(src.name)));
                }
            
            default:
                
        }
        return super.expressionNodeConvert(e);

        // var ret:TExpr = super.expressionNodeConvert(e);
        // switch ret.expr {
        //     case TField({expr:TReference(tbl=_.label()=>tableName)}, column) if (tblNames.has(tableName)):
        //         return new TExpr(TColumn(column, tbl));
            
        //     case TReference(idSym=_.label()=>ident):
        //         for (i in 0...context.querySources.length) {
        //             var src = context.querySources[i];
        //             if (src.schema.column(ident) != null) {
        //                 return new TExpr(TColumn(idSym, symbol(src.name)));
        //             }
        //         }
                
        //     default:
        // }
        // return ret;
    }

    function typeBinopExpr(op:BinaryOperator, left:TExpr, right:TExpr):SType {
        typeExpr(left);
        typeExpr(right);
        var types = [left.type, right.type];
        switch op {
            case OpEq, OpGt, OpGte, OpLt, OpLte, OpNEq, OpIn:
                return TBool;
            case OpAdd:
                return switch types {
                    case [TString, _]|[_, TString]: TString;
                    case [TInt, r=TInt|TFloat]: r;
                    case [TFloat, TFloat|TInt]: TFloat;
                    default:
                        throw new pm.Error();
                }
            case OpSubt:
                return switch types {
                    case [TInt, r=TInt|TFloat]: r;
                    case [TFloat, TFloat|TInt]: TFloat;
                    default:
                        throw new pm.Error();
                }
            case OpMult:
                return switch types {
                    case [l=TInt|TFloat|TString|TArray(_), TInt]: l;
                    case [TInt, r=TInt|TFloat|TString|TArray(_)]: r;
                    default:
                        throw new pm.Error();
                }
            case OpDiv:
                return switch types {
                    case [TInt|TFloat, TUnknown]: right.type = TFloat;
                    case [TUnknown, TInt|TFloat]: left.type = TFloat;
                    case [TInt|TFloat, TInt|TFloat]: TFloat;
                    case [_.print()=>lt, _.print()=>rt]:
                        throw new pm.Error('InvalidOp: $lt / $rt');
                    case _:
                        throw new pm.Error('$types');
                }
            case OpMod:
                throw new pm.Error();
            case OpBoolAnd:
                return TBool;
            case OpBoolOr:
                return TBool;
            case OpBoolXor:
                return TBool;
        }
    }

    function typeUnopExpr(op:UnaryOperator, value:TExpr):SType {
        typeExpr(value);
        switch op {
            case OpNot:
                return TBool;
            case OpNegBits:
                throw new pm.Error();
            case OpPositive:
                return TFloat;
            case OpNegative:
                return TFloat;
        }
    }

    function typeExpr(e: TExpr) {
        var compLvl:Int = e.extra.compilationLevel;
        switch compLvl {
            case 1://* Expected value
            default:
                //Console.error('Unhandled: Expr.compilationLevel = $compLvl');
        }

        switch e.expr {
            case TReference({identifier:id}):
                // throw new pm.Error('TODO: TReference($id)');
                e.type = TUnknown;
            
            case TBinop(op, left, right):
                typeExpr(left);
                typeExpr(right);
                e.type = typeBinopExpr(op, left, right);

            case TUnop(op, post, operand):
                typeExpr(operand);
                e.type = typeUnopExpr(op, operand);

            case TParam({label:{identifier:name}}):
                if (e.isTyped()) return ;
                var pool = parameters[name];
                var typedOne = pool.find(e -> e.isTyped());
                if (typedOne != null) {
                    for (p in pool)
                        p.type = typedOne.type;
                }

            case TColumn(name, table):
                e.type = switch getColumnField(name.identifier, table != null ? table.identifier : null) {
                    case null: TUnknown;
                    case c: c.type;
                }
                

            case TFunc(f):
                throw new pm.Error('Unreachable');
                
            case TTable(name):
				throw new pm.Error('Unreachable');
                
            case TField(o, field):
                typeExpr(o);
                // throw new pm.Error('TODO');
                Console.error('type TField($o, $field)');
                
            case TCall(f, params):
                typeExpr(f);
                for (p in params)
                    typeExpr(p);
                
            case TConst(_):
                e.type = exprt(e);
            
            case TArray(array, index):
                typeExpr(array);
                typeExpr(index);
                e.type = switch array.type {
                    case TArray(itemType): 
                        // index.typeHint = TInt;
                        itemType;
                    case TMap(keyType, valueType):
                        // index.typeHint = keyType;
                        valueType;
                    case TUnknown:
                        TUnknown;
                    default:
                        throw new pm.Error('Unhandled ${array.type}');
                }

            case TCase(Standard(branches, def)):
                for (b in branches) {
                    typeSelPredicate(b.e);
                    typeExpr(b.result);
                }
                var t:SType = null;
                for (b in branches) {
                    if (t == null)
                        t = b.result.type;
                    else {
                        if (b.result.type.eq(t))
                            continue;
                        else
                            throw new pm.Error('TypeMismatch($t, ${b.result.type})');
                    }
                }
                if (def != null) {
                    typeExpr(def);
                }
                if (t != null)
                    e.type = t;

            case TArrayDecl(values):
                for (v in values)
                    typeExpr(v);
                var t:SType = null;
                for (v in values) {
                    if (t == null || t.eq(v.type))
                        continue;
                    throw new pm.Error('TypeMismatch($t, ${v.type})');
                }

            case other:
                throw new pm.Error('Unhandled $e');
        }

        if (e.type == TUnknown)
            Console.error('`${e.print()}` is untyped');
    }

	/**
	 * computes the `SType` of the given expression
	 * @param e the Expression
	 * @return the computed type
	 */
	function exprt(e:TExpr):SType {
		switch e.expr {
			case TConst(value):
				value.validate();
                return value.type;
            
			case TColumn(name, table):
				// var schema = context.getTableSchema(table.identifier);
                // return schema.column(name.identifier).type;
                return context.getColumnField(name.identifier, table != null ? table.identifier : null).type;
            case TReference(name):
                throw new pm.Error.NotImplementedError();
            case TParam(name):
				throw new pm.Error.NotImplementedError();
			case TTable(name):
				throw new pm.Error.NotImplementedError();
			case TField(o, field):
                throw new pm.Error.NotImplementedError();
            case TArray(_, _):
				throw new pm.Error.NotImplementedError();
			case TFunc(f):
				throw new pm.Error.NotImplementedError();
			case TCall(f, params):
				throw new pm.Error.NotImplementedError();
			case TBinop(op, left, right):
				throw new pm.Error.NotImplementedError();
			case TUnop(op, _, e):
				throw new pm.Error.NotImplementedError();
			case TArrayDecl(_):
				throw new pm.Error.NotImplementedError();
			case TObjectDecl(_):
                throw new pm.Error.NotImplementedError();
            case TCase(t):
				throw new pm.Error.NotImplementedError();
		}
	}

    function typeSelPredicate(p: SelPredicate) {
        switch p.type {
            case Rel(relation):
                typeExpr(relation.left);
                typeExpr(relation.right);
                final l = relation.left;
                final r = relation.right;

                inline function compOp(opStr: String) {
                    switch [l.type, r.type] {
                        case [TUnknown, TUnknown]:
                            //

                        case [TUnknown, def], [def, TUnknown]:
                            l.type = r.type = def;

                        case [ltype, rtype]:
                            if (!ltype.eq(rtype)) {
                                throw new pm.Error('Invalid operation $ltype $opStr $rtype');
                            }
                    }
                }

                switch relation.op {
                    case Equals:
                        compOp('=');
                    case NotEquals:
                        compOp('!=');
                    case Greater:
                        compOp('>');
                    case Lesser:
                        compOp('<');
                    case GreaterEq:
                        compOp('>=');
                    case LesserEq:
                        compOp('<=');
                    case In:
                        switch r.type {
                            case TString:
                                switch l.type {
                                    case TString:
                                    case other:
                                        throw new pm.Error('Invalid operation ${l.type.print()} in String');
                                }

                            case TArray(itemType):
                                switch l.type {
                                    case _.eq(itemType)=>true:
                                    case TArray(_.eq(itemType)=>true):
                                    case TUnknown:
                                        l.type = itemType;
                                    case other:
                                        throw new pm.Error('Invalid operation ${l.type.print()} in Array<String>');
                                }

                            case other:
								throw new pm.Error('Invalid operation ${l.type.print()} in ${other.print()}');
                        }
                }

            case And(p):
                for (x in p)
                    typeSelPredicate(x);

            case Or(p):
                for (x in p)
                    typeSelPredicate(x);

            case Not(sub):
                typeSelPredicate(sub);
        }
    }

    function compileInsertStmt(insert: InsertStatement):InsertStmt {
        var rows = [];
        for (x in insert.values) {
            rows.push([for (e in x) compileExpression(e)]);
        }
        return new InsertStmt(insert.target, insert.columns, rows);
    }

	function compileUpdateStmt(update:UpdateStatement):UpdateStmt {
		var ops = update.operations.map(op -> updateOpConvert(op));
		for (op in ops) {
			compileUpdateOp(op);
        }
        var predicate = update.where != null ? compileSelectPredicate(update.where) : null;
        var stmt = new UpdateStmt(new TablePath(update.target.tableName), ops, predicate);
        stmt.context = new UpdateStmtContext(this.context);

        return stmt;
    }
    
    function compileUpdateOp(op: UpdateOperation) {
        compileTExpr(op.e);
    }

    function getTableSchema(name: String):Null<SqlSchema<Dynamic>> {
        return context.getTableSchema(name);
    }
    function getColumnField(name:String, ?table:String) {
        return context.getColumnField(name, table);
    }

    inline function symbol(s:String, ?pos:haxe.PosInfos):SqlSymbol {
        return if (s.empty()) null else new SqlSymbol(s.unwrap('unwrap failed', pos));
    }
}

private typedef Predicate = Expr;