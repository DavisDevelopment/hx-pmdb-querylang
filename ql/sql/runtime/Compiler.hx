package ql.sql.runtime;

import ql.sql.runtime.Stmt.CSTNode;
import ql.sql.runtime.Stmt.SelectStmt;
import ql.sql.runtime.Sel.TableStream;
import ql.sql.runtime.Sel.TableSource as TblSrc;
import ql.sql.runtime.Sel.TableSourceItem as TblSrcItem;
import ql.sql.runtime.Sel.TableRef;
import ql.sql.runtime.Sel.TableJoin;
import ql.sql.runtime.Sel.Aliased;
import haxe.ds.Either;
import ql.sql.runtime.Sel.SelectImpl;
import pmdb.core.Object;
import pmdb.core.Arch;

import ql.sql.TsAst;
import ql.sql.grammar.CommonTypes;
import ql.sql.grammar.expression.Expression;

import ql.sql.runtime.VirtualMachine;
import ql.sql.runtime.TAst;
import ql.sql.common.TypedValue;
import ql.sql.common.SqlSchema;
import ql.sql.common.internal.ObjectPure as ImmutableStruct;

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

    // public function loadTable(name: String):Null<Dynamic> {
    //     Console.examine(name);
    //     if (context.tables.exists(name))
    //         return context.tables.get(name);
    //     var res = context.tables[name] = glue.dbLoadTable(context.database, name);
    //     if (res == null) {
    //         res = context.tables[name] = context.scope.lookup(name);
    //     }

    //     var schema = glue.tblGetSchema(res);
    //     visitSqlSchema(schema);
    //     trace('LOADED "$name"');
    //     return res;
    // }
    override function loadTable(name:String):Null<Dynamic> {
        var table = super.loadTable(name);
        if (table != null) {
            var schema = context.glue.tblGetSchema(table);
            visitSqlSchema(schema);
        }
        return table;
    }

    public function compile(stmt: SqlStatement):Stmt {
        // var selectStmt = stmt.toSelect();
        // return compileSelectStmt(selectStmt);
        switch stmt.data {
            case DmlStatement(dml): 
                switch dml {
                    case SelectStatement(select):
                        // var node:CSTNode = new SelectStmt(compileSelectStmt(select));
						var node = compileSelectStmt(select);
                        return new Stmt(node, SelectStatement(cast node));
                }
        }
    }

    function visitSqlSchema(schema: SqlSchema<Dynamic>) {
		schema.context = this.context;
		for (field in schema.fields) {
			if (field.defaultValueString != null && field.defaultValueExpr == null) {
				var e = NewSqlParser.readExpression(field.defaultValueString);
				var e = this.compileExpression(e);
				field.defaultValueExpr = e;
			}
		}
    }

    function compileSelectStmt(select : SelectStatement):Sel<Dynamic, Dynamic, Dynamic> {
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
        trace(thisSource);
        querySource.push(thisSource);
        /*
            thisSource = new ql.sql.runtime.VirtualMachine.TableSpec(
                csrc.getName(context), 
                csrc.getSchema(context),
                switch csrc.item {
                    case Table({alias:null, term:r}): {table:r.table};
                    case Table({alias:alias, term:r}):
                        querySource.push(new TableSpec(alias, glue.tblGetSchema(r.table), {table:r.table}));
                        {src: querySource[0]};
                    case Stream({alias:name, term:stream}):
                        {
                            stream: stream,
                            stmt: stream.select
                        };
                }
            )
        */

        // if (context.querySources.length == 0)
            // context.querySources = querySource;
        // else {
            for (x in querySource)
                context.addQuerySource(x);
            // querySource = context.querySources;
        // }

        if (csrc.joins != null) {
            for (j in csrc.joins) {
                context.addQuerySource(j.mJoinWith.unwrap().src.unwrap());
            }
        }
        
        var predicate = query.from.where != null ? compileSelectPredicate(query.from.where.predicate) : null;
        
        var items:Array<SelItem> = [for (el in query.elements) compileSelectElement(el)];
        var out:SelOutput = items;
        compileSelOutput(out, thisSource);
        
        var result = new Sel(
            this.context,
            csrc,
            new SelectImpl(select, predicate, out)
        );
        var pre = result.evalHead;
		// switch csrc.item {
		// 	case Table({alias: alias, term: {table: table, name: name}}):
        //         pre.push(function(g) {
        //             g.context.currentDefaultTable = alias.nor(name);
        //             return null;
        //         });

		// 	case Stream({alias:name}):
        //         pre.push(function(g) {
        //             g.context.currentDefaultTable = name;
        //             return null;
        //         });
        // }
        var tmp = @:privateAccess context.mSrcStack.top().copy();
        pre.push(g -> {
            g.context.pushSourceScope(tmp);
            g.context.beginScope();
            g.context.use(thisSource);
        });
        result.evalTail.push(g -> {
            g.context.popSourceScope();
            g.context.endScope();
        });

        if (query.orderBy != null) {
            result.i.order = query.orderBy.expressions.map(function(o) {
                return {
                    accessor: compileExpression(o.expression),
                    direction: switch o.sortType {
                        case Asc: 1;
                        case Desc: -1;
                    }
                };
            });
        }

        return result;
    }

	inline function toTblSrcItem(i:TableSourceItem):TblSrcItem {
		var res = tableSourceItemConvert(i);
		compileTblSrcItem(res);
		return res;
	}

    function tableSourceConvert(src: TableSource):TblSrc {
        // Console.examine(src);
        var item = toTblSrcItem(src.tableSourceItem);

        // var csrc:TblSrc = new TblSrc(Table(new Aliased(alias, new TableRef(tableSpec.tableName.identifier, table))));
        var csrc = new TblSrc(item);
		var joins = src.joins;
		if (joins != null) {
			var cjoins:Array<TableJoin> = new Array();
			for (join in joins) {
				// var tbl = tableSourceItemConvert(join.joinWith).
                var cjoin = new TableJoin(join.joinType, toTblSrcItem(join.joinWith));
                var tmp = new TblSrc(cjoin.joinWith);
                var joinSrc = tmp.getSpec(context);
                context.resolveTableFrom(joinSrc);
                cjoin.mJoinWith = {src:joinSrc};
                
                if (join.on != null) {
                    context.pushSourceScope([joinSrc]);
                    // context.querySources.push(joinSrc);
                    cjoin.on = compileSelectPredicate(cast join.on);
                    // context.querySources = tmp;
                    context.popSourceScope();
                }
                
				if (join.used != null) {
					throw new pm.Error('TODO');
				}
				cjoins.push(cjoin);
			}
			// (csrc.joins != null ? csrc.joins : (csrc.joins = [])).push()
			csrc.joins = cjoins;
        }
        
        return csrc;
    }

    private var targetQuerySource:Null<TableSpec> = null;
    function tableSourceItemConvert(item: TableSourceItem):TblSrcItem {
        var alias:Null<String> = null;
        var tableSpec:Null<ql.sql.TsAst.TableSpec> = null;
        // var cjoins = null;
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
                tableSpec = (term : TableSourceItem).toTableSpec();
            }

            if ((term is ql.sql.NestedSelectStatement)) {
                var subStmt:NestedSelectStatement = cast term;

                // context.pushSourceScope([
                    // new TableSpec('$alias:${}')
                // ])
                var stmt = compileSelectStmt(subStmt.statement);
                //Console.examine(stmt.i._exporter);

                tableStream = new TableStream();
                tableStream.astNode = term;
                tableStream.schema = stmt.resultSchema;
                tableStream.open = function(g) {
                    stmt.context = g.context;
                    return stmt.eval().iterator().map(function(row: Dynamic) {
                        // g.context.currentRows[alias] = row;
                        g.context.focus(row, alias);
                        // //Console.examine(g.context.currentRows);
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
				stmt.context = g.context;
				return stmt.eval().iterator().map(function(row:Dynamic) {
                    // g.context.currentRow = row;
                    g.context.focus(row, '_');
					// //Console.examine(g.context.currentRows);
					return row;
				});
			};
        }

        if (tableSpec != null) {
            var table = context.resolveTableFrom(context.getSource(tableSpec.tableName.identifier));

            if (table == null) throw new pm.Error();
            if (alias != null) {
                context.scope.define(alias, table);
            }

            return TblSrcItem.Table(new Aliased(alias, new TableRef(tableSpec.tableName.identifier, table)));
        }

        if (tableStream != null) {
            return TblSrcItem.Stream(new Aliased(alias, (tableStream : TableStream)));
        }

        throw new pm.Error.ValueError(item);
    }

    function compileTblSrcItem(item: TblSrcItem) {
        switch item {
            case Table({term:{table:table}}):
                var schema = glue.tblGetSchema(table);
                visitSqlSchema(schema);

            case Stream(a={term:{schema:schema}}):
                //Console.examine(a);
                visitSqlSchema(schema);
        }
    }

    function compileSelectElement(element: ESelectElement) {
        switch element {
            case AllColumns(el):
                resolveTableSymbol(el);
                return SelItem.All(el.table);
            
            case ColumnName(el):
                resolveTableSymbol(el);
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
                Console.debug({table:s(table), column:s(column), alias:s(alias)});
                
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
                    key = 'ass';
                    throw new pm.Error('AST-printing not implemented yet');
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

    inline function exportValue(value: Dynamic):Dynamic {
        if (TypedValue.is(value)) {
            return (untyped value : TypedValue).export();
        }
        else {
            return value;
        }
    }

	function compileSelOutSchema(out:SelOutput, src:TableSpec) {
        var columns = new Array();
        for (item in out.items) {
            switch item {
                case All(null):
                    for (c in src.schema.fields)
                        columns.push(c.getInit());
                
                case All(table):
                    var schema = getTableSchema(table.identifier);
                    for (c in schema.fields) {
                        columns.push(c.getInit());
                    }

                case Column(table, column, alias):
                    // final col = context.glue.tblGetSchema(table.table).column(column.identifier).getInit();
                    final col = getColumnField(column.identifier, if (table != null) table.identifier else null).getInit();
                    if (alias != null)
                        col.name = alias.identifier;
                    columns.push(col);

                case Expression(alias, expr):
                    if (alias == null)
                        throw new pm.Error('Alias must be defined');
                    columns.push({
                        name: alias.identifier,
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

    static function toMutableStruct(o: ImmutableStruct<TypedValue>):Doc {
		return o.map(function(key, value:TypedValue) {
			var v = value.export();
			return {
				key: key,
				value: v
			};
		}).toMutable();
    }

    function compileSelOutput(exporter:SelOutput, src:TableSpec) {
        var schema = compileSelOutSchema(exporter, src);
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
        // var schema = compileSelOutSchema(exporter);

        var f = function(g) {
            var out:Doc = new Doc();
            for (mut in items)
                out = mut(g, out);
            // var obj:Doc = toMutableStruct(out);
            // obj = Doc.unsafe(schema.induct(obj));
            // return obj;
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
    extern inline private function compileExpression(e: Expression) {
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

            // case TParam({label:label}):
            //     e.mCompiled = exprc(e);

            // case TFunc(f):
            //     e.mCompiled = exprc(e);

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

            case most:
                e.mCompiled = exprc(e);
        }

        //*[Probably Useless(?)]
        e.extra.set('compilationLevel', 1);

		typeExpr(e);
    }

    /**
     * returns a lambda function for performing an optimized version of the expression's computation
     * @param expr 
     * @return JITFn<Dynamic>
     */
    private function exprc(expr: TExpr):JITFn<Dynamic> {
        if (expr.extra.exists('compilationLevel')) {
            Console.error('Expression already compiled');
            return expr.mCompiled;
        }

        return switch expr.expr {
            case TConst(value):
                // var v = value.clone();
                g -> value.value;

            case TReference(name):
                g -> g.context.get(name);

			case TParam({label: label, offset: offset}):
				if (!parameters.exists(label.identifier)) {
					parameters[label.identifier] = [expr];
                } 
                else if (!parameters[label.identifier].has(expr)) {
					parameters[label.identifier].push(expr);
                }

                function(g:{context:Context<Dynamic, Dynamic, Dynamic>}):Dynamic {
                    return new pm.Error('No value bound to ${label.identifier}');
                }
                
            case TTable(name):
                g -> {
                    throw new pm.Error.NotImplementedError();
                };

            case TColumn(name, table):
                final name = name.identifier;
                final tableName = table.label();


                function(g: Contextual) {
                    final c = g.context;
                    final row:Doc = Doc.unsafe(c.getCurrentRow(tableName));

                    if (!row.exists(name))
                        throw new pm.Error('$row has no property named "$name"');

                    return row.get(name);
                }

                
            case TField(o, field):
                function(g) {
                    var res:Dynamic = g.context.glue.valGetField(o.eval(g), field.identifier);
                    return res;
                };
                    
            case TFunc(f):
                var fname = f.symbol;
                function(g) {
                    Console.error('TFunc expression construct is deprecated');
                    return g.context.get(fname);
                };
            
            //* Method calls
            case TCall(m={expr:TField(object, method)}, args):
                g -> throw new pm.Error.NotImplementedError();
                
            case TCall(f, params):
                final allConst = params.every(e -> e.mConstant != null);
                final constantParameters = allConst ? [for (p in params) p.mConstant] : null;

                if (allConst) {
                    switch f.expr {
                        case TReference(_.identifier=>fname), TFunc(_.symbol.identifier=>fname):
                            if (context.scope.isDeclared(fname)) {
                                var fptr = cast(context.scope.lookup(fname), F);
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

                    if ((fn is F)) {
                        var ret = cast(fn, F).call(args);
                        return ret;
                    }
                    else {
                        throw new pm.Error('${f} is not callable');
                    }
                };
            
            case TBinop(op, left, right):
                var opf = BinaryOperators.getMethodHandle(op);
                function(g) {
                    return opf(left.eval(g), right.eval(g)).value;
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
        }
    }

    function typeBinopExpr(op:BinaryOperator, left:TExpr, right:TExpr):SType {
        typeExpr(left);
        typeExpr(right);
        var types = [left.type, right.type];
        switch op {
            case OpEq, OpGt, OpGte, OpLt, OpLte, OpNEq:
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
                    case [TInt|TFloat, TInt|TFloat]: TFloat;
                    default:
                        throw new pm.Error();
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
                Console.error('Unhandled: Expr.compilationLevel = $compLvl');
        }

        switch e.expr {
            case TReference({identifier:id}):
                throw new pm.Error('TODO');
            
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
                // throw new pm.Error('TODO');
                
            case TTable(name):
                // throw new pm.Error('TODO');
                
            case TField(o, field):
                // throw new pm.Error('TODO');
                typeExpr(o);
                
            case TCall(f, params):
                typeExpr(f);
                for (p in params)
                    typeExpr(p);
                
            case TConst(_):
                e.type = exprt(e);
            case TArrayDecl(_)://TODO
            case TObjectDecl(_)://TODO
        }
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

    function getTableSchema(name: String):Null<SqlSchema<Dynamic>> {
        // Console.examine(name);
        return context.getTableSchema(name);
    }
    function getColumnField(name:String, ?table:String) {
        return context.getColumnField(name, table);
    }
}