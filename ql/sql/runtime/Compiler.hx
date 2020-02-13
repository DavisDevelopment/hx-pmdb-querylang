package ql.sql.runtime;

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

class Compiler {
    var querySource: Array<TableSpec>;
    public var context(default, set): Null<Context<Dynamic, Dynamic, Dynamic>> = null;

    public function new(?ctx) {
        // this.context = new Context();
        this.context = ctx;
    }

    public var glue(get, never):Glue<Dynamic, Dynamic, Dynamic>;
    private inline function get_glue():Glue<Dynamic, Dynamic, Dynamic> return this.context.glue;

	private function set_context(c:Null<Context<Dynamic, Dynamic, Dynamic>>) {
        this.context = c;
        if (this.context != null) {
            for (n in this.context.glue.dbListTables(this.context.database)) {
                try {
                    loadTable(n);
                }
                catch (e: Dynamic) {
                    #if Console.hx Console.error(e); #else trace(e); #end
                }
            }
            // for (tbl in tables) {
            //     var schema = context.glue.tblGetSchema(tbl);

            // }
        }
        return c;
    }

    public function loadTable(name: String):Null<Dynamic> {
        if (context.tables.exists(name))
            return context.tables.get(name);
        var res = context.tables[name] = glue.dbLoadTable(context.database, name);
        var schema = glue.tblGetSchema(res);
        schema.context = this.context;
        for (field in schema.fields) {
            if (field.defaultValueString != null && field.defaultValueExpr == null) {
                var e = NewSqlParser.readExpression(field.defaultValueString);
                var e = this.compileExpression(e);
                field.defaultValueExpr = e;
            }
        }
        trace('LOADED "$name"');
        return res;
    }

    public function compile(stmt: SqlStatement) {
        var selectStmt = stmt.toSelect();
        return compileSelectStmt(selectStmt);
    }

    function compileSelectStmt(select : SelectStatement) {
        var query = select.query.query;
        if (query.from == null)
            throw new pm.Error.WTFError();
        var tableSpec = query.from.tables[0].tableSourceItem.toTableSpec();
        this.querySource = [tableSpec];
        this.resolveSources();
        var items:Array<SelItem> = [for (el in query.elements) compileSelectElement(el)];
        var predicate = query.from.where != null ? compileSelectPredicate(query.from.where.predicate) : null;

        // 
        var out:SelOutput = items;
        compileSelOutput(out);
        var result = new Sel(this.context, tableSpec.tableName.table, new SelectImpl(select, predicate, out));

        return result;
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
    function compileSelItem(item: SelItem):(g:{context:Context<Dynamic, Dynamic, Dynamic>}, out:ImmutableStruct<TypedValue>)->ImmutableStruct<TypedValue> {
        return switch item {
            case All(table):
                resolveTableToSymbol(table);
                var schema = context.glue.tblGetSchema(table.table);

                function(g, out:ImmutableStruct<TypedValue>) {
                    var row:Doc = g.context.currentRow;
                    for (field in schema.fields) {
                        out = out.append(field.name, (row[field.name] : TypedValue));
                    }
                    return out;
                }
            
            case Column(table, column, alias):
                resolveTableToSymbol(table);
                var schema = context.glue.tblGetSchema(table.table);
                var outKey = alias.nor(column);
				var outColumn = schema.column(column.identifier);
                function(g, out:ImmutableStruct<TypedValue>) {
                    var row:Doc = g.context.currentRow;
                    var value:Dynamic = row[column.identifier];
                    var value = new TypedValue(value, outColumn.type);
                    return out.append(outKey.identifier, value);
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

                function(g, out:ImmutableStruct<TypedValue>):ImmutableStruct<TypedValue> {
                    return out.append(key, TypedValue.ofAny(expr.eval(g)));
                }
        }
    }

    function compileSelOutSchema(out: SelOutput) {
        var columns = new Array();
        for (item in out.items) {
            switch item {
                case All(table):
                    var schema = context.glue.tblGetSchema(table.table);
                    for (c in schema.fields) {
                        columns.push(c.getInit());
                    }

                case Column(table, column, alias):
                    final col = context.glue.tblGetSchema(table.table).column(column.identifier).getInit();
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

    function compileSelOutput(exporter: SelOutput) {
        var items = exporter.items.map(item -> compileSelItem(item)).toArray();
        var schema = compileSelOutSchema(exporter);

        var f = function(g) {
            var out:ImmutableStruct<TypedValue> = new ImmutableStruct();
            for (mut in items)
                out = mut(g, out);
            var obj:Doc = toMutableStruct(out);
            obj = Doc.unsafe(schema.induct(obj));
            return obj;
        };
        exporter.mCompiled = f;
    }

    function compileSelectPredicate(predicate: Predicate):SelPredicate {
        if ((predicate is RelationPredicate)) {
            var rp = cast(predicate, RelationPredicate);
            
            var left = compileExpression1(rp.left);
            var right = compileExpression1(rp.right);
            compileExpression2(left);
            compileExpression2(right);
            var rpo = RelationPredicateOperator, rel;
            var res = new SelPredicate(SelPredicateType.Rel(rel = new RelationalPredicate(switch rp.op {
                case OpEq: rpo.Equals;
                case OpNEq: rpo.NotEquals;
                case OpGt: rpo.Greater;
                case OpGte: rpo.GreaterEq;
                case OpLt: rpo.Lesser;
                case OpLte: rpo.LesserEq;
            }, left, right)));
            res.mCompiled = g -> rel.eval(g);
            return res;
        }

        if ((predicate is NotPredicate)) {
            var np = cast(predicate, NotPredicate);
            var p = compileSelectPredicate(np.predicate);
            var tmp = p.mCompiled;
            p.mCompiled = g -> !tmp(g);
            return p;
        }

        if ((predicate is ql.sql.grammar.expression.InPredicate)) {
            var p = cast(predicate, InPredicate);
            var left = compileExpression(p.left);
            switch p.right.data {
                case LExpression(e):
                    var right = compileExpression(e);
                    var rel;
                    var res = new SelPredicate(SelPredicateType.Rel(rel = new RelationalPredicate(In, left, right)));
                    res.mCompiled = g -> rel.eval(g);
                    return res;

                case LSubSelect(select):
                    throw new pm.Error('TODO');
            }
        }

        if ((predicate is ql.sql.grammar.expression.AndPredicate)) {
            var p = cast(predicate, AndPredicate);
            var left = compileSelectPredicate(p.left);
            var right = compileSelectPredicate(p.right);
            var res = new SelPredicate(SelPredicateType.And(left, right));
            res.mCompiled = g -> (left.eval(g) && right.eval(g));
            return res;
        }

		if ((predicate is ql.sql.grammar.expression.OrPredicate)) {
			var p = cast(predicate, OrPredicate);
			var left = compileSelectPredicate(p.left);
			var right = compileSelectPredicate(p.right);
			var res = new SelPredicate(SelPredicateType.Or(left, right));
            res.mCompiled = g -> (left.eval(g) || right.eval(g));
			return res;
		}
        
        throw new pm.Error('Unhandled ' + Type.getClassName(Type.getClass(predicate)));
    }

    /**
      compiles the given expression into some shit
     **/
    extern inline private function compileExpression(e: Expression) {
        var expr = compileExpression1(e);
        compileExpression2(expr);
        return expr;
    }

    function compileExpression1(e: Expression):TExpr {
        if ((e is Predicate)) throw new pm.Error('Compile predicate nodes with compileSelectPredicate');
        if ((e is C<Dynamic>)) {
            var value:Dynamic = (cast e : C<Dynamic>).getConstValue();
            final type = TExprType.TConst(value);
            return new TExpr(type);
        }

        if ((e is ql.sql.grammar.expression.ColumnName)) {
			var c = cast(e, ql.sql.grammar.expression.Expression.ColumnName);
            resolveTableSymbol(c);
            c.name.type = SqlSymbolType.Field;
            return new TExpr(TExprType.TColumn(c.name, c.table));
        }

        if ((e is ArithmeticOperation)) {
            var ao:ArithmeticOperation = cast(e, ArithmeticOperation);
            var left = compileExpression(ao.left),
                right = compileExpression(ao.right);
            var type = TExprType.TBinop(ao.op.toBinaryOperator(), left, right);
            return new TExpr(type);
        }

        if ((e is FunctionCallBase<Dynamic>)) {
            if ((e is ql.sql.grammar.expression.SimpleFunctionCall)) {
                var call:SimpleFunctionCall = cast e;
                // return new TExpr(TExprType.TFunc(new TFunction(call.symbol, call.kind, function() {
                //     throw -9;
                // })));
                var f = new TFunction(call.symbol, call.kind);
                var type = TExprType.TFunc(f);
                var fe = new TExpr(type);
                type = TExprType.TCall(fe, call.args.map(x -> compileExpression(x)));
                return new TExpr(type);
            }

            throw new pm.Error('Invalid FunctionCallBase<?> instance ${Type.getClassName(Type.getClass(e))}');
        }

        throw new pm.Error(Type.getClassName(Type.getClass(e)));
    }

    /**
     [TODO]
     - `TArray(arr:TExpr, index:TExpr)`
     - `TArrayDecl(values: Array<TExpr>)`
     - `TObjectDecl(fields: Array<{field:String, value:TExpr}>)`
   **/
    var parameters:Map<String, Array<TExpr>> = new Map();
    private function compileExpression2(e: TExpr) {
        typeExpr(e);

        switch e.expr {
            case TConst(value):
                e.mConstant = value.value;
                e.mCompiled = exprc(e);
            
            case TTable(name):
                e.mCompiled = exprc(e);
            
            case TColumn(name, table):
                resolveTableToSymbol(table);
                var column = glue.tblGetSchema(table.table).column(name.identifier);
                e.type = column.type;
                e.mCompiled = exprc(e);

            case TField(o, _):
                compileExpression2(o);
                e.mCompiled = exprc(e);

            case TParam({label:label}):
                e.mCompiled = exprc(e);

            case TFunc(f):
                e.mCompiled = exprc(e);

            case TCall(f, args):
                compileExpression2(f);
                for (x in args)
                    compileExpression2(x);
                e.mCompiled = exprc(e);

            case TBinop(op, left, right):
                compileExpression2(left);
                compileExpression2(right);
                e.mCompiled = exprc(e);
                
            case TUnop(_, _, value):
                compileExpression2(value);
                e.mCompiled = exprc(e);

            case TArrayDecl(values):
                throw new pm.Error.NotImplementedError();

            case TObjectDecl(fields):
                throw new pm.Error.NotImplementedError();
        }
    }

    private function exprc(expr: TExpr):JITFn<Dynamic> {
        return switch expr.expr {
            case TConst(value):
                // var v = value.clone();
                g -> value.value;

			case TParam({label: label, offset: offset}):
				if (!parameters.exists(label.identifier)) {
					parameters[label.identifier] = [expr];
                } 
                else if (!parameters[label.identifier].has(expr)) {
					parameters[label.identifier].push(expr);
                }

                function(g:{context:Context<Dynamic, Dynamic, Dynamic>}):Dynamic {
                    throw new pm.Error.NotImplementedError();
                }
                
            case TTable(name):
                g -> {
                    throw new pm.Error.NotImplementedError();
                };

            case TColumn(name, table):
				function(g) {
					var res:Dynamic = g.context.glue.rowGetColumnByName(g.context.currentRow, name.identifier);
						// return res;
					// });
					return res;
                };

            case TField(o, field):
				function(g) {
					var res:Dynamic = g.context.glue.valGetField(o.eval(g), field.identifier);
					return res;
                };
                
            case TFunc(f):
                var fname = f.symbol;
                function(g) {
                    return g.context.get(fname);
                };

            case TCall(f, params):
                function(g) {
                    var fn:Dynamic = f.eval( g );
                    var args = [for (p in params) p.eval(g)];

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
                function(g):Dynamic {
                    throw new pm.Error.NotImplementedError();
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
        switch e.expr {
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
                resolveTableToSymbol(table);
                var schema = glue.tblGetSchema(table.table);
                e.type = schema.column(name.identifier).type;

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
            case TArrayDecl(_):
            case TObjectDecl(_):
        }
    }

    function exprt(e: TExpr):SType {
        switch e.expr {
            case TConst(value):
                value.validate();
                return value.type;
            case TColumn(name, table):
                var schema = glue.tblGetSchema(table.table);
                return schema.column(name.identifier).type;
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

    function resolveTableToSymbol(name: SqlSymbol) {
        name.type = Table;
        var res = name.table = getTable(name);
        this.loadTable(name.identifier);
        return res;
    }

    function resolveSources() {
        for (src in querySource) {
            resolveTableToSymbol(src.tableName);
        }
    }

    function resolveTableSymbol(el: {table:Null<SqlSymbol>}) {
		if (el.table == null) {
			switch querySource {
				case [] | null:
					throw 'wtf?';

				case [spec]:
					el.table = spec.tableName;

				default:
					throw new pm.Error('Invalid');
			}
        }

        assert(el.table != null, new pm.Error('table symbol must be defined'));
    }

    function getTable(name: SqlSymbol):Null<Dynamic> {
        if (context == null) return null;
        
        return loadTable(name.identifier);
    }
}