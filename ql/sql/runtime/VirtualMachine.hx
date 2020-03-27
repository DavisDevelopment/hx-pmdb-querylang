package ql.sql.runtime;

// import ql.sql.runtime.Sel.Context;
// import ql.sql.runtime.Sel.Scope;

import ql.sql.common.index.IndexCache;
import ql.sql.grammar.CommonTypes.Contextual;
import haxe.ds.ReadOnlyArray;
import ql.sql.runtime.Stmt.SelectStmt;
import ql.sql.runtime.Sel.TableStream;
import pm.LinkedStack;
import ql.sql.Dummy.DummyTable;
import ql.sql.Dummy.AbstractDummyDatabase;
import ql.sql.common.DateTime;
import ql.sql.common.TypedValue;
import pm.Functions;
import ql.sql.common.SqlSchema;
import ql.sql.common.SqlSchema.SchemaField;
import ql.sql.grammar.CommonTypes.SqlSymbol;
import ql.sql.runtime.TAst;
import ql.sql.runtime.DType;
import ql.sql.runtime.SType;
import ql.sql.runtime.Callable;
import ql.sql.runtime.Glue;
import pmdb.core.ValType;
import haxe.Constraints.Function;

import pm.Helpers.nor;
import pm.Helpers.nn;
import pm.strings.HashCode;
import pm.strings.HashCode.sdbm;
using pm.strings.HashCodeTools;

class VirtualMachine {
	public var context: Context<Dynamic, Dynamic, Dynamic>;
	public var parser: VMParser;
	public var compiler: Compiler;
	public var optimizer: Optimizer;
	public var interp: Interpreter;

	public function new(ctx) {
		this.context = ctx;
		this.parser = new VMParser();
		this.compiler = new Compiler(this.context);
		this.interp = new Interpreter(this.context);
	}
}

class Environment {
	//TODO
}

private typedef Schema<Tbl> = Dynamic;

private typedef Db = pmdb.core.Database;
private typedef Doc = pmdb.core.Object.Doc;
private typedef Tbl = pmdb.core.Store<Doc>;

#if !Console.hx
private typedef Console = pm.utils.LazyConsole;
#end

/**
 * ====================================================================
 * ====================================================================
 * ====================================================================
 * =========================
 * ===  [Context.hx]  ===
 * =========================
 * ====================================================================
 * ====================================================================
 * ====================================================================
 */

/**
 * node in the runtime CST
 */
interface VmNode {
	public var context: Context<Dynamic, Dynamic, Dynamic>;
}

class Context<TDb, Table, Row> {
//{region fields
    // @:allow(ql.sql.runtime.VirtualMachine)
    #if !debug @:noCompletion #end
    public var glue:Glue<TDb, Table, Row>;
	// public var params: Array<Dynamic>;
	public var database:TDb;
	public var tables:Map<String, Table>;
	public var aliases:Scope<SqlSymbol>;
	public var functions:Map<String, Callable>;

	private var mSrcStack:LinkedStack<Array<TableSpec>>;
	public var querySources(get, never):haxe.ds.ReadOnlyArray<TableSpec>;

	// public var currentDefaultTable:Null<String> = null;
	public var currentDefaultTable(get, set):Null<String>;
	function get_currentDefaultTable():Null<String> {
		return scope.isDeclaredLocally(':srcid') ? scope.lookupLocal(':srcid') : null;
	}
	function set_currentDefaultTable(id: Null<String>):Null<String> {
		scope.define(':srcid', id.unwrap(new pm.Error('Null-assignment not allowed')));
		return id;
	}

	public var currentRows:Null<Map<String, Row>> = null;
	// @:isVar
	// public var currentRow(get, set):Row;
	private var currentRow(default, null):Row;
    
	public var scope:Scope<Dynamic>;
//}endregion fields

	public function new(?g: Glue<TDb, Table, Row>) {
		this.glue = (g != null ? g : new Glue());

		this.database = null;
		this.scope = new Scope();
		this.scope.define('parameters', new Doc());
		this.aliases = new Scope();
		this.functions = new Map();
		this.tables = new Map();

		this.currentRow = null;
		this.currentRows = new Map();
		this.mSrcStack = new LinkedStack<Array<TableSpec>>([new Array<TableSpec>()]);
        
		
		_init();
	}

	// public var parameters(get, set):Doc;
	// private function get_parameters():Doc {return this.scope.lookup('parameters');}
	// private function set_parameters(p: Doc):Doc {return this.scope.assign('parameters', p);}

	// public var currentRow(get, set):Row;
	// function get_currentRow():Row {return this.scope.lookup(':currentRow');}
	// function set_currentRow(v: Row):Row {return this.scope.assign(':currentRow', currentRow);}

	function _init() {
		// this.scope.define(':currentRow', null)
		
		inline function reg(name, callable) {
			functions[name] = callable;
			scope.define(name, callable);
			return callable;
		}

		inline function f(name, o) {
			var f = F.declare(o);
			functions[name] = f;
			scope.define(name, f);
		}

		inline function nf(name, signature:String, ?ret:String, fn:Function) {
			var f = NF.declare(signature, ret, fn);
			functions[name] = f;
			scope.define(name, f);
		}

		f('int', {
			'int -> int': (Functions.identity : Function),
			'float -> int': (n: Float) -> Std.int(n),
			'String -> int': s -> Std.parseInt(s),
			'Bool -> int': (b:Bool)->b?1:0,
			'Date -> int': (d: Date) -> d.getTime()
		});


		f('str', {'Any -> String': Std.string});
		nf('regexp', 'String,String->Any', function(pattern:String, flags:String) {
			return new EReg(pattern, flags);
		});
		nf('strlower', 'String->String', (s:String) -> s.toLowerCase());
		nf('strupper', 'String->String', (s:String) -> s.toUpperCase());

		f('min', {
			'int, int -> int': ((x:Int, y:Int) -> pm.Numbers.Ints.min(x, y) : Function),
			'float, float -> float': (x:Float, y:Float) -> Math.min(x, y)
		});

		reg('hash', new NF(s -> sdbm.hash(s), [DType.TString], TInt));
		reg('cat', new NF(
			Reflect.makeVarArgs(function(args: Array<Dynamic>) {
				var b:String = '';
				for (v in args)
					b += Std.string(v);
				return b;
			}),
			[DType.TArray(TString)],
			TString
		));

		scope.define('now', F.native(function():TypedValue return DateTime.now()));
		scope.define('len', F.native(function(x: Dynamic):TypedValue {
			var v:TypedValue = x;
			if (v.isNull) return 0;
			if (v.isOfType(TString)) return v.stringValue.length;
			if (v.type.match(TArray(_))) return v.arrayAnyValue.length;
			throw new pm.Error('Invalid call');
		}));
	}

	inline function get_querySources() {
		return mSrcStack.top();
	}

	/**
	 * find in the source-stack
	 */
	function ssiter(f: TableSpec -> Bool) {
		for (scope in mSrcStack)
			for (t in scope)
				if (!f(t))
					break;
	}

	public function pushSourceScope(srcs: Array<TableSpec>) {
		mSrcStack.push(srcs);
		return this;
	}

	public function popSourceScope() {
		mSrcStack.pop();
		return this;
	}

	public function addQuerySource(src: TableSpec) {
		var qs = mSrcStack.top();
		for (i in 0...qs.length) {
			var qs = qs[i];
			if (qs == src) 
				return this;
			if (qs.name == src.name) {
				mergeSpecs(qs, src);
				return this;
			}
		}

		qs.push(src);
		return this;
	}

	public function removeQuerySource(src: TableSpec) {
		return mSrcStack.top().remove(src);
	}

	function mergeSpecs(target:TableSpec, other:TableSpec) {
		if (target.name != other.name) throw new pm.Error('Naming mismatch');
		if (other.schema != target.schema) target.schema = other.schema;
		if (other.table != target.table) target.table = nor(target.table, other.table);
	}

	@:generic
	static function has2d<T, A:Iterable<T>, A2d:Iterable<A>>(container:A2d, value:T, ?eq:T->T->Bool):Bool {
		eq = nor(eq, Functions.equality);
		var r = false;
		for (it in container)
			for (x in it) {
				if (eq(x, value)) {
					r = true;
					break;
				}
			}
		return r;
	}

	#if !debug @:noCompletion #end
	public var unaryCurrentRow:Bool = false;

	public function use(src: TableSpec) {
		final src = src.unwrap();
		if (!has2d(mSrcStack, src, (x, y) -> (x.name == y.name)))
			throw new pm.Error('${src.name} not found');
		currentDefaultTable = src.name;
		
		return this;
	}

	public function focus(row:Row, ?tableName:String) {
		if (unaryCurrentRow) {
			currentRow = row;
			return this;
		}

		tableName = tableName.nor(currentDefaultTable);
		if (tableName == null)
			throw new pm.Error('source name must be provided either explicitly or by means of Context.currentDefaultTable');

		currentRows[tableName] = row;
		
		return this;
	}

	public function getCurrentRow(?tableName: String):Row {
		if (unaryCurrentRow) {
			return this.currentRow;
		}

		tableName = tableName.nor(currentDefaultTable);
		if (tableName == null)
			throw new pm.Error('source name must be provided either explicitly or by means of Context.currentDefaultTable');

		return currentRows[tableName];
	}

	public function multipleSources():Bool {
		return querySources.length > 1;
	}

	// private inline function get_currentRow():Row {
	// 	if (currentDefaultTable == null) {
	// 		return this.currentRow;
	// 	}
	// 	else {
	// 		Console.examine(currentDefaultTable);
	// 		return this.currentRows[this.currentDefaultTable];
	// 	}
	// }
	// private function set_currentRow(row: Row):Row {
		
	// }

	public function get(name:SqlSymbol):Null<Dynamic> {
		return innerGet(name.identifier);
		if (name.type.match(Unknown)) {
			return innerGet(name.identifier);
		} else {
			return innerGetTypeAware(name);
		}
	}

	private function innerGet(name:String):Dynamic {
		if (currentRow != null) {
			if (rowHasColumn(currentRow, name)) {
				return rowGetColumn(currentRow, name);
			}
		}

		if (scope.isDeclared(name)) {
			return scope.lookup(name);
		}

		throw new pm.Error('NotFound("${name}")');
	}

	public function aliasTable(table:SqlSymbol, alias:SqlSymbol) {
		table = table.clone(Table);
		alias = alias.clone(Alias);
		aliases.define(alias.identifier, table);
		return this;
	}

	private function innerGetTypeAware(name:SqlSymbol):Null<Dynamic> {
		// switch name.type {
		// 	case Unknown:
		// 		throw new pm.Error.WTFError();
			
		// 	// case Variable:
		// 	// 	final varn = name.identifier;
		// 	// 	if (scope.isDeclared(varn)) {
		// 	// 		var value = scope.lookup(varn);
		// 	// 		if (TypedValue.is(value)) {
		// 	// 			var value:TypedValue = value;
		// 	// 			return value;
		// 	// 		}
		// 	// 		else {
		// 	// 			throw new pm.Error('Expected TypedValue', 'TypeError');
		// 	// 		}
		// 	// 	}
		// 	// 	throw new pm.Error('Not found "$varn"');

		// 	// case Function:
		// 	// 	final fname = name.identifier;
		// 	// 	if ()
			
		// 	case Field | Function | Variable:
		// 		throw new pm.Error('Unhandled $name');
		// 	case Table:
		// 		if (name.table != null) {
		// 			return name.table;
		// 		}
		// 		else if (tables.exists(name.identifier)) {
		// 			var tbl = tables[name.identifier];
		// 			name.table = tbl;
		// 			return tbl;
		// 		}
		// 		else {
		// 			// trace(this.database, name.identifier);
		// 			var tbl = glue.dbLoadTable(this.database, name.identifier);
		// 			name.table = tbl;
		// 			return tbl;
		// 		}
		// 		throw new pm.Error('Not Found');
		// 	// case Field:
		// 	// case Variable:
		// 	case Alias:
		// 		if (aliases.isDeclared(name.identifier)) {
		// 			return get(aliases.lookup(name.identifier));
		// 		}
		// 		// case Function:
		// }
		throw new pm.Error.WTFError();
	}

	public function beginScope():Context<Dynamic, Table, Row> {
		this.scope = new Scope(this.scope);
		this.aliases = new Scope(this.aliases);
		return this;
	}

	public function endScope():Context<Dynamic, Table, Row> {
		this.scope = this.scope.parent;
		this.aliases = this.aliases.parent;
		return this;
	}

	function rowHasColumn(row:Row, name:String):Bool {
		var row:Doc = Doc.unsafe(row);
		return row.exists(name);
	}

	function rowGetColumn(row:Row, name:String):Null<Dynamic> {
		// return null;
		// throw new pm.Error.NotImplementedError();
		return Doc.unsafe(row).get(name);
	}

	public function getColumnField(column:String, ?table:String) {
		if (table != null) {
			var schema = getTableSchema(table);
			if (schema == null) return null;
			return schema.column(column);
		}
		else {
			for (src in querySources) {
				if (src.schema != null) {
					var c = src.schema.column(column);
					if (c != null)
						return c;
				}
			}
		}
		throw new pm.Error(if (table != null) '$table.$column' else column, 'NotFound');
	}

	public function getTableSchema(?table: String) {
		var src = getSource(table);
		return src != null ? src.schema : null;
	}

	private function connectSource(t:TableSpec):TableSpec {
		if (t.src != null && !mSrcStack.top().has(t.src))
			addQuerySource(t.src);
		return t;
	}

	public function getSource(?n:String):Null<TableSpec> {
		if (n == null) {
			switch querySources {
				case [src]:
					connectSource(src);
					return src;

				default:
					throw new pm.Error('Unhandled');
			}
		}

		for (scope in mSrcStack) for (src in scope) {
			if (src.name == n) {
				connectSource(src);

				if (!tables.exists(src.name) && src.table != null)
					tables[src.name] = src.table;
				
				return src;
			}
		}

		if (tables.exists(n)) {
			var tbl = tables.get(n);
			assert(glue.valIsTable(tbl), new pm.Error('$tbl is not a Table pointer'));

			var schema = glue.tblGetSchema(tbl);
			
			var spec = new TableSpec(n, schema, {table:tbl});
			// spec.table = tbl;
			addQuerySource(spec);

			return spec;
		}

		try {
			var table = glue.dbLoadTable(this.database.unwrap(), n).unwrap();
			tables.set(n, table);
			var schema = glue.tblGetSchema(table);
			var spec = new TableSpec(n, schema, {table:table});
			// spec.table = table;
			addQuerySource(spec);

			return getSource(n);
		}
		catch (e: Dynamic) {
			//Console.error(e);
		}

		throw new pm.Error(n, 'NotFound');
	}

	/**
	 * attempts to resolve any given value to a reference to a Table object
	 * if `r` is `null`, `null` is returned
	 * if `r` is already a `Table` reference, return `r`
	 * if `r` is a `TableSpec` object, its `src` field is followed until its `table` field can be returned
	 * if `r` is a `String`, attempt to load a/the table whose name is `r` in the current context
	 * @param r any value for which `resolveTableFrom`'s behavior is specified
	 * @return Null<Table>
	 * @see TableSpec
	 */
	public function resolveTableFrom(r: Dynamic):Null<Table> {
		if (r == null) return null;
		if (glue.valIsTable(r)) return cast r;

		if ((r is TableSpec)) {
			var t:TableSpec = cast r;
			while (t.src != null) {
				t = t.src;
				// return resolveTableFrom(t.src);
			}

			if (t.table != null) {
				return t.table = resolveTableFrom(t.table);
			}
			
			// return t.table;
			throw new pm.Error('$t cannot be resolved to a table reference');
		}

		if ((r is String)) {
			var s:String = cast r;
			try {
				var src = getSource(s).unwrap();
				return resolveTableFrom(src);
			}
			catch (error: pm.Error) {
				// if (error.name == 'NotFound' || error.name == 'NullIsBadError') {
				// 	throw new pm.Error(s, 'NotFound');
				// }
				// throw error;
			}

			var tbl:Null<Table> = glue.dbLoadTable(this.database, s);
			if (tbl != null) {
				return resolveTableFrom(tbl);
			}
		}

		throw new pm.Error('Cannot resolve table from ${Type.typeof(r)}', 'ResolutionFailed');
	}


}

@:tink 
class NodeContext<Ctx:Context<Dynamic, Dynamic, Dynamic>> extends Context<Dynamic, Dynamic, Dynamic> {
	@:forward
	public final parent: Ctx;

	public function new(parent:Ctx, ?glue) {
		super(glue != null ? glue : parent.glue);
		
		this.parent = parent;
	}

	override function getSource(?n:String):Null<TableSpec> {
		return try super.getSource(n) catch (e: pm.Error) parent.getSource(n);
	}
	override function resolveTableFrom(r:Dynamic):Null<Dynamic> {
		return super.resolveTableFrom(r);
	}
	override function addQuerySource(src:TableSpec):Context<Dynamic, Dynamic, Dynamic> {
		return super.addQuerySource(src);
	}
}

class Variable<T> {
	public final scope: Scope<T>;
	public final type: SType;
	public var value: T;

	public function new(scope, type, ?value) {
		this.scope = scope;
		this.type = type;
		this.value = value;
	}
}

class Scope<T> {
	public var parent:Null<Scope<T>>;
	public var declared:Map<String, /* Variable<T> */T>;

	public function new(?parent) {
		this.declared = new Map();
		this.parent = parent;
	}

	private function createVar(t:SType, ?v:T):Variable<T> {
		return new Variable(this, t, v);
	}

	public function lookupLocal(name):Null<T> {
		if (declared.exists(name)) {
			var variable = declared.get(name);//.unwrap(new pm.Error('name \'$name\' is not defined'));
			return variable;
		} 
		else {
			return null;
		}
	}

	public function assignLocal(name, value:T):Null<T> {
		if (declared.exists(name)) {
			return declared[name] = value;
		}
		else {
			throw new NameError(name);
		}
	}

	public function lookup(name):Null<T> {
		return switch lookupLocal(name) {
			case null:
				return switch parent {
					case null:
						throw new NameError(name);

					default:
						return parent.lookup(name);
				}

			case result:
				return result;
		}
	}

	public function assign(name, value:T):Null<T> {
		if (declared.exists(name)) {
			return declared[name] = value;
		}
		else if (parent != null) {
			return parent.assign(name, value);
		}
		else {
			// throw new pm.Error('Name $name is not defined in this Scope');
			throw new NameError(name);
		}
	}

	public function define(name, value:T) {
		if (declared.exists(name)) {
			throw new pm.Error('$name already defined in this scope');
		} 
		else {
			declared[name] = value;
		}
	}

	public function isDeclared(name):Bool {
		if (declared.exists(name)) {
			return true;
		} 
		else if (parent != null) {
			return parent.isDeclared(name);
		} 
		else {
			return false;
		}
	}

	public function isDeclaredLocally(name):Bool {
		return declared.exists(name);
	}

	function freeLocal(name) {
		if (declared.exists(name)) {
			//TODO
			return declared.remove(name);
		}
		return false;
	}

	function free(name) {
		if (freeLocal(name))
			return true;
		else if (parent != null)
			return parent.free(name);
		else
			return false;
	}
}

/**
```haxe
	//!should be named
	class SelectSourceHandle {...}
```
 */
@:yield
class TableSpec {
	public var name:String;
	public var schema:SqlSchema<Dynamic>;
	public var stream: TableStream;
	//// public var alias:Null<String> = null;
	public var table:Null<Dynamic> = null;
	public var stmt:Null<SelectStmt> = null;
	public var src(default, set): Null<TableSpec> = null;

	// public var
	public function new(name, schema, data:{?stream:TableStream, ?stmt:SelectStmt, ?src:TableSpec, ?table:Dynamic}) {
		assert(nn(name) && nn(schema));
		
		this.name = name;
		this.schema = schema;
		this.stream = data.stream;
		
		if (data.src != null) src = data.src;
		if (data.stmt != null) stmt = data.stmt;
		if (data.table != null) table = data.table;

		// switch data {
		// 	case {stmt: null, table: null}:
		// 		Console.examine(stream);
		// 	case {stmt:q, table:_} if (q != null):
		// 		nestedQuery = q;
		// 	case {table:table}:
		// 		this.table = table;
		// }
	}

	/**
	 * TODO write a pre-return test that verifies that the first and second values yielded by the returned iterator are not identical (the iterator actually iterates!)
	 * @param g 
	 * @return Iterator<Dynamic>
	 */
	public function open(g: Contextual):Iterator<Dynamic> {
		var pre = openItr(g);
		var a:Dynamic = pre.next(), b:Dynamic = pre.next();
		assert(!pmdb.core.Arch.areThingsEqual(a, b), new pm.Error('$a == $b'));
		return openItr(g);
	}
	 function openItr(g: Contextual):Iterator<Dynamic> {
		var it = switch this {
			case {src: null, table: null, stream: null}:
				throw new pm.Error('Wtf');
			case {src: _, table: null, stream: null}: src.open(g);
			case {src: null, table: _, stream: null}: 
				var all = g.context.glue.tblGetAllRows(table);
				// Console.debug(all.slice(0, 3));
				return all.iterator();
			case {src: null, table: null, stream: _}: stream.open(g);
			default:
				throw new pm.Error('Unhandled, sha');
		};
		return it;
	}

	private function set_src(v: Null<TableSpec>) {
		if (this == v)
			throw new pm.Error('Infinite loop created');
		return this.src = v;
	}

	public function toString():String {
		var s = 'TableSpec("$name", source=';
		s += switch this {
			case {src:null, table:null, stream:null}: '??';
			case {src:_, table:null, stream:null}: 'Alias($src)';
			case {src:null, table:_, stream:null}: 'Table';
			case {src:null, table:null, stream:_}: 'NestedSelectStmt';
			default:
				throw new pm.Error('Unreachable');
		}
		s += ')';
		return s;
	}
}

private typedef Ctx = Context<Dynamic, Dynamic, Dynamic>;

class PmdbContext extends Context<Dynamic, Dynamic, Dynamic> {
	public function new(db) {
		super(new PmdbGlue(db));
		this.database = db;

		init();
	}

	function init() {
		trace('TODO');
	}
}

class DummyContext<Row> extends Context<Dynamic/*ql.sql.Dummy.AbstractDummyDatabase<ql.sql.Dummy.DummyTable<Row>, Row>*/, ql.sql.Dummy.DummyTable<Row>, Row> {
	public function new(db) {
		super(new DummyGlue(db));
		this.database = db;
	}
}

class NameError extends pm.Error {
	public final id: String;
	public function new(id, ?pos) {
		super('name \'$id\' is not defined', 'NameError', pos);
		this.id = id;
	}
}