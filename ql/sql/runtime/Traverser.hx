package ql.sql.runtime;

import haxe.Constraints.Function;
import ql.sql.runtime.sel.SelectStmtContext;
import ql.sql.runtime.Stmt.SelectStmt;
import ql.sql.runtime.Stmt.StmtNode;
import ql.sql.grammar.CommonTypes.Contextual;
import ql.sql.grammar.CommonTypes.JoinType;
import pmdb.core.Object;
import pmdb.core.Object.Doc as JsonObject;
import pmdb.core.FrozenStructSchema as JsonSchema;
import ql.sql.common.SqlSchema;
import ql.sql.runtime.TAst;
import ql.sql.grammar.CommonTypes.SqlSymbol;
import ql.sql.ast.Query.SelectStatement;
import ql.sql.runtime.VirtualMachine;
// import ql.sql.runtime.
import pm.Helpers.nor;
import pm.Helpers.nn;

using pm.Functions;

@:yield
class Traverser {
	// public var iter:Null<(sel:Sel<Dynamic, Dynamic, Dynamic>, source:Dynamic, fn:Dynamic->Void) -> Void> = null;
	// public var sel: Sel<Dynamic>;
	#if js
	public var generator:haxe.Constraints.Function;
	public var coroutine:js.lib.Iterator<Dynamic>;
	#end

	public function new() {
		// this.sel = sel;
		#if js
		this.iterator = jsGenItrWrap(this.iterator);
		// Console.log(this.iterator);
		#end
	}

	// public dynamic function apply

	/**
	 * constructs and returns an `Iterator<?>` over the candidate and/or operand rows of the query in question
	 * TODO the primary iterator unit doesn't need to be ()->Iterator<Dynamic>, and this harms performance. Correct this
	 * FIXME needs serious performance tuning
	 * 
	 * @param sel the `SelectStmt` instance this Traverser belongs to
	 * @param g
	 * @param filter (when one exists) the function to be invoked to obtain the boolean value determining whether the currently focused row is operated upon
	 * @param extract the function invoked to obtain the output (result) row for a given step of the traversal
	 * @param interp (when being called by `Interpreter`) the Interpreter instance executing the statement
	 * @return an `Iterator<?>` object over the candidate rows
	 */
	public dynamic function iterator(sel:Sel<Dynamic, Dynamic, Dynamic>, g:Contextual, ?filter:Null<JITFn<Bool>>, ?extract:JITFn<Dynamic>, ?interp:Interpreter):Iterator<Dynamic> {
		filter = null;
		extract = null;

		var c = g.context;
		var src = sel.source;

		var itr:() -> Iterator<Dynamic> = () -> src.getSpec(c).open(g);
		var proceed:Bool = true;

		if (proceed && src.joins != null) {
			final join = src.joins[0].unwrap();
			var jsrc = join.mJoinWith;
			if (jsrc == null)
				throw new pm.Error('Missing source item');
			// sel.context.sources.push(jsrc.src);
			sel.context.addSource(jsrc.src);
			var sourceNames:Array<String> = [src.getName(g.context), jsrc.src.name];

			switch join.joinType {
				case Cross:
					final joinIter:() -> Iterator<Dynamic> = () -> jsrc.src.open(g);
					var srcIter:() -> Iterator<Dynamic> = itr;

					#if (js || neko || py)
					itr = () -> Traverser.nestedLoopCrossJoinItr(g, sourceNames, srcIter, joinIter);
					#else
					itr = () -> Traverser.nestedLoopCrossJoin(g, sourceNames, srcIter, joinIter);
					#end

				case Inner:
					final joinIter = () -> jsrc.src.open(g);
					var srcIter:() -> Iterator<Dynamic> = itr;
					
					var filter:JITFn<Bool> = function(g:Contextual) {
						return join.on != null ? (interp != null ? interp.pred(join.on) : join.on.eval(g)) : true;
					};

					#if (js || neko || py)
					itr = () -> Traverser.nestedLoopInnerJoinItr(g, sourceNames, srcIter, joinIter, filter);
					#else
					itr = () -> Traverser.nestedLoopInnerJoin(g, sourceNames, srcIter, joinIter, filter);
					#end
				// return it;

				default:
					// Console.error('TODO');
					Console.error('TODO: Implement ${join.joinType}');
			}
		}

		return itr();
	}

	#if js
	extern inline public static function jsGenItrWrap<T, Fn:Function>(f:Fn):Fn {
		final tmp = f.toGenerator();

		return cast Reflect.makeVarArgs(function(args:Array<Dynamic>):js.lib.HaxeIterator<T> {
			var coro:js.lib.Iterator<T> = cast Reflect.callMethod(null, tmp, args);
			var iter = new js.lib.HaxeIterator(coro);
			return iter;
		});
	}
	#end

	public static function nestedLoopCrossJoinItr(g:Contextual, sources:Array<String>, a_set:Void->Iterator<Dynamic>,
			b_set:Void->Iterator<Dynamic>):Iterator<Array<Dynamic>> {
		// var acc:Array<Dynamic> = new Array();
		var rows:Array<Dynamic> = [null, null];

		for (a in a_set()) {
			// g.context.focus(a, sources[0]);
			rows[0] = a;
			for (b in b_set()) {
				// g.context.focus(b, sources[1]);
				rows[1] = b; @yield return rows;
			}
		}

		// return [].iterator();
		#if macro
		return [].iterator();
		#end
	}

	public static function nestedLoopInnerJoinItr(g:Contextual, sources:Array<String>, aSet:Void->Iterator<Dynamic>, bSet:Void->Iterator<Dynamic>,
			filter:JITFn<Bool>):Iterator<Array<Dynamic>> {
		var rows:Array<Dynamic> = [null, null];
		for (a in aSet()) {
			rows[0] = a;
			for (b in bSet()) {
				g.context.focus(a, sources[0]);
				g.context.focus(b, sources[1]);
				var doJoin:Bool = filter(g);
				if (doJoin) {
					rows[1] = b; @yield return rows;
				}
			}
		}

		#if macro
		return [].iterator();
		#end
	}

	public static function nestedLoopInnerJoin(g:Contextual, sources:Array<String>, aSet:Void->Iterator<Dynamic>, bSet:Void->Iterator<Dynamic>,
			filter:JITFn<Bool>):Iterator<Array<Dynamic>> {
		// return aSet().flatMap(function(a: Dynamic):Iterator<Array<Dynamic>> {
		//     return bSet().map(function(b: Dynamic) {
		//         return [a, b];
		//     })
		//     .filter(function(arr: Array<Dynamic>) {
		//         g.context.focus(arr[0], sources[0]);
		//         g.context.focus(arr[1], sources[1]);
		//         return arr[0] != arr[1] && filter(g);
		//     });
		// });
		var aa = aSet().array();
		var ba = bSet().array();
		var res = [];
		for (a in aa) {
			for (b in ba) {
				if (a != b) {
					g.context.focus(a, sources[0]);
					g.context.focus(b, sources[1]);
					if (filter(g)) {
						res.push([a, b]);
					}
				}
			}
		}
		return res.iterator();
	}

	public static function nestedLoopCrossJoin(g:Contextual, sources:Array<String>, a_set:Void->Iterator<Dynamic>,
			b_set:Void->Iterator<Dynamic>):Iterator<Array<Dynamic>> {
		// var rows:Array<Dynamic> = [null, null];
		// return a_set.iterator().flatMap(function(a:Dynamic):Iterator<Array<Dynamic>> {
		//     return b_set.iterator().map(function(b: Dynamic) {
		//         return [a, b];
		//     });
		// });
		var aa = a_set().array();
		var ba = b_set().array();
		var res = [];
		for (a in aa) {
			for (b in ba) {
				res.push([a, b]);
			}
		}
		return res.iterator();
	}
}