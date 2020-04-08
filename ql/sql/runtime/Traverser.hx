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
import pm.Helpers.*;
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

	/**
	 * build array of candidate rows to be processed
	 * @param sel the `SelectStmt` instance this Traverser belongs to
	 * @param g
	 * @param output
	 * @param filter (when one exists) the function to be invoked to obtain the boolean value determining whether the currently focused row is operated upon
	 * @param interp (when being called by `Interpreter`) the Interpreter instance executing the statement
	 * @return Int
	 */
	public dynamic function computeCandidates(
		sel:Sel<Dynamic, Dynamic, Dynamic>, 
		g:Contextual, 
		output:Array<Dynamic>,
		?filter:Null<JITFn<Bool>>, 
		// ?extract:JITFn<Dynamic>, 
		?interp:Interpreter,
		?flags:{?streamed:Bool}
	):Int {
		/**
		 * TODO
		 *   refactor to use callbacks to aid clarity of control flow, and reduce repetition of heavyweight context-modifying method-calls
		 */
		if (flags == null) flags = {streamed:false};

		final c = g.context;
		final src = sel.source;

		// process flags
		final STREAMED = nor(flags.streamed, false);//TODO also automatically decide whether to stream based on data sizessd

		// var itr:() -> Iterator<Dynamic> = () -> src.getSpec(c).open(g);
		var candidates: Array<Dynamic> = [];
		var candidateCount:Int = -1;
		var proceed:Bool = true;
		var sourceNames:Array<String> = [src.getName(g.context)];

		if (proceed && src.joins != null) {
			final join = src.joins[0].unwrap();
			var jsrc = join.mJoinWith;
			if (jsrc == null)
				throw new pm.Error('Missing source item');
			sel.context.addSource(jsrc.src);
			sourceNames.push(jsrc.src.name);
			// var itr:()->Iterator<Dynamic>;

			switch join.joinType {
				case Cross:
					throw new pm.Error.NotImplementedError();

				case Inner:
					// final joinIter = () -> jsrc.src.open(g);
					// var srcIter:() -> Iterator<Dynamic> = itr;
					var l = [], r = [];
					src.getSpec(c).dump(g, l);
					jsrc.src.dump(g, r);

					var filter:JITFn<Bool> = function(g:Contextual) {
						return join.on != null ? (interp != null ? interp.pred(join.on) : join.on.eval(g)) : true;
					};

					candidateCount = 0;
					for (leftRow in l) {
						for (rightRow in r) {
							var row = [leftRow, rightRow];
							focusRows(g, row, sourceNames);
							if (filter(g)) {
								candidates.push(row);
								candidateCount++;
							}
						}
					}

				default:
					throw new pm.Error('TODO: Implement ${join.joinType}');
			}

			// for (item in itr()) {
			// 	candidates.push(item);
			// 	candidateCount++;
			// }

			proceed = false;
		}

		if (proceed) {
			if (STREAMED) {
				candidateCount = 0;
				for (item in src.getSpec(c).open(g)) {
					candidates[candidateCount++] = item;
				}
			}
			else {
				candidateCount = src.getSpec(c).dump(g, candidates);
			}
		}

		/* if (proceed && src.joins != null) {
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
		} */

		// return itr();
		if (candidateCount == -1) {
			Console.examine(candidateCount, candidates);
			throw new pm.Error('Unhandled');
		}

		if (filter == null) {
			var i = 0;
			while (i < candidates.length) {
				output.push(candidates[i++]);
				// i++;
			}
		}
		else {
			var i = 0;
			while (i < candidates.length) {
				var step:Dynamic = candidates[i];
				focusRows(g, step, sourceNames);
				if (filter(g)) {
					output.push(step);
				}
				i++;
			}
		}

		return candidateCount;
	}

//{region callback_api

	public dynamic function computeCandidatesIOC(sel:Sel<Dynamic, Dynamic, Dynamic>, g:Contextual, output:(Dynamic, offset:Int)->Void, ?filter:Null<JITFn<Bool>>, ?interp:Interpreter, ?flags:{?streamed:Bool}):Int {
		/**
		 * TODO
		 *   refactor to use callbacks to aid clarity of control flow, and reduce repetition of heavyweight context-modifying method-calls
		 */
		if (flags == null)
			flags = {streamed: false};

		final c = g.context;
		final src = sel.source;

		// process flags
		final STREAMED = nor(flags.streamed, false); // TODO also automatically decide whether to stream based on data sizessd
		
		var candidates:Array<Dynamic> = [];//array that rows are buffered into to be processed in the second stage
		var candidateCount:Int = -1;// the number of items that have been pushed onto `candidates`
		var proceed:Bool = true;// at a given step in this function, denotes whether there is still for further steps
		var sourceNames:Array<String> = [src.getName(g.context)];// list of ids for query-sources used in this SELECT

		if (proceed && src.joins != null) {// this query has one or more JOIN clause
			final join = src.joins[0].unwrap();
			var jsrc = join.mJoinWith;
			if (jsrc == null)
				throw new pm.Error('Missing source item');
			sel.context.addSource(jsrc.src);
			sourceNames.push(jsrc.src.name);

			switch join.joinType {
				case Cross:// for now, cross-joins are just not supported because I won't have much need of them and they're slow (sorry)
					throw new pm.Error.NotImplementedError();

				case Inner:
					/*TODO fix this mess :c*/
					var l = [], r = [];
					src.getSpec(c).dump(g, l);
					jsrc.src.dump(g, r);

					//FIXME filter is declared every time, whether it's necessary or not.
					final filterJoin = join.on != null;
					var filter:JITFn<Bool> = function(g:Contextual) {
						return join.on != null ? (interp != null ? interp.pred(join.on) : join.on.eval(g)) : true;
					};

					//TODO implement ability to override the iteration behavior here
					for (leftRow in l) {
						for (rightRow in r) {
							var row = [leftRow, rightRow];
							//FIXME if filter isn't used, focusRows need not be invoked here, but filter is used every time
							if (filterJoin) {
								focusRows(g, row, sourceNames);
								if (filter(g)) {
									candidates.push(row);
									candidateCount++;
								}
							}
							else {
								candidates.push(row);
								candidateCount++;
							}
						}
					}

				default:
					throw new pm.Error('TODO: Implement ${join.joinType}');
			}

			proceed = false;
		}

		if (proceed) {
			//Standard SELECT
			if (STREAMED) {
				candidateCount = 0;
				for (item in src.getSpec(c).open(g)) {
					candidates[candidateCount++] = item;
					// output(item, candidateCount++);
				}
			} 
			else {
				candidateCount = src.getSpec(c).dump(g, candidates);
				// var i = 0;
				// while (i < candidateCount) {
				// 	output(candidates[i], i);
				// 	i++;
				// }
			}
		}

		if (candidateCount == -1) {
			Console.examine(candidateCount, candidates);
			throw new pm.Error('Unhandled');
		}

		if (filter == null) {
			var i = 0;
			while (i < candidateCount) {
				output(candidates[i], i++);
			}
		} 
		else {
			var i = 0;
			while (i < candidateCount) {
				final step:Dynamic = candidates[i];
				focusRows(g, step, sourceNames);
				if (filter(g)) {
					output(step, i);
				}
				i++;
			}
		}

		return candidateCount;
	}

//}endregion

	public static function focusRows(g:Contextual, rowItem:Dynamic, sourceNames:Array<String>) {
		if ((rowItem is Array<Dynamic>)) {
			final a = cast(rowItem, Array<Dynamic>);

			for (i in 0...a.length) {
				var src:String = sourceNames[i];

				if (src != null) {
					g.context.focus(a[i], src);
				}
			}
		} else {
			var row:Dynamic = rowItem;
			var src = sourceNames[0];
			if (src != null)
				g.context.focus(row, src);
			else if (g.context.unaryCurrentRow)
				g.context.focus(row);
		}
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