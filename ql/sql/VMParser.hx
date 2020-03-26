package ql.sql;

import pm.OneOrMany;
import haxe.ds.GenericStack;
import ql.sql.ast.Query.SelectSourceItem as SelSrc;
import ql.sql.ast.Query.SelectElement as SelEl;
import ql.sql.grammar.CommonTypes;
import ql.sql.ast.Query.Binop;
import ql.sql.ast.Query.Binop.*;
import ql.sql.ast.Query;

import haxe.io.Input;
import haxe.io.StringInput;

using Type;


@:access(hscript)
class VMParser {
	private var query : String;
	private var pos : Int;
	private var keywords : Map<String, Bool>;
	private var binops: Map<String, Binop>;
	private var sqlTypes : Map<String, SqlType>;
	private var idChar : Array<Bool>;
	private var cache : Array<Token>;
	private var opPriority : Map<Binop, Int>;
	private var opRightAssoc : Map<String, Bool>;
	private var opChars: Map<String, Bool>;
	private var eofStack : Array<Token>;
	private var symbolTable: SymbolTable<SqlSymbolType, SqlSymbol>;
	private var parameters: Array<String>;

    /* Constructor Functions */
	public function new() {
		idChar = [];
		for (i in 'A'.code...'Z'.code + 1)
			idChar[i] = true;
		for (i in 'a'.code...'z'.code + 1)
			idChar[i] = true;
		for (i in '0'.code...'9'.code + 1)
			idChar[i] = true;
		idChar['_'.code] = true;

		keywords = [for (k in KWDS) k => true];
		opChars = [for (c in STDOPCHARS.split('')) c => true];
		var binaryOps = [
			'=' => Binop.Eq,
			'!=' => Binop.NEq,
			'IN' => Binop.In,
			'LIKE' => Binop.Like,
			'+' => Binop.Add,
			'-' => Binop.Sub,
			'/,DIV' => Binop.Div,
			'MULT,*' => Binop.Mult,
			'%,MOD' => Binop.Mod,
			'>' => Binop.Gt,
			'>=' => Binop.Gte,
			'<' => Binop.Lt,
			'<=' => Binop.Lte,
			'&&,AND' => Binop.LogAnd,
			'||,OR' => Binop.LogOr
		];
		binops = new Map();
		for (k=>op in binaryOps) {
			var keys = k.split(',');
			for (key in keys) {
				binops[key] = op;
			}
		}

		sqlTypes = [
			// "DATE" => SDate,
			"DATETIME" => SDateTime,
			"FLOAT" => SFloat,
			// "DOUBLE" => SDouble,
			"INT" => SInt,
			"INTEGER" => SInt,
			// "BIGINT" => SBigInt,
			"TEXT" => SText,
			"BLOB" => SBlob,
			"BYTES" => SBlob
		];

		var priorities = [
			[Binop.Mod],
		    [Binop.Mult, Binop.Div],
		    [Binop.Add, Binop.Sub],
		    [],
			[],
			[Binop.Eq, Binop.NEq, Binop.Gt, Binop.Lt, Binop.Gte, Binop.Lte, Binop.In, Binop.Like],
			[/*Binop.Interval*/],
			[Binop.LogAnd],
			[Binop.LogOr],
			[/*"=", "+=", "-=", "*=", "/=", "%=", "<<=", ">>=", ">>>=", "|=", "&=", "^=", "=>"*/]
		];

        opPriority = new Map();
	    for (i in 0...priorities.length) {
	        for (x in priorities[i]) {
	            opPriority.set(x, i);
	        }
	    }

	    eofStack = [Token.Eof];
		symbolTable = new SymbolTable(function(sym: SqlSymbol) {
			return sym.identifier;
		});

		parameters = new Array();
	}

	static inline function _setup(p:VMParser, s:String){
	    p.query = s;
	    p.pos = 0;
	    p.cache = [];
	}

	private function symbol(id: String, ?pos:haxe.PosInfos):SqlSymbol {
		if (id == null) {
			return null;
		}

		// return SqlSymbol.named(id);
		return switch symbolTable.lookup(id) {
			case null, _[0]=>null:
				symbolTable.add(id, new SqlSymbol(id));
				var r = symbol(id);
				r.type = SqlSymbolType.Unknown;
				return r;

			case _[0]=>r if (r != null):
				return r;

			default:
				throw new pm.Error('Aww, dis not gud sha');
		}
	}

    /**
	  parse out a Query statement from the given String
	  [TODO] parse multiple SQL statements in sequence
     **/
	public function parse(q : String) {
		this.query = q;
		this.pos = 0;
		cache = [];

		#if neko
		try {
			return parseQuery();
		}
		catch( e : Dynamic ) {
			neko.Lib.rethrow(e+" in " + q);
			return null;
		}
		#else
		return parseQuery();
		#end
	}

	private inline function _setInput(q: String) {
		this.query = q;
		this.pos = 0;
		cache = [];
	}

	private inline function push(t) {
		cache.push( t );
	}

	private function nextChar():Int {
		var c:Int = StringTools.fastCodeAt(query, pos++);
		if (Math.isNaN(c) || c < 0) {
			c = -1;
		}
		return c;
	}

	// private inline function createLinkedInput() {
	// 	return new SlaveInput(this);
	// }
	// private inline function createLinkedHScriptParser():hscript.Parser {
	// 	var p = new hscript.Parser();
	// 	p.allowJSON = p.allowMetadata = p.allowTypes = true;
	// 	p.input = createLinkedInput();
	// 	return p;
	// }

	private inline function isIdentChar( c : Int ) {
		return idChar[c];
	}

	private inline function isOpChar(c: Int) {
		return c != -1 && opChars[String.fromCharCode(c)];
	}

	inline function invalidChar(c, ?pos:haxe.PosInfos) {
		throw new pm.Error("Unexpected char '" + String.fromCharCode(c)+"'", null, pos);
	}

	function token() {
		var t = cache.pop();
		if (t != null) return t;
		var binop = Binop;
		while( true ) {
			var c = nextChar();
			switch ( c ) {
			case -1:
				return Eof;
			case ' '.code, '\r'.code, '\n'.code, '\t'.code:
				continue;
			case '*'.code:
				return Star;
			case '('.code:
				return POpen;
			case ')'.code:
				return PClose;
			case '['.code:
				return Token.BrOpen;
			case ']'.code:
				return Token.BrClose;
			case '{'.code:
				return Token.CBrOpen;
			case '}'.code:
				return Token.CBrClose;
			case ','.code:
				return Comma;
			case '.'.code:
				return Dot;
			case ':'.code:
				return DoubleDot;
			case ';'.code:
				return Semicolon;
			case '?'.code:
				return QMark;
			case '!'.code:
			    if (nextChar() == '='.code)
			        return Op(Binop.NEq);
			    --pos;
				return Not;
			
			// operators
			case _ if (#if neko c != null && #end isOpChar(c)):
				var start = pos - 1;
				do {
					c = nextChar();
				} 
				while (#if neko c != null && #end isOpChar(c));
				pos--;
				var strOp = query.substr(start, pos - start);
				if (binops.exists(strOp)) {
					return Op(binops[strOp]);
				}
				else {
					throw new pm.Error('Unhandled $strOp operator');
				}
			
			case '='.code:
				return Op(Binop.Eq);
			case '+'.code:
			    return Op(Binop.Add);
			case '-'.code:
			    return Op(Binop.Sub);
			case '/'.code:
				return Op(Binop.Div);
			case '&'.code:
				if (nextChar() == '&'.code)
					return Op(Binop.LogAnd);
				--pos;
				invalidChar(nextChar());
			case '|'.code:
				if (nextChar() == '|'.code)
					return Op(Binop.LogOr);
				--pos;
				invalidChar(nextChar());
			case '>'.code:
				if (nextChar() == '='.code)
					return Op(Binop.Gte);
				--pos;
				return Op(Binop.Gt);
			case '<'.code:
				if (nextChar() == '='.code)
					return Op(Binop.Lte);
				--pos;
				return Op(Binop.Lt);
			case '`'.code:
				var start = pos;
				do {
					c = nextChar();
				} 
				while (isIdentChar( c ));
				if (c != '`'.code)
					throw "Unclosed `";
				return Ident(query.substr(start, (pos - 1) - start));

			case '"'.code:
			    var start = pos;
			    var escaped = false;
			    do {
			        c = nextChar();
			        if ( escaped ) {
			            escaped = false;
			            continue;
			        }

			        switch c {
                        case '"'.code:
                            if (!escaped)
                                return CString(query.substr(start, (pos - 1) - start));

                        case '\\'.code:
                            if (!escaped)
                                escaped = true;

                        default:
                            //
			        }
			    }
			    while ( true );

			case '0'.code, '1'.code, '2'.code, '3'.code, '4'.code, '5'.code, '6'.code, '7'.code, '8'.code, '9'.code:
				var n = (c - '0'.code) * 1.0;
				var exp = 0.;
				while( true ) {
					c = nextChar();
					exp *= 10;
					switch( c ) {
					case 48,49,50,51,52,53,54,55,56,57:
						n = n * 10 + (c - 48);
					case '.'.code:
						if( exp > 0 )
							invalidChar(c);
						exp = 1.;
					default:
						pos--;
						var i = Std.int(n);
						return (exp > 0) ? CFloat(n * 10 / exp) : ((i == n) ? CInt(i) : CFloat(n));
					}
				}
			default:
				if( (c >= 'A'.code && c <= 'Z'.code) || (c >= 'a'.code && c <= 'z'.code) ) {
					var start = pos - 1;
					do {
						c = nextChar();
					}
					while( #if neko c != null && #end isIdentChar(c) );
					pos--;
					var i = query.substr(start, pos - start);
					var iup = i.toUpperCase();
					if (keywords.exists(iup)) {
						// var kwd = Kwd(iup);
						if (binops.exists(iup)) {
							return Op(binops[iup]);
						}
						return Kwd(iup);
					}
					return Ident(i);
				}
				if (StringTools.isEof(c))
					return Eof;
				invalidChar(c);
			}
		}
	}

	private function tokenStr(t) {
		return switch( t ) {
		case Kwd(k): k;
		case Ident(k): k;
		case CString(k): '"$k"';
		case Star: "*";
		case Dot: '.';
		case DoubleDot: ':';
		case Semicolon: ';';
	    case Not: '!';
		case Eof: "<eof>";
		case POpen: "(";
		case PClose: ")";
		case BrOpen: '[';
		case BrClose: ']';
		case CBrOpen: '{';
		case CBrClose: '}';
		case QMark: "?";
		case Comma: ",";
		case Op(o): opStr(o);
		case CInt(i): "" + i;
		case CFloat(f): "" + f;
		};
	}

	function opStr( op : Binop ) {
		switch op {
			case Eq:
			case NEq:
			case In:
			case Like:
			case Match:
			case Add:
			case Sub:
			case Div:
			case Mult:
			case Mod:
			case Gt:
			case Gte:
			case Lt:
			case Lte:
			case LogAnd:
			case LogOr:
		}
		return switch( op ) {
		case Eq: "=";
		case NEq: "!=";
		case Gt: ">";
		case Lt: "<";
		case Gte: ">=";
		case Lte: "<=";
	    case In: "in";
		case Like: "like";
		case Match: "match";

		case Add: '+';
		case Sub: '-';
		case Div: '/';
		case Mult: '*';
		case Mod: "%";

		case LogAnd: '&&';
		case LogOr: '||';
		}
	}

	function req(tk:Token, ?pos:haxe.PosInfos) {
		var t = token();
		if (!Type.enumEq(t, tk))
		    unexpected(t, pos);
	}

	function maybe(tk: Token):Bool {
	    var t = token();
	    if (Type.enumEq(tk, t)) {
	        return true;
	    }
        else {
            push( t );
            return false;
        }
	}

	function unexpected(t, ?expectedInstead:OneOrMany<Token>, ?pos:haxe.PosInfos) : Dynamic {
		//throw "Unexpected " + tokenStr(t);
		var exp = expectedInstead == null ? null : expectedInstead.asMany();
		var expectedString = exp == null ? null : (
			exp.map(t -> Std.string(t)).join('|') + 
			', or ' +
			haxe.Json.stringify(exp.map(t -> tokenStr(t)))
		);
		throw SqlError.UnexpectedToken(t, expectedString, pos);
		return null;
	}

	function ident() : String {
		return switch (token()) {
		    case Ident(i): i;
		    case t: unexpected( t );
		}
	}

	function end():Bool {
		var t = token();
        if (Type.enumEq(eofStack[eofStack.length - 1], t)) {
            if (eofStack.length > 1) {
                eofStack.pop();
            }
        }
        else {
            unexpected( t );
        }
		return true;
	}
	inline function asEndOfFile(tk: Token) {
	    eofStack.push( tk );
	}

	function parseQueryNext(q) {
	    var t = token();
	    switch t {
            case Eof:
                return q;

            case _:
                push( t );
                return q;
	    }
	}

/* 	function parseSelectElemList():Array<SelectElement> {
		return parseSelectElementList();
	}

	function parseSelectElementList() {
		var results:Array<SelectElement> = new Array();
		var t = token();
		push(t);
		while (true) {
			t = token();
			switch t {
				case Star:
					results.push(new AllColumns());

				case _:
					push(t);
					var expr = parseExpr();
					//trace('' + expr);
					switch expr {
						case Expr.CTrue|CFalse|CNull:
							throw 'Constants disallowed';

						case EId(name):
							results.push(new ColumnName(symbol(name)));

						case EField(EId(table), column):
							results.push(switch column {
								case All: new AllColumns(symbol(table));
								case Name(column): new ColumnName(symbol(column), symbol(table));
							});

						case other:
							var expression:Expression = convertExprToExpression(other);
							results.push(expression);

						// default:
					}

					if (maybe(Kwd('AS'))) {
						var alias =	symbol(ident());
						var last:ESelectElement = results.pop();
						switch last {
							case ColumnName(el):
								results.push(new AliasedTerm(el, alias));

							case Expression(el):
								results.push(new AliasedTerm(el, alias));

							case other:
								throw new pm.Error('Unexpected $other');
						}
					}
			}

			if (maybe(Comma)) {
				continue;
			}
			else {
				break;
			}
		}

		if (results.length == 0) {
			unexpected(t);
		}
		return results;
	} */

	function parseSelEls():Array<SelEl> {
		var results:Array<SelEl> = new Array();
		var t:Token = null;
		// push(t);
		while (true) {
			t = token();
			switch t {
				case Star:
					results.push(SelEl.AllColumns(null));

				case _:
					push(t);
					var expr = parseExpr();
					// trace('' + expr);
					switch expr {
						case CTrue | CFalse | CNull:
							throw 'Constants disallowed';

						case EId(name):
							// results.push(new ColumnName(symbol(name)));
							results.push(SelEl.Column(null, name, null));

						case EField(EId(table), column):
							results.push(switch column {
								case All: AllColumns(table);
								case Name(column): Column(table, column, null);
							});

						case other:
							// var expression:Expression = convertExprToExpression(other);
							// results.push(expression);
							results.push(SelEl.Expr(expr, null));
					}

					if (maybe(Kwd('AS'))) {
						var alias = ident();
						var last:SelEl = results.pop();
						switch last {
							case Column(table, column, null):
								results.push(Column(table, column, alias));
								// results.push(new AliasedTerm(el, alias));

							case Expr(e, null):
								// results.push(new AliasedTerm(el, alias));
								results.push(Expr(e, alias));

							case other:
								throw new pm.Error('Unexpected $other');
						}
					}
			}

			if (maybe(Comma))
				continue;
			else
				break;
		}

		if (results.length == 0) {
			unexpected(t);
		}

		return results;
	}

	/*
	function parseQuerySourceItem():TableSourceItem {
		var t = token();
		var src:Null<TableSourceItem> = null;
		// var tbl: Null<TableSourceItem> = null;
		switch (t) {
			case POpen:
				src = parseQuerySourceItem();
				req(PClose);

			case Kwd('SELECT'):
				// throw new pm.Error('TODO: nested SELECT stmt');
				push(t);
				var stmt = new NestedSelectStatement(parseQuery().toSelect());
				src = stmt;
				// if (!maybe(Kwd('AS')))
				// 	unexpected(token(), Kwd('AS'));

			case Ident(name):
				var tableSpec = new TableSpec(symbol(name));
				src = tableSpec;

			case _:
				unexpected(t);
		}

		if (maybe(Kwd('AS'))) {
			src = new AliasedTerm(src, symbol(ident()));
		}

		return src;
	}
	*/

	function parseSelectSourceItem():SelSrc {
		var t = token();
		var src:Null<SelSrc> = null;
		switch t {
			case POpen:
				src = parseSelectSourceItem();
				req(PClose);

			case Kwd('SELECT'):
				push(t);
				var stmt = parseQuery();
				src = SelSrc.Subquery(switch stmt {
					case Select(select): select;
					case other:
						throw new pm.Error('Unexpected $other');
				});

			case Ident(name):
				var tableSpec = new TableSpec(name);
				src = SelSrc.Table(tableSpec);

			default:
				unexpected(t);
		}

		if (maybe(Kwd('AS'))) {
			src = SelSrc.Aliased(src, ident());
		}

		return src;
	}

	function parseQuerySource():SelectSource {
		var src:Null<SelectSource> = new SelectSource(parseSelectSourceItem());
		
		var t = token();
		switch t {
			case Kwd(jk = ("INNER" | "OUTER" | "LEFT" | "RIGHT" | "CROSS")):
				final joinModKwds = [jk];
				while (true) {
					t = token();
					switch t {
						case Kwd(kwd = ("INNER" | "OUTER" | "LEFT" | "RIGHT" | "CROSS")):
							joinModKwds.push(kwd);

						default:
							push(t);
							break;
					}
				}
				req(Kwd('JOIN'));
				var src2 = parseQuerySource();
				var pred = null;
				if (maybe(Kwd('ON'))) {
					pred = parsePredicate();
				}
				
				if (src.joins == null) 
					src.joins = [];
				src.joins.push(new JoinClause(src2.sourceItem, switch jk {
					case 'INNER': JoinType.Inner;
					case 'OUTER': JoinType.Outer;
					case 'CROSS': JoinType.Cross;
					case 'LEFT': JoinType.OuterLeft;
					case 'RIGHT': JoinType.OuterRight;
					default:
						throw new pm.Error('Unhandled $jk JOIN!');
				}, pred));

			default:
				push(t);
		}

        return src;
	}

	function parseSqlType():SqlType {
	    var t = token();
	    switch t {
            case Ident(i), Kwd(i):
                var st = sqlTypes.get(i.toUpperCase()), params=null;
                if (st != null) {
                    if (maybe(POpen))
                        params = parseExprList(PClose);
                }
                return st;

            default:
                unexpected( t );
                throw t;
        }
	}

	/*
	function parseTableCreateEntries():Array<CreateTableEntry> {
	    var entries:Array<CreateTableEntry> = new Array();
        maybe( POpen );
        while ( true ) {
            switch (token()) {
                case Ident( name ):
                    var f:FieldDesc = {
                        name: name 
                    };

                    entries.push(CreateTableEntry.TableField( f ));
                    f.type = parseSqlType();

                    while ( true ) {
                        var t = token();
                        switch t {
                            case Kwd("NOT"):
                                req(Kwd("NULL"));
                                f.notNull = true;
                                continue;

                            case Kwd("AUTO_INCREMENT"|"AUTOINCREMENT"):
                                f.autoIncrement = true;
                                continue;

                            case Kwd("PRIMARY"):
                                req(Kwd("KEY"));
                                f.primaryKey = true;
                                continue;

                            case Kwd("UNIQUE"):
                                f.unique = true;
                                continue;

                            case PClose, Eof, Comma:
                                push( t );
                                break;

                            case t:
                                unexpected(t);
                        }
                    }

                case Kwd("PRIMARY"):
                    req(Kwd("KEY"));
                    req(POpen);
                    var key = [];
                    while ( true ) {
                        key.push(ident());
                        switch (token()) {
                            case PClose:
                                break;
                            case Comma:
                                continue;
                            case t:
                                unexpected( t );
                        }
                    }
                    entries.push(CreateTableEntry.TableProp(PrimaryKey(key)));

                case t:
                    unexpected(t);
            }

            switch (token()) {
                case Comma:
                    continue;

                case PClose, Eof:
                    break;

                case t:
                    unexpected(t);
            }
        }

        while ( true ) {
            switch (token()) {
                case Eof:
                    break;

                case Kwd("ENGINE"):
                    req(Op(Eq));
                    entries.push(TableProp(Engine(ident())));

                case t:
                    unexpected(t);
            }
        }

        return entries;
	}
	*/

	#if !poop
	public function parseQuery():Query return parseQueryStmt();
	#end

	function parseQueryStmt():Query {
		var t = token();
		switch t {
			case Kwd('SELECT'):
				/* 
				var fields = parseSelEls();
				req(Kwd('FROM'));
				var src = parseSelectSourceItem();
				var parseWherePredicate = false;

				while (true) {
					try {
						end();
						// cond = Expr.CTrue;
						break;
					} 
					catch (e:SqlError) {
						switch e {
							case UnexpectedToken(Kwd("WHERE"), _):
								parseWherePredicate = true; // parseExprNext(parseExpr());
								break;
								
								case UnexpectedToken(Kwd("GROUP"), _):
									push(Kwd('GROUP'));
									// readGroupByClause(queryExpression);
									continue;
									
									case UnexpectedToken(t, _):
										push(t);
										break;
										
										default:
											throw e;
										}
									}
								}
								
								var whereTerm = null;
								if (parseWherePredicate)
									parseExpr();
								return null;
								*/

				var fields = parseSelEls();
				req(Kwd("FROM"));

				inline function readGroupByClause(q:QueryExpression) {
					req(Kwd('GROUP'));
					req(Kwd('BY'));
					var items:Array<{e:Expr, desc:Bool}> = new Array();
					while (true) {
						var groupExpr = parseExpr();
						var item = {e: groupExpr, desc: false};
						var tk = token();
						items.push(item);
						switch tk {
							case Kwd('ASC'):
								item.desc = false;

							case Kwd('DESC'):
								item.desc = true;

							case Comma:
								continue;

							case _:
								push(tk);
								break;
						}
					}

					var groupByItems = [for (itm in items) new GroupByItem(itm.e, itm.desc)];
					q.from.groupBy = new GroupByClause(groupByItems);
				}

				var queryExpression:QueryExpression = new QueryExpression(fields);
				var src = parseQuerySource();
				queryExpression.from = new FromClause([src]);

				var parseWherePredicate = false;
				while (true) {
					try {
						end();
						break;
					} 
					catch (e:SqlError) {
						switch e {
							case UnexpectedToken(Kwd("WHERE"), _):
								parseWherePredicate = true; // parseExprNext(parseExpr());
								break;

							case UnexpectedToken(Kwd("GROUP"), _):
								push(Kwd('GROUP'));
								readGroupByClause(queryExpression);
								continue;

							case UnexpectedToken(t, _):
								push(t);
								break;

							default:
								throw e;
						}
					}
				}

				var sel = new SelectStatement(new QueryIntoExpression(queryExpression));
				if (parseWherePredicate) {
					var whereExpression = parsePredicate();
					queryExpression.from.where = new WhereClause(whereExpression);
				}

				// parse SELECT clauses
				while (true) {
					/**  ORDER BY clause  **/
					if (maybe(Kwd('ORDER'))) {
						req(Kwd('BY'));
						var orderExpr = parseExpr();
						var ordering:Null<SortType> = switch token() {
							case Kwd('ASC'): SortType.Asc;
							case Kwd('DESC'): SortType.Desc;
							case t:
								push(t);
								null;
						}
						queryExpression.orderBy = new OrderByClause([
							new OrderByExpression(orderExpr, false, ordering)
						]);
						/**
							[TODO] parse multiple `order by` expressions
						**/
					}
					/**  LIMIT clause  **/
					else if (maybe(Kwd('LIMIT'))) {
						var limit:Int = -1;
						switch token() {
							case t = CInt(i):
								limit = i;
								if (i <= 0) {
									unexpected(t);
								}
							
							case t:
								unexpected(t);
						}

						queryExpression.limit = new LimitClause(limit);
						if (maybe(Kwd('OFFSET'))) {
							var offset:Int = -1;
							switch token() {
								case t = CInt(i):
									offset = i;
									if (i <= 0) {
										unexpected(t);
									}

								case t:
									unexpected(t);
							}
							queryExpression.limit.offset = offset;
						}
					}
					/**
						[TODO/FIXME] the order of (parsing) operations is incorrect here.
						The `GROUP BY` clause is a child of the `FROM clause`, **not** the `SELECT` statement as a whole.
						As such, it should be parsed, when present, **before** any `LIMIT|ORDER BY` clauses.
					**/
					else if (maybe(Kwd('GROUP'))) {
						readGroupByClause(queryExpression);
					}
					else {
						break;
					}
				}

				// parse INTO clause
				if (maybe(Kwd('INTO'))) {
					throw new pm.Error('Unhandled');
				}

				return Query.Select(sel);

			/* UPDATE $table SET $operations [WHERE $condition(s)] */
			case Kwd("UPDATE"):
				var tableName = ident();
				req(Kwd('SET'));
				var ops = parseUpdateOpList('SET');
				var where = null;
				if (maybe(Kwd('WHERE'))) {
					where = parsePredicate();
				}
				// return new UpdateStatement(tableName, ops, where);
				throw new pm.Error('TODO: UpdateStatement');

			case Kwd("INSERT"):
				// Console.error('INSERT');
				req(Kwd('INTO'));
				var path = parseTablePath();
				var columns = null;
				if (maybe(POpen)) {
					columns = parseIdentList(Comma, PClose);
				}
				req(Kwd('VALUES'));
				var values:Array<Array<Expr>> = new Array();
				while (true) {
					req(POpen);
					// push(POpen);
					if (true) {
						var row = parseExprList(PClose);
						values.push(row);

						if (maybe(Comma)) {
							continue;
						} else {
							break;
						}
					} else {}
				}

				throw new pm.Error('TODO: InsertStatement');
				// return new InsertStatement(path, columns, values);

			case other:
				throw new pm.Error('Unhandled $t');
		}
	}

	/**
	 * parses the next SQL statement from the source
	 * @return SqlStatement
	 */
	#if poop
	function parseQuery():SqlStatement {
		var t = token();
		switch( t ) {
		    /* --SELECT STATEMENT-- */
            case Kwd("SELECT"):
				var fields = parseSelEls();
				req(Kwd("FROM"));
				
				inline function readGroupByClause(q:QueryExpression) {
					req(Kwd('GROUP'));
					req(Kwd('BY'));
					var items:Array<{e:Expr, desc:Bool}> = new Array();
					while (true) {
						var groupExpr = parseExpr();
						var item = {e: groupExpr, desc: false};
						var tk = token();
						items.push(item);
						switch tk {
							case Kwd('ASC'):
								item.desc = false;
	
							case Kwd('DESC'):
								item.desc = true;
	
							case Comma:
								continue;
	
							case _:
								push(tk);
								break;
						}
					}
					var groupByItems = [for (itm in items) new GroupByItem(convertExprToExpression(itm.e), itm.desc)];
					q.from.groupBy = new GroupByClause(groupByItems);
				}

				var queryExpression:QueryExpression = new QueryExpression(fields);
				var src = parseQuerySource();
				//Console.debug(src);
				queryExpression.from = new FromClause([src]);
                var parseWherePredicate = false;
				
                while (true) try {
                    end();
					// cond = Expr.CTrue;
					break;
                }
                catch(e: SqlError) switch e {
                    case UnexpectedToken(Kwd("WHERE"), _):
						parseWherePredicate = true;//parseExprNext(parseExpr());
						break;

					case UnexpectedToken(Kwd("GROUP"), _):
						push(Kwd('GROUP'));
						readGroupByClause(queryExpression);
						continue;
						
					case UnexpectedToken(t, _):
						push(t);
						break;

                    default:
                        throw e;
				}

                var sel = new SelectStatement(new QueryIntoExpression(queryExpression));
				if (parseWherePredicate) {
					var whereExpression = parsePredicate();
					queryExpression.from.where = new WhereClause(whereExpression);
				}

				// parse SELECT clauses
				while (true) {
					if (maybe(Kwd('ORDER'))) {
						req(Kwd('BY'));
						var orderExpr = parseExpr();
						queryExpression.orderBy = new OrderByClause([
							new OrderByExpression(convertExprToExpression(orderExpr))
						]);
						/**
						  [TODO] parse full `ORDER BY` clause, to SQL spec
						 **/
					}
					else if (maybe(Kwd('LIMIT'))) {
						var limit:Int = -1;
						switch token() {
							case t=CInt(i):
								limit = i;
								if (i <= 0) {
									unexpected(t);
								}
							case t:
								unexpected(t);
						}

						queryExpression.limit = new LimitClause(limit);
						if (maybe(Kwd('OFFSET'))) {
							var offset:Int = -1;
							switch token() {
								case t=CInt(i):
									offset = i;
									if (i <= 0) {
										unexpected(t);
									}

								case t:
									unexpected(t);
							}
							queryExpression.limit.offset = offset;
						}
						//trace(queryExpression.limit);
					}
					/**
					  [TODO/FIXME] the order of (parsing) operations is incorrect here. 
						  The `GROUP BY` clause is a child of the `FROM clause`, **not** the `SELECT` statement as a whole.
						  As such, it should be parsed, when present, **before** any `LIMIT|ORDER BY` clauses.
					 **/
					else if (maybe(Kwd('GROUP'))) {
						readGroupByClause(queryExpression);
					}
					else {
						break;
					}
				}

				// parse INTO clause
				if (maybe(Kwd('INTO'))) {
					throw new pm.Error('Unhandled');
					var targetExpr = parseExpr();
					switch targetExpr {
						case EId(tableName):
							//

						default:
					}
				}

				return (sel : SqlStatement);
			
			/* UPDATE $table SET $operations [WHERE $condition(s)] */
			case Kwd("UPDATE"):
				var tableName = symbol(ident());
				req(Kwd('SET'));
				var ops = parseUpdateOpList('SET');
				var where = null;
				if (maybe(Kwd('WHERE'))) {
					where = parsePredicate();
				}
				return new UpdateStatement(tableName, ops, where);


			case Kwd("INSERT"):
				//Console.error('INSERT');
				req(Kwd('INTO'));
				var path = parseTablePath();
				var columns = null;
				if (maybe(POpen)) {
					columns = parseIdentList(Comma, PClose);
				}
				req(Kwd('VALUES'));
				var values:Array<Array<Expression>> = new Array();
				while (true) {
					req(POpen);
					// push(POpen);
					if (true) {
						var row = parseExpressionList(PClose);
						values.push(row);

						if (maybe(Comma)) {
							continue;
						}
						else {
							break;
						}
					}
					else {

					}
				}

				//Console.examine(path, columns, values);
				return new InsertStatement(path, columns, values);

			default:
		}
		
		throw new pm.Error("Unsupported query " + query);
	}
	#end

	function parseTablePath():TablePath {
		var all:Array<String> = new Array();
		while (true) {
			var t = token();
			switch t {
				case Ident(n):
					all.unshift(n);

					if (maybe(Token.Dot)) {
						t = token();
						push(t);
						if (t.match(Ident(_))) {
							continue;
						}
						else {
							break;
						}
					}
					else {
						break;
					}

				case Dot:
					throw new pm.Error('Unreachable');

				default:
					push(t);
					break;
			}
		}
		
		if (all.length == 0)
			throw new pm.Error('Empty path');
		
		var name = all.pop();
		return new TablePath(name, all.length == 0 ? null : all);
	}

	function parseUpdateOpList(opType: String):Array<UpdateOp> {
		var type:UpdateOpType = switch opType.toUpperCase() {
			case 'SET': SET;
			case other:
				throw new pm.Error('Invalid UpdateOpType: "$other"');
		};

		var res = [];
		var t;// = token();
		while (true) {
			t = token();
			//Console.debug(t);
			switch t {
				case Eof:
					push(t);
					break;

				case Comma:
					continue;

				case Kwd('WHERE'):
					push(t);
					break;

				case Ident(columnName):
					req(Op(Binop.Eq));
					var e = parseExpr();
					res.push(new UpdateOp(type, columnName, e));
					if (maybe(Comma)) {
						continue;
					}
					else if (maybe(Kwd('WHERE'))) {
						break;
					}

				default:
					unexpected(t, Ident('<columnName>'));
					// var e = parseExprNext(parseExpr());
					// var e = convertExprToExpression(e);
					// res.push();
			}
		}

		return res;
	}

	function parsePredicate():Expr {
		var predicateExpr = parseExpr();
		var predicate = predicateExpr;
		while (true) {
			var tmp = predicate;
			predicate = parsePredicateNext(tmp);
			if (tmp == predicate) break;
		}
		return predicate;
	}

	function parsePredicateNext(pred: Expr):Expr {
		var tk = token();
		switch tk {
			// case Op(LogAnd), Kwd('AND'):
			// 	var pred2 = convertExprToPredicate(parseExpr());
			// 	return parsePredicateNext(new AndPredicate(pred, pred2));

			// case Op(LogOr), Kwd('OR'):
			// 	var pred2 = convertExprToPredicate(parseExpr());
			// 	return parsePredicateNext(new OrPredicate(pred, pred2));

			default:
				push(tk);
				return pred;
		}
	}

	/* var anonParameterLabelCounter:Int = 1;
	function __convertExprToExpression(e:Expr, ?pos:haxe.PosInfos):Expression {
		switch e {
			case CTrue, CFalse, CNull:
				return switch e {
					case CTrue: new BoolValue(true);
					case CFalse: new BoolValue(false);
					case CNull: new NullValue();
					default:
						throw new pm.Error.WTFError();
				}

			case CInt(i):
				return new IntValue(i);

			case CFloat(n):
				return new FloatValue(n);

			case CString(v):
				return new StringValue(v);

			case CParam(name):
				if (name == null) name = '?tmp_${anonParameterLabelCounter++}';
				return new ParameterExpression(new SqlSymbol(name), parameters.push(name) - 1);

			case EParent(e):
				return convertExprToExpression(e);

			case EList(arr):
				return new ListExpression([for (e in arr) convertExprToExpression(e)]);

			case EId(id):
				return new ql.sql.grammar.expression.Expression.ColumnName(symbol(id));

			case EField(_, All):
				throw new pm.Error('EField(_, All)');

			case EField(e2, Name(field)):
				switch e2 {
					case EId(table):
						return (new ql.sql.grammar.expression.Expression.ColumnName(symbol(field), symbol(table)));

					default:
						// throw new pm.Error('$e disallowed', 'InvalidArgument');
						return new ql.sql.grammar.expression.Expression.FieldAccess(convertExprToExpression(e2), field);
				}

			case EBinop(op, l, r):
				var oper = switch op {
					// case Eq:  ComparisonOperator.OpEq;
					// case NEq: ComparisonOperator.OpNEq;
					case Eq:  BinaryOperator.OpEq;
					case NEq: BinaryOperator.OpNEq;
					// case In: TsAst.BinaryOperator.OpIn;
					// case Like: TsAst.BinaryOperator.OpLike;
					case Add: BinaryOperator.OpAdd;
					case Sub: BinaryOperator.OpSubt;
					case Div: BinaryOperator.OpDiv;
					case Mult: BinaryOperator.OpMult;
					case Mod: BinaryOperator.OpMod;
					// case Gt:  ComparisonOperator.OpGt;
					// case Gte: ComparisonOperator.OpGte;
					// case Lt:  ComparisonOperator.OpLt;
					// case Lte: ComparisonOperator.OpLte;
					case Gt:  BinaryOperator.OpGt;
					case Gte: BinaryOperator.OpGte;
					case Lt:  BinaryOperator.OpLt;
					case Lte: BinaryOperator.OpLte;
					case LogAnd: BinaryOperator.OpBoolAnd;
					case LogOr: BinaryOperator.OpBoolOr;
					default:
						throw new pm.Error('${this.opStr(op)} operator unhandled');
				}
				var left=null, right=null;// = convertExprToExpression(l);
				// //trace(left);
				// var right = convertExprToExpression(r);
				// //trace(right);
				inline function convertOperands() {
					left = convertExprToExpression(l);
					right = convertExprToExpression(r);
				}

				var isLogical = oper.match(OpBoolAnd | OpBoolOr),
					isRelational = oper.match(OpEq | OpNEq | OpGt | OpGte | OpLt | OpLte);
				
				if (isRelational || isLogical) {
					// var operands = [convertExprToPredicate(l), convertExprToPredicate(r)];
					var op:EnumValue = Type.createEnum((
						if (isRelational) (ComparisonOperator : Enum<Dynamic>)
						else if (isLogical) (ELogicalOperator : Enum<Dynamic>)
						else throw new pm.Error.WTFError()
					), oper.getName());
					//trace('$op', op.typeof()+'');
					return new PredicateExpression(convertExprToPredicate(e));
				}
				else {
					convertOperands();
					return new ArithmeticOperation(Type.createEnum(EMathOperator, oper.getName()), cast left, cast right);
				}

			case EUnop(_, _, _):
				throw new pm.Error('$e');

			case ECall(EId(funcName), args):
				// throw new pm.Error('$e disallowed', 'InvalidArgument');
				var funcArgs = [for (x in args) convertExprToExpression(x)];
				return new ql.sql.grammar.expression.Expression.SimpleFunctionCall(symbol(funcName), funcArgs);

			case ECall(_, _):
				throw new pm.Error('Unhandled $e');

			case EQuery(q):
				throw new pm.Error('$e disallowed', 'InvalidArgument');

			case EArray(arr, index):
				return new ql.sql.grammar.expression.Expression.ArrayAccess(convertExprToExpression(arr), convertExprToExpression(index));

			case EArrayDecl(values):
				return new ListExpression([for (v in values) convertExprToExpression(v)], true);

			case ee=EComprehension(e, what, of, pred):
				throw new pm.Error('$ee');

			case ECase(branches, elseExpr, ex):
				// throw new pm.Error('$e');
				var caseExpr = new CaseExpression();
				caseExpr.branches = [for (branch in branches) new CaseBranchExpression(convertExprToPredicate(branch.e), convertExprToExpression(branch.body))];
				if (elseExpr != null) {
					caseExpr.elseBranch = convertExprToExpression(elseExpr);
				}
				if (ex != null) {
					caseExpr.expression = convertExprToExpression(ex);
				}
				return caseExpr;

			case ETernary(cond, t, f):
				return new TernaryExpression(convertExprToPredicate(cond), convertExprToExpression(t), convertExprToExpression(f));

			case CUnderscore:
				throw new pm.Error('$e');
		}
		
		throw new pm.Error.WTFError();
	}

	function __convertExprToPredicate(e:Expr, ?pos:haxe.PosInfos):Predicate {
		switch e {
			case EParent(e):
				return convertExprToPredicate(e);
			case EBinop(op, l, r):
				var comparisonOperator:Null<ComparisonOperator> = null;
				var logicalOperator:Null<LogicalOperator> = null;
				switch op {
					case Eq: comparisonOperator = ComparisonOperator.OpEq;
					case NEq: comparisonOperator = ComparisonOperator.OpNEq;
					case Gt: comparisonOperator = ComparisonOperator.OpGt;
					case Gte: comparisonOperator = ComparisonOperator.OpGte;
					case Lt: comparisonOperator = ComparisonOperator.OpLt;
					case Lte: comparisonOperator = ComparisonOperator.OpLte;
					case LogAnd: logicalOperator = ELogicalOperator.OpBoolAnd;
					case LogOr: logicalOperator = ELogicalOperator.OpBoolOr;
					default: 
						//throw new pm.Error('Disallowed');
				}
				if (comparisonOperator != null) {
					return new RelationPredicate(comparisonOperator, convertExprToExpression(l), convertExprToExpression(r));
				}
				else if (logicalOperator != null) {
					var left = convertExprToPredicate(l), right = convertExprToPredicate(r);
					switch (logicalOperator : ELogicalOperator) {
						case OpBoolAnd:
							return new AndPredicate(left, right);

						case OpBoolOr:
							return new OrPredicate(left, right);

						case OpBoolXor:
							throw new pm.Error('Unsupported');
					}
				}
				else {
					// throw new pm.Error.WTFError();
					if (op.equals(Binop.In)) {
						var left = convertExprToExpression(l);
						var right:PredicateListExpression = new PredicateListExpression(LExpression(convertExprToExpression(r)));
						return new InPredicate(left, right);
					}
					else if (op.equals(Binop.Like)) {
						var left = convertExprToExpression(l);
						var right = switch r {
							case CString(s): s;
							default:throw new pm.Error('Unexpected $r');
						};
						return new LikePredicate(left, right);
					}
				}

			case EUnop(Not, false, e):
				return new NotPredicate(convertExprToPredicate(e));

			default:
				throw new pm.Error('Unhandled $e', null, pos);
		}

		throw new pm.Error('$e');
	} */

	function makeBinop(op:Binop, l:Expr, r:Expr) {
	    return switch ( r ) {
            case EBinop(op2, l2, r2):
                if (opPriority.get(op) <= opPriority.get(op2))
                    EBinop(op2, makeBinop(op, l, l2), r2);
                else
                    EBinop(op, l, r);

            default:
                EBinop(op, l, r);
	    }
	}

	function parseExprNext(e1: Expr):Expr {
	    var t = token();
	    switch ( t ) {
            /* --EndOfFile-- */
            case _ if (eofStack[eofStack.length-1].equals( t )):
                eofStack.pop();
                return e1;

            case Dot:
				t = token();
				switch t {
					case Star:
						return parseExprNext(EField(e1, ql.sql.ast.Query.FieldAccess.All));

					case Ident(field):
						return parseExprNext(EField(e1, ql.sql.ast.Query.FieldAccess.Name(field)));

					default:
						// unexpected(t);
				}
				push(t);
				push(Dot);
				return e1;
				// return parseExprNext(EField(e1, ident()));
				
			case POpen:
				var args = parseExprList(PClose);
				return parseExprNext(ECall(e1, args));

			case QMark:
				var condition = e1;
				var t = parseExpr();
				req(DoubleDot);
				var f = parseExpr();
				return parseExprNext(ETernary(condition, t, f));

			case Kwd('NOT'):
				t = token();
				switch t {
					case Kwd('LIKE'|'IN'):
						throw new pm.Error('TODO');
					default:
						push(t);
				}
				return makeBinop(Binop.NEq, e1, parseExpr());

            case Op(op):
				return makeBinop(op, e1, parseExpr());
				
			case Kwd("FOR"):
				req(POpen);
				var iterE = parseExpr();
				switch iterE {
					case EBinop(Binop.In, what, of), EParent(EBinop(In, what, of)):
						if (maybe(Kwd('IF'))) {
							throw new pm.Error('TODO');
						}
						return parseExprNext(EComprehension(e1, what, of, null));

					default:
						throw new pm.Error('Unhandled $iterE');
				}

			case Kwd("DIV"):
				push(Op(Binop.Div));
				return parseExprNext(e1);

			case Star:
				push(Op(Binop.Mult));
				return parseExprNext(e1);

			case Kwd("MOD"):
				push(Op(Binop.Mod));
				return parseExprNext(e1);

            case Kwd("AND"):
                push(Op(Binop.LogAnd));
                return parseExprNext(e1);

            case Kwd("OR"):
                push(Op(Binop.LogOr));
                return parseExprNext(e1);

            case Kwd("IN"):
                push(Op(Binop.In));
                return parseExprNext(e1);

            case Kwd("LIKE"):
                push(Op(Binop.Like));
				return parseExprNext(e1);
				
			case BrOpen:
				var idx = parseExpr();
				req(BrClose);
				return parseExprNext(EArray(e1, idx));

			// case QMark:
			// 	var cond = e1;
			// 	var t = parseExpr();
			// 	req(DoubleDot);
			// 	var f = parseExpr();
			// 	return parseExprNext(ETernary(cond, t, f));

            default:
                push( t );
                return e1;
	    }
	}

	function parseExpr():Expr {
		var E = ql.sql.ast.Query.Expr;
		var t = token();
		switch( t ) {
            case Kwd('NULL'), Ident(_.toUpperCase()=>'NULL'): 
                return parseExprNext(CNull);

            case Kwd('TRUE'), Ident(_.toUpperCase()=>'TRUE'): 
                return parseExprNext(CTrue);

            case Kwd('FALSE'), Ident(_.toUpperCase()=>'FALSE'):
				return parseExprNext(CFalse);
				
			case Kwd('NOT'):
				var e = parseExpr();
				return parseExprNext(E.EUnop(Unop.Not, false, e));

            case Ident(id): 
				return parseExprNext(E.EId(id));
				
            case CInt(i): 
                return parseExprNext(E.CInt( i ));

            case CFloat(n): 
                return parseExprNext(E.CFloat( n ));

            case CString(s):
				return parseExprNext(E.CString(s));
				
			// case QMark:
			// 	return parseExprNext(Expr.CParam(null));

			case DoubleDot:
				var name = ident();
				return parseExprNext(ql.sql.ast.Query.Expr.CParam(name));

            case POpen:
                var el = parseExprList(PClose);
                switch (el) {
                    case []: unexpected(PClose);
                    case [e]:
                        return parseExprNext(E.EParent(e));
                    case _:
                        return parseExprNext(E.EList(el));
				}
				
			case BrOpen:
				var el = parseExprList(BrClose);
				switch el {
					case []: unexpected(BrClose);
					case _:
						return parseExprNext(E.EArrayDecl(el));
				}

			case Kwd('CASE'):
				// var subject = parseExpr();
				var branches = [];
				var defaultBranch = null;
				var expr = null;

				var t = token();
				switch t {
					case Kwd('WHEN'|'ELSE'|'END'):
						push(t);

					default:
						push(t);
						expr = parseExpr();
				}

				while (true) {
					var t = token();
					switch t {
						case Kwd('WHEN'):
							var cond = parseExpr();
							req(Token.Kwd('THEN'));
							var valu = parseExpr();
							branches.push({e:cond, body:valu});

						case Kwd('ELSE'):
							defaultBranch = parseExpr();
							req(Kwd('END'));
							break;

						default:
							unexpected(t, [Kwd('WHEN'), Kwd('ELSE'), Kwd('END')]);
							throw 0;
					}
				}

				var r:Expr = E.ECase(branches, defaultBranch, expr);
				t = token();
				//Console.debug(t);
				push(t);
				//Console.debug(r);
				return r;

            default:
                unexpected(t);
		}
		
		return null;
	}

	function parseIdentList(sep:Token, ?end:Token):Array<String> {
		var res:Array<String> = [];
		var t = null;
		while (true) {
			t = token();
			switch t {
				case Ident(n):
					res.push(n);
					if (maybe(sep)) {
						continue;
					}
					else if (end != null && maybe(end)) {
						break;
					}
					else {
						unexpected(token(), sep);
					}

				// case _ if (end != null && t.equals(end)):
				// 	push(t);
				// 	break;

				default:
					if (end == null) {
						push(t);
						break;
					}
					else {
						unexpected(t, end);
					}
			}
		}
		//Console.examine(t);
		return res;
	}

	function parseExprList(end: Token):Array<Expr> {
	    var res = [];
	    var t = token();
	    if (t.equals( end ))
	        return res;
	    push( t );
	    while (true) {
	        res.push(parseExpr());
			t = token();
			//Console.examine(t);
	        switch t {
                case Comma:
                    //

                default:
                    if (t.equals(end)) {
						break;
					}
                    unexpected( t );
	        }
	    }
	    return res;
	}

	private static var KWDS = [
		"ALTER", "SELECT", "UPDATE", "INSERT", "WHERE", "CREATE", "FROM", "TABLE", "NOT", "NULL", "PRIMARY", "KEY", "ENGINE", "AUTO_INCREMENT", "UNIQUE",
		"ADD", "CONSTRAINT", "FOREIGN", "REFERENCES", "ON", "DELETE", "SET", "NULL", "CASCADE", "ASC", "DESC", "ORDER", "BY", "AS", "LEFT", "RIGHT", "INNER",
		"OUTER", "CROSS", "JOIN", "IN", "LIKE", "MOD", "DIV", "LIMIT", "OFFSET", "GROUP", "INTO", "VALUES", "IF", "ELSE", "CASE", "WHEN", "MATCH", "THEN",
		"FOR", "END"
	];
	private static var OPKWDS = ["IN", "LIKE", "MOD", "DIV"];
	private static var STDOPCHARS:String = "+/-=!><&|^%~";

	private static var global = new VMParser();
	private static inline function p<T>(sql:String, f:VMParser->T):T {
		var parser = new VMParser();
		parser._setInput(sql);
		return f(parser);
	}

	public static inline function readExpression(sql:String):Expr return p(sql, r -> r.parseExpr());
}

private enum Token {
	Eof;
	CInt(v : Int);
	CFloat(v : Float);
	CString(v: String);
	Kwd(s : String);
	Ident(s : String);
	Op(op : Binop);

	Star;
	POpen;
	PClose;
	BrOpen;//[
	BrClose;//]
	CBrOpen;//{
	CBrClose;//}
	QMark;// ?
	Comma;
	Dot;
	DoubleDot;// :
	Not;
	Semicolon;// ;
}

private enum SqlError {
    UnexpectedChar(c: String);
    UnexpectedToken(t:Token, ?expected:String, pos:haxe.PosInfos);
	Unclosed(c: String);
	NotYetImplemented(?desc:String, pos:haxe.PosInfos);
}

private class ParserState {
	public var query:String;
	public var pos:Int;
	public var cache: Array<Token>;
	public var eofStack:Array<Token>;
	// public var symbolTable:SymbolTable<SqlSymbolType, SqlSymbol>;

	public function new(query, pos, cache, eofStack) {
		this.query = query;
		this.pos = pos;
		this.cache = cache;
		this.eofStack = eofStack;
	}

	public static inline function fromParser(p: VMParser) {
		return @:privateAccess new ParserState(p.query, p.pos, p.cache.copy(), p.eofStack.copy());
	}
}

@:access(haxe.io)
@:access(ql.sql)
private class SlaveInput extends Input {
	var parser: VMParser;
	var _closed: Bool = false;
	var _startPos: Int;

	public function new(owner) {
		// super()
		this.parser = owner;
		_startPos = parser.pos;
		// this.
	}

	override function readByte():Int {
		if (_closed)
			throw haxe.io.Error.Custom(new pm.Error('Attempted to read after closing'));
		return parser.nextChar();
	}

	override function close() {
		super.close();
		_closed = true;
	}

	public inline function rollback() {
		this.parser.pos = _startPos;
	}

	public inline function slice():String {
		return parser.query.substring(_startPos, parser.pos);
	}
}