package ql.sql;

using pm.core.BaseTools;
import pm.strings.HashCode.*;
using pm.strings.HashCodeTools;

import Reflect.hasField;
import Reflect.field as getField;
import Reflect.getProperty;

import haxe.extern.EitherType as Or;
import haxe.ds.Option;
using pm.Options;
import pm.Helpers.nn;
import pm.Helpers.same;

typedef AstSymbol<TType> = {
    var identifier: String;
    // var hashKey: Int;

    var type: TType;
    var parent: AstSymbol<TType>;
};

#if pewp
@:tink interface IAstSymbol<TType> {
    var identifier(default, set): String;
    private function set_identifier(v:String):String {
        if (identifier == null && hashKey == null) {
            this.identifier = v;
            this.hashKey = v.hash(java);
        }
        else {
            #if debug throw new pm.Error('Cannot reassign identifier or hashKey field values'); #end
        }
        return identifier;
    }

    // @:computed(identifier.hash(java))
    var hashKey: Null<Int>;// = identifier.hash(java);
    var type:Null<TType>;
    var parent:Null<IAstSymbol<TType>>;
}
#end

/**
  [TODO] further Haxe-ify this module
 **/
class SymbolTable<TType, TSymbol:AstSymbol<TType>> {
    public var keyFunc:(s: TSymbol)->String;
    public var symbols: Map<String, Array<TSymbol>>;
    public var parent:Null<SymbolTable<TType, TSymbol>> = null;
    public var allowDuplicates:Bool = true;

    private var _globalSymbols: Map<String, Array<TSymbol>>;
    private var scope: SymbolTableScope<TType, TSymbol>;

    public function new(?symbolKeyProvider) {
        this.symbols = new Map();
        this.keyFunc = (symbolKeyProvider != null) ? symbolKeyProvider : cast _defaultSymbolKeyProvider;
        this._globalSymbols = this.symbols;
        this.scope = new SymbolTableScope(this, this.symbols, null);
    }

/* === [API Methods] === */

    public function localLookup(key:Key<TSymbol>, ?type:TType, ?parent:TSymbol):Null<Array<TSymbol>> {
        return lookupInternal(symbols, cast key, type.opt(), parent.opt()).getValue();
    }
    
    public function lookup(key:Key<TSymbol>, ?type:TType, ?parent:TSymbol):Null<Array<TSymbol>> {
        var keyToLookup = getKey(key);
        var result = localLookup(keyToLookup, type, parent);
        return !result.empty() ? result : (nn(parent) ? this.parent.lookup(keyToLookup, type, parent) : null);
    }

    public function add(key:Key<TSymbol>, ?value:TSymbol):Void {
        addSymbol(key, value, symbols);
    }
    
    public function addToGlobalScope(key:Key<TSymbol>, ?value:TSymbol):Void {
        addSymbol(key, value, _globalSymbols);
    }

    public function iter(stepper:(sym:TSymbol)->Bool) {
        for (entry in symbols.iterator()) {
            for (sym in entry) if (!stepper(sym)) return ;
        }
        
        if (nn(parent)) 
            parent.iter(stepper);
    }

    public function iterator():Iterator<TSymbol> {
        throw new pm.Error('TODO: implement SymbolTable.iterator');
    }

    public function enterScope() {
        var newParent = new SymbolTable<TType, TSymbol>(this.keyFunc);
        newParent.symbols = this.symbols;
        newParent.parent = this.parent;
        newParent._globalSymbols = this._globalSymbols;

        this.parent = newParent;
        this.symbols = new Map();
    }

    public function exitScope() {
        if (parent == null) {
            throw new pm.Error('Already at the root scope');
        }

        this.symbols = parent.symbols;
        this.parent = parent.parent;
    }

/* === [Internal Methods] === */

    private function lookupInternal(map:Map<String,Array<TSymbol>>, key:Or<TSymbol, String>, ?type:Option<Dynamic>, ?parent:Option<TSymbol>):Option<Array<TSymbol>> {
        var matchedSymbols = map.get(getKey(key));
        var result = [];
        var notype = type == null || type.isNone();
        var noparent = parent == null || parent.isNone();

        if (nn(matchedSymbols) && matchedSymbols.length != 0) {
            if (false && notype && noparent) // prefixed with `false` condition to "kill" statement
                return Some(matchedSymbols.copy()); // short circuit for filter-less lookups?

            // traverse
            var i = 0;
            while (i < matchedSymbols.length) {
                var s = matchedSymbols[i];
                var s_parent:TSymbol = cast s.parent;
                if ((notype || same(type.getValue(), s.type) && (noparent || same(parent.getValue(), s_parent)))) {
                    result.push(s);
                }
                i++;
            }
        }

        return result.length!=0?Some(result):None;
    }

    private function addSymbol(key:Or<TSymbol, String>, value:Null<TSymbol>, map:Map<String, Array<TSymbol>>) {
        value = nn(value) ? value : (key : TSymbol);
        var key:String = getKey(key);

        if (!map.exists(key)) {
            return map.set(key, [value]);
        }

        var matchedSymbols = map.get(key);
        if (!allowDuplicates) {
            throw new pm.Error('Symbol $key already found in desired scope');
        }
        for (s in matchedSymbols) {
            if (same(s.type, value.type) && same(s.parent, value.parent)) {
				throw new pm.Error('Symbol $key already found in desired scope');
            }
        }
        matchedSymbols.push(value);
    }

    private function getKey(key: Or<TSymbol, String>):String {
        return (
            (key is String)
            ? cast(key, String)
            : keyFunc((key : TSymbol))
        );
    }

    static function _defaultSymbolKeyProvider<O>(sym: O):String {
        // return (Math.floor(Math.random() * 256)).toBase(16, 'FEDCBA987654321');
        if (try hasField(sym, 'identifier') catch (e: Dynamic) false) {
            var id = getProperty(sym, 'identifier');
            if (id == null) return Std.string(sym);
            // if ((id is String)) return cast(id, String);
            return try Std.string(id) catch (e: Dynamic) throw pm.Error.withData(sym, 'identifier field found on object, but value is unusable\n');
        }

        #if (java||python)
        try {
            var id:Int = cast((untyped sym : Dynamic).hashCode(), Int);
            return id.toBase(16);
        }
        catch(e: Dynamic) {
            return 'poop';
        }
        #end

        return Std.string(sym);
    }
}

private typedef Key<T> = String;

class SymbolTableScope<TType, TSymbol:AstSymbol<TType>> {
	public var table:SymbolTable<TType, TSymbol>;
	public var symbols: Map<String, Array<TSymbol>>;
	public var parent: Null<SymbolTableScope<TType, TSymbol>> = null;

    public function new(table, symbols, ?parent) {
        this.table = table;
        this.symbols = symbols;
        this.parent = parent;
    }

    public function child(?symbols: Map<String, Array<TSymbol>>) {
        if (symbols == null) symbols = new Map();
        return new SymbolTableScope(table, symbols, this);
    }

    public inline function getRoot():SymbolTableScope<TType, TSymbol> {
        var scope = this;
        while (scope.parent != null) {
            scope = scope.parent;
        }
        return scope;
    }
}