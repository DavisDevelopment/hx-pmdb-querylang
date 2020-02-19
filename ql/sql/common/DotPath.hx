package ql.sql.common;

import pm.ImmutableList;
import pm.ImmutableList.ListRepr;
import pm.ImmutableList.Il;

import pmdb.core.Object;

enum Node {
    Access(field: Symbol);
}
class Symbol {
    public final label: String;
    public inline function new(s: String) {
        this.label = s;
    }
}
private typedef NodeList = ListRepr<Node>;

abstract DotPath (ImmutableList<Node>) from NodeList {
    @:from
    public static function parse(s: String):DotPath {
        var chunks:Array<String> = s.split('.');
        var tokens:Array<Node> = chunks.map(function(tok: String):Node {
            return Node.Access(new Symbol(tok));
        });
        var list:NodeList = ImmutableList.fromArray(tokens);
        return list;
    }

    public inline function get(o: Doc):Dynamic {
        return _get_(this, o);
    }

    static function _get_(node:ListRepr<Node>, value:Object<Dynamic>, ?_default:Dynamic):Dynamic {
        return switch node {
            case Tl:
                throw E.Unreachable;
            case Hd(n, Tl): _get_access_(n, value);
            case Hd(n, next):
                value = _get_access_(n, value);
                _get_(next, value, _default);
        }
    }

    static inline function _get_access_(node:Node, value:Object<Dynamic>):Dynamic {
        switch node {
            case Access(field):
                return value[field.label];
        }
    }
}

enum E {
    Unreachable;
}