package ql.sql.common.internal;

import pmdb.core.Object;

@:forward
abstract ObjectPure<T>(Null<Node<T>>) from Null<Node<T>> to Null<Node<T>> {
	public inline function new() {
		this = null;
	}

	public inline function append(k:String, v:T):ObjectPure<T> {
		return this.append(k, v);
	}

	public inline function compact():ObjectPure<T> {
		return this = this.compact();
	}

	@:arrayAccess
	@:op(a.b)
	public function get(key: String):T {
		return switch this.lookup(key) {
			case null: null;
			case node: node.value;
		}
	}

	@:arrayAccess
	@:op(a.b)
	public inline function set(key:String, value:T):T {
		this = this.append(key, value);
		return value;
	}

	public inline function map<O>(f: (key:String, value:T)->{key:String, value:O}):ObjectPure<O> {
		var res:ObjectPure<O> = new ObjectPure();
		for (key=>value in this) {
			var pair = f(key, value);
			res[pair.key] = pair.value;
		}
		return res;
	}

	@:to
	public inline function toMutable():Object<T> {
		var out:Object<T> = new Object();
		for (key=>value in this) {
			out[key] = value;
		}
		return out;
	}
}

@:generic
class Node<T> {

	public var hashKey:Int;
	public var key:String;
	public var value:T;

    public var head:Null<Node<T>> = null;
    // public var prev:Null<Node<T>> = null;
    
    public var delegate:Null<Node<T>> = null;

	public function new(?prev:Node<T>, key:String, value:T) {
		this.head = prev;
		this.hashKey = pm.HashKey.next();
		this.key = key;
		this.value = value;
	}

	extern inline public function append(k:String, v:T):Node<T> {
		return Nodes.append(this, k, v);
	}

	extern inline public function compact():Node<T> {
		return Nodes.compact(this);
	}

	public inline function lookup(key: String):Node<T> {
		return Nodes.look_up(this, key);
	}

	public function nodes() {
		return Nodes.createNodeIterator(this);
	}
	public function keyValueIterator() {
		return nodes();
	}
}

class Nodes {
	public static function look_up<T>(node:Node<T>, k:String):Node<T> {
        while (node != null && node.head != null) {
            if (node.key == k)
                return node;
            node = node.head;
        }
        throw NotFound();
    }
    
    public static function append<T>(node:Node<T>, key:String, value:T):Node<T> {
        return new Node(node, key, value);
	}
	
	public static function compact<T>(node: Node<T>):Node<T> {
		var pairs = new Array();
		var visited = new Map();
		var n = node;
		while (n != null) {
			if (!visited.exists(n.key)) {
				pairs.push({
					key: n.key, 
					value: n.value
				});
				visited[n.key] = true;
			}
			n = node.head;
		}
		pairs.reverse();
		n = null;
		for (k=>v in pairs.iterator()) {
			n = new Node(n, k, v);
		}
		return n;
	}

	public static function createNodeIterator<T>(node: Node<T>):Iterator<Node<T>> {
		return {
			hasNext: () -> node != null,
			next: () -> {
				var ret = node;
				node = node.head;
				ret;
			}
		};
	}
}

enum E {
	NotFound(?p:haxe.PosInfos);
	UStupid;
}