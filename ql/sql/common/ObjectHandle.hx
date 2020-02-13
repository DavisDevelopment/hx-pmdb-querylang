package ql.sql.common;

import haxe.ds.Option;
using pm.Options;
import pmdb.core.Object;

import haxe.rtti.CType;
import haxe.rtti.Meta;

import ql.sql.common.TypedValue;
import ql.sql.runtime.SType;

import pm.map.Dictionary;

using pm.Arrays;
using pm.Iterators;
using Type;
using ql.sql.runtime.SType;

/**
 * [TODO!] finish coherent alpha-phase data model
 */
class ObjectHandle {
    private var _data: Object<Dynamic>;
    private var _properties: Dictionary<Property>;
    public var allowImplicitPropertyDefinition:Bool = true;

    public function new(?c: Class<Dynamic>) {
        if (c != null) {
            _data = Object.unsafe(c.createEmptyInstance());
        }
        else {
            _data = new Object();
        }
        _properties = new Dictionary();
    }

    public function defineProperty(o: PropertyDescriptor) {
        if (!o.name.empty() && !_properties.exists(o.name)) {
            _properties.set(o.name, Property.make(this, o));
        }
        return this;
    }

    public function set(f:String, v:TypedValue):TypedValue {
        switch _properties[f] {
            case null if (allowImplicitPropertyDefinition):
                defineProperty({
                    name: f,
                    value: v,
                    type: null,//TODO: make implicit properties typed by default
                    get: null,
                    set: null
                });
                return v;

            case null:
                throw new pm.Error('Attribute `$f` not found');

            case property:
                return property.set(v);
        }
    }

    public function get(f: String):TypedValue {
        switch _properties[f] {
            case null:
                switch _resolve_(f) {
                    case None:
                        throw new pm.Error('Attribute `$f` not found', 'NameError');
                        
                    case Some(v):
                        return v;
                }

            case property:
                return property.get();
        }

        throw new pm.Error.WTFError();
    }

    /**
     * checks if `this` has an attribute `f`
     * @param f 
     * @return Bool
     */
    public inline function has(f: String):Bool {
        return _properties.exists(f);
    }

    public function fields():Array<String> {
        return _properties.keyArray();
    }

    public function keyValueIterator() {
        return _properties.iterator().map(item -> {
            key: item.name,
            value: item.get()
        });
    }

    public function export():Doc {
        var out = new Doc();
        for (k=>v in this) {
            out[k] = v.export();
        }
        return out;
    }

    function _resolve_(f: String):Option<TypedValue> {
        return None;
    }
}

@:allow(ql.sql.common.ObjectHandle)
class Property {
    private final owner: ObjectHandle;
    public final name: String;
    public var value(default, set): TypedValue;
    public var type:Null<SType> = null;
    public var getter:Null<Void->TypedValue> = null;
    public var setter:Null<TypedValue->TypedValue> = null;

    public var enumerable: Bool = true;
    public var writable: Bool = true;
    public var configurable: Bool = true;

    function new(owner, name:String, config:PropertyDescriptor) {
        this.owner = owner;
        this.name = name;

        _init_(this, config);
    }

    private inline function set_value(v: TypedValue):TypedValue {
        if (type != null)
            assert(v.isOfType(type) || type.validateValue(v.value));
        return this.value = v;
    }

    public function get():TypedValue {
        if (getter == null)
            return this.value;
        else
            return this.getter();
    }
    public function set(v: TypedValue):TypedValue {
        if (setter == null)
            return this.value = v;
        else
            return this.setter(v);
    }

    static function _init_(p:Property, o:PropertyDescriptor) {
        p.enumerable = o.enumerable = o.enumerable.nor(true);
        p.writable = o.writable = o.writable.nor(true);
        p.configurable = o.configurable = o.configurable.nor(true);
        
        switch o {
            case {value:v, get:null, set:null}:
                p.value = v;

            case {value:null, get:get, set:set}:
                p.getter = get;
                if (p.writable)
                    p.setter = set;

            default:
        }
    }

    public static function make(o:ObjectHandle, cfg:PropertyDescriptor) {
        var name:String = cfg.name;
        if (name.empty()) throw new pm.Error('name must be provided');
        cfg.name = null;
        return new Property(o, name, cfg);
    }
}

typedef PropertyDescriptor = {
    ?name: String,
    ?value: TypedValue,
    ?type: SType,
    ?get: Void->TypedValue,
    ?set: (v:TypedValue)->TypedValue,
    ?enumerable: Bool,
    ?writable: Bool,
    ?configurable: Bool
};

@:forward
abstract O (ObjectHandle) from ObjectHandle to ObjectHandle {
    public function new() {
        this = new ObjectHandle();
    }

    @:arrayAccess
    public inline function get(f: String) return this.get(f);
    @:arrayAccess
    public inline function set(f:String, v:TypedValue) return this.set(f, v);

    @:op(a.b)
    public function fieldGet(f: String):TypedValue {
        return this.get(f);
    }

    @:op(a.b)
    public function fieldSet(f:String, v:TypedValue):TypedValue {
        return this.set(f, v);
    }

    public var _(get, never):Uo;
    private inline function get__():Uo return ((this : O) : Uo);
}

abstract Uo (O) from O {
    @:op(a.b)
    public inline function fieldGet(f: String):Dynamic {
        return inline this[f].export();
    }

    @:op(a.b)
    public inline function fieldSet(f:String, v:Dynamic):Dynamic {
        return inline this.fieldSet(f, v).export();
    }
}