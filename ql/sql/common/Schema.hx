package ql.sql.common;

import ql.sql.runtime.SType;
import ql.sql.runtime.DType;

import pmdb.core.ds.Incrementer;
import pmdb.core.Arch;
import pmdb.core.Object;
import pmdb.core.Object.Doc;

import pm.Error;
import pm.HashKey;
import pm.ImmutableList;
import pm.ImmutableList.ListRepr as Cell;
import pm.map.Dictionary;

import haxe.extern.EitherType as Or;
import haxe.ds.ReadOnlyArray;
import haxe.ds.Option;
import haxe.ds.Either;

import ql.sql.runtime.SType;
import ql.sql.common.TypedValue;

using Lambda;
using pm.Arrays;
using pm.Iterators;
using pm.Options;
using pm.Outcome;
// using pm.Ei

typedef Schema = {
    /**
     * validates that the given value matches [this] schema
     * @throws `pm.Error` if failed
     * @param x 
     * @return Bool
     */
    function validateValue(x: Dynamic):Bool;

    function testValue(value: Dynamic):pm.Outcome<Bool, pm.ImmutableList<pm.Error>>;

    /**
     * checks that the given TypedValue is described by this Schema
     * @param value 
     * @return Bool
     */
    function validateTypedValue(value: TypedValue):Bool;

    function getFieldReference(name: String):Option<Field>;
};

typedef Field = {
    var name: String;
    var type: SType;
    var notNull: Bool;
};

typedef TOutcome<TRes, TErr> = {
    var result: Null<TRes>;
    var error: Null<TErr>;
    var status: Bool;
};
abstract Outcome<Res, Err> (TOutcome<Res, Err>) from TOutcome<Res, Err> {
    public static inline function make<R, E>(result:Null<R>, error:Null<E>, ?status:Bool):Outcome<R, E> {
        return {
            result: result,
            error: error,
            status: if (error != null) false else true
        };
    }

    @:from public static function success<R, E>(result: R) return make(result, null, true);
    @:from public static function failure<R, E:pm.Error>(error: E) return make(null, error, false);
}

/**
 * `CType` - Object-Oriented representation of supported data type, and supported operators, casts, conversions, etc. thereof
 */
class CType {
    /**
     * [TODO!!]
     */
    public function new() {
        //
    }
}