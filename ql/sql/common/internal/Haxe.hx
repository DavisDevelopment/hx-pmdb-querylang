package ql.sql.common.internal;

import haxe.macro.*;
import haxe.macro.Expr;
import haxe.macro.Type;
// import haxe.macro.Complex
import haxe.macro.Printer;

import hscript.Macro as HScript;
// import haxeparser.HaxeParser;

using haxe.macro.ExprTools;
using haxe.macro.ComplexTypeTools;

import ql.sql.common.SqlSchema;
import ql.sql.runtime.SType;

import pm.Pair;

#if !false
class Haxe {}
#else

class Haxe {
    public static function parseExprBlockAsSchema(expr: Expr) {
        var topLvl:Array<Expr> = switch expr.expr {
            case EBlock(a): a;
            default: [expr];
        };

        var fields:Array<SchemaFieldInit> = new Array();
        var indexes:Array<SchemaIndexInit> = new Array();
        var options:SchemaInit = {
            fields: fields,
            indexes: indexes
        };

        for (expr in topLvl) {
            
        }
    }
	function parseExprFromBlock(e:Expr, idx:Int, block:Array<Expr>, schema:{fields:Array<SchemaFieldInit>, indexes:Array<SchemaIndexInit>, _:SchemaInit}) {
        var tmp = metadata(e);
        e = tmp.e;
        var meta = tmp.meta;

        switch e {
            case {expr:EVars([{name: name, type: type, expr: valueInitializerExpr, isFinal: isFinal}])}:
                if (isFinal) throw new pm.Error('final fields not allowed');
                var f:SchemaFieldInit = {
                    name: name,
                    type: STypes.ofComplexType(type),
                    expr: null
                };
        }
    }

    static function metadata(e:Expr, ?res:Array<MetadataEntry>):{meta:Array<MetadataEntry>, e:Expr} {
        var meta:Array<MetadataEntry> = res == null ? [] : res;
        return switch e {
			case {expr: EMeta(kwd, params, next)}:
                meta.push({name:kwd, params:params, pos:e.pos});
                metadata(next, meta);

            case tail: {meta:meta, e:tail};
        }
    }
}
#end