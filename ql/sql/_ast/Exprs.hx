package ql.sql.ast;

import haxe.macro.Expr.Binop;
import haxe.macro.Context;
import pm.datetime.DateTime;
import haxe.io.Bytes;
import ql.sql.ast.Expr;

using pm.Strings;
using pm.Arrays;
using pm.Numbers;

import pmdb.core.Arch;
using pmdb.ql.ts.TypeChecks;

import haxe.macro.Expr as HxExpr;
import haxe.macro.Expr.ExprOf as HxExprOf;
using haxe.macro.ExprTools;

class Exprs {}

class ExprEnums {
    public static function print(e: ExprData):String {
        return switch e {
            case EUnOp(op, a, postfix):
                var sop = ExprUnops.print(op);
                var space = (if (!postfix) ~/$[A-Za-z_]/gi else ~/[\w\d_]^/gi).match(sop);//, trailingSpace=.match(sop);
                sop = if (postfix) (if (space) ' $sop' else sop) else (if (space) '$sop ' else sop);
                return postfix ? print(a) + sop : sop + print(a);
            
            case EBinOp(op, a, b):
                var sop = ExprBinops.print(op);
                var space = true;//~/[\w]/gi.match(sop);
                if (space) sop = ' $sop ';
                var res = print(a)+sop+print(b);
                if (space) res = '($res)';
                res;
            
            case EId(id): id;
            case EField(o, f): print(o)+'.$f';
            case ECall(f, args): print(f)+'('+(args.map(e -> print(e.data)).join(','))+')';
            case EValue(value, type): ExprValues.print(value, type);
            
            case other:
                'TODO: '+other.getName();

            // default: 
                // throw new pm.Error('Unhandled $e');
        };
    }

    public static function getValue(e: ExprData):Dynamic {
        return switch e {
            case EValue(value, type): ExprValues.toValue(value, type);
            case EParent(e): getValue(e);
            
            default: throw new pm.Error('Invalid call');
        }
    }
}

class ExprValues {
    @:noUsing
    public static function print(value:Dynamic, type:ValueType<Dynamic>):String {
        return try haxe.Json.stringify(value) catch (e: Dynamic) Std.string(value);
    }

    @:noUsing
    public static function toValue(v:Dynamic, type:ValueType<Dynamic>):Dynamic {
        if (pm.Helpers.same(ValueType.VAny, cast type)) return v;
        switch type {
            case VString:
                return '$v';
            
            case VBool:
                if ((v is Bool)) return cast(v, Bool);
                return pm.Arch.isTruthy(v);
            
            case VFloat:
                if ((v is Float)) return cast(v, Float);
                if ((v is String)) return Std.parseFloat(cast(v, String));

            case VInt:
                if (v.is_integer()) return cast(v, Int);
                if (v.is_string()) return Std.parseInt(cast(v, String));
                if (v.is_boolean()) return switch cast(v, Bool) {
                    case false: 0;
                    case true: 1;
                }

            case VArray(type):
                //TODO, check item types
                if (v.is_uarray()) return v;

            case VBytes:
                if (inline v.is_direct_instance(haxe.io.Bytes)) return cast(v, Bytes);

            case VDate:
                if (DateTime.is(v)) return (cast v : DateTime);
                if (v.is_date()) return cast(v, Date);

            case t:
                throw new pm.Error('Unhandled $type');
        }

        return new pm.Error('Invalid type: $v should be $type');
    }

    @:noUsing
    public static function toEValue(v: Dynamic):ExprData {
        if (v == null) return null;
        return switch Type.typeof(v) {
            case TNull: throw new pm.Error.WTFError();
            case TInt: ExprData.EValue(cast(v, Int), ValueType.VInt);
			case TFloat: ExprData.EValue(cast(v, Float), ValueType.VFloat);
			case TBool: ExprData.EValue(cast(v, Bool), ValueType.VBool);
			case TClass(String): ExprData.EValue(cast(v, String), ValueType.VString);
			case TClass(Date): ExprData.EValue(cast(v, Date), ValueType.VDate);
			case TClass(Bytes): ExprData.EValue(cast(v, Bytes), ValueType.VBytes);
			case TClass(Array): ExprData.EValue(cast(v, Array<Dynamic>), ValueType.VArray(VAny));
            case t: throw new pm.Error('Invalid type: $t');
        }
    }
}

class ExprBinops {
    public static function print(op:BinOp):String {
        return switch op {
            case Add: '+';
            case Subt: '-';
            case Mult: '*';
            case Mod: '%';
            case Div: '/';
            case Greater: '>';
            case Equals: '==';
            case And: '&&';
            case Or: '||';
            case Like: 'like';
            case In: 'in';
        };
    }
}

class ExprUnops {
    public static function print(op:UnOp):String {
        return switch op {
            case Not: '!';
            case IsNull: '?';
            case Neg: '-';
        }
    }
}

class ExprMacros {
    public static function convert(expr: HxExpr):Expr {
        switch expr {
            case (macro @search ${src}=>${item}):
                return EBinOp(BinOp.In, convert(item), convert(src));
            default:
        }
        switch expr.expr {
            case EConst(c): return switch c {
                case CIdent(id): EId(id);
                case CInt(Std.parseInt(_)=>i): EValue(i, ValueType.VInt);
                case CFloat(Std.parseFloat(_)=>n): EValue(n, ValueType.VFloat);
                case CString(s, _): EValue(s, VString);
                case CRegexp(r, opt): throw 'Unsupported';
            };
            case EBinop(op, e1, e2):
                var myBinop = switch op {
                    case OpAdd: BinOp.Add;
                    case OpMult: BinOp.Mult;
                    case OpDiv: BinOp.Div;
                    case OpSub: BinOp.Subt;
                    case OpAssign: throw 'TODO: BinOp.Assign';
                    case OpEq: BinOp.Equals;
                    // case OpNotEq: BinOp
                    case OpGt: BinOp.Greater;
                    // case OpGte: 
                    // case OpLt:
                    // case OpLte:
                    // case OpAnd: BinOp.And;
                    // case OpOr: BinOp.Or;
                    // case OpXor:
                    case OpBoolAnd: BinOp.And;
                    case OpBoolOr: BinOp.Or;
                    // case OpShl:
                    // case OpShr:
                    // case OpUShr:
                    // case OpMod:
                    // case OpAssignOp(op):
                    // case OpInterval:
                    // case OpArrow: 
                    case OpIn: BinOp.In;
                    default: throw 'TODO: $op';
                };
                return EBinOp(myBinop, convert(e1), convert(e2));
            
            case EUnop(op, postfix, e):
                var myOp = switch op {
                    // case OpIncrement:
                    // case OpDecrement:
                    case OpNot: UnOp.Not;
                    case OpNeg: UnOp.Neg;
                    // case OpNegBits:
                    default: throw 'TODO: $op';
                }
                return EUnOp(myOp, convert(e), postfix);
            
            case ECall(e, params):
                return ECall(convert(e), params.map(convert));
            
            case EVars(vars):
                return EVars([for (v in vars) {
                    name: v.name,
                    isFinal: v.isFinal,
                    expr: if (v.expr != null) convert(v.expr) else null,
                    type: null
                }]);
            
            case EField(e, field):
                return EField(convert(e), field);
            
            case EParenthesis(e):
                return EParent(convert(e));
            
            case EBlock(exprs): 
                return EBlock(exprs.map(convert));

            case EIf(econd, eif, eelse):
                return EIf(convert(econd), convert(eif), convert(eelse));

            case EFunction(kind, f):
                return EFunction(
                    null,
                    {
                        args: [for (arg in f.args) {name:arg.name,type:null,opt:arg.opt,value:if (arg.value!=null)convert(arg.value)else null}],
                        expr: if (f.expr != null) convert(f.expr) else null,
                        ret: null
                    }
                );
            
            default:
                throw new pm.Error('Unsupported expression ${expr.toString()}');
        }
    }
#if macro
    public static function getHaxeExprFor(e: ExprData):HxExpr {
        var r:HxExpr = macro null;
        var cls:HxExpr = macro ql.sql.ast.Expr;
        var en:HxExpr = macro ql.sql.ast.Expr.ExprData;

        switch e {
            case EId(id): 
                r = macro $cls.g.$id;
            
            case EValue(value, type):
                en = macro $en.EValue;
                var ve = macro ERR;
                if (value != null) {
                    if ((value is Bool)) ve = (cast(value, Bool)?macro true:macro false);
                    if ((value is String)) ve = {expr:EConst(CString(cast(value, String))), pos:ve.pos};
                    if ((value is Float)) ve = {expr:EConst(CFloat(''+cast(value, Float))), pos:ve.pos};
                    if ((value is Int)) ve = {expr:EConst(CInt(''+cast(value, Int))), pos:ve.pos};
                    //TODO..
                    switch ve {
                        case macro ERR:
                            throw new pm.Error('$value cannot be macroized');
                        default:
                    }
                }
                else ve = macro null;
                // Context.makeExpr(value, Context.currentPos());
                r = macro ($ve : ql.sql.ast.Expr);
            case EParent(e):
                r = macro (${getHaxeExprFor(e.expr)});
            case ECast(e, type, safe): 
                r = getHaxeExprFor(e.data);
            case EField(o, f): 
                // macro ${getHaxeExprFor(o.expr)}.$f;
                throw 'TODO';
            case ECall(f, args): 
                // macro ${getHaxeExprFor(f.expr)}($a{[for (a in args) getHaxeExprFor(a)]});
                r = getHaxeExprFor(f);
                var rargs = [for (a in args) getHaxeExprFor(a)].map(e -> macro ($e : ql.sql.ast.Expr));
                var eargs = macro $a{rargs};
                r = macro $r.call($eargs);
            case EUnOp(op, a, postfix): 
                r = {
                    expr: EUnop(switch op {
                        case Not: OpNot;
                        case Neg: OpNeg;
                        case IsNull: throw new pm.Error.WTFError();
                    }, postfix, getHaxeExprFor(a)),
                    pos: Context.currentPos()
                };

            case EBinOp(In, a, b):
                var src = getHaxeExprFor(b), item = getHaxeExprFor(a);
                // r = macro @search $src => $item;
                r = macro ${item}.inArray(${src});
            
            case EBinOp(op, a, b):
                var macOp = switch op {
                    case Add: OpAdd;
                    case Subt: OpSub;
                    case Mult: OpMult;
                    case Mod: OpMod;
                    case Div: OpDiv;
                    case Greater: OpGt;
                    case Equals: OpEq;
                    case And: OpBoolAnd;
                    case Or: OpBoolOr;
                    case Like: throw 'Unsupported Like';
                    case In: OpIn;
                };
                r = {expr:EBinop(macOp, getHaxeExprFor(a.expr), getHaxeExprFor(b.expr)), pos:Context.currentPos()};
            default: 
            // macro null;
            // case EBreak:
            // case EContinue:
            // case EBlock(e):
            // case EThrow(e):
            // case EVars(vars):
            // case EFunction(kind, f):
            // case EIf(econd, eif, eelse):
        }
        return r;
    }
#end
}