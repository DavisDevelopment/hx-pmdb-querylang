package ql.utils.anonstruct;

import ql.sql.common.DateTime;
import haxe.ds.StringMap;

class AnonMessages {
    public static var NULL_VALUE_NOT_ALLOWED = "Value cannot be null";

    public static var DATE_VALUE_INVALID = "Value must be a valid date";
    public static var DATE_VALUE_MUST_BE_BEFORE = "Value must be before ?VALUE0";
    public static var DATE_VALUE_MUST_BE_BEFORE_OR_EQUAL = "Value must be before or equal ?VALUE0";
    public static var DATE_VALUE_MUST_BE_AFTER = "Value must be after ?VALUE0";
    public static var DATE_VALUE_MUST_BE_AFTER_OR_EQUAL = "Value must be after or equal ?VALUE0";

    public static var ARRAY_VALUE_INVALID = "Value must be an array";
    public static var ARRAY_VALUE_MIN_ITEM_SINGLE = "Value must have at least 1 item";
    public static var ARRAY_VALUE_MIN_ITEM_PLURAL = "Value must have at least ?VALUE0 items";
    public static var ARRAY_VALUE_MAX_ITEM_SINGLE = "Value must have at most 1 item";
    public static var ARRAY_VALUE_MAX_ITEM_PLURAL = "Value must have at most ?VALUE0 items";

    public static var STRING_VALUE_INVALID = "Value must be a string";
    public static var STRING_VALUE_CANNOT_BE_EMPTY = "Value cannot be empty";
    public static var STRING_VALUE_MIN_CHAR_SINGLE = "Value must have at least 1 character";
    public static var STRING_VALUE_MIN_CHAR_PLURAL = "Value must have at least ?VALUE0 characters";
    public static var STRING_VALUE_MAX_CHAR_SINGLE = "Value must have at most 1 character";
    public static var STRING_VALUE_MAX_CHAR_PLURAL = "Value must have at most ?VALUE0 characters";
    public static var STRING_VALUE_SHOULD_STARTS_WITH = "Value should starts with ?VALUE0";
    public static var STRING_VALUE_SHOULD_ENDS_WITH = "Value should ends with ?VALUE0";
    public static var STRING_VALUE_CHAR_NOT_ALLOWED = "Character '?VALUE0' not allowed";
    public static var STRING_VALUE_OPTION_NOT_ALLOWED = "Value '?VALUE0' is not allowed. Accepted values: ?VALUE1";

    public static var INT_VALUE_INVALID = "Value must be an int number";
    public static var INT_VALUE_GREATER_THAN = "Value must be greater than ?VALUE0";
    public static var INT_VALUE_GREATER_OR_EQUAL_THAN = "Value must be greater or equal than ?VALUE0";
    public static var INT_VALUE_LESS_THAN = "Value must be less than ?VALUE0";
    public static var INT_VALUE_LESS_OR_EQUAL_THAN = "Value must me less or equal than ?VALUE0";

    public static var FLOAT_VALUE_INVALID = "Value must be a number";
    public static var FLOAT_VALUE_GREATER_THAN = "Value must be greater than ?VALUE0";
    public static var FLOAT_VALUE_GREATER_OR_EQUAL_THAN = "Value must be greater or equal than ?VALUE0";
    public static var FLOAT_VALUE_LESS_THAN = "Value must be less than ?VALUE0";
    public static var FLOAT_VALUE_LESS_OR_EQUAL_THAN = "Value must be less or equal than ?VALUE0";

    public static var BOOL_VALUE_INVALID = "Value must be a bool";
    public static var BOOL_VALUE_EXPECTED = "Value should be ?VALUE0";

    public static var OBJECT_VALUE_INVALID = "Value must be an object";
    
    public static var FUNCTION_VALUE_INVALID = "Value must be a function";

    static public function setLanguage_PT_BR() {
        NULL_VALUE_NOT_ALLOWED = "O valor não pode ser nulo";
        DATE_VALUE_INVALID = "O valor deve ser uma data válida";
        DATE_VALUE_MUST_BE_BEFORE = "A data deve ser anterior a ?VALUE0";
        DATE_VALUE_MUST_BE_BEFORE_OR_EQUAL = "A data deve ser anterior ou igual a ?VALUE0";
        DATE_VALUE_MUST_BE_AFTER = "A data deve ser posterior a ?VALUE0";
        DATE_VALUE_MUST_BE_AFTER_OR_EQUAL = "A data deve ser posterior ou igual a ?VALUE0";
        ARRAY_VALUE_INVALID = "O valor esperado deve ser um array";
        ARRAY_VALUE_MIN_ITEM_SINGLE = "O array deve ter pelo menos 1 item";
        ARRAY_VALUE_MIN_ITEM_PLURAL = "O array deve ter pelo menos ?VALUE0 itens";
        ARRAY_VALUE_MAX_ITEM_SINGLE = "O array deve ter no máximo 1 item";
        ARRAY_VALUE_MAX_ITEM_PLURAL = "O array deve ter no máximo ?VALUE0 itens";
        STRING_VALUE_INVALID = "O valor deve ser um texto";
        STRING_VALUE_CANNOT_BE_EMPTY = "O texto não pode ser vazio";
        STRING_VALUE_MIN_CHAR_SINGLE = "O texto deve ter pelo menos 1 caractere";
        STRING_VALUE_MIN_CHAR_PLURAL = "O texto deve ter pelo menos ?VALUE0 caracteres";
        STRING_VALUE_MAX_CHAR_SINGLE = "O texto deve ter no máximo 1 caractere";
        STRING_VALUE_MAX_CHAR_PLURAL = "O texto deve ter no máximo ?VALUE0 caracteres";
        STRING_VALUE_SHOULD_STARTS_WITH = "O texto deve começar com ?VALUE0";
        STRING_VALUE_SHOULD_ENDS_WITH = "O texto deve terminar em ?VALUE0";
        STRING_VALUE_CHAR_NOT_ALLOWED = "O caractere ?VALUE0 não é permitido";
        STRING_VALUE_OPTION_NOT_ALLOWED = "'?VALUE0' não é um valor permitido. Os valores aceitos são: ?VALUE1";
        INT_VALUE_INVALID = "O valor deve ser um número inteiro";
        INT_VALUE_GREATER_THAN = "O valor deve ser maior que ?VALUE0";
        INT_VALUE_GREATER_OR_EQUAL_THAN = "O valor deve ser maior ou igual a ?VALUE0";
        INT_VALUE_LESS_THAN = "O valor deve ser menor que ?VALUE0";
        INT_VALUE_LESS_OR_EQUAL_THAN = "O valor deve ser menor ou igual a ?VALUE0";
        FLOAT_VALUE_INVALID = "O valor deve ser um número";
        FLOAT_VALUE_GREATER_THAN = "O valor deve ser maior que ?VALUE0";
        FLOAT_VALUE_GREATER_OR_EQUAL_THAN = "O valor deve ser maior ou igual a ?VALUE0";
        FLOAT_VALUE_LESS_THAN = "O valor deve ser menor que ?VALUE0";
        FLOAT_VALUE_LESS_OR_EQUAL_THAN = "O valor deve ser menor ou igual a ?VALUE0";
        BOOL_VALUE_INVALID = "O valor deve ser booleano";
        BOOL_VALUE_EXPECTED = "O valor esperado era ?VALUE0";
        OBJECT_VALUE_INVALID = "O valor deve ser um objeto";
        FUNCTION_VALUE_INVALID = "O valor deve ser uma função";
    }

}


@:access(anonstruct.AnonProp)
class AnonStruct {
    private var _allowNull:Bool = false;
    private var propMap:StringMap<AnonProp>;
    private var currentStruct:Null<AnonProp> = null;
    private var _validateFunc:Array<Dynamic -> Void> = [];

    public function new() {
        this.propMap = new StringMap<AnonProp>();
    }

    public function addValidation(func:Dynamic->Void):Void {
        this._validateFunc.push(func);
    }

    public function valueBool():AnonPropBool {
        var value:AnonPropBool = new AnonPropBool();
        this.currentStruct = value;
        return value;
    }

    public function valueString():AnonPropString {
        var value:AnonPropString = new AnonPropString();
        this.currentStruct = value;
        return value;
    }

    public function valueInt():AnonPropInt {
        var value:AnonPropInt = new AnonPropInt();
        this.currentStruct = value;
        return value;
    }

    public function valueFloat():AnonPropFloat {
        var value:AnonPropFloat = new AnonPropFloat();
        this.currentStruct = value;
        return value;
    }

    public function valueArray():AnonPropArray {
        var value:AnonPropArray = new AnonPropArray();
        this.currentStruct = value;
        return value;
    }

    public function valueDate():AnonPropDate {
        var value:AnonPropDate = new AnonPropDate();
        this.currentStruct = value;
        return value;
    }

    public function valueObject():AnonPropObject {
        var value:AnonPropObject = new AnonPropObject();
        this.currentStruct = value;
        return value;
    }

    public function valueFunction():AnonPropFunction {
        var value:AnonPropFunction = new AnonPropFunction();
        this.currentStruct = value;
        return value;
    }

    public function refuseNull():Void this._allowNull = false;
    public function allowNull():Void this._allowNull = true;

    public function propertyInt(prop:String):AnonPropInt {
        var propInt:AnonPropInt = new AnonPropInt();
        this.propMap.set(prop, propInt);
        return propInt;
    }

    public function propertyFloat(prop:String):AnonPropFloat {
        var propFloat:AnonPropFloat = new AnonPropFloat();
        this.propMap.set(prop, propFloat);
        return propFloat;
    }

    public function propertyString(prop:String):AnonPropString {
        var propString:AnonPropString = new AnonPropString();
        this.propMap.set(prop, propString);
        return propString;
    }

    public function propertyObject(prop:String):AnonPropObject {
        var propObject:AnonPropObject = new AnonPropObject();
        this.propMap.set(prop, propObject);
        return propObject;
    }

    public function propertyArray(prop:String):AnonPropArray {
        var propArray:AnonPropArray = new AnonPropArray();
        this.propMap.set(prop, propArray);
        return propArray;
    }

    public function propertyDate(prop:String):AnonPropDate {
        var propDate:AnonPropDate = new AnonPropDate();
        this.propMap.set(prop, propDate);
        return propDate;
    }

    public function propertyBool(prop:String):AnonPropBool {
        var propBool:AnonPropBool = new AnonPropBool();
        this.propMap.set(prop, propBool);
        return propBool;
    }

    public function propertyFunction(prop:String):AnonPropFunction {
        var propFunction:AnonPropFunction = new AnonPropFunction();
        this.propMap.set(prop, propFunction);
        return propFunction;
    }

    public function validateAll(data:Dynamic, stopOnFirstError:Bool = false):Void {
        this.validateTree(data, stopOnFirstError, []);
    }

    private function validateTree(data:Dynamic, stopOnFirstError:Bool = false, tree:Array<String> = null) {
        if (tree == null) tree = [];
        var errors:Array<AnonStructError> = [];

        var addDynamicError = function(e:Dynamic, possibleLabel:String, possibleKey:String):Void {
            if (Std.is(e, Array)) {
                var erroList:Array<Dynamic> = cast e;

                for (item in erroList) {
                    if (Std.is(item, AnonStructError)) errors.push(item);
                    else errors.push(new AnonStructError(possibleLabel, possibleKey, Std.string(e)));
                }
            } 
            else if (Std.is(e, AnonStructError)) errors.push(cast e);
            else errors.push(new AnonStructError(possibleLabel, possibleKey, Std.string(e)));

            if (stopOnFirstError) throw errors;
        }

        if (data == null && !this._allowNull) {
            addDynamicError(AnonMessages.NULL_VALUE_NOT_ALLOWED, '', tree.join('.'));
        } 
        else {
            
            try {
                if (this.currentStruct != null) 
                    this.currentStruct.validate(data, tree);
            } 
            catch(e:Dynamic) {
                addDynamicError(e, this.currentStruct.propLabel, tree.join('.'));
            }

            for (key in this.propMap.keys()) {
                var value:Dynamic = null;

                try {
                    value = Reflect.getProperty(data, key);
                } 
                catch (e:Dynamic) {}

                var tempTree:Array<String> = tree.concat([key]);

                try {
                    var p = this.propMap.get(key);
                    
                    p.validate(value, tempTree);
                } 
                catch (e:Dynamic) {
                    addDynamicError(e, this.propMap.get(key).propLabel, tempTree.join('.'));
                }
            }

            for (func in this._validateFunc) {
                try {
                    func(data);
                } 
                catch (e:Dynamic) {
                    addDynamicError(e, '', tree.join('.'));
                }
            }
        }

        if (errors.length != 0) 
            throw(errors);
    }

    public function validate(data:Dynamic):Void {
        try {
            this.validateAll(data, true);
        } catch (e:Dynamic) {
            var arr:Array<AnonStructError> = cast e;
            throw e[0];
        }
    }

    public function getErrors(data:Dynamic):Array<AnonStructError> {
        try {
            this.validateAll(data);
            return [];
        } catch (e:Dynamic) {
            var arr:Array<AnonStructError> = cast e;
            return arr;
        }
    }

    public function pass(data:Dynamic):Bool {
        try {
            this.validate(data);
            return true;
        } catch(e:Dynamic) {
            return false;
        }
    }
}

class AnonProp {
    private var _validateFunc:Array<Dynamic> = [];
    public var propLabel:String = "";

    public function new() {}

    private function validate(value:Dynamic, ?tree:Array<String>):Void {
        
    }

    private function validateFuncs(val:Dynamic):Void {
        for (func in this._validateFunc) func(val);
    }
}

class AnonPropDate extends AnonProp {
    private var _allowNull:Bool = false;

    private var _minDate:Null<DateTime>;
    private var _maxDate:Null<DateTime>;

    private var _minEqual:Bool;
    private var _maxEqual:Bool;

    public function new() {
        super();
    }

    public inline function addErrorLabel(label:String):AnonPropDate {
        this.propLabel = label;
        return this;
    }

    override private function validateFuncs(val:Dynamic):Void {
        var currVal:DateTime = val;
        for (func in this._validateFunc) {
            var currFunc:DateTime->Void = func;
            currFunc(currVal);
        }
    }

    public inline function addValidation(func:DateTime->Void):AnonPropDate {
        this._validateFunc.push(func);
        return this;
    }

    public inline function refuseNull():AnonPropDate {
        this._allowNull = false;
        return this;
    }

    public inline function allowNull():AnonPropDate {
        this._allowNull = true;
        return this;
    }

    public inline function greaterOrEqualThan(date:Null<DateTime>):AnonPropDate {
        this._minDate = date;
        this._minEqual = true;
        return this;
    }

    public inline function greaterThan(date:Null<DateTime>):AnonPropDate {
        this._minDate = date;
        this._minEqual = false;
        return this;
    }

    public inline function lessThan(date:Null<DateTime>):AnonPropDate {
        this._minDate = date;
        this._maxEqual = false;
        return this;
    }

    public inline function lessOrEqualThan(date:Null<DateTime>):AnonPropDate {
        this._minDate = date;
        this._maxEqual = true;
        return this;
    }

    inline private function validate_allowedNull(value:Dynamic, allowNull:Bool):Bool {
        return (value != null || (value == null && allowNull));
    }

    /**
     * checks if a value is a valid `DateTime` value
     * @param value 
     * @return Bool
     */
    private function validate_isDateTime(value: Dynamic):Bool {
        try {
            DateTime.fromAny(value);
            return true;
        } catch (e: Dynamic) {}
        return DateTime.is(value);
    }

    inline private function validate_min(value:DateTime, min:Null<DateTime>, equal:Null<Bool>):Bool {
        return ((min == null) || ((equal == null || !equal) && value > min) || (equal && value >= min));
    }

    inline private function validate_max(value:DateTime, max:Null<DateTime>, equal:Null<Bool>):Bool {
        return ((max == null) || ((equal == null || !equal) && value < max) || (equal && value <= max));
    }

    override private function validate(value:Dynamic, ?tree:Array<String>):Void {
        super.validate(value, tree);
        
        if (!this.validate_allowedNull(value, this._allowNull)) throw AnonMessages.NULL_VALUE_NOT_ALLOWED;
        else if (value != null) {
            if (!this.validate_isDateTime(value)) throw AnonMessages.DATE_VALUE_INVALID;
            else {
                var date:DateTime = DateTime.fromAny(value);

                if (!this.validate_min(date, this._minDate, this._minEqual))
                    throw (
                        this._minEqual
                        ? AnonMessages.DATE_VALUE_MUST_BE_BEFORE_OR_EQUAL
                        : AnonMessages.DATE_VALUE_MUST_BE_BEFORE
                    ).split('?VALUE0').join(this._minDate.toString());

                if (!this.validate_max(date, this._maxDate, this._maxEqual))
                    throw (
                        this._maxEqual
                        ? AnonMessages.DATE_VALUE_MUST_BE_AFTER_OR_EQUAL
                        : AnonMessages.DATE_VALUE_MUST_BE_AFTER
                    ).split('?VALUE0').join(this._maxDate.toString());

                this.validateFuncs(date);
            }
        }
    }
}

@:access(anonstruct.AnonStruct)
class AnonPropArray extends AnonProp {
    private var _maxLen:Null<Int> = null;
    private var _minLen:Null<Int> = null;

    private var _allowNull:Bool = false;
    private var _childStruct:Null<AnonStruct> = null;

    public function new() {
        super();
    }

    public function minLen(len:Int):AnonPropArray {
        this._minLen = len;
        return this;
    }

    public function maxLen(len:Int):AnonPropArray {
        this._maxLen = len;
        return this;
    }

    public function addErrorLabel(label:String):AnonPropArray {
        this.propLabel = label;
        return this;
    }

    public function setStruct(structure:AnonStruct):AnonPropArray {
        this._childStruct = structure;
        return this;
    }

    public function refuseNull():AnonPropArray {
        this._allowNull = false;
        return this;
    }

    public function allowNull():AnonPropArray {
        this._allowNull = true;
        return this;
    }

    public function addValidation(func:Array<Dynamic>->Void):AnonPropArray {
        this._validateFunc.push(func);
        return this;
    }

    inline private function validate_allowedNull(value:Dynamic, allowNull:Bool):Bool return (value != null || (value == null && allowNull));
    inline private function validate_isArray(value:Dynamic):Bool return (Std.is(value, Array));
    inline private function validate_minLen(value:Array<Dynamic>, minLen:Null<Int>):Bool return (minLen == null || minLen < 0 || value.length >= minLen);
    inline private function validate_maxLen(value:Array<Dynamic>, maxLen:Null<Int>):Bool return (maxLen == null || maxLen < 0 || value.length <= maxLen);
    
    override private function validate(value:Dynamic, ?tree:Array<String>):Void {
        if (tree == null) tree = [];
        super.validate(value, tree);

        if (!this.validate_allowedNull(value, this._allowNull)) throw AnonMessages.NULL_VALUE_NOT_ALLOWED;
        else if (value != null) {
            if (!this.validate_isArray(value)) throw AnonMessages.ARRAY_VALUE_INVALID;
            else {

                var val:Array<Dynamic> = cast value;

                if (!this.validate_minLen(val, this._minLen))
                    throw (
                        this._minLen <= 1
                        ? AnonMessages.ARRAY_VALUE_MIN_ITEM_SINGLE
                        : AnonMessages.ARRAY_VALUE_MIN_ITEM_PLURAL
                    ).split('?VALUE0').join(Std.string(this._minLen));

                if (!this.validate_maxLen(val, this._maxLen))
                    throw (
                        this._maxLen <= 1
                        ? AnonMessages.ARRAY_VALUE_MAX_ITEM_SINGLE
                        : AnonMessages.ARRAY_VALUE_MAX_ITEM_PLURAL
                    ).split('?VALUE0').join(Std.string(this._maxLen));

                if (this._childStruct != null) {

                    for (i in 0 ... val.length) {
                        var item = val[i];
                        
                        this._childStruct.validateTree(item, tree.concat(['[$i]']));
                    }

                }

                this.validateFuncs(val);
            }
        }

    }

}

@:access(anonstruct.AnonStruct)
class AnonPropObject extends AnonProp {

    private var _allowNull:Bool = false;
    private var _struct:Null<AnonStruct> = null;

    public function new() {
        super();
    }

    public function addErrorLabel(label:String):AnonPropObject {
        this.propLabel = label;
        return this;
    }

    public function addValidation(func:Dynamic->Void):AnonPropObject {
        this._validateFunc.push(func);
        return this;
    }

    public function refuseNull():AnonPropObject {
        this._allowNull = false;
        return this;
    }

    public function allowNull():AnonPropObject {
        this._allowNull = true;
        return this;
    }

    public function setStruct(structure:AnonStruct):AnonPropObject {
        this._struct = structure;
        return this;
    }

    inline private function validate_allowedNull(value:Dynamic, allowNull:Bool):Bool return (value != null || (value == null && allowNull));
    inline private function validate_isObject(value:Dynamic):Bool {
        // this is the best approach??
        if (
            value == null ||
            Std.is(value, String) ||
            Std.is(value, Float) ||
            Std.is(value, Bool) ||
            Std.is(value, Array) ||
            Std.is(value, Class) ||
            Reflect.isFunction(value) 
        ) return false;
        
        return true;
    }

    override private function validate(value:Dynamic, ?tree:Array<String>):Void {
        if (tree == null) tree = [];
        super.validate(value, tree);

        if (!this.validate_allowedNull(value, this._allowNull)) throw AnonMessages.NULL_VALUE_NOT_ALLOWED;
        else if (value != null) {

            if (!this.validate_isObject(value)) throw AnonMessages.STRING_VALUE_INVALID;
            else {
        
                if (this._struct != null) this._struct.validateTree(value, tree.copy());
                this.validateFuncs(value);

            }
        }

    }

}

class AnonPropString extends AnonProp {

    private var _allowNull:Bool = false;
    private var _allowEmpty:Bool = false;

    private var _maxChar:Null<Int> = null;
    private var _minChar:Null<Int> = null;
    private var _startsWith:Null<String> = null;
    private var _endsWidth:Null<String> = null;

    private var _allowedChars:Null<String> = null;

    private var _allowedOptions:Null<Array<String>> = null;
    private var _allowedOptionsMatchCase:Bool = false;

    public function new() {
        super();
    }

    public function setAllowedOptions(values:Null<Array<String>>, matchCase:Bool = true):AnonPropString {
        this._allowedOptions = values;
        this._allowedOptionsMatchCase = matchCase;
        return this;
    }

    public function addErrorLabel(label:String):AnonPropString {
        this.propLabel = label;
        return this;
    }

    public function addValidation(func:String->Void):AnonPropString {
        this._validateFunc.push(func);
        return this;
    }

    public function startsWith(value:Null<String>):AnonPropString {
        this._startsWith = value;
        return this;
    }

    public function endsWith(value:Null<String>):AnonPropString {
        this._endsWidth = value;
        return this;
    }

    public function refuseNull():AnonPropString {
        this._allowNull = false;
        return this;
    }

    public function allowNull():AnonPropString {
        this._allowNull = true;
        return this;
    }

    public function refuseEmpty():AnonPropString {
        this._allowEmpty = false;
        return this;
    }

    public function allowEmpty():AnonPropString {
        this._allowEmpty = true;
        return this;
    }

    public function maxChar(chars:Null<Int>):AnonPropString {
        this._maxChar = chars;
        return this;
    }

    public function minChar(chars:Null<Int>):AnonPropString {
        this._minChar = chars;
        return this;
    }

    public function allowChars(chars:Null<String>):AnonPropString {
        this._allowedChars = chars;
        return this;
    }

    inline private function validate_allowedNull(value:Dynamic, allowNull:Bool):Bool return (value != null || (value == null && allowNull));
    inline private function validate_isString(value:Dynamic):Bool return (Std.is(value, String));
    inline private function validate_isEmpty(value:String):Bool return (StringTools.trim(value).length == 0);
    inline private function validate_allowedEmpty(value:String, allowEmpty:Null<Bool>):Bool {
        var len:Int = StringTools.trim(value).length;
        return (len > 0 || (len == 0 && allowEmpty == true));
    }
    inline private function validate_minChar(value:String, minChar:Null<Int>):Bool return (minChar == null || minChar < 0 || value.length >= minChar);
    inline private function validate_maxChar(value:String, maxChar:Null<Int>):Bool return (maxChar == null || maxChar < 0 || value.length <= maxChar);
    inline private function validate_startsWith(value:String, startsWith:Null<String>):Bool return (startsWith == null || startsWith.length == 0 || StringTools.startsWith(value, startsWith));
    inline private function validate_endsWith(value:String, endsWith:Null<String>):Bool return (endsWith == null || endsWith.length == 0 || StringTools.endsWith(value, endsWith));
    inline private function validate_allowedChars(value:String, allowedChars:Null<String>):String {
        var result:String = '';
        if (allowedChars != null && allowedChars.length > 0) {
            for (i in 0 ... value.length) 
                if (allowedChars.indexOf(value.charAt(i)) == -1) {
                    result = value.charAt(i);
                    break;
                }
        }
        return result;
    }
    private function validate_allowedOptions(value:String, options:Null<Array<String>>, matchCase:Null<Bool>):Bool {
        if (options == null || options.length == 0) return true;
        else if (matchCase) return (options.indexOf(value) > -1);
        else {
            for (item in options) if (item.toLowerCase() == value.toLowerCase()) return true;
            return false;
        }
    }

    override private function validate(value:Dynamic, ?tree:Array<String>):Void {
        super.validate(value, tree);

        if (!this.validate_allowedNull(value, this._allowNull)) {
            throw AnonMessages.NULL_VALUE_NOT_ALLOWED;
        } else if (value != null) {
            if (!this.validate_isString(value)) {
                throw AnonMessages.STRING_VALUE_INVALID;
            } else {

                var val:String = cast value;

                if (!this.validate_allowedEmpty(val, _allowEmpty)) throw AnonMessages.STRING_VALUE_CANNOT_BE_EMPTY;

                if (!this.validate_isEmpty(val)) {
                    if (!this.validate_minChar(val, this._minChar))
                        throw (
                            this._minChar <= 1
                            ? AnonMessages.STRING_VALUE_MIN_CHAR_SINGLE
                            : AnonMessages.STRING_VALUE_MIN_CHAR_PLURAL
                        ).split('?VALUE0').join(Std.string(this._minChar));

                    if (!this.validate_maxChar(val, this._maxChar))
                        throw (
                            this._maxChar <= 1
                            ? AnonMessages.STRING_VALUE_MAX_CHAR_SINGLE
                            : AnonMessages.STRING_VALUE_MAX_CHAR_PLURAL
                        ).split('?VALUE0').join(Std.string(this._maxChar));

                    if (!this.validate_startsWith(val, this._startsWith)) throw AnonMessages.STRING_VALUE_SHOULD_STARTS_WITH.split("?VALUE0").join(this._startsWith);
                    if (!this.validate_endsWith(val, this._endsWidth)) throw AnonMessages.STRING_VALUE_SHOULD_ENDS_WITH.split("?VALUE0").join(this._endsWidth);
                    
                    var char:String = this.validate_allowedChars(val, this._allowedChars);
                    if (char.length > 0) throw AnonMessages.STRING_VALUE_CHAR_NOT_ALLOWED.split("?VALUE0").join(char);

                    if (!this.validate_allowedOptions(val, this._allowedOptions, this._allowedOptionsMatchCase))
                        throw AnonMessages.STRING_VALUE_OPTION_NOT_ALLOWED
                            .split('?VALUE0').join(val)
                            .split('?VALUE1').join(this._allowedOptions.join(', '));   
                }
                
                this.validateFuncs(val);
            }
        }
    }
}

class AnonPropInt extends AnonProp {

    private var _allowNull:Bool = false;

    private var _max:Null<Int> = null;
    private var _min:Null<Int> = null;

    private var _maxEqual:Bool = false;
    private var _minEqual:Bool = false;

    public function new() {
        super();
    }

    public function addErrorLabel(label:String):AnonPropInt {
        this.propLabel = label;
        return this;
    }

    public function addValidation(func:Int->Void):AnonPropInt {
        this._validateFunc.push(func);
        return this;
    }

    public function refuseNull():AnonPropInt {
        this._allowNull = false;
        return this;
    }

    public function allowNull():AnonPropInt {
        this._allowNull = true;
        return this;
    }

    public function lessThan(maxValue:Null<Int>):AnonPropInt {
        this._max = maxValue;
        this._maxEqual = false;
        return this;
    }

    public function lessOrEqualThan(maxValue:Null<Int>):AnonPropInt {
        this._max = maxValue;
        this._maxEqual = true;
        return this;
    }

    public function greaterThan(minValue:Null<Int>):AnonPropInt {
        this._min = minValue;
        this._minEqual = false;
        return this;
    }

    public function greaterOrEqualThan(minValue:Null<Int>):AnonPropInt {
        this._min = minValue;
        this._minEqual = true;
        return this;
    }

    inline private function validate_allowedNull(value:Dynamic, allowNull:Bool):Bool return (value != null || (value == null && allowNull));
    inline private function validate_isInt(value:Dynamic):Bool return (Std.is(value, Int));
    inline private function validate_min(value:Int, min:Null<Int>, equal:Null<Bool>):Bool return ((min == null) || ((equal == null || !equal) && value > min) || (equal && value >= min));
    inline private function validate_max(value:Int, max:Null<Int>, equal:Null<Bool>):Bool return ((max == null) || ((equal == null || !equal) && value < max) || (equal && value <= max));

    override private function validate(value:Dynamic, ?tree:Array<String>):Void {
        super.validate(value, tree);

        if (!this.validate_allowedNull(value, this._allowNull)) throw AnonMessages.NULL_VALUE_NOT_ALLOWED;
        else if (value != null) {
            if (!this.validate_isInt(value)) throw AnonMessages.INT_VALUE_INVALID;
            else {

                var val:Int = cast value;

                if (!this.validate_min(val, this._min, this._minEqual))
                    throw (
                        this._minEqual
                        ? AnonMessages.INT_VALUE_GREATER_OR_EQUAL_THAN
                        : AnonMessages.INT_VALUE_GREATER_THAN
                    ).split('?VALUE0').join(Std.string(this._min));

                if (!this.validate_max(val, this._max, this._maxEqual))
                    throw (
                        this._maxEqual
                        ? AnonMessages.INT_VALUE_LESS_OR_EQUAL_THAN
                        : AnonMessages.INT_VALUE_LESS_THAN
                    ).split('?VALUE0').join(Std.string(this._max));

                this.validateFuncs(val);
            }
        }
    }
}

class AnonPropFloat extends AnonProp {

    private var _allowNull:Bool = false;

    private var _max:Null<Float> = null;
    private var _min:Null<Float> = null;

    private var _maxEqual:Bool = false;
    private var _minEqual:Bool = false;

    public function new() {
        super();
    }

    public function addErrorLabel(label:String):AnonPropFloat {
        this.propLabel = label;
        return this;
    }

    public function addValidation(func:Float->Void):AnonPropFloat {
        this._validateFunc.push(func);
        return this;
    }

    public function refuseNull():AnonPropFloat {
        this._allowNull = false;
        return this;
    }

    public function allowNull():AnonPropFloat {
        this._allowNull = true;
        return this;
    }

    public function lessThan(maxValue:Null<Float>):AnonPropFloat {
        this._max = maxValue;
        this._maxEqual = false;
        return this;
    }

    public function lessOrEqualThan(maxValue:Null<Float>):AnonPropFloat {
        this._max = maxValue;
        this._maxEqual = true;
        return this;
    }

    public function greaterThan(minValue:Null<Float>):AnonPropFloat {
        this._min = minValue;
        this._minEqual = false;
        return this;
    }

    public function greaterOrEqualThan(minValue:Null<Float>):AnonPropFloat {
        this._min = minValue;
        this._minEqual = true;
        return this;
    }

    inline private function validate_allowedNull(value:Dynamic, allowNull:Bool):Bool return (value != null || (value == null && allowNull));
    inline private function validate_isFloat(value:Dynamic):Bool return (Std.is(value, Float));
    inline private function validate_min(value:Float, min:Null<Float>, equal:Null<Bool>):Bool return ((min == null) || ((equal == null || !equal) && value > min) || (equal && value >= min));
    inline private function validate_max(value:Float, max:Null<Float>, equal:Null<Bool>):Bool return ((max == null) || ((equal == null || !equal) && value < max) || (equal && value <= max));

    override private function validate(value:Dynamic, ?tree:Array<String>):Void {
        super.validate(value, tree);

        if (!this.validate_allowedNull(value, this._allowNull)) throw AnonMessages.NULL_VALUE_NOT_ALLOWED;
        else if (value != null) {
            if (!this.validate_isFloat(value)) throw AnonMessages.FLOAT_VALUE_INVALID;
            else {

                var val:Float = cast value;

                if (!this.validate_min(val, this._min, this._minEqual))
                    throw (
                        this._minEqual
                        ? AnonMessages.INT_VALUE_GREATER_OR_EQUAL_THAN
                        : AnonMessages.INT_VALUE_GREATER_THAN
                    ).split('?VALUE0').join(Std.string(this._min));

                if (!this.validate_max(val, this._max, this._maxEqual))
                    throw (
                        this._maxEqual
                        ? AnonMessages.INT_VALUE_LESS_OR_EQUAL_THAN
                        : AnonMessages.INT_VALUE_LESS_THAN
                    ).split('?VALUE0').join(Std.string(this._max));

                this.validateFuncs(val);
            }
        }

    }
}

class AnonPropBool extends AnonProp {

    private var _allowNull:Bool = false;
    private var _expectedValue:Null<Bool>;

    public function new() {
        super();
    }

    public function addErrorLabel(label:String):AnonPropBool {
        this.propLabel = label;
        return this;
    }

    public function addValidation(func:Bool->Void):AnonPropBool {
        this._validateFunc.push(func);
        return this;
    }

    public function expectedValue(value:Bool):AnonPropBool {
        this._expectedValue = value;
        return this;
    }

    public function refuseNull():AnonPropBool {
        this._allowNull = false;
        return this;
    }

    public function allowNull():AnonPropBool {
        this._allowNull = true;
        return this;
    }

    inline private function validate_allowedNull(value:Dynamic, allowNull:Bool):Bool return (value != null || (value == null && allowNull));
    inline private function validate_isBool(value:Dynamic):Bool return (Std.is(value, Bool));
    inline private function validate_expected(value:Bool, expected:Null<Bool>):Bool return ((expected == null) || (expected != null && value == expected));

    override private function validate(value:Dynamic, ?tree:Array<String>):Void {
        super.validate(value, tree);

        if (!this.validate_allowedNull(value, this._allowNull)) throw AnonMessages.NULL_VALUE_NOT_ALLOWED;
        else if (value != null) {
            if (!this.validate_isBool(value)) throw AnonMessages.BOOL_VALUE_INVALID;
            else {
                var val:Bool = cast value;

                if (!this.validate_expected(val, this._expectedValue)) {
                    throw AnonMessages.BOOL_VALUE_EXPECTED
                        .split('?VALUE0')
                        .join(this._expectedValue ? 'true' : 'false');
                }

                this.validateFuncs(val);
            }
        }
    }
}


class AnonPropFunction extends AnonProp {

    private var _allowNull:Bool = false;

    public function addErrorLabel(label:String):AnonPropFunction {
        this.propLabel = label;
        return this;
    }

    public function addValidation(func:Dynamic->Void):AnonPropFunction {
        this._validateFunc.push(func);
        return this;
    }

    public function refuseNull():AnonPropFunction {
        this._allowNull = false;
        return this;
    }

    public function allowNull():AnonPropFunction {
        this._allowNull = true;
        return this;
    }

    inline private function validate_allowedNull(value:Dynamic, allowNull:Bool):Bool return (value != null || (value == null && allowNull));
    inline private function validate_isFunction(value:Dynamic):Bool return Reflect.isFunction(value);

    override private function validate(value:Dynamic, ?tree:Array<String>):Void {
        super.validate(value, tree);

        if (!this.validate_allowedNull(value, this._allowNull)) throw AnonMessages.NULL_VALUE_NOT_ALLOWED;
        else if (value != null) {
            if (!this.validate_isFunction(value)) throw AnonMessages.FUNCTION_VALUE_INVALID;
            else {
                var val:Dynamic = cast value;
                this.validateFuncs(val);
            }
        }
    }


}

class AnonStructError {

    public var label:String;
    public var property:String;
    public var errorMessage:String;

    public function new(label:String, property:String, errorMessage:String) {
        this.label = label;
        this.property = property;
        this.errorMessage = errorMessage;

        if (this.label == null) this.label = "";
    }

    public function toString():String {
        if (this.property != "") return this.property + ": " + this.errorMessage;
        else return this.errorMessage;
    }

    public function toStringFriendly():String {
        if (this.label != "") return this.label + ": " + this.errorMessage;
        else return this.errorMessage;
    }
}