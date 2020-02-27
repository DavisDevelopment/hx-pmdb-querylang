package ql.sql.common;

import ql.sql.runtime.TAst.JITFn;
import ql.sql.runtime.TAst.TExpr;
import pmdb.core.ds.Incrementer;
import pm.map.Dictionary;
import pmdb.core.Arch;
import pmdb.core.Object;
import pmdb.core.Object.Doc;
import pm.Error;
import pm.HashKey;
import pm.ImmutableList;
import pm.ImmutableList.ListRepr as Cell;

using pm.Options;
using pm.Outcome;

import ql.sql.runtime.SType;
import ql.sql.runtime.DType;
import ql.sql.runtime.VirtualMachine.Context;

import haxe.rtti.Rtti;
import haxe.rtti.Meta;
import haxe.rtti.CType.Classdef;
import haxe.ds.Either;
import haxe.ds.Option;
import haxe.ds.ReadOnlyArray;
import haxe.extern.EitherType as Or;

import pm.Helpers.nor;

import haxe.macro.Expr;

import pm.Error;

using Lambda;
using pm.Arrays;
using ql.sql.runtime.SType;

using haxe.macro.ExprTools;

class SqlSchema<Row> {
    public final mode: SchemaMode;
    public final fields: SchemaFields;
    public final indexes: ReadOnlyArray<SchemaIndex>;

    public final primaryKey: Null<SchemaField>;
    public final incrementers: Null<Dictionary<Incrementer>>;

    public var context: Null<Context<Dynamic, Dynamic, Dynamic>> = null;

    /**
     * allocates and initializes a new SqlSchema object
     * @param state 
     */
    public function new(state: SchemaInit) {
        // var myfields = [];

        this.context = state.context;
        this.mode = getInitMode(state.mode);
        this.indexes = new Array();
        this.incrementers = new Dictionary();
        var fieldList = getInitFields(state.fields).map(initSchemaField);

        this.fields = new SchemaFields(this, fieldList);
        this.primaryKey = switch this.fields.primary {
            case null: null;
            case f: f.field;
        };
    }

    public function column(name: String):SchemaField {
        // return (cast fields : Array<SchemaField>).find(f -> f.name == name);
        return this.fields.mapping.get(name);
    }

    private var _objectMode:Null<Bool> = null;
    public function isObjectMode():Bool {
        if (_objectMode == null) {
            _objectMode = this.mode == ObjectMode;
        }
        return _objectMode;
    }

    /**
     * ensures that the given `Row` object is shaped correctly and with the correct types as described by [this] Schema
     * @param row 
     * @return Bool
     * @throws Error if any validation check fails;  Exception is always instance of `pm.Error`
     */
    public function validate(row:Row, ?pos:haxe.PosInfos):Bool {
        var row:Doc = Doc.unsafe(row);
        for (field in fields) {
            var val = row.get(field.name);
            if (val == null) {
                if (field.notNull) {
                    throw new Error('Field `${field.name}` missing from structure', pos);
                }
                else {
                    continue;
                }
            }
            else {
                if (!field.type.validateValue(val)) {
                    throw new Error('Expected ${field.type}, got $val', pos);
                }
            }
        }
        return true;
    }

    public function test(row: Row):Outcome<Bool, Array<Error>> {
		var doc:Doc = Doc.unsafe(row);
        var errors:Array<Error> = [];
        var rowFields = new set.StringSet(doc.keys());

        for (field in fields) {
            final name = field.name;
            if (rowFields.exists(name)) {
                rowFields.remove(name);
                var value:Dynamic = doc.get(name);

                if (value == null && (field.notNull)) {
                    errors.push(new Error('Field `${field.name}` missing from structure'));
                    continue;
                }

                try {
                    final correctType = field.type.validateValue(value);
                    if (!correctType) {
                        errors.push(new Error('Expected ${field.type}, got $value'));
                        continue;
                    }
                }
                catch (err: Error) {
                    errors.push(err);
                }
                catch (err: Dynamic) {
                    errors.push(Error.withData(err));
                }
            }
            else if (field.notNull) {
                errors.push(new Error('Field `${field.name}` missing from structure'));
                continue;
            }
            else {
                rowFields.remove(name);
            }
        }

        /**
          [TODO] add `allowExtraFields`/`allowRestFields` setting
         **/
        var extraFields:Null<Doc> = null;
        switch [rowFields.length, errors.length] {
            case [0, 0]:
                return Success(true);

            case [_, _]:
                if (/*TODO: allowExtraFields*/false) {
                    extraFields = doc.pick(rowFields.toArray());
                    return Failure(errors);
                }
                else {
                    for (k in rowFields) {
                        errors.unshift(new Error('Object has extra field `$k`'));
                    }

                    return Failure(errors);
                }
        }
    }

    public function induct(o: Dynamic):Row {
        #if !debug
        if (_compiledInductObject == null) {
            _compiledInductObject = cast objectImporter();
        }
        return _compiledInductObject(o);
        #end

        var obj:Object<Dynamic> = Object.unsafe(o);
        return inductObject(obj);
    }

    var _compiledInductObject:Null<Dynamic->Row> = null;
    /**
     * convert from a Haxe object into a PmDbDocument
     * @param o 
     * @return Row
     */
    public function inductObject(o: Doc):Row {
        var row:Doc = new Doc();
		final doc:Doc = Doc.unsafe(o);
		// var errors:Array<Error> = [];
        var rowFields = new set.StringSet(doc.keys());
        
        inline function handleNull(field: SchemaField) {
            final name = field.name;
			if (field.autoIncrement && this.incrementers.exists(name)) {
				row[name] = incrementers[name].next();
            } 
            else if (field.defaultValueExpr != null) {
				row[name] = field.type.importValue(field.defaultValueExpr.eval(this));
            } 
            else {
				throw(new Error('Field `${field.name}` missing from structure'));
			}
        }

		for (field in fields) {
			final name = field.name;
			if (rowFields.exists(name)) {
				rowFields.remove(name);
				var value:Dynamic = doc.get(name);

				if (value == null && (field.notNull)) {
					handleNull(field);
                }
                else if (value == null) {
                    row[name] = null;
                    continue;
                }

                row[name] = field.type.importValue(value);
            } 
            else if (field.notNull) {
                handleNull(field);
            }
            else {
                rowFields.remove(name);
                row[name] = null;
			}
		}

        // var extraFields:Null<Doc> = rowFields.length == 0 ? null : o.pick(rowFields.toArray());
        // if (extraFields != null) trace('Extra: ', extraFields);
        
        return convertDocToRow(row);
    }

    // function objectImporter():Doc -> Doc {
    //     var fieldNames = fields.map(f -> f.name);
    //     var picker = (o: Doc) -> inline o.pick(fieldNames);
    //     var field_fns = fields.fields.map(f -> objectImporterFieldFn(f));

    //     var imp:Doc->Doc = function(o: Doc):Doc {
    //         var row:Doc = picker(o);

    //         for (f in field_fns)
    //             f(o, row);
			
    //         return row;
    //     }

    //     return imp;
    // }
    // function objectImporterFieldFn(field: SchemaField) {
    //     final f = field;
    //     final name = field.name;

	// 	inline function handleNull(field:SchemaField, row:Doc) {
	// 		final name = field.name;
	// 		if (field.autoIncrement && this.incrementers.exists(name)) {
	// 			row[name] = incrementers[name].next();
    //         } 
    //         else if (field.defaultValueExpr != null) {
	// 			row[name] = field.type.importValue(field.defaultValueExpr.eval(this));
    //         } 
    //         else {
	// 			throw(new Error('Field `${field.name}` missing from structure'));
    //         }
    //     }
        
    //     function handleField(input:Doc, output:Doc) {
	// 		if (input.exists(name)) {
	// 			var value:Dynamic = input[name];
	// 			if (value == null && f.notNull) {
	// 				handleNull(f, output);
    //             } 
    //             else if (value == null) {
	// 				output[name] = null;
	// 				return ;
	// 			}

	// 			output[name] = f.type.importValue(value);
    //         } 
    //         else if (f.notNull) {
	// 			handleNull(f, output);
    //         } 
    //         else {
	// 			output[name] = null;
	// 		}
    //     }

    //     return handleField;
    // }

    /**
     * checks the validity of the given object as the 'constructor' argument
     * @param row 
     */
    public function validateInput(row: Doc):{status:Bool, errors:Array<Error>} {
        var result = {
            status: true,
            errors: new Array()
        };

        inline function assert(cond:Void->Bool, ?err:Error) {
            try {
                var b = cond();
                result.status = result.status && b;
                if (!b)
                    result.errors.push(err != null ? err : new Error('Assertion failed'));
            }
            catch (error: Error) {
                result.status = false;
                result.errors.push(error);
            }
            catch (e: Dynamic) {
                result.status = false;
                result.errors.push(Error.withData(e, 'Condition threw:'));
            }
        }

        for (field in fields) {
            if (field.ctor_field != null) {
                var cfield = field.ctor_field;
                if (cfield.omissionMandatory) {
                    assert(()->row.get(cfield.name) == null, new Error('Extra field ${cfield.name}:${cfield.type.print()}'));
                }
                else if (row.exists(cfield.name)) {
                    assert(()->cfield.type.validateValue(row[cfield.name]));
                }
                else if (cfield.omittable) {
                    continue;
                }
                else {
                    // Console.examine(row[cfield.name], field.type, field.ctor_field.type);
                    throw new Error('Failure, sha');
                }
            }
        }

        return result;
    }

    /**
     * given a `Row` object, convert to a clone thereof which is prepared to be inserted into a Store
     * @param row 
     * @return Row
     */
    public function prepareForInsertion(row: Row):Row {
        var copy:Doc = new Doc();
        var doc:Doc = Doc.unsafe(row);
        
        for (field in fields) {
            var value:Dynamic = doc[field.name];
            if (field.notNull && value == null) {
                switch field {
                    case {autoIncrement:true}:
                        throw new Error('TODO');

                    default:
                }

                throw new ValueError(doc, 'Missing column ${field.name}:${field.type} in ${''+doc}');
            }
            else if (value == null) {
                copy[field.name] = null;
                continue;
            }

            // if (!field.type.validateValue(value)) {
            //     value = field.type.importValue(value);
            // }

            copy[field.name] = field.type.importValue(value);
        }

        return convertDocToRow(copy);
    }

    /**
     * conversion of the pmdb utility type `Doc` (Object<Dynamic>) to the local parameter type `Row`
     * intended to be overridden by subclasses as needed
     * @param o 
     * @return Row
     */
    public function convertDocToRow(o: Object<Dynamic>):Row {
        return untyped o;
    }

	static function getInitMode(mode: Null<Or<SchemaMode, String>>):SchemaMode {
        if (mode == null) {
            return RowMode;
        }
        else {
            if ((mode is String)) {
                switch (cast(mode, String).toLowerCase()) {
                    case 'object':
                        return ObjectMode;

                    default:
                        return RowMode;
                }
            }
            else {
                return cast(mode, SchemaMode);
            }
        }
    }

    private inline static function initSchemaField(init: SchemaFieldInit):SchemaField {
        if (init.name == null) throw new pm.Error('missing "name" field');
        if (init.type == null) init.type = TUnknown;
        
        init.notNull = init.notNull.nor(false);
        init.unique = init.unique.nor(false);
        init.autoIncrement = init.autoIncrement.nor(false);
        init.primaryKey = init.primaryKey.nor(false);

        var state:SchemaFieldState = {
            name: init.name,
            type: init.type,
            defaultValue: if ((init.defaultValue is String)) Left(cast(init.defaultValue, String)) else if ((init.defaultValue is TExpr)) Right(cast(init.defaultValue, TExpr)) else null,
            primaryKey: init.primaryKey,
            unique: init.unique,
            notNull: init.notNull,
            autoIncrement: init.autoIncrement
        };

        return SchemaField.make(state);
    }

	static function getInitFields(fields:Or<Array<SchemaFieldInit>, SchemaInitFieldMap>):Array<SchemaFieldInit> {
        if ((fields is Array<Dynamic>)) {
            var fields:Array<SchemaFieldInit> = cast fields;
            assert(fields.all(f -> f.name.nn()), new Error('missing name'));
            return fields;
        }
        else {
            var map:haxe.DynamicAccess<SchemaFieldInit> = cast fields;
            
            return map.keyValueIterator().array().map(kv -> {
                kv.value.name = kv.key;
                kv.value;
            });
        }
    }

    public static function fromClass<T>(type: Class<T>):SqlSchema<T> {
        if (!Rtti.hasRtti(type)) throw new pm.Error('Class must be flagged with the @:rtti metadata');
        var rtti = haxe.rtti.Rtti.getRtti(type);

        var init:SchemaInit = {
            fields: new Array()
        };
        parseRtti(rtti, init);

        return new SqlSchema(init);
    }

    static function parseRtti(info:Classdef, init:SchemaInit) {
        for (field in info.fields) {
            if (!field.meta.search(e -> e.name == 'exclude')) {
                var column:SchemaFieldInit = {
                    name: field.name,
                    type: TUnknown,
                    notNull: false,
                    unique: false,
                    primaryKey: false,
                    autoIncrement: false
                };

                var exclude:Bool = false;
                var typeHandled = false;

                switch field.type {
                    case CFunction(_, _):
                        continue;

                    case CTypedef('Null', [t]):
                        column.type = STypes.fromCType(t);
                        typeHandled = true;

                    default: 
                }

                for (entry in field.meta) {
                    switch entry.name {
                        case 'notNull', 'sql.notNull':
                            column.notNull = true;

                        case 'primary', 'sql.primary', 'id', 'sql.id':
                            column.primaryKey = true;

                        case 'unique', 'sql.unique':
                            column.unique = true;

                        case 'default', 'defaultTo', 'sql.default', 'sql.defaultTo':
                            column.defaultValue = entry.params[0];

                        default:
                    }
                }

                if (!typeHandled)
                    column.type = STypes.fromCType(field.type);

                if (!exclude) 
                    (cast init.fields : Array<SchemaFieldInit>).push(column);
            }
        }
    }

    public static function fromHaxeMacroFieldArray(fields: Array<Field>):SqlSchema<Dynamic> {
        var init:SchemaInit = {
            fields: new Array()
        };
        addHaxeMacroFieldArray(init, fields);
        return new SqlSchema(init);
    }

    static function addHaxeMacroFieldArray(schema:SchemaInit, fields:Array<Field>) {
        for (f in fields)
            addHaxeMacroField(schema, f);
    }

	private static function addHaxeMacroField(schema:SchemaInit, f:haxe.macro.Expr.Field) {
		var column:SchemaFieldInit = {
			name: f.name,
			type: TUnknown,
			notNull: false,
			unique: false,
			primaryKey: false,
			autoIncrement: false
		};
        // var flags:Array<FieldFlag> = new Array();
		// var type:ValType = DataType.TAny;
        var idx = false;
        var exclude = if (f.meta == null) false else f.meta.search(e -> ['exclude','ignore'].has(e.name));
        if (exclude) return ;
        //
        // f.eatAss;

		if (f.meta != null) {
			for (m in f.meta) {
				switch m.name.toLowerCase() {
					case ':notnull', 'notnull':
                        column.notNull = true;

					case ':primary', 'primary':
                        // flags.push(FieldFlag.Primary);
                        column.primaryKey = true;

					case ':unique', 'unique':
                        // flags.push(FieldFlag.Unique);
                        column.unique = true;

					case ':autoincrement', 'autoincrement':
                        // flags.push(FieldFlag.AutoIncrement);
                        column.autoIncrement = true;

                    case ':defaultto' | 'defaultto' | 'default' | ':default':
						column.defaultValue = switch m.params[0] {
                            case {expr:EConst(CString(codeForValue))}: codeForValue;
                            case other:
                                throw other;
                        }

					case ':index', 'index':
						idx = true;
				}
			}
		}

		switch f.kind {
			case FieldType.FVar(null, _):
				column.type = SType.TUnknown;

			case FieldType.FVar(t, _):
                
				// switch t {
				// 	// case null: false;
				// 	case TParent(t): false;
				// 	case TOptional(t): false;
				// 	case TNamed(n, t): false;
				// 	case TExtend(p, fields): false;
				// 	case TPath(p): false;

				// 	case TFunction(args, ret): false;
				// 	case TAnonymous(fields): false;
				// 	case TIntersection(types):
				// 		throw new pm.Error.NotImplementedError('intersection (A & B) types not implemented yet');
				// }
				column.type = STypes.ofComplexType(t);

			default:
				throw new pm.Error('Wut ${f.kind}');
		}

		//
        
		if (idx) {
            // schema.indexes.push({name: f.name});
            Console.error('Indexing needs implementing!');
        }

        (cast schema.fields : Array<SchemaFieldInit>).push(column);
    }
    
    public static function fromComplexType(ctype: ComplexType):SqlSchema<Dynamic> {
		return switch ctype {
			case ComplexType.TAnonymous(fields): SqlSchema.fromHaxeMacroFieldArray(fields);
			case ComplexType.TParent(ctype), ComplexType.TNamed(_, ctype): fromComplexType(ctype);
            case ComplexType.TOptional(ctype): fromComplexType(ctype);
            case ComplexType.TIntersection(ctypes):
                throw new pm.Error('TODO');
            case other:
                throw new pm.Error('Unhandled type ${haxe.macro.ComplexTypeTools.toString(other)}');
        }
    }
}

class SchemaFields {
    public final fields: ReadOnlyArray<SchemaField>;
    public final mapping: Dictionary<SchemaField>;
    public var primary(default, null): Null<SchemaPrimaryField> = null;

    public function new(schema:SqlSchema<Dynamic>, a:Array<SchemaField>) {
        this.fields = a;
        this.mapping = new Dictionary();

        var primary_key:Null<String> = null;
        
        for (field in this.fields) {
            mapping[field.name] = field;

            if (field.primaryKey) {
                primary_key = field.name;
            }
        }

        if (primary_key != null) {
            this.primary = new SchemaPrimaryField(mapping[primary_key]);
        }
    }

    public function iterator() {
        return this.fields.iterator();
    }
}

class SchemaPrimaryField {
    public final field: SchemaField;
    
    public inline function new(f) {
        this.field = f;
    }
}

typedef SchemaInit = {
    ?mode: Or<SchemaMode, String>,
    ?incrementers: Map<String, Int>,
    ?context: Context<Dynamic, Dynamic, Dynamic>,
    fields: Or<Array<SchemaFieldInit>, SchemaInitFieldMap>,
    ?indexes: Array<SchemaIndexInit>
};
typedef SchemaInitFieldMap = Dynamic<SchemaFieldInit>;

typedef SchemaFieldInit = {
    ?name: String,
    ?type: SType,
    ?notNull: Bool,
    ?unique: Bool,
    ?autoIncrement: Bool,
    ?primaryKey: Bool,
    ?defaultValue: Or<String, TExpr>
};

typedef SchemaIndexInit = {
    ?path: String,
    ?type: SType,
    ?sparse: Bool,
    ?unique: Bool,
    ?overrideAlgorithm: SchemaIndexAlgorithm
};

enum SchemaIndexAlgorithm {
    AlgMap;
    AlgAvl;
}

typedef SchemaFieldState = {
    final name: String;
    final type: SType;
    final defaultValue: Null<Either<String, TExpr>>;
    final primaryKey: Bool;
    final unique: Bool;
    final notNull: Bool;
    final autoIncrement: Bool;
};

// @:structInit
class SchemaField {
    public final name: String;
    public final type: SType;
    public var defaultValueFn:Null<JITFn<TypedValue>> = null;
    public var defaultValueExpr: Null<TExpr> = null;
	public var defaultValueString: Null<String> = null;
    public final notNull: Bool;
    public final unique: Bool;
    public final autoIncrement: Bool;
    public final primaryKey: Bool;

    public function new(name, type, notNull=false, unique=false, autoIncrement=false, primaryKey=false, defaultValue:Either<String, TExpr>=null) {
        this.name = name;
        this.type = type;
        this.notNull = notNull;
        this.unique = unique;
        this.autoIncrement = autoIncrement;
        this.primaryKey = primaryKey;
        switch defaultValue {
            case null:
            case Left(v):
                defaultValueString = v;
            case Right(v):
                defaultValueExpr = v;
        }
    }

    public static inline function make(state: SchemaFieldState):SchemaField {
        return new SchemaField(state.name, state.type, state.notNull, state.unique, state.autoIncrement, state.primaryKey, state.defaultValue);
    }

    public function getInit():SchemaFieldInit {
        var ass:SchemaFieldInit = {
            name: name,
            type: type,
            notNull: notNull,
            unique: unique,
            autoIncrement: autoIncrement,
            primaryKey: primaryKey,
            defaultValue: nor((untyped defaultValueExpr), cast defaultValueString)
        };
        return ass;
    }
    
    public var access_type(get, never):DType;
    private function get_access_type():DType {
        if (!notNull) {
            return this.type.toDType();
        }
        else {
            return TNull(this.type.toDType());
        }
    }

    var _ctor_field: ConstructorField;
    public var ctor_field(get, never): ConstructorField;
    private function get_ctor_field() {
        if (this._ctor_field == null) {
            this._ctor_field = new ConstructorField(this);
        }
        return _ctor_field;
    }
}

class ConstructorField {
    public final f: SchemaField;
    public final name: String;
    public var type(default, null): DType;

    public var omittable(default, null):Bool;
    public var omissionMandatory(default, null):Bool;
    // public final primaryKey: Bool;
    // public final unique: Bool;
    // public final notNull: Bool;
    // public final autoIncrement: Bool;

    public function new(owner, ?name, ?type) {
        this.f = owner;
        this.name = name == null ? f.name : name;
        this.type = type != null ? type : f.type.toDType();

        this.omissionMandatory = switch owner {
            case {notNull:true, autoIncrement:true, unique:true}: true;
            default: false;
        }

        this.omittable = omissionMandatory || (owner.autoIncrement || !owner.notNull);
        if (!this.omissionMandatory && this.omittable) {
            this.type = DType.TNull(this.type);
        }
    }

    public function eval(g:{context:Context<Dynamic,Dynamic,Dynamic>}, input:Doc, output:Doc) {
        throw new pm.Error.NotImplementedError('ConstructorField.eval');
    }
}

class Constructor {
    public final fields: Array<ConstructorField>;

    public function new() {
        fields = [];
    }
}

class SchemaIndex {
    public var pathName: String;
    // public final path: ql.sql.common.DotPath;
    public var keyType: SType;
    public var sparse: Bool;
    public var unique: Bool;

    public function new(init: SchemaIndexInit) {
        this.pathName = init.path;
        this.keyType = init.type.nor(TUnknown);
        this.sparse = init.sparse.nor(false);
        this.unique = init.unique.nor(false);
        // this.path = this.pathName;
    }
}

enum SchemaMode {
    RowMode;
    ObjectMode;
}