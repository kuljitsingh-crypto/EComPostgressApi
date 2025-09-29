import { PgDataType } from '../constants/dataTypes';
import { WHERE_KEYWORD } from '../constants/dbkeywords';
import { OP } from '../constants/operators';
import { setOperation } from '../constants/setOperations';
import { TABLE_JOIN, TableJoinType } from '../constants/tableJoin';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  CallableField,
  CallableFieldParam,
  CaseSubquery,
  DerivedModel,
  FieldMetadata,
  GroupByFields,
  InOperationSubQuery,
  JoinQuery,
  NonNullPrimitive,
  Nullable,
  PreparedValues,
  SetQueryArrField,
  Subquery,
  SubqueryMultiColFlag,
  WhereClause,
} from '../internalTypes';
import { isValidInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import { symbolFuncRegister } from './symbolHelper';

type FieldQuoteReturn<T extends boolean> = T extends false
  ? string
  : string | null;

type ValidOption = Exclude<
  CallableFieldParam[keyof CallableFieldParam],
  undefined
>;

const MIN_COLUMN_LENGTH = 1;
const MAX_COLUMN_LENGTH = 63;
const validColumnNameRegex = /^([a-zA-Z_][a-zA-Z0-9_$]*)(\.[a-zA-Z0-9_$]*)*$/;
const digitRegex = /^([0-9]+)$/;

const allowedWhereKeyWOrds = new Set([
  '$and',
  '$or',
  '$exists',
  '$notExists',
  '$matches',
]);

const callableFieldValidator: Record<
  keyof CallableFieldParam,
  (val: unknown) => boolean
> = {
  preparedValues: isValidPreparedValues,
  allowedFields: isValidAllowedFields,
  groupByFields: isValidGroupByFieldsFields,
  isAggregateAllowed: isValidAggregateValue,
  customAllowedFields: isValidCustomALlowedFields,
};

const filterOutValidDbData =
  (shouldTrimStr = true) =>
  (a: Primitive) => {
    const trimmedStrLength = shouldTrimStr ? 0 : -1;
    if (
      a === null ||
      typeof a === 'boolean' ||
      typeof a === 'number' ||
      isValidArray(a)
    ) {
      return true;
    } else if (typeof a == 'string' && a.trim().length > trimmedStrLength) {
      return true;
    }
    return false;
  };

const attachArrayWithSep = (
  array: Array<Primitive>,
  sep: string,
  shouldTrimStr?: boolean,
) => array.filter(filterOutValidDbData(shouldTrimStr)).join(sep);

const attachArrayWithDotSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, '.', shouldTrimStr);

const attachArrayWithSpaceSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, ' ', shouldTrimStr);

const attachArrayWithNoSpaceSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, '', shouldTrimStr);

const attachArrayWithComaSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, ',', shouldTrimStr);

const attachArrayWithAndSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, ` ${OP.$and} `, shouldTrimStr);

const attachArrayWithComaAndSpaceSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, ', ', shouldTrimStr);

function isValidAllowedFields(
  allowedFields: unknown,
): allowedFields is AllowedFields {
  return isValidSetObj<string>(allowedFields);
}

function isValidGroupByFieldsFields(
  groupByFields: unknown,
): groupByFields is GroupByFields {
  return isValidSetObj<string>(groupByFields);
}

function isValidPreparedValues(
  preparedValues: unknown,
): preparedValues is PreparedValues {
  return (
    isValidObject(preparedValues) &&
    preparedValues.hasOwnProperty('index') &&
    typeof (preparedValues as any).index === 'number' &&
    preparedValues.hasOwnProperty('values') &&
    isValidArray((preparedValues as any).values, -1)
  );
}

function isValidAggregateValue(value: unknown): value is boolean {
  return typeof value === 'boolean';
}
function isValidCustomALlowedFields(value: unknown): boolean {
  return isValidArray(value);
}

const isFieldAllowed =
  (allowed: AllowedFields, customAllowFields: string[]) => (field: string) =>
    allowed.has(field) || customAllowFields.includes(field);

const checkForJsonField =
  (
    allowed: AllowedFields,
    preparedValues: PreparedValues | null,
    customAllowFields: string[],
    asJson: boolean,
  ) =>
  (field: string) => {
    if (isNullableValue(preparedValues)) {
      return null;
    }
    const fieldArr = field.split('.');
    const simpleField = fieldArr[0];
    const aliasField = `${fieldArr[0]}.${fieldArr[1]}`;
    if (isFieldAllowed(allowed, customAllowFields)(simpleField)) {
      return prepareFieldForJson(fieldArr, preparedValues, 1, asJson);
    } else if (isFieldAllowed(allowed, customAllowFields)(aliasField)) {
      return prepareFieldForJson(fieldArr, preparedValues, 2, asJson);
    }
    return null;
  };

const validateField = (
  field: string,
  allowed: AllowedFields,
  preparedValues: PreparedValues | null,
  options?: {
    customAllowFields: string[];
    metadata?: FieldMetadata;
    asJson?: boolean;
  },
) => {
  const {
    customAllowFields = [],
    metadata = {},
    asJson = false,
  } = options || {};
  field = simpleFieldValidate(field, customAllowFields);
  const isAllowedField = isFieldAllowed(allowed, customAllowFields)(field);
  if (isAllowedField) {
    return field;
  }
  const jsonField = checkForJsonField(
    allowed,
    preparedValues,
    customAllowFields,
    asJson,
  )(field);
  if (isNonEmptyString(jsonField)) {
    metadata.isJSONField = true;
    return jsonField;
  }
  return throwError.invalidColumnNameType(field, allowed);
};

const callableCol = (col: CallableField, options: CallableFieldParam) => {
  const validOptions = Object.entries(options || {}).reduce(
    (pre, acc) => {
      const [key, val] = acc;
      if (typeof val !== 'undefined') {
        pre[key] = val;
      }
      return pre;
    },
    {} as Record<string, ValidOption>,
  );
  return col(validOptions);
};

const createSymbolMethodRef = (method: CallableField, ...keys: string[]) => {
  const symbolName = attachArrayWith.dot(keys);
  const symbol = Symbol(symbolName);
  symbolFuncRegister.add(symbol, method);
  return symbol;
};

//=================== export functions ======================//

export function prepareFieldForJson(
  fieldArr: string[],
  preparedValues: PreparedValues,
  startIndex: number,
  asJson: boolean,
) {
  const fieldName = attachArrayWith.dot(fieldArr.slice(0, startIndex)); //
  const placeholders = fieldArr
    .slice(startIndex)
    .map((val) =>
      getPreparedValues(preparedValues, val, { returnNumAsItIs: true }),
    );
  const lastIndex = placeholders.length - 1;
  const lastFieldKey = asJson ? '->' : '->>';
  if (placeholders.length < 2) {
    return attachArrayWith.noSpace([
      fieldName,
      lastFieldKey,
      placeholders[lastIndex],
    ]);
  }
  const middleFields = attachArrayWith.customSep(
    placeholders.slice(0, lastIndex),
    '->',
  );
  return attachArrayWith.noSpace([
    fieldName,
    '->',
    middleFields,
    lastFieldKey,
    placeholders[lastIndex],
  ]);
}

export const createPlaceholder = (index: number, type?: string) => {
  return type ? `$${index}${type}` : `$${index}`;
};
const prepareVal = (val: Primitive | Primitive[]) =>
  isValidArray(val)
    ? val
    : digitRegex.test((val as any) || '')
      ? Number(val)
      : val;

export const getPreparedValues = <T extends boolean = false>(
  preparedValues: PreparedValues,
  value: Primitive | Primitive[],
  options?: { type?: string; returnNumAsItIs?: T },
): T extends true ? string | number : string => {
  const { type, returnNumAsItIs = false } = options || {};
  const val = prepareVal(value);
  if (isValidNumber(val) && returnNumAsItIs) {
    return val as any;
  }
  const placeholder = createPlaceholder(preparedValues.index + 1, type);
  preparedValues.values[preparedValues.index] = val;
  preparedValues.index++;
  return placeholder;
};

export const simpleFieldValidate = (
  field: string | null,
  customAllowFields: string[],
) => {
  if (!isNonEmptyString(field)) {
    return throwError.invalidColType();
  }
  field = field.trim();
  if (field.length < MIN_COLUMN_LENGTH || field.length > MAX_COLUMN_LENGTH) {
    return throwError.invalidColNameLenType(field, {
      min: MIN_COLUMN_LENGTH,
      max: MAX_COLUMN_LENGTH,
    });
  }
  if (customAllowFields.includes(field)) {
    return field;
  }
  const isValidRegexField = validColumnNameRegex.test(field);
  if (!isValidRegexField) {
    return throwError.invalidColumnNameRegexType(field);
  }
  return field;
};

export const quote = (str: string) => `${String(str).replace(/"/g, '""')}`;

export const dynamicFieldQuote = (
  field: string,
  customAllowFields: string[] = [],
) => {
  field = simpleFieldValidate(field, customAllowFields);
  return quote(field);
};

export const fieldQuote = <T extends boolean = false>(
  allowedFields: AllowedFields,
  preparedValues: PreparedValues | null,
  str: string | null,
  options?: {
    isNullColAllowed?: T;
    customAllowFields?: string[];
    metadata?: FieldMetadata;
    asJson?: boolean;
  },
): FieldQuoteReturn<T> => {
  const {
    isNullColAllowed = false,
    customAllowFields = [],
    metadata,
    asJson,
  } = options || {};
  if (str === null && isNullColAllowed) {
    return str as any;
  }
  if (!isNonEmptyString(str)) {
    return throwError.invalidColumnNameType(str, allowedFields);
  }
  str = validateField(str, allowedFields, preparedValues, {
    customAllowFields,
    metadata,
    asJson,
  });
  return quote(str);
};

export function isValidArray<T>(arr: unknown, len?: number): arr is Array<T> {
  len = len ?? 0;
  return isNonNullableValue(arr) && Array.isArray(arr) && arr.length > len;
}

export function isEmptyArray<T>(arr: unknown): arr is Array<T> {
  return isValidArray(arr, -1) && arr.length === 0;
}
export function isValidFunction(func: unknown): func is Function {
  return typeof func === 'function' && func.constructor === Function;
}

export function isNonEmptyString(str: unknown): str is string {
  return typeof str === 'string' && str.trim().length > 0;
}

export function isValidObject(obj: unknown): obj is object {
  return typeof obj === 'object' && obj !== null && obj.constructor === Object;
}

export function isValidSetObj<T>(obj: unknown): obj is Set<T> {
  return typeof obj === 'object' && obj !== null && obj.constructor === Set;
}

export const isNonNullableValue = <T>(v: T): v is NonNullable<T> =>
  v !== null && v !== undefined;

export const isNullableValue = (v: unknown): v is Nullable => v == null;
export const isPrimitiveValue = (value: unknown): value is Primitive => {
  return (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    value === null
  );
};

export const isNotNullPrimitiveValue = (
  value: unknown,
): value is NonNullPrimitive => {
  return (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  );
};

export const isValidNumber = (value: unknown): value is number =>
  typeof value === 'number';

export const isValidBoolean = (value: unknown): value is boolean =>
  typeof value === 'boolean';

export const isValidSymbol = (value: unknown): value is Symbol =>
  typeof value === 'symbol' && value.constructor === Symbol;

export const isValidSimpleModel = <T>(model: any): model is T => {
  if (!isValidFunction(model)) {
    return false;
  }
  if (!isNonEmptyString(model.tableName)) {
    return false;
  }
  if (!isValidSetObj<string>(model.tableColumns)) {
    return false;
  }
  return true;
};

export const isValidColumn = (
  column: unknown,
  arrayAllowedUptoLvl = 0,
  lvl = 0,
): boolean => {
  const isColumn = isNonEmptyString(column) || isValidFunction(column);
  const isArrayAllowed = lvl <= arrayAllowedUptoLvl;
  if (isArrayAllowed && isValidArray(column)) {
    return lvl === arrayAllowedUptoLvl
      ? isValidColumn(column[0], arrayAllowedUptoLvl, lvl + 1)
      : column.every((col) => isValidColumn(col, arrayAllowedUptoLvl, lvl + 1));
  }

  return isColumn;
};

export const isValidSubQuery = <Model, W extends SubqueryMultiColFlag>(
  subQuery: unknown,
): subQuery is InOperationSubQuery<Model, 'WhereNotReq', W> => {
  if (!isValidObject(subQuery)) {
    return false;
  }
  const { model, column, columns } = subQuery as any;
  const arrayAllowedUptoLvl = column ? 0 : columns ? 1 : -1;

  if (!isValidDerivedModel(model)) {
    return false;
  }
  if (!isValidColumn(column || columns, arrayAllowedUptoLvl)) {
    return false;
  }
  return true;
};

export const isValidCaseQuery = <Model>(
  query: unknown,
  options: { treatSimpleObjAsWhereSubQry: boolean },
): query is CaseSubquery<Model> => {
  const q = query as any;
  if (typeof q !== 'object' || q === null) return false;
  const { treatSimpleObjAsWhereSubQry } = options || {};
  const isValidResultQry = (val: unknown) =>
    isPrimitiveValue(val) ||
    isCallableColumn(val) ||
    isValidSubQuery(val) ||
    isValidWhereQuery(null, val, { treatSimpleObjAsWhereSubQry });
  const isValidElse = isValidResultQry(q?.else);
  const isValidCond = isValidResultQry(q?.then) && isValidObject(q?.when);
  if (isValidElse || isValidCond) return true;
  return false;
};

const isValidWhereSubQuery = (
  value: object,
  treatSimpleObjAsWhereSubQry: boolean,
) => {
  const isValidObjValue = (val: unknown) =>
    isNonEmptyObject(val) || isCallableColumn(val);
  for (let key in value) {
    if (!value.hasOwnProperty(key)) continue;
    if (allowedWhereKeyWOrds.has(key)) {
      return true;
    }
    const val = (value as any)[key];
    if (isValidObjValue(val)) {
      return true;
    }
  }
  for (let sym of Object.getOwnPropertySymbols(value)) {
    if (symbolFuncRegister.has(sym)) {
      return true;
    }
  }
  return treatSimpleObjAsWhereSubQry;
};

export const isValidWhereQuery = <Model>(
  key: string | null,
  value: unknown,
  options: { treatSimpleObjAsWhereSubQry: boolean },
): value is WhereClause<Model> => {
  if (!isValidObject(value)) return false;
  const { treatSimpleObjAsWhereSubQry = true } = options || {};
  const hasWhereKey =
    (value as any)[WHERE_KEYWORD] !== undefined || key === WHERE_KEYWORD;
  if (hasWhereKey) {
    return true;
  }
  return isValidWhereSubQuery(value, treatSimpleObjAsWhereSubQry);
};

export function isValidDerivedModel<Model>(
  derivedModel: unknown,
): derivedModel is DerivedModel<Model> {
  if (isValidSimpleModel(derivedModel)) {
    return true;
  }
  if (isNonEmptyObject(derivedModel)) {
    return isValidDerivedModel((derivedModel as any).model);
  }
  return false;
}

export const isEmptyObject = (obj: unknown) =>
  isValidObject(obj) && Object.keys(obj).length === 0;

export const isNonEmptyObject = (obj: unknown): obj is object =>
  isValidObject(obj) && Object.keys(obj).length > 0;

export const isCallableColumn = (col: unknown): col is CallableField => {
  return typeof col === 'function' && col.length === 1;
};

export const getJoinSubqueryFields = <Model>(subQuery: Subquery<Model>) => {
  return Object.entries(subQuery || {}).reduce(
    (pre, acc) => {
      const [key, value] = acc;
      if (key in TABLE_JOIN) {
        (pre as any)[key] = value;
      }
      return pre;
    },
    {} as Record<TableJoinType, JoinQuery<TableJoinType, Model>>,
  );
};

export const getSetSubqueryFields = <Model>(
  subQuery: Subquery<Model>,
): SetQueryArrField<Model>[] => {
  return Object.entries(subQuery || {}).reduce((pre, acc) => {
    const [key, value] = acc;
    if (key in setOperation) {
      pre.push(createNewObj(value as any, { type: key }));
    }
    return pre;
  }, [] as SetQueryArrField<Model>[]);
};

export const getValidCallableFieldValues = <T extends keyof CallableFieldParam>(
  options: CallableFieldParam,
  ...requiredValues: T[]
) => {
  options = options || {};
  const validOptions = {} as {
    [k in T]: Exclude<CallableFieldParam[k], undefined>;
  };
  requiredValues.forEach((key) => {
    const isRequiredValid =
      options.hasOwnProperty(key) && callableFieldValidator[key](options[key]);
    if (isRequiredValid) {
      (validOptions as any)[key] = options[key];
    } else {
      throwError.invalidFieldFuncCallType();
    }
  });
  return validOptions;
};

export const validCallableColCtx = (
  col: CallableField,
  options: CallableFieldParam,
) => {
  const { ctx, ...rest } = callableCol(col, options);
  if (!isValidInternalContext(ctx)) {
    return throwError.invalidFieldFuncCallType();
  }
  return rest;
};

export const ensureArray = <T>(val: T | T[]): T[] => {
  return isValidArray(val, -1) ? [...val] : [val];
};

export const isColAliasNameArr = (
  col: unknown,
): col is [string | CallableField, string | null] => {
  if (!isValidArray(col)) return false;
  if (col.filter(Boolean).length !== 2) return false;
  return true;
};

export const prepareMultipleValues = <T extends string>(
  preparedValues: PreparedValues,
  arg: {
    [key in T]: {
      type: 'string' | 'number' | 'primitive' | 'boolean';
      val: unknown;
    };
  },
): Record<T, string> => {
  const finalObject = {} as Record<T, string>;
  return Object.entries(arg).reduce((acc, [key, value]) => {
    let isValidValue = false;
    const { type, val } = value as {
      type: 'string' | 'number' | 'primitive' | 'boolean';
      val: Primitive;
    };
    (acc as any)[key] = '';
    if (type === 'string') {
      isValidValue = isNonEmptyString(val);
    } else if (type === 'number') {
      isValidValue = isValidNumber(val);
    } else if (type === 'boolean') {
      isValidValue = isValidBoolean(val);
    } else if (type === 'primitive') {
      isValidValue = isPrimitiveValue(val);
    }
    if (isValidValue) {
      (acc as any)[key] = getPreparedValues(preparedValues, val);
    }
    return acc;
  }, finalObject);
};

export const getAllEntries = (obj: unknown): Array<[string | symbol, any]> => {
  if (!isValidObject(obj)) {
    return [];
  }
  const symbolKeys = Object.getOwnPropertySymbols(obj);
  const keyEntries = Object.entries(obj) as [string | symbol, any];
  for (let symbol of symbolKeys) {
    keyEntries.push([symbol, (obj as any)[symbol]]);
  }
  return keyEntries;
};

export const validateColumn =
  (
    col: string | symbol,
    colOptions?: {
      shouldSkipFieldValidation?: boolean;
      isNullColAllowed?: false | undefined;
      customAllowFields?: string[];
      isAggregateAllowed?: boolean | undefined;
    },
  ) =>
  (options: {
    preparedValues: PreparedValues;
    groupByFields: GroupByFields;
    allowedFields: AllowedFields;
  }) => {
    const { shouldSkipFieldValidation, isAggregateAllowed, ...rest } =
      colOptions || {};
    const { allowedFields, preparedValues } = options;
    if (isValidSymbol(col)) {
      const registry = symbolFuncRegister.get(col);
      if (!isValidFunction(registry)) {
        return throwError.invalidColumnNameRegexType(col.toString());
      }
      const { col: val } = registry({
        ...options,
        isAggregateAllowed,
        customAllowedFields: rest.customAllowFields,
      });
      symbolFuncRegister.delete(col);
      return val;
    }
    if (isNonEmptyString(col)) {
      if (shouldSkipFieldValidation) {
        return col;
      }
      return fieldQuote(allowedFields, preparedValues, col, rest);
    }
    return throwError.invalidColumnNameRegexType(
      ((col as any) || 'null').toString(),
    );
  };

export const attachMethodToSymbolRegistry = (
  method: any,
  ...keys: string[]
) => {
  method.toString = () => {
    const symbol = createSymbolMethodRef(
      method,
      ...keys,
      Date.now().toString(),
    );
    return symbol;
  };
};

export const isFloatVal = (val: unknown): val is number => {
  return isValidNumber(val) && !Number.isInteger(val);
};

export const isIntegerVal = (val: unknown): val is number => {
  return isValidNumber(val) && Number.isInteger(val);
};

export const covertJSDataToSQLData = (data: unknown): string => {
  if (data === null) {
    return PgDataType.null;
  } else if (typeof data === 'boolean') {
    return PgDataType.boolean;
  } else if (typeof data === 'string') {
    return PgDataType.text;
  } else if (typeof data === 'bigint') {
    return PgDataType.bigInt;
  } else if (isFloatVal(data)) {
    return PgDataType.double;
  } else if (isIntegerVal(data)) {
    return PgDataType.int;
  } else if (isValidArray(data)) {
    return `${covertJSDataToSQLData(data[0])}[]`;
  }
  return throwError.invalidDataType(data);
};

export const prepareSQLDataType = (data: Primitive) =>
  `::${covertJSDataToSQLData(data)}`;
//===================================== Object wrapped functions =======================//

export const attachArrayWith = {
  space: attachArrayWithSpaceSep,
  coma: attachArrayWithComaSep,
  and: attachArrayWithAndSep,
  dot: attachArrayWithDotSep,
  noSpace: attachArrayWithNoSpaceSep,
  comaAndSpace: attachArrayWithComaAndSpaceSep,
  customSep: attachArrayWithSep,
};

//==================================== Field helper depend on object ============================//
export const covertStrArrayToStr = (
  value: string | string[],
  options?: { by: keyof typeof attachArrayWith; sep?: string },
): string => {
  const { by = 'coma', sep } = options || {};
  return isValidArray<string>(value)
    ? by === 'customSep'
      ? attachArrayWith.customSep(value, sep ?? ',')
      : attachArrayWith[by](value)
    : value;
};

export const createNewObj = (...objs: object[]) => {
  return Object.assign({}, ...objs);
};

export const repeatValInArrUpto = <T extends Primitive>(
  ch: T,
  upto: number,
) => {
  const arr: T[] = [];
  for (let i = 0; i < upto; i++) {
    arr.push(ch);
  }
  return arr;
};
