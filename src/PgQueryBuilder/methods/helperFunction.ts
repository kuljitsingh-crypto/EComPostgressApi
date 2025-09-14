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

type FieldQuoteReturn<T extends boolean> = T extends false
  ? string
  : string | null;

type ValidOption = Exclude<
  CallableFieldParam[keyof CallableFieldParam],
  undefined
>;

const MIN_COLUMN_LENGTH = 1;
const MAX_COLUMN_LENGTH = 63;
const validColumnNameRegex = /^[a-zA-Z_][a-zA-Z0-9_$]*$/;
const validAliasColumnNameRegex =
  /^([a-zA-Z_][a-zA-Z0-9_$]*\.[a-zA-Z_][a-zA-Z0-9_$]*)$/;

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
    if (a === null || typeof a === 'boolean' || typeof a === 'number') {
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

const attachArrayWithSpaceSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, ' ', shouldTrimStr);

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

const validateField = (
  field: string,
  allowed: AllowedFields,
  options?: { customAllowFields: string[] },
) => {
  const { customAllowFields = [] } = options || {};
  field = simpleFieldValidate(field, customAllowFields);
  const isAllowedField =
    allowed.has(field) || customAllowFields.includes(field);
  if (!isAllowedField) {
    return throwError.invalidColumnNameType(field, allowed);
  }
  return field as string;
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

//=================== export functions ======================//

export const createPlaceholder = (index: number, type?: string) => {
  return type ? `$${index}${type}` : `$${index}`;
};

export const getPreparedValues = (
  preparedValues: PreparedValues,
  value: Primitive,
  options?: { type: string },
) => {
  const { type } = options || {};
  const placeholder = createPlaceholder(preparedValues.index + 1, type);
  preparedValues.values[preparedValues.index] = value;
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
  const isValidRegexField =
    validColumnNameRegex.test(field) || validAliasColumnNameRegex.test(field);
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
  str: string | null,
  options?: { isNullColAllowed?: T; customAllowFields?: string[] },
): FieldQuoteReturn<T> => {
  const { isNullColAllowed = false, customAllowFields = [] } = options || {};
  if (str === null && isNullColAllowed) {
    return str as any;
  }
  if (!isNonEmptyString(str)) {
    return throwError.invalidColumnNameType(str, allowedFields);
  }
  str = validateField(str, allowedFields, { customAllowFields });
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
): query is CaseSubquery<Model> => {
  const q = query as any;
  if (typeof q !== 'object' || q === null) return false;
  const isValidResultQry = (val: unknown) =>
    isPrimitiveValue(val) ||
    isCallableColumn(val) ||
    isValidSubQuery(val) ||
    isValidWhereQuery(val);
  const isValidElse = isValidResultQry(q?.else);
  const isValidCond = isValidResultQry(q?.then) && isValidObject(q?.when);
  if (isValidElse || isValidCond) return true;
  return false;
};

export const isValidWhereQuery = <Model>(
  value: unknown,
): value is WhereClause<Model> => {
  if (isNonEmptyObject(value)) {
    return true;
  }
  return false;
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
//===================================== Object wrapped functions =======================//

export const attachArrayWith = {
  space: attachArrayWithSpaceSep,
  coma: attachArrayWithComaSep,
  and: attachArrayWithAndSep,
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
