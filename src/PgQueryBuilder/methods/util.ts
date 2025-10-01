import { WHERE_KEYWORD } from '../constants/dbkeywords';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  CallableField,
  CaseSubquery,
  DerivedModel,
  GroupByFields,
  InOperationSubQuery,
  NonNullPrimitive,
  Nullable,
  PreparedValues,
  SubqueryMultiColFlag,
  WhereClause,
} from '../internalTypes';
import { symbolFuncRegister } from './symbolHelper';

const allowedWhereKeyWOrds = new Set([
  '$and',
  '$or',
  '$exists',
  '$notExists',
  '$matches',
]);

export function isValidGroupByFieldsFields(
  groupByFields: unknown,
): groupByFields is GroupByFields {
  return isValidSetObj<string>(groupByFields);
}

export function isValidPreparedValues(
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

export function isValidAggregateValue(value: unknown): value is boolean {
  return typeof value === 'boolean';
}
export function isValidCustomALlowedFields(value: unknown): boolean {
  return isValidArray(value);
}

export function isValidAllowedFields(
  allowedFields: unknown,
): allowedFields is AllowedFields {
  return isValidSetObj<string>(allowedFields);
}

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

export const filterOutValidDbData =
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
