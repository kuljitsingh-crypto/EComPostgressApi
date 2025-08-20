import {
  aggregateFunctionName,
  FieldFunctionType,
} from '../constants/fieldFunctions';
import { OP } from '../constants/operators';
import { TABLE_JOIN, TableJoinType } from '../constants/tableJoin';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  GroupByFields,
  InOperationSubQuery,
  Join,
  JoinQuery,
  NonNullPrimitive,
  PreparedValues,
  Subquery,
  SubqueryMultiColFlag,
  SubqueryWhereReq,
  WhereAndOtherSubQuery,
} from '../internalTypes';
import { throwError } from './errorHelper';

type FieldQuoteReturn<T extends boolean> = T extends false
  ? string
  : string | null;

const MIN_COLUMN_LENGTH = 1;
const MAX_COLUMN_LENGTH = 63;
const validColumnNameRegex = /^[a-zA-Z_][a-zA-Z0-9_$]*$/;
const validAliasColumnNameRegex =
  /^([a-zA-Z_][a-zA-Z0-9_$]*\.[a-zA-Z_][a-zA-Z0-9_$]*)$/;
const validExistsColumnNameRegex = /^[1]$/;

const filterOutValidDbData = (a: Primitive) => {
  if (a === null || typeof a === 'boolean' || typeof a === 'number') {
    return true;
  } else if (typeof a == 'string' && a.trim().length > 0) {
    return true;
  }
  return false;
};

const attachArrayWithSep = (array: Array<Primitive>, sep: string) =>
  array.filter(filterOutValidDbData).join(sep);

const attachArrayWithSpaceSep = (array: Array<Primitive>) =>
  attachArrayWithSep(array, ' ');

const attachArrayWithComaSep = (array: Array<Primitive>) =>
  attachArrayWithSep(array, ',');

const attachArrayWithAndSep = (array: Array<Primitive>) =>
  attachArrayWithSep(array, ` ${OP.$and} `);

const attachArrayWithComaAndSpaceSep = (array: Array<Primitive>) =>
  attachArrayWithSep(array, ', ');

const fnJoiner = {
  joinFnAndColumn: (fn: FieldFunctionType, column: string) => `${column},${fn}`,
  sepFnAndColumn: (fnAndCol: string | null) =>
    fnAndCol ? fnAndCol.split(',') : [fnAndCol],
};

const aggregateFunc = (fn: FieldFunctionType, column: string) => {
  const func = aggregateFunctionName[fn];
  if (!func) {
    return throwError.invalidAggFuncType(
      fn,
      Object.keys(aggregateFunctionName),
    );
  }
  return fnJoiner.joinFnAndColumn(func, column);
};
const simpleFieldValidate = (field: string | null) => {
  if (typeof field !== 'string') {
    return throwError.invalidColType();
  }
  field = field.trim();
  if (field.length < MIN_COLUMN_LENGTH || field.length > MAX_COLUMN_LENGTH) {
    return throwError.invalidColNameLenType(field, {
      min: MIN_COLUMN_LENGTH,
      max: MAX_COLUMN_LENGTH,
    });
  }
  const isValidRegexField =
    validColumnNameRegex.test(field) ||
    validAliasColumnNameRegex.test(field) ||
    validExistsColumnNameRegex.test(field);
  if (!isValidRegexField) {
    return throwError.invalidColumnNameRegexType(field);
  }
  return field;
};

const validateField = (field: string, allowed: AllowedFields) => {
  field = simpleFieldValidate(field);
  if (!allowed.has(field)) {
    return throwError.invalidColumnNameType(field, allowed);
  }
  return field as string;
};

const aggregateFunctionCreator = (
  field: string,
  functionName: FieldFunctionType,
  alias?: string,
) => {
  const func = aggregateFunctionName[functionName];
  if (!func) {
    return throwError.invalidAggFuncType(
      func,
      Object.keys(aggregateFunctionName),
    );
  }
  const aliasMaybe = alias ? ` ${alias}` : '';
  const funcUpr = func.toUpperCase();
  return `${funcUpr}(${field})${aliasMaybe}`;
};

//=================== export functions ======================//

export const getAggregatedColumn = <T extends boolean = false>({
  column: col,
  allowedFields,
  isNullColAllowed,
  shouldSkipFieldValidation = false,
  isAggregateAllowed = true,
}: {
  column: string | null;
  allowedFields: AllowedFields;
  shouldSkipFieldValidation?: boolean;
  isAggregateAllowed?: boolean;
  isNullColAllowed?: T;
}): FieldQuoteReturn<T> => {
  const [column, fn] = shouldSkipFieldValidation
    ? [col]
    : fnJoiner.sepFnAndColumn(col);
  let validCol = shouldSkipFieldValidation
    ? (column as FieldQuoteReturn<T>)
    : fieldQuote(allowedFields, column, isNullColAllowed);
  if (!isAggregateAllowed && fn) {
    return throwError.invalidAggFuncPlaceType(fn, column || 'null');
  }
  if (fn && validCol) {
    validCol = aggregateFunctionCreator(validCol, fn as FieldFunctionType);
  }
  return validCol;
};

export const quote = (str: string) => `${String(str).replace(/"/g, '""')}`;

export const dynamicFieldQuote = (field: string) => {
  field = simpleFieldValidate(field);
  return quote(field);
};

export const fieldQuote = <T extends boolean = false>(
  allowedFields: AllowedFields,
  str: string | null,
  isNullColAllowed?: T,
): FieldQuoteReturn<T> => {
  if (str === null && isNullColAllowed) {
    return str as any;
  }
  if (typeof str !== 'string') {
    return throwError.invalidColumnNameType(str, allowedFields);
  }
  str = validateField(str, allowedFields);
  return quote(str);
};

export const prepareColumnForHavingClause = (
  key: string,
  groupByFields: GroupByFields,
  allowedFields: AllowedFields,
  isHavingFilter: boolean,
) => {
  let validKey: string;
  if (isHavingFilter) {
    const [k, fn] = fnJoiner.sepFnAndColumn(key);
    if (!fn && !k) {
      return throwError.invalidGrpColumnNameType(k || 'null');
    } else if (k && !groupByFields.has(k)) {
      return throwError.invalidGrpColumnNameType(k);
    }
    validKey = fieldQuote(allowedFields, k);
    if (fn) {
      validKey = aggregateFunctionCreator(validKey, fn as FieldFunctionType);
    }
  } else {
    validKey = fieldQuote(allowedFields, key);
  }
  return validKey;
};

export const isPrimitiveValue = (value: Primitive | undefined) => {
  return (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    typeof value === 'undefined' ||
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

export const createPlaceholder = (val: number) => {
  return `$${val}`;
};

export const getPreparedValues = (
  preparedValues: PreparedValues,
  value: Primitive,
) => {
  const placeholder = createPlaceholder(preparedValues.index + 1);
  preparedValues.values[preparedValues.index] = value;
  preparedValues.index++;
  return placeholder;
};

export const isValidModel = (model: any) => {
  if (typeof model !== 'function') {
    return false;
  }
  if (typeof model.tableName !== 'string') {
    return false;
  }
  if (!(model.tableColumns instanceof Set)) {
    return false;
  }
  return true;
};

export const isValidColumn = (
  column: any,
  arrayAllowedUptoLvl = 0,
  lvl = 0,
): boolean => {
  const isColumn =
    (typeof column === 'string' || typeof column === 'function') && !!column;
  const isArrayAllowed = lvl <= arrayAllowedUptoLvl;
  if (isArrayAllowed && Array.isArray(column)) {
    return lvl === arrayAllowedUptoLvl
      ? isValidColumn(column[0], arrayAllowedUptoLvl, lvl + 1)
      : column.every((col) => isValidColumn(col, arrayAllowedUptoLvl, lvl + 1));
  }

  return isColumn;
};

export const isValidSubQuery = <Model, W extends SubqueryMultiColFlag>(
  subQuery: InOperationSubQuery<Model, 'WhereNotReq', W> | null,
  isArrayAllowedInColumn = false,
): subQuery is InOperationSubQuery<Model, 'WhereNotReq', W> => {
  if (typeof subQuery !== 'object' || subQuery === null) {
    return false;
  }
  const { model, column, columns } = subQuery as any;
  const arrayAllowedUptoLvl = column ? 0 : columns ? 1 : -1;
  if (!isValidModel(model)) {
    return false;
  }
  if (!isValidColumn(column || columns, arrayAllowedUptoLvl)) {
    return false;
  }
  return true;
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

//===================================== Object wrapped functions =======================//

export const attachArrayWith = {
  space: attachArrayWithSpaceSep,
  coma: attachArrayWithComaSep,
  and: attachArrayWithAndSep,
  comaAndSpace: attachArrayWithComaAndSpaceSep,
  customSep: attachArrayWithSep,
};

export const aggregateFn = Object.freeze({
  [aggregateFunctionName.count]: (column: string) =>
    aggregateFunc('count', column),
  [aggregateFunctionName.avg]: (column: string) => aggregateFunc('avg', column),
  [aggregateFunctionName.max]: (column: string) => aggregateFunc('max', column),
  [aggregateFunctionName.min]: (column: string) => aggregateFunc('min', column),
  [aggregateFunctionName.sum]: (column: string) => aggregateFunc('sum', column),
});

export const isEmptyObject = (obj: unknown) =>
  typeof obj === 'object' &&
  obj !== null &&
  !Array.isArray(obj) &&
  Object.keys(obj).length > 0;

export const isNonEmptyObject = (obj: unknown): obj is object =>
  !isEmptyObject(obj);
