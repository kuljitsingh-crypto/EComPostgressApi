import {
  aggregateFunctionName,
  AggregateFunctionType,
} from '../constants/fieldFunctions';
import { OP } from '../constants/operators';
import { TABLE_JOIN, TableJoinType } from '../constants/tableJoin';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  CallableField,
  CallableFieldParam,
  GroupByFields,
  InOperationSubQuery,
  JoinQuery,
  NonNullPrimitive,
  PreparedValues,
  Subquery,
  SubqueryMultiColFlag,
} from '../internalTypes';
import { isValidInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';

type FieldQuoteReturn<T extends boolean> = T extends false
  ? string
  : string | null;

const MIN_COLUMN_LENGTH = 1;
const MAX_COLUMN_LENGTH = 63;
const validColumnNameRegex = /^[a-zA-Z_][a-zA-Z0-9_$]*$/;
const validAliasColumnNameRegex =
  /^([a-zA-Z_][a-zA-Z0-9_$]*\.[a-zA-Z_][a-zA-Z0-9_$]*)$/;

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
  joinFnAndColumn: (fn: AggregateFunctionType, column: string) =>
    `${column},${fn}`,
  sepFnAndColumn: (fnAndCol: string | null) =>
    fnAndCol ? fnAndCol.split(',') : [fnAndCol],
};

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

type ValidOption = Exclude<
  CallableFieldParam[keyof CallableFieldParam],
  undefined
>;
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

export const simpleFieldValidate = (
  field: string | null,
  customAllowFields: string[],
) => {
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
  if (typeof str !== 'string') {
    return throwError.invalidColumnNameType(str, allowedFields);
  }
  str = validateField(str, allowedFields, { customAllowFields });
  return quote(str);
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

export const createPlaceholder = (index: number) => {
  return `$${index}`;
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

export const isEmptyObject = (obj: unknown) =>
  typeof obj === 'object' &&
  obj !== null &&
  !Array.isArray(obj) &&
  Object.keys(obj).length < 1;

export const isNonEmptyObject = (obj: unknown): obj is object =>
  !isEmptyObject(obj);

export const isCallableColumn = (col: unknown): col is CallableField => {
  return typeof col === 'function' && col.length === 1;
};

export const isValidAllowedFields = (
  allowedFields: unknown,
): allowedFields is AllowedFields => {
  return (
    typeof allowedFields === 'object' &&
    allowedFields !== null &&
    allowedFields.constructor === Set
  );
};

export const isValidGroupByFieldsFields = (
  groupByFields: unknown,
): groupByFields is GroupByFields => {
  return (
    typeof groupByFields === 'object' &&
    groupByFields !== null &&
    groupByFields.constructor === Set
  );
};

export const isValidPreparedValues = (
  preparedValues: unknown,
): preparedValues is PreparedValues => {
  return (
    typeof preparedValues === 'object' &&
    preparedValues !== null &&
    preparedValues.hasOwnProperty('index') &&
    typeof (preparedValues as any).index === 'number' &&
    preparedValues.hasOwnProperty('values') &&
    Array.isArray((preparedValues as any).values)
  );
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
