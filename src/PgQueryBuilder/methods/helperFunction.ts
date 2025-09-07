import { OP } from '../constants/operators';
import { TABLE_JOIN, TableJoinType } from '../constants/tableJoin';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  CallableField,
  CallableFieldParam,
  CaseSubquery,
  GroupByFields,
  InOperationSubQuery,
  JoinQuery,
  NonNullPrimitive,
  PreparedValues,
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

function isValidAllowedFields(
  allowedFields: unknown,
): allowedFields is AllowedFields {
  return (
    typeof allowedFields === 'object' &&
    allowedFields !== null &&
    allowedFields.constructor === Set
  );
}

function isValidGroupByFieldsFields(
  groupByFields: unknown,
): groupByFields is GroupByFields {
  return (
    typeof groupByFields === 'object' &&
    groupByFields !== null &&
    groupByFields.constructor === Set
  );
}

function isValidPreparedValues(
  preparedValues: unknown,
): preparedValues is PreparedValues {
  return (
    typeof preparedValues === 'object' &&
    preparedValues !== null &&
    preparedValues.hasOwnProperty('index') &&
    typeof (preparedValues as any).index === 'number' &&
    preparedValues.hasOwnProperty('values') &&
    Array.isArray((preparedValues as any).values)
  );
}

function isValidAggregateValue(value: unknown): value is boolean {
  return typeof value === 'boolean';
}
function isValidCustomALlowedFields(value: unknown): boolean {
  return Array.isArray(value) && value.length > 0;
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

export const isNonNullableValue = <T>(v: T): v is NonNullable<T> =>
  v !== null && v !== undefined;

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
  subQuery: unknown,
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
  const isValidCond =
    isValidResultQry(q?.then) &&
    typeof q?.when === 'object' &&
    q?.when !== null &&
    !Array.isArray(q?.when);
  if (isValidElse || isValidCond) return true;
  return false;
};

export const isValidWhereQuery = <Model>(
  value: unknown,
): value is WhereClause<Model> => {
  if (
    typeof value === 'object' &&
    value !== null &&
    value.constructor === Object
  ) {
    return true;
  }
  return false;
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

//===================================== Object wrapped functions =======================//

export const attachArrayWith = {
  space: attachArrayWithSpaceSep,
  coma: attachArrayWithComaSep,
  and: attachArrayWithAndSep,
  comaAndSpace: attachArrayWithComaAndSpaceSep,
  customSep: attachArrayWithSep,
};
