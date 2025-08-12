import {
  aggregateFunctionName,
  FieldFunctionType,
} from '../constants/fieldFunctions';
import { OP } from '../constants/operators';
import { Primitive } from '../globalTypes';
import { AllowedFields, GroupByFields, PreparedValues } from '../internalTypes';
import { throwError } from './errorHelper';

const MIN_COLUMN_LENGTH = 1;
const MAX_COLUMN_LENGTH = 63;
const validColumnNameRegex = /^[a-zA-Z_][a-zA-Z0-9_$]*$/;
const validAliasColumnNameRegex =
  /^([a-zA-Z_][a-zA-Z0-9_$]*\.[a-zA-Z_][a-zA-Z0-9_$]*)$/;
const validExistsColumnNameRegex = /^[1]$/;

const attachArrayWithSep = (array: Array<Primitive>, sep: string) =>
  array.join(sep);

const attachArrayWithSpaceSep = (array: Array<Primitive>) =>
  attachArrayWithSep(array, ' ');

const attachArrayWithComaSep = (array: Array<Primitive>) =>
  attachArrayWithSep(array, ',');

const attachArrayWithAndSep = (array: Array<Primitive>) =>
  attachArrayWithSep(array, ` ${OP.$and} `);

const attachArrayWithComaAndSpaceSep = (array: Array<Primitive>) =>
  attachArrayWithSep(array, ', ');

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

const validateField = (field: string | null, allowed: AllowedFields) => {
  field = simpleFieldValidate(field);
  if (!allowed.has(field)) {
    return throwError.invalidColumnNameType(field, allowed);
  }
  return field as string;
};

//=================== export functions ======================//

export const aggregateFunctionCreator = (
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

export const quote = (str: string) => `${String(str).replace(/"/g, '""')}`;

export const dynamicFieldQuote = (field: string) => {
  field = simpleFieldValidate(field);
  return quote(field);
};

export const fieldQuote = (
  allowedFields: AllowedFields,
  str: string | null,
) => {
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
    if (!fn && !groupByFields.has(k)) {
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

//===================================== Object wrapped functions =======================//

export const attachArrayWith = {
  space: attachArrayWithSpaceSep,
  coma: attachArrayWithComaSep,
  and: attachArrayWithAndSep,
  comaAndSpace: attachArrayWithComaAndSpaceSep,
  customSep: attachArrayWithSep,
};

export const fnJoiner = {
  joinFnAndColumn: (fn: FieldFunctionType, column: string) => `${column},${fn}`,
  sepFnAndColumn: (fnAndCol: string) => fnAndCol.split(','),
};

export const aggregateFn = Object.freeze({
  [aggregateFunctionName.count]: (column: string) =>
    aggregateFunc('count', column),
  [aggregateFunctionName.avg]: (column: string) => aggregateFunc('avg', column),
  [aggregateFunctionName.max]: (column: string) => aggregateFunc('max', column),
  [aggregateFunctionName.min]: (column: string) => aggregateFunc('min', column),
  [aggregateFunctionName.sum]: (column: string) => aggregateFunc('sum', column),
});
