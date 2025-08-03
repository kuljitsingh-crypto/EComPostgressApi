import {
  fieldFunctionName,
  FieldFunctionType,
} from '../constants/fieldFunctions';
import { OP } from '../constants/operators';
import { Primitive } from '../globalTypes';
import { throwError } from './errorHelper';

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

const fieldFunc = (fn: FieldFunctionType, column: string) => {
  const func = fieldFunctionName[fn];
  if (!func) {
    return throwError.invalidAggFuncType(fn, Object.keys(fieldFunctionName));
  }
  return fnJoiner.joinFnAndColumn(func, column);
};

//=================== export functions ======================//

export const quote = (str: string) => `${String(str).replace(/"/g, '""')}`;

export const fieldFunctionCreator = (
  field: string,
  functionName: FieldFunctionType,
  alias?: string,
) => {
  const func = fieldFunctionName[functionName];
  if (!func) {
    return throwError.invalidAggFuncType(func, Object.keys(fieldFunctionName));
  }
  const aliasMaybe = alias ? ` ${alias}` : '';
  return `${func}(${field})${aliasMaybe}`;
};

export const validateField = (field: string, allowed: Set<string>) => {
  if (!allowed.has(field)) {
    throw new Error(
      `Invalid column name ${field}. Allowed Column names are: ${attachArrayWithComaAndSpaceSep(
        Array.from(allowed),
      )}.`,
    );
  }
};

export const FieldQuote = (allowedFields: Set<string>, str: string) => {
  validateField(str, allowedFields);
  return quote(str);
};

export const prepareColumnForHavingClause = (
  key: string,
  groupByFields: Set<string>,
  allowedFields: Set<string>,
  isHavingFilter: boolean,
) => {
  let validKey: string;
  if (isHavingFilter) {
    const [k, fn] = fnJoiner.sepFnAndColumn(key);
    if (!fn && !groupByFields.has(k)) {
      throw new Error(
        `Invalid column "${k}" for HAVING clause. Column should be part of GROUP BY or an aggregate function.`,
      );
    }
    validKey = FieldQuote(allowedFields, k);
    if (fn) {
      validKey = fieldFunctionCreator(validKey, fn as FieldFunctionType);
    }
  } else {
    validKey = FieldQuote(allowedFields, key);
  }
  return validKey;
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
  [fieldFunctionName.COUNT]: (column: string) => fieldFunc('COUNT', column),
  [fieldFunctionName.AVG]: (column: string) => fieldFunc('AVG', column),
  [fieldFunctionName.MAX]: (column: string) => fieldFunc('MAX', column),
  [fieldFunctionName.MIN]: (column: string) => fieldFunc('MIN', column),
  [fieldFunctionName.SUM]: (column: string) => fieldFunc('SUM', column),
});
