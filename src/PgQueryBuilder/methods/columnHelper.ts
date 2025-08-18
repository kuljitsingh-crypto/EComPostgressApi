import { DB_KEYWORDS } from '../constants/dbkeywords';
import { FieldFunctionType } from '../constants/fieldFunctions';
import {
  AllowedFields,
  CallableField,
  FindQueryAttribute,
  FindQueryAttributes,
  GroupByFields,
  PreparedValues,
} from '../internalTypes';
import { isValidInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  dynamicFieldQuote,
  aggregateFunctionCreator,
  fieldQuote,
  fnJoiner,
} from './helperFunction';

const isValidArray = (
  col: FindQueryAttribute,
): col is [string | CallableField, string | null] => {
  if (col === null) return false;
  if (!Array.isArray(col)) return false;
  if (col.filter(Boolean).length < 1) return false;
  return true;
};

const getColNameAndAlias = (
  col: FindQueryAttribute,
  allowedFields: AllowedFields,
  preparedValues?: PreparedValues,
  groupByFields?: GroupByFields,
): {
  col: string;
  value: string | null;
  shouldSkipFieldValidation?: boolean;
} => {
  if (typeof col === 'string') {
    return { col, value: null };
  } else if (typeof col === 'function' && preparedValues && groupByFields) {
    const { ctx, ...rest } = col(preparedValues, groupByFields, allowedFields);
    if (!isValidInternalContext(ctx)) {
      return throwError.invalidFieldFuncCallType();
    }
    return rest;
  } else if (isValidArray(col)) {
    const [column, value] = col;
    const { col: validColumn, shouldSkipFieldValidation } = getColNameAndAlias(
      column,
      allowedFields,
      preparedValues,
      groupByFields,
    );
    return { col: validColumn, value, shouldSkipFieldValidation };
  }
  return throwError.invalidColumnNameType(col.toString(), allowedFields);
};

export class ColumnHelper {
  static getSelectColumns(
    allowedFields: AllowedFields,
    columns?: FindQueryAttributes,
    options?: {
      preparedValues?: PreparedValues;
      groupByFields?: GroupByFields;
      isAggregateAllowed?: boolean;
    },
  ) {
    if (!columns || !Array.isArray(columns) || columns.length < 1) return '*';
    columns = columns.filter(Boolean);
    if (columns.length < 1) return '*';
    const {
      groupByFields,
      preparedValues,
      isAggregateAllowed = true,
    } = options || {};
    const fields = columns
      .map((attr) => {
        const { col, value, shouldSkipFieldValidation } = getColNameAndAlias(
          attr,
          allowedFields,
          preparedValues,
          groupByFields,
        );
        const [column, fn] = shouldSkipFieldValidation
          ? [col]
          : fnJoiner.sepFnAndColumn(col);
        let validCol = shouldSkipFieldValidation
          ? column
          : fieldQuote(allowedFields, column);
        if (!isAggregateAllowed && fn) {
          return throwError.invalidAggFuncPlaceType(fn, column);
        }
        if (fn) {
          validCol = aggregateFunctionCreator(
            validCol,
            fn as FieldFunctionType,
          );
        }
        if (value === null) {
          return validCol;
        } else if (typeof value === 'string') {
          const validValue = dynamicFieldQuote(value);
          allowedFields.add(validValue);
          return attachArrayWith.space([validCol, DB_KEYWORDS.as, validValue]);
        }
        return null;
      })
      .filter(Boolean);
    return attachArrayWith.comaAndSpace(fields);
  }
}
