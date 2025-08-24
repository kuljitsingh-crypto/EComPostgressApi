import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  AllowedFields,
  CallableField,
  FindQueryAttribute,
  FindQueryAttributes,
  FourCallableField,
  GroupByFields,
  PreparedValues,
} from '../internalTypes';
import { isValidInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  callableCol,
  dynamicFieldQuote,
  fieldQuote,
  getAggregatedColumn,
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
  isAggregateAllowed: boolean,
  preparedValues?: PreparedValues,
  groupByFields?: GroupByFields,
): {
  col: string;
  alias: string | null;
} => {
  if (typeof col === 'string') {
    return { col: fieldQuote(allowedFields, col), alias: null };
  } else if (typeof col === 'function' && preparedValues && groupByFields) {
    const { ctx, ...rest } = callableCol(
      col,
      allowedFields,
      isAggregateAllowed,
      preparedValues,
      groupByFields,
    );
    if (!isValidInternalContext(ctx)) {
      return throwError.invalidFieldFuncCallType();
    }
    return rest;
  } else if (isValidArray(col)) {
    const [column, alias] = col;
    const { col: validColumn } = getColNameAndAlias(
      column,
      allowedFields,
      isAggregateAllowed,
      preparedValues,
      groupByFields,
    );
    return { col: validColumn, alias };
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
        const { col, alias } = getColNameAndAlias(
          attr,
          allowedFields,
          isAggregateAllowed,
          preparedValues,
          groupByFields,
        );

        if (alias === null) {
          return col;
        } else if (typeof alias === 'string') {
          const validValue = dynamicFieldQuote(alias);
          allowedFields.add(validValue);
          return attachArrayWith.space([col, DB_KEYWORDS.as, validValue]);
        }
        return null;
      })
      .filter(Boolean);
    return attachArrayWith.comaAndSpace(fields);
  }
}
