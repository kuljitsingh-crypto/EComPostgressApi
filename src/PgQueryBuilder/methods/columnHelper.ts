import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  AllowedFields,
  CallableField,
  FindQueryAttribute,
  FindQueryAttributes,
  GroupByFields,
  PreparedValues,
} from '../internalTypes';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  dynamicFieldQuote,
  fieldQuote,
  isCallableColumn,
  validCallableColCtx,
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
  options?: { customAllowFields: string[] },
): {
  col: string;
  alias: string | null;
} => {
  const { customAllowFields = [] } = options || {};
  if (typeof col === 'string') {
    return {
      col: fieldQuote(allowedFields, col, { customAllowFields }),
      alias: null,
    };
  } else if (isCallableColumn(col) && preparedValues && groupByFields) {
    const rest = validCallableColCtx(col, {
      allowedFields,
      isAggregateAllowed,
      preparedValues,
      groupByFields,
    });
    return rest;
  } else if (isValidArray(col)) {
    const [column, alias] = col;
    const { col: validColumn } = getColNameAndAlias(
      column,
      allowedFields,
      isAggregateAllowed,
      preparedValues,
      groupByFields,
      options,
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
      customAllowFields?: string[];
    },
  ) {
    if (!columns || !Array.isArray(columns) || columns.length < 1) return '*';
    columns = columns.filter(Boolean);
    if (columns.length < 1) return '*';
    const {
      groupByFields,
      preparedValues,
      isAggregateAllowed = true,
      customAllowFields = [],
    } = options || {};
    const fields = columns
      .map((attr) => {
        const { col, alias } = getColNameAndAlias(
          attr,
          allowedFields,
          isAggregateAllowed,
          preparedValues,
          groupByFields,
          { customAllowFields },
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
