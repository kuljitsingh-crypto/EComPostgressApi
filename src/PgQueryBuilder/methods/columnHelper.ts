import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  AllowedFields,
  FindQueryAttribute,
  FindQueryAttributes,
  GroupByFields,
  PreparedValues,
} from '../internalTypes';
import { throwError } from './errorHelper';
import { getFieldValue } from './fieldFunc';
import { attachArrayWith, fieldQuote } from './helperFunction';
import { isColAliasNameArr, isNonEmptyString, isValidArray } from './util';

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
  let column: string | null = null,
    alias: string | null = isColAliasNameArr(col) ? col[1] : null;
  if (preparedValues && groupByFields) {
    column = getFieldValue(
      null,
      col,
      preparedValues,
      groupByFields,
      allowedFields,
      {
        treatStrAsCol: true,
        isFromCol: true,
        isAggregateAllowed,
        customAllowedFields: customAllowFields,
      },
    );
  } else if (isNonEmptyString(col)) {
    column = fieldQuote(allowedFields, null, col, {
      customAllowFields,
    });
  }
  if (column) {
    return { col: column, alias };
  }
  return throwError.invalidColumnNameType(
    (col || 'null').toString(),
    allowedFields,
  );
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
    if (!isValidArray(columns)) return '*';
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
        } else if (isNonEmptyString(alias)) {
          const validValue = alias; // dynamicFieldQuote(alias);
          allowedFields.add(validValue);
          return attachArrayWith.space([col, DB_KEYWORDS.as, validValue]);
        }
        return null;
      })
      .filter(Boolean);
    return attachArrayWith.comaAndSpace(fields);
  }
}
