import { DB_KEYWORDS } from '../constants/dbkeywords';
import { FieldFunctionType } from '../constants/fieldFunctions';
import {
  AllowedFields,
  FindQueryAttribute,
  FindQueryAttributes,
  GroupByFields,
  PreparedValues,
} from '../internalTypes';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  dynamicFieldQuote,
  aggregateFunctionCreator,
  fieldQuote,
  fnJoiner,
} from './helperFunction';

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
    return col(preparedValues, groupByFields, allowedFields);
  } else if (typeof col === 'object' && col !== null) {
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
        const [column, fn] = fnJoiner.sepFnAndColumn(col);
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
