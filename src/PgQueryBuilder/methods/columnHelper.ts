import { DB_KEYWORDS } from '../constants/dbkeywords';
import { FieldFunctionType } from '../constants/fieldFunctions';
import { FindQueryAttribute, FindQueryAttributes } from '../internalTypes';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  dynamicFieldQuote,
  aggregateFunctionCreator,
  FieldQuote,
  fnJoiner,
} from './helperFunction';

const getColNameAndAlias = (
  col: FindQueryAttribute,
  allowedFields: Set<string>,
) => {
  if (typeof col === 'string') {
    return { col, value: null };
  } else if (typeof col === 'object' && col !== null) {
    const [column, value] = col;
    return { col: column, value };
  }
  return throwError.invalidColumnNameType(col, allowedFields);
};

export class ColumnHelper {
  static getSelectColumns(
    allowedFields: Set<string>,
    columns?: FindQueryAttributes,
    isAggregateAllowed = true,
  ) {
    if (!columns || !Array.isArray(columns) || columns.length < 1) return '*';
    columns = columns.filter(Boolean);
    if (columns.length < 1) return '*';
    const fields = columns
      .map((attr) => {
        const { col, value } = getColNameAndAlias(attr, allowedFields);
        const [column, fn] = fnJoiner.sepFnAndColumn(col);
        let validCol = FieldQuote(allowedFields, column);
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
