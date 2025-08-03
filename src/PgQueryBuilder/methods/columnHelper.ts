import { DB_KEYWORDS } from '../constants/dbkeywords';
import { FieldFunctionType } from '../constants/fieldFunctions';
import { FindQueryAttributes } from '../internalTypes';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  dynamicFieldQuote,
  fieldFunctionCreator,
  FieldQuote,
  fnJoiner,
} from './helperFunction';

export class ColumnHelper {
  static getSelectColumns(
    allowedFields: Set<string>,
    columns?: FindQueryAttributes,
    isAggregateAllowed = true,
  ) {
    if (!columns || Object.keys(columns).length < 1) return '*';
    const fields = Object.entries(columns)
      .map((attr) => {
        const [col, value] = attr;
        const [column, fn] = fnJoiner.sepFnAndColumn(col);
        let validCol = FieldQuote(allowedFields, column);
        if (!isAggregateAllowed && fn) {
          return throwError.invalidAggFuncPlaceType(fn, column);
        }
        if (fn) {
          validCol = fieldFunctionCreator(validCol, fn as FieldFunctionType);
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
