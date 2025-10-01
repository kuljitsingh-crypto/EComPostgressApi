import { DB_KEYWORDS } from '../constants/dbkeywords';
import { Primitive } from '../globalTypes';
import { PreparedValues, RawQuery } from '../internalTypes';
import { throwError } from './errorHelper';
import { attachArrayWith, getPreparedValues } from './helperFunction';
import { isNonEmptyObject, isNonEmptyString, isValidArray } from './util';

const checkAndAddQuery = (
  queries: string[],
  attrName?: string[],
  prefix?: string,
  replaceWithIndex?: number,
) => {
  if (!isValidArray(attrName)) {
    return;
  }
  attrName = attrName?.filter(Boolean) as string[];
  if (attrName.length < 1) {
    return;
  }
  const attrStr = attachArrayWith.coma(attrName);
  const data: string[] = [];
  if (prefix) {
    data.push(prefix);
  }
  data.push(attrStr);
  if (typeof replaceWithIndex == 'number') {
    queries[replaceWithIndex] = attachArrayWith.space(data);
  } else {
    queries.push(attachArrayWith.space(data));
  }
};

export class RawQueryHandler {
  static buildRawQuery(query: RawQuery, params: Primitive[] = []) {
    if (isNonEmptyString(query)) {
      return {
        query,
        values: params,
      };
    }
    if (isNonEmptyObject(query)) {
      const queries: string[] = [DB_KEYWORDS.select];
      const {
        table: tableName = '',
        columns,
        distinct,
        limit,
        offset,
        ...rest
      } = query;
      const preparedValues: PreparedValues = {
        index: params.length,
        values: params,
      };

      if (distinct) {
        queries.push(DB_KEYWORDS.distinct);
      }
      queries.push('*');
      const lastIndx = queries.length - 1;
      checkAndAddQuery(queries, columns, undefined, lastIndx);
      queries.push(DB_KEYWORDS.from, tableName);
      Object.entries(rest).forEach((entry: [string, string[]]) => {
        const [key, val] = entry;
        checkAndAddQuery(queries, val, (DB_KEYWORDS as any)[key]);
      });
      if (limit) {
        const placeholder = getPreparedValues(preparedValues, limit);
        queries.push(attachArrayWith.space([DB_KEYWORDS.limit, placeholder]));
      }
      if (offset) {
        const placeholder = getPreparedValues(preparedValues, offset);
        queries.push(attachArrayWith.space([DB_KEYWORDS.offset, placeholder]));
      }
      return {
        query: attachArrayWith.space(queries),
        values: preparedValues.values,
      };
    }
    return throwError.invalidRawQueryType();
  }
}
