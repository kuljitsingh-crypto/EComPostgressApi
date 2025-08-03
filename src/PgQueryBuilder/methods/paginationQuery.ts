import { DB_KEYWORDS } from '../constants/dbkeywords';
import { PAGINATION, PreparedValues } from '../internalTypes';
import { attachArrayWith, getPreparedValues } from './helperFunction';

export class PaginationQuery {
  static preparePaginationStatement(
    preparedValues: PreparedValues,
    limit?: PAGINATION['limit'],
    offset?: PAGINATION['offset'],
  ) {
    if (!limit || typeof limit !== 'number') {
      return '';
    }
    const limitPlaceholder = getPreparedValues(preparedValues, limit);
    const limitStatements = [`${DB_KEYWORDS.limit} ${limitPlaceholder}`];
    if (offset && typeof offset === 'number') {
      const offsetPlaceholder = getPreparedValues(preparedValues, offset);
      limitStatements.push(`${DB_KEYWORDS.offset} ${offsetPlaceholder}`);
    }
    return attachArrayWith.space(limitStatements);
  }
}
