export const OP = {
  eq: '=',
  neq: '!=',
  lte: '<=',
  lt: '<',
  gte: '>=',
  gt: '>',
  like: 'LIKE',
  iLike: 'ILIKE',
  in: 'IN',
  between: 'BETWEEN',
  isNull: 'IS',
  notNull: 'IS NOT',
  notLike: 'NOT LIKE',
  notILike: 'NOT ILIKE',
  notIn: 'NOT IN',
  notBetween: 'NOT BETWEEN',
  startsWith: 'LIKE',
  endsWith: 'LIKE',
  substring: 'LIKE',
  iStartsWith: 'ILIKE',
  iEndsWith: 'ILIKE',
  iSubstring: 'ILIKE',
  $exists: 'EXISTS',
  $notExists: 'NOT EXISTS',
  $and: 'AND',
  $or: 'OR',
} as const;

export const conditionalOperator = new Set(['$or', '$and'] as const);
export const subqueryOperator = new Set(['$exists', '$notExists'] as const);

export const validOperations = Object.keys(OP).join(', ');

export type OP_KEYS = keyof typeof OP;

export type SIMPLE_OP_KEYS = Exclude<
  OP_KEYS,
  '$and' | '$or' | '$exists' | '$notExists'
>;
