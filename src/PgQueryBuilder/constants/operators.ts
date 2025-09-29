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
  isTrue: 'IS TRUE',
  notTrue: 'IS NOT TRUE',
  isFalse: 'IS FALSE',
  notFalse: 'IS NOT FALSE',
  isUnknown: 'IS UNKNOWN',
  notUnknown: 'IS NOT UNKNOWN',
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
  match: '~',
  iMatch: '~*',
  notMatch: '!~',
  iNotMatch: '!~*',
  ANY: 'ANY',
  ALL: 'ALL',
  arrayContains: '@>',
  arrayContainsBy: '<@',
  jsonbContains: '@>',
  jsonbContainsBy: '<@',
  arrayOverlap: '&&',
  $exists: 'EXISTS',
  $notExists: 'NOT EXISTS',
  $and: 'AND',
  $or: 'OR',
  $matches: '',
  jsonbHasKey: '?',
  jsonbHasAny: '?|',
  jsonbHasAll: '?&',
  jsonbMatch: '@@',
  jsonbExists: '@?',
} as const;

export const conditionalOperator = new Set(['$or', '$and'] as const);
export const subqueryOperator = new Set(['$exists', '$notExists'] as const);
export const matchQueryOperator = new Set(['$matches'] as const);
export const validOperations = Object.keys(OP).join(', ');

export type OP_KEYS = keyof typeof OP;

export type SIMPLE_OP_KEYS = Exclude<
  OP_KEYS,
  '$and' | '$or' | '$exists' | '$notExists' | '$matches'
>;

export type SUBQUERY_OP_KEYS = Extract<
  OP_KEYS,
  'eq' | 'neq' | 'lte' | 'lt' | 'gte' | 'gt'
>;

export type PRIMITIVE_OP_KEYS = Extract<
  OP_KEYS,
  | 'startsWith'
  | 'endsWith'
  | 'substring'
  | 'iStartsWith'
  | 'iEndsWith'
  | 'iSubstring'
>;

export type ARRAY_OP_KEYS = Extract<
  OP_KEYS,
  | 'like'
  | 'iLike'
  | 'notLike'
  | 'notILike'
  | 'match'
  | 'iMatch'
  | 'notMatch'
  | 'iNotMatch'
>;

export type ARRAY_OPERATION_KEYS = Extract<
  OP_KEYS,
  | 'jsonbContains'
  | 'jsonbContainsBy'
  | 'arrayOverlap'
  | 'arrayContains'
  | 'arrayContainsBy'
>;
