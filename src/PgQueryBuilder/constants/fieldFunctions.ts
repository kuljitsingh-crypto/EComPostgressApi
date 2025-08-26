export const aggregateFunctionName = {
  min: 'min',
  max: 'max',
  count: 'count',
  avg: 'avg',
  sum: 'sum',
  boolOr: 'bool_or',
  boolAnd: 'bool_and',
  arrayAgg: 'array_agg',
  stringAgg: 'string_agg',
} as const;

export const COL_PREFIX = 'col#';

export const MATH_FIELD_OP = {
  add: '+',
  sub: '-',
  multiple: '*',
  divide: '/',
  modulo: '%',
  exponent: '^',
} as const;

export const SINGLE_FIELD_OP = {
  abs: 'ABS',
  ceil: 'CEIL',
  floor: 'FLOOR',
  sqrt: 'SQRT',
  exp: 'EXP',
  ln: 'LN',
  log: 'LOG',
  sign: 'SIGN',
  degrees: 'DEGREES',
  radians: 'RADIANS',
  sin: 'SIN',
  cos: 'COS',
  tan: 'TAN',
  upper: 'UPPER',
  lower: 'LOWER',
  initcap: 'INITCAP',
  length: 'LENGTH',
  charLength: 'CHAR_LENGTH',
  bitLength: 'BIT_LENGTH',
  octetLength: 'OCTET_LENGTH',
} as const;

export const DOUBLE_FIELD_OP = {
  trunc: 'TRUNC',
  round: 'ROUND',
  power: 'POWER',
} as const;

export const STR_FIELD_OP = {
  strPos: 'STRPOS',
} as const;

export const STR_IN_FIELD_OP = { position: 'POSITION' } as const;

export type SimpleMathOpKeys = keyof typeof MATH_FIELD_OP;
export type SingleFieldOpKeys = keyof typeof SINGLE_FIELD_OP;
export type DoubleFieldOpKeys = keyof typeof DOUBLE_FIELD_OP;
export type StrFieldOpKeys =
  | keyof typeof STR_FIELD_OP
  | keyof typeof STR_IN_FIELD_OP;
export type AggregateFunctionType = keyof typeof aggregateFunctionName;
