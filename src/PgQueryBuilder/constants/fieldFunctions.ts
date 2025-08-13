export const aggregateFunctionName = {
  min: 'min',
  max: 'max',
  count: 'count',
  avg: 'avg',
  sum: 'sum',
} as const;

export const COL_PREFIX = 'col#';

export const SIMPLE_MATH_FIELD_OP = {
  add: '+',
  sub: '-',
  multiple: '*',
  divide: '/',
  modulo: '%',
  exponent: '^',
} as const;

export const ADVANCE_SINGLE_FIELD_OP = {
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

export const ADVANCE_DOUBLE_FIELD_OP = {
  trunc: 'TRUNC',
  round: 'ROUND',
  power: 'POWER',
} as const;

export const ADVANCE_STR_DOUBLE_FIELD_OP = {
  strPos: 'STRPOS',
  position: 'POSITION',
} as const;

export type SimpleMathOpKeys = keyof typeof SIMPLE_MATH_FIELD_OP;
export type AdvanceSingleFieldOpKeys = keyof typeof ADVANCE_SINGLE_FIELD_OP;
export type AdvanceDoubleFieldOpKeys = keyof typeof ADVANCE_DOUBLE_FIELD_OP;
export type AdvanceStrDoubleFieldOpKeys =
  keyof typeof ADVANCE_STR_DOUBLE_FIELD_OP;
export type FieldFunctionType = keyof typeof aggregateFunctionName;
