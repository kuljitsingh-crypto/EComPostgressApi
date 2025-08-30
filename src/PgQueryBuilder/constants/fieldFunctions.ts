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
  stdDev: 'stddev',
  variance: 'variance',
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
  lTrim: 'LTRIM',
  rTrim: 'RTRIM',
  reverse: 'REVERSE',
  ascii: 'ASCII',
  chr: 'CHR',
  toHex: 'TO_HEX',
  md5: 'MD5',
} as const;

export const DOUBLE_FIELD_OP = {
  trunc: 'TRUNC',
  round: 'ROUND',
  power: 'POWER',
  repeat: 'REPEAT',
  left: 'LEFT',
  right: 'RIGHT',
} as const;

export const TRIPLE_FIELD_OP = {
  subStr: 'SUBSTR',
  replace: 'REPLACE',
  translate: 'TRANSLATE',
  lPad: 'LPAD',
  rPad: 'RPAD',
  splitPart: 'SPLIT_PART',
} as const;

export const MULTIPLE_FIELD_OP = {
  concat: 'CONCAT',
} as const;

export const STR_FIELD_OP = {
  strPos: 'STRPOS',
} as const;

export const SUBSTRING_FIELD_OP = {
  substring: 'SUBSTRING',
} as const;

export const TRIM_FIELD_OP = {
  trim: 'TRIM',
} as const;

export const STR_IN_FIELD_OP = { position: 'POSITION' } as const;
export const DATE_EXTRACT_FIELD_OP = {
  extractYear: 'EXTRACT',
  extractMonth: 'EXTRACT',
  extractDay: 'EXTRACT',
  extractHour: 'EXTRACT',
  extractMinute: 'EXTRACT',
  extractSecond: 'EXTRACT',
  extractDow: 'EXTRACT',
  extractDoy: 'EXTRACT',
  extractWeek: 'EXTRACT',
  extractQuarter: 'EXTRACT',
  extractEpoch: 'EXTRACT',
} as const;

export const dateExtractFieldMapping = {
  extractYear: 'YEAR',
  extractMonth: 'MONTH',
  extractDay: 'DAY',
  extractHour: 'HOUR',
  extractMinute: 'MINUTE',
  extractSecond: 'SECOND',
  extractDow: 'DOW', // Day of week
  extractDoy: 'DOY', // Day of year
  extractWeek: 'WEEK',
  extractQuarter: 'QUARTER',
  extractEpoch: 'EPOCH',
};

type SimpleMathOpKeys = keyof typeof MATH_FIELD_OP;
type SingleOpKeys = keyof typeof SINGLE_FIELD_OP;
type TrimFieldOpKeys = keyof typeof TRIM_FIELD_OP;
type DoubleOpKeys = keyof typeof DOUBLE_FIELD_OP;
type TripleOpKeys = keyof typeof TRIPLE_FIELD_OP;
type SubstringFieldOpKeys = keyof typeof SUBSTRING_FIELD_OP;
type StrFieldOpKeys = keyof typeof STR_FIELD_OP | keyof typeof STR_IN_FIELD_OP;
type MultipleOpKeys = keyof typeof MULTIPLE_FIELD_OP;
type DateExtractOpKeys = keyof typeof DATE_EXTRACT_FIELD_OP;

export type SingleFieldOpKeys = SingleOpKeys | DateExtractOpKeys;
export type DoubleFieldOpKeys =
  | DoubleOpKeys
  | TrimFieldOpKeys
  | StrFieldOpKeys
  | SimpleMathOpKeys;
export type TripleFieldOpKeys = TripleOpKeys | SubstringFieldOpKeys;
export type MultipleFieldOpKeys = MultipleOpKeys;
export type AggregateFunctionType = keyof typeof aggregateFunctionName;
