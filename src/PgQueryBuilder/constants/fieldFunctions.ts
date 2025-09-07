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
  varPop: 'VAR_POP',
  varSamp: 'VAR_SAMP',
  stddevPop: 'STDDEV_POP',
  stddevSamp: 'STDDEV_SAMP',
  corr: 'CORR',
  covarPop: 'COVAR_POP',
  covarSamp: 'COVAR_SAMP',
  regrSlope: 'REGR_SLOPE',
  regrIntercept: 'REGR_INTERCEPT',
  regrCount: 'REGR_COUNT',
  regrR2: 'REGR_R2',
  regrAvgX: 'REGR_AVGX',
  regrAvgY: 'REGR_AVGY',
  regrSxx: 'REGR_SXX',
  regrSyy: 'REGR_SYY',
  regrSxy: 'REGR_SXY',
} as const;

export const doubleParamAggrFunctionNames = new Set([
  'corr',
  'covarPop',
  'covarSamp',
  'regrSlope',
  'regrIntercept',
  'regrCount',
  'regrR2',
  'regrAvgX',
  'regrAvgY',
  'regrSxx',
  'regrSyy',
  'regrSxy',
]);

//=========================================== Window Functions====================================//

// 1. No-arg window functions
export const noArgWindowFns = {
  rowNumber: 'ROW_NUMBER',
  rank: 'RANK',
  denseRank: 'DENSE_RANK',
  percentRank: 'PERCENT_RANK',
  cumeDist: 'CUME_DIST',
} as const;

// 2. Integer argument only
export const intArgWindowFns = {
  ntile: 'NTILE',
} as const;

// 3. Single expression argument
export const exprArgWindowFns = {
  firstValue: 'FIRST_VALUE',
  lastValue: 'LAST_VALUE',
} as const;

// 4. Expression + extra args
export const exprWithExtraWindowFns = {
  nthValue: 'NTH_VALUE', // expr, n
  lag: 'LAG', // expr [, offset [, default]]
  lead: 'LEAD', // expr [, offset [, default]]
} as const;

//======================================= No Param Field OP ======================================//
export const NO_PRAM_FIELD_OP = {
  now: 'NOW',
  clockTimestamp: 'CLOCK_TIMESTAMP',
  statementTimestamp: 'STATEMENT_TIMESTAMP',
  transactionTimestamp: 'TRANSACTION_TIMESTAMP',
};

export const CURRENT_DATE_FIELD_OP = {
  currentDate: 'CURRENT_DATE',
  currentTime: 'CURRENT_TIME',
};

//====================================== Single Field Op ======================================//
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
  typeOf: 'pg_typeof',
} as const;

//====================================== Double Field Op ======================================//
export const MATH_FIELD_OP = {
  add: '+',
  sub: '-',
  multiple: '*',
  divide: '/',
  modulo: '%',
  exponent: '^',
} as const;

export const DOUBLE_FIELD_OP = {
  trunc: 'TRUNC',
  round: 'ROUND',
  power: 'POWER',
  repeat: 'REPEAT',
  left: 'LEFT',
  right: 'RIGHT',
  dateTrunc: 'DATE_TRUNC',
  datePart: 'DATE_PART',
  toChar: 'TO_CHAR',
  toDate: 'TO_DATE',
  toTimestamp: 'TO_TIMESTAMP',
  toNumber: 'TO_NUMBER',
  nullIf: 'NULLIF',
  coalesce: 'COALESCE',
  encode: 'ENCODE',
  decode: 'DECODE',
} as const;

export const STR_FIELD_OP = {
  strPos: 'STRPOS',
} as const;

export const TRIM_FIELD_OP = {
  trim: 'TRIM',
} as const;

export const STR_IN_FIELD_OP = { position: 'POSITION' } as const;

//====================================== Triple Field Op ======================================//
export const TRIPLE_FIELD_OP = {
  subStr: 'SUBSTR',
  replace: 'REPLACE',
  translate: 'TRANSLATE',
  lPad: 'LPAD',
  rPad: 'RPAD',
  splitPart: 'SPLIT_PART',
} as const;

export const SUBSTRING_FIELD_OP = {
  substring: 'SUBSTRING',
} as const;

//====================================== Multiple Field Op ======================================//
export const MULTIPLE_FIELD_OP = {
  concat: 'CONCAT',
  age: 'AGE',
  greatest: 'GREATEST',
  least: 'LEAST',
} as const;

export const CASE_FIELD_OP = {
  case: 'CASE',
} as const;

//====================================== Helper Constants ======================================//
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

//====================================== Type Definitions ======================================//

//====================================== No Param Field OP ======================================//
type NoParamOpKeys = keyof typeof NO_PRAM_FIELD_OP;
type CurrentDateOpKeys = keyof typeof CURRENT_DATE_FIELD_OP;

//====================================== Single Field Op ======================================//
type SingleOpKeys = keyof typeof SINGLE_FIELD_OP;
type DateExtractOpKeys = keyof typeof DATE_EXTRACT_FIELD_OP;

//====================================== Double Field Op ======================================//
type SimpleMathOpKeys = keyof typeof MATH_FIELD_OP;
type TrimFieldOpKeys = keyof typeof TRIM_FIELD_OP;
type DoubleOpKeys = keyof typeof DOUBLE_FIELD_OP;
type StrFieldOpKeys = keyof typeof STR_FIELD_OP | keyof typeof STR_IN_FIELD_OP;

//====================================== Triple Field Op ======================================//
type TripleOpKeys = keyof typeof TRIPLE_FIELD_OP;
type SubstringFieldOpKeys = keyof typeof SUBSTRING_FIELD_OP;

//====================================== Multiple Field Op ======================================//
type MultipleOpKeys = keyof typeof MULTIPLE_FIELD_OP;
export type CaseOpKeys = keyof typeof CASE_FIELD_OP;

export type NoPramFieldOpKeys = NoParamOpKeys | CurrentDateOpKeys;
export type SingleFieldOpKeys = SingleOpKeys | DateExtractOpKeys;
export type DoubleFieldOpKeys =
  | DoubleOpKeys
  | TrimFieldOpKeys
  | StrFieldOpKeys
  | SimpleMathOpKeys;
export type TripleFieldOpKeys = TripleOpKeys | SubstringFieldOpKeys;
export type MultipleFieldOpKeys = MultipleOpKeys | CaseOpKeys;
export type AggregateFunctionType = keyof typeof aggregateFunctionName;
