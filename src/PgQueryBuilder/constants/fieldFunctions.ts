export const aggregateFunctionName = {
  MIN: 'MIN',
  MAX: 'MAX',
  COUNT: 'COUNT',
  AVG: 'AVG',
  SUM: 'SUM',
} as const;

export const COL_PREFIX = 'col#';

export const SIMPLE_MATH_FIELD_OP = {
  add: '+',
  sub: '-',
  multi: '*',
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
} as const;
export const ADVANCE_DOUBLE_FIELD_OP = {
  trunc: 'TRUNC',
  round: 'ROUND',
  power: 'POWER',
} as const;

// FLOOR(score) AS floor_score,
//     ROUND(price, 2) AS rounded_price,
//     TRUNC(value, 1) AS truncated_value,
//     SQRT(area) AS square_root,
//     POWER(base, 2) AS power_result,
//     EXP(log_value) AS exponential,
//     LN(natural_value) AS natural_log,
//     LOG(base_value) AS logarithm,
//     SIGN(difference) AS sign_value,
//     RANDOM() AS random_number,
//     PI() AS pi_value,
//     DEGREES(radians) AS degree_value,
//     RADIANS(degrees) AS radian_value,
//     SIN(angle) AS sine_value,
//     COS(angle) AS cosine_value,
//     TAN(angle) AS tangent_value

export type SimpleMathOpKeys = keyof typeof SIMPLE_MATH_FIELD_OP;
export type AdvanceSingleFieldOpKeys = keyof typeof ADVANCE_SINGLE_FIELD_OP;
export type AdvanceDoubleFieldOpKeys = keyof typeof ADVANCE_DOUBLE_FIELD_OP;
export type FieldFunctionType = keyof typeof aggregateFunctionName;
