export const aggregateFunctionName = {
  MIN: 'MIN',
  MAX: 'MAX',
  COUNT: 'COUNT',
  AVG: 'AVG',
  SUM: 'SUM',
} as const;

export const fieldFunction = {
  ADD: '+',
  SUB: '-',
  MULTIPLE: '*',
  DIVIDE: '/',
  MODULO: '%',
  EXPONENT: '^',
  SQUARE_ROOT: '|/',
  CUBE_ROOT: '||/',
  POWER: '',
};

export type FieldFunctionType = keyof typeof aggregateFunctionName;
