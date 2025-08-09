export const aggregateFunctionName = {
  MIN: 'MIN',
  MAX: 'MAX',
  COUNT: 'COUNT',
  AVG: 'AVG',
  SUM: 'SUM',
} as const;

export const fieldFunction = {};

export type FieldFunctionType = keyof typeof aggregateFunctionName;
