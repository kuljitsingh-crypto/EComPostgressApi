export const fieldFunctionName = {
  MIN: 'MIN',
  MAX: 'MAX',
  COUNT: 'COUNT',
  AVG: 'AVG',
  SUM: 'SUM',
} as const;

export type FieldFunctionType = keyof typeof fieldFunctionName;
