export const setOperation = {
  UNION: 'UNION',
  UNION_ALL: 'UNION ALL',
  INTERSECT: 'INTERSECT',
  EXCEPT: 'EXCEPT',
} as const;

export type SetOperationType = keyof typeof setOperation;
