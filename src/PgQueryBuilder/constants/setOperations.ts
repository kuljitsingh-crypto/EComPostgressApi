export const setOperation = {
  union: 'UNION',
  unionAll: 'UNION ALL',
  intersect: 'INTERSECT',
  except: 'EXCEPT',
} as const;

export type SetOperationType = keyof typeof setOperation;
