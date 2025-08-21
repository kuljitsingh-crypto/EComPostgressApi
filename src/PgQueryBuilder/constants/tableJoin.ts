export const TABLE_JOIN = {
  innerJoin: 'INNER JOIN',
  leftJoin: 'LEFT JOIN',
  rightJoin: 'RIGHT JOIN',
  fullJoin: 'FULL OUTER JOIN',
  selfJoin: 'INNER JOIN',
  crossJoin: 'CROSS JOIN',
} as const;

export type TableJoinType = keyof typeof TABLE_JOIN;
