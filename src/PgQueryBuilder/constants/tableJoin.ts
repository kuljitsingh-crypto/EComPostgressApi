export const TABLE_JOIN = {
  INNER: 'INNER JOIN',
  LEFT: 'LEFT JOIN',
  RIGHT: 'RIGHT JOIN',
  FULLOUTER: 'FULL OUTER JOIN',
  SELF: 'INNER JOIN',
  CROSS: 'INNER JOIN',
} as const;

export type TABLE_JOIN_TYPE = keyof typeof TABLE_JOIN;

type ColumnRef = `${string}.${string}` | string;
type BaseColumn = ColumnRef;
type TargetColumn = ColumnRef;
export type JOIN_COLUMN = Record<BaseColumn, TargetColumn>;
export type OTHER_JOIN<T extends TABLE_JOIN_TYPE, Model extends any> = {
  type: T;
  model: Model;
  /**
   * {baseColumn:joinColumn} or {baseColumn:joinColumn, baseColumn2:joinColumn2}
   */
  on: JOIN_COLUMN;
  alias?: string;
};

type SELF_JOIN<T extends TABLE_JOIN_TYPE> = {
  type: T;
  alias?: string;
  on: JOIN_COLUMN;
};
type CROSS_JOIN<T extends TABLE_JOIN_TYPE, Model extends any> = {
  type: T;
  alias?: string;
  model: Model;
};

export type TABLE_JOIN_COND<
  Model extends any = any,
  T extends TABLE_JOIN_TYPE = TABLE_JOIN_TYPE,
> = T extends 'SELF'
  ? SELF_JOIN<T>
  : T extends 'CROSS'
    ? CROSS_JOIN<T, Model>
    : OTHER_JOIN<T, Model>;

export type JOIN<T extends TABLE_JOIN_TYPE, Model extends any> = {
  type: T;
  tableName?: string;
  model?: Model;
  on: JOIN_COLUMN;
  alias?: string;
};
