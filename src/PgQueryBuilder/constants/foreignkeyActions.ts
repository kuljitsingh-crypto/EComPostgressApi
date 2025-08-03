export const foreignKeyActions = {
  noAction: 'NO ACTION',
  cascade: 'CASCADE',
  restrict: 'RESTRICT',
  null: 'SET NULL',
  default: 'SET DEFAULT',
} as const;

export type ForeignKeyActions =
  (typeof foreignKeyActions)[keyof typeof foreignKeyActions];

export type ReferenceTable = {
  parentColumn: string | string[];
  column: string | string[];
  constraintName?: string;
  onDelete?: ForeignKeyActions;
  onUpdate?: ForeignKeyActions;
};

export type Reference = {
  [parentTable in string]: ReferenceTable;
};
