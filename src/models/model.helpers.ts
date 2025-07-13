import { query } from './db.config';
const enumQryPrefix = `DO $$ BEGIN CREATE TYPE`;
const enumQrySuffix = `EXCEPTION WHEN duplicate_object THEN null; END $$;`;

export type DbTable = {
  [key in string]: {
    type: string;
    isPrimary?: boolean;
    defaultValue?: string;
    unique?: boolean;
    notNull?: boolean;
    customDefaultValue?: string;
    check?: string;
  };
};

export const foreignKeyActions = {
  noAction: 'NO ACTION',
  cascade: 'CASCADE',
  restrict: 'RESTRICT',
  null: 'SET NULL',
  default: 'SET DEFAULT',
} as const;

type ForeignKeyActions =
  (typeof foreignKeyActions)[keyof typeof foreignKeyActions];

export const DataTypes = {
  boolean: 'BOOLEAN',
  true: 'TRUE',
  false: 'FALSE',
  text: 'VARCHAR',
  float: 'DOUBLE PRECISION',
  real: 'REAL',
  smallInt: 'SMALLINT',
  int: 'INTEGER',
  bigInt: 'BIGINT',
  serial: 'SERIAL',
  date: 'DATE',
  timestamp: 'TIMESTAMP',
  timestamptz: 'TIMESTAMPTZ',
  time: 'TIME',
  json: 'JSON',
  jsonb: 'JSONB',
  uuid: 'UUID',
  string(n: number) {
    return `VARCHAR(${n})`;
  },
  numeric(precision: number, scale = 0) {
    return `NUMERIC(${precision}, ${scale})`;
  },
  enum(values: string[]) {
    const valueStr = values.map((v) => `'${v}'`).toString();
    return `ENUM(${valueStr})`;
  },
};

export const dbDefaultValue = {
  currentDate: 'CURRENT_DATE',
  currentTimestamp: 'CURRENT_TIMESTAMP',
  currentTime: 'CURRENT_TIME',
  uuidV4: 'gen_random_uuid()',
  notNull: 'NOT NULL',
  unique: 'UNIQUE',
  default: 'DEFAULT',
  primaryKey: 'PRIMARY KEY',
  foreignKey: 'FOREIGN KEY',
  constraint: 'CONSTRAINT',
  references: 'REFERENCES',
  onDelete: 'ON DELETE',
  onUpdate: 'ON UPDATE',
  check: 'CHECK',
};

type Reference = {
  parentModel: string;
  parentColumn: string | string[];
  column: string | string[];
  constraintName?: string;
  onDelete?: ForeignKeyActions;
  onUpdate?: ForeignKeyActions;
};
type ExtraOptions = {
  modelName: string;
  references?: Reference[];
};

export class DBModel {
  static init(modelObj: DbTable, option: ExtraOptions) {
    const { modelName, references = [] } = option;
    const primaryKeys: string[] = [];
    const columns: string[] = [];
    const enums: string[] = [];
    Object.entries(modelObj).forEach((entry) => {
      const [key, value] = entry;
      columns.push(DBModel.#createColumn(key, value, primaryKeys, enums));
    });
    if (primaryKeys.length <= 0) {
      throw new Error(
        `At least one primary key column is required in model ${modelName}.`,
      );
    }
    columns.push(DBModel.#createPrimaryColumn(primaryKeys));
    references.forEach((ref) => {
      columns.push(DBModel.#createForeignColumn(ref));
    });
    const createEnumQryPromise = Promise.all(enums.map((e) => query(e)));
    const createTableQry = `CREATE TABLE IF NOT EXISTS "${modelName}" (${columns.toString()})`;
    createEnumQryPromise.then(() => query(createTableQry));
  }

  static #createColumn(
    columnName: string,
    value: DbTable[keyof DbTable],
    primaryKeys: string[],
    enums: string[],
  ) {
    let valueStr = `${columnName} `;
    const colUpr = columnName.toUpperCase();
    Object.entries(value).forEach((entry) => {
      const [key, keyVale] = entry as [
        keyof DbTable[keyof DbTable],
        string | boolean,
      ];

      switch (key) {
        case 'type': {
          if ((keyVale as any).startsWith('ENUM')) {
            const enumQry = `${enumQryPrefix} ${colUpr} AS ${keyVale}; ${enumQrySuffix}`;
            enums.push(enumQry);
            valueStr += colUpr + ' ';
          } else {
            valueStr += keyVale + ' ';
          }
          break;
        }
        case 'isPrimary':
          primaryKeys.push(columnName);
          break;
        case 'defaultValue':
          valueStr += `${dbDefaultValue.default} ${keyVale} `;
          break;
        case 'unique':
          valueStr += dbDefaultValue.unique + ' ';
          break;
        case 'notNull':
          valueStr += dbDefaultValue.notNull + ' ';
          break;
        case 'customDefaultValue':
          valueStr += `${dbDefaultValue.default} '${keyVale}' `;
          break;
        case 'check':
          valueStr += `${dbDefaultValue.check} (${keyVale}) `;
          break;
      }
    });
    return valueStr.trimEnd();
  }
  static #createPrimaryColumn(primaryKeys: string[]) {
    return `${dbDefaultValue.primaryKey} (${primaryKeys.toString()})`;
  }
  static #createForeignColumn(ref: Reference) {
    const {
      parentModel,
      parentColumn,
      column,
      constraintName,
      onDelete,
      onUpdate,
    } = ref;
    let valueStr = '';
    const colStr = column.toString();
    const parentColStr = parentColumn.toString();
    if (constraintName) {
      valueStr += `${dbDefaultValue.constraint} ${constraintName} `;
    }
    valueStr += `${dbDefaultValue.foreignKey} (${colStr}) `;
    valueStr += `${dbDefaultValue.references} "${parentModel}" (${parentColStr}) `;
    if (onDelete) {
      valueStr += `${dbDefaultValue.onDelete} ${onDelete} `;
    }
    if (onUpdate) {
      valueStr += `${dbDefaultValue.onUpdate} ${onUpdate} `;
    }
    return valueStr.trimEnd();
  }
}
