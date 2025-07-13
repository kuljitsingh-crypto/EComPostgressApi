import { query } from './db.config';

export type DbTable = {
  [key in string]: {
    type: string;
    isPrimary?: boolean;
    defaultValue?: string;
    unique?: boolean;
    notNull?: boolean;
    customDefaultValue?: string;
  };
};

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
  enum(...values: string[]) {
    const valueStr = values.toString();
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
};

type ExtraOptions = {
  modelName: string;
};

export class DBModel {
  static init(modelObj: DbTable, option: ExtraOptions) {
    const { modelName } = option;
    const primaryKeys: string[] = [];
    const columns: string[] = [];
    Object.entries(modelObj).forEach((entry) => {
      const [key, value] = entry;
      columns.push(DBModel.#createColumn(key, value, primaryKeys));
    });
    columns.push(DBModel.#createPrimaryColumn(primaryKeys));
    const createTableQry = `CREATE TABLE IF NOT EXISTS "${modelName}" (${columns.toString()})`;
    query(createTableQry);
  }

  static #createColumn(
    columnName: string,
    value: DbTable[keyof DbTable],
    primaryKeys: string[],
  ) {
    let valueStr = `${columnName} `;
    Object.entries(value).forEach((entry) => {
      const [key, keyVale] = entry as [
        keyof DbTable[keyof DbTable],
        string | boolean,
      ];
      switch (key) {
        case 'type':
          valueStr += keyVale + ' ';
          break;
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
      }
    });
    return valueStr.trimEnd();
  }
  static #createPrimaryColumn(primaryKeys: string[]) {
    return `${dbDefaultValue.primaryKey} (${primaryKeys.toString()})`;
  }
}
