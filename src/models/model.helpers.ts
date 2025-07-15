import { query } from './db.config';
const enumQryPrefix = `DO $$ BEGIN CREATE TYPE`;
const enumQrySuffix = `EXCEPTION WHEN duplicate_object THEN null; END $$;`;

type ORDER_OPTION = 'ASC' | 'DESC';
type NULL_OPTION = 'NULLS FIRST' | 'NULLS LAST';

type ORDER_BY =
  | { [key in string]: ORDER_OPTION }
  | { key: string; order: ORDER_OPTION; nullOption?: NULL_OPTION }[];

type WHERE_TYPE = {};

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

const dbKeywords = {
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
  distinct: 'DISTINCT',
  orderBy: 'ORDER BY',
};

export const dbDefaultValue = {
  currentDate: 'CURRENT_DATE',
  currentTimestamp: 'CURRENT_TIMESTAMP',
  currentTime: 'CURRENT_TIME',
  uuidV4: 'gen_random_uuid()',
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

type QueryAttributes = string | { column: string; alias: string }; // {columnName:aliasName}
type QueryParams = {
  attributes?: QueryAttributes[];
  isDistinct?: boolean;
  orderBy?: ORDER_BY;
};

export class DBQuery {
  static modelName: string = '';
  static modelFields: Set<string> = new Set();
  static async findAll(queryParams?: QueryParams) {
    const { attributes, isDistinct, orderBy } = queryParams || {};
    const distinctMaybe = isDistinct ? `${dbKeywords.distinct} ` : '';
    const colStr = DBQuery.#getSelectColumns(this.modelFields, attributes);
    const orderStr = DBQuery.#prepareOrderByStatement(orderBy);
    const findAllQuery =
      `SELECT ${distinctMaybe}${colStr} FROM "${this.modelName}" ${orderStr}`.trimEnd();
    const result = await query(findAllQuery);
    return { rows: result.rows, count: result.rowCount };
  }
  static async create(
    fields: Record<string, any>,
    returnOnly?: QueryAttributes[],
  ) {
    const keys: string[] = [];
    const values: string[] = [];
    const valuePlaceholder: string[] = [];
    const returnStr = DBQuery.#getSelectColumns(this.modelFields, returnOnly);
    Object.entries(fields).forEach((entry, index) => {
      const [key, value] = entry;
      keys.push(DBQuery.#FieldQuote(this.modelFields, key));
      values.push(value);
      valuePlaceholder.push(`$${index + 1}`);
    });
    const createQry = `INSERT INTO "${this.modelName}"(${keys.toString()}) VALUES(${valuePlaceholder.toString()}) RETURNING ${returnStr}`;
    const result = await query(createQry, values);
    return { rows: result.rows, count: result.rowCount };
  }

  static #getSelectColumns(
    allowedFields: Set<string>,
    attributes?: QueryAttributes[],
  ) {
    if (!Array.isArray(attributes) || attributes.length === 0) return '*';
    return attributes
      .map((attr) => {
        if (typeof attr === 'string') {
          return DBQuery.#FieldQuote(allowedFields, attr);
        }
        return `${DBQuery.#FieldQuote(allowedFields, attr.column)} AS ${DBQuery.#quote(attr.alias)}`;
      })
      .join(', ');
  }
  static #quote = (str: string) => `${String(str).replace(/"/g, '""')}`;
  static #validateField(field: string, allowed: Set<string>) {
    if (!allowed.has(field)) {
      throw new Error(
        `Invalid column name ${field}. Allowed Column names are: ${Array.from(allowed).join(', ')}.`,
      );
    }
  }
  static #FieldQuote = (allowedFields: Set<string>, str: string) => {
    DBQuery.#validateField(str, allowedFields);
    return DBQuery.#quote(str);
  };
  static #prepareOrderByStatement(orderBy?: ORDER_BY) {
    if (!orderBy || (Array.isArray(orderBy) && orderBy.length <= 0)) return '';
    let orderByStatemnt = dbKeywords.orderBy + ' ';
    if (Array.isArray(orderBy)) {
      orderByStatemnt += orderBy
        .map((ord) =>
          `${ord.key} ${ord.order} ${ord.nullOption || ''}`.trimEnd(),
        )
        .join(', ');
    } else {
      orderByStatemnt += Object.entries(orderBy)
        .map(([key, val]) => `${key} ${val}`)
        .join(', ');
    }

    return orderByStatemnt;
  }
}

export class DBModel extends DBQuery {
  static init(modelObj: DbTable, option: ExtraOptions) {
    const { modelName, references = [] } = option;
    this.modelName = modelName;
    const primaryKeys: string[] = [];
    const columns: string[] = [];
    const enums: string[] = [];
    const modelFields: Set<string> = new Set();
    Object.entries(modelObj).forEach((entry) => {
      const [key, value] = entry;
      modelFields.add(key);
      columns.push(DBModel.#createColumn(key, value, primaryKeys, enums));
    });
    this.modelFields = modelFields;
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
          valueStr += `${dbKeywords.default} ${keyVale} `;
          break;
        case 'unique':
          valueStr += dbKeywords.unique + ' ';
          break;
        case 'notNull':
          valueStr += dbKeywords.notNull + ' ';
          break;
        case 'customDefaultValue':
          valueStr += `${dbKeywords.default} '${keyVale}' `;
          break;
        case 'check':
          valueStr += `${dbKeywords.check} (${keyVale}) `;
          break;
      }
    });
    return valueStr.trimEnd();
  }
  static #createPrimaryColumn(primaryKeys: string[]) {
    return `${dbKeywords.primaryKey} (${primaryKeys.toString()})`;
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
      valueStr += `${dbKeywords.constraint} ${constraintName} `;
    }
    valueStr += `${dbKeywords.foreignKey} (${colStr}) `;
    valueStr += `${dbKeywords.references} "${parentModel}" (${parentColStr}) `;
    if (onDelete) {
      valueStr += `${dbKeywords.onDelete} ${onDelete} `;
    }
    if (onUpdate) {
      valueStr += `${dbKeywords.onUpdate} ${onUpdate} `;
    }
    return valueStr.trimEnd();
  }
}
