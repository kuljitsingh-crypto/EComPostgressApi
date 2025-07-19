import { query } from './db.config';
const enumQryPrefix = `DO $$ BEGIN CREATE TYPE`;
const enumQrySuffix = `EXCEPTION WHEN duplicate_object THEN null; END $$;`;

const tableJoin = {
  innerJoin: 'INNER JOIN',
  leftJoin: 'LEFT JOIN',
  rightJoin: 'RIGHT JOIN',
  fullOuterJoin: 'FULL OUTER JOIN',
  selfJoin: 'INNER JOIN',
  crossJoin: 'CROSS JOIN',
} as const;
type TABLE_JOIN_TYPE = keyof typeof tableJoin;
type ORDER_OPTION = 'ASC' | 'DESC';
type NULL_OPTION = 'NULLS FIRST' | 'NULLS LAST';
type PAGINATION = { limit: number; offset?: number };
type ColumnRef = `${string}.${string}`;
type JOIN_COND = Array<{ baseColumn: ColumnRef; joinColumn: ColumnRef }>;
type JOIN_MODEL = {
  tableName: string;

  on: JOIN_COND;
  alias?: string;
};
type OTHER_JOIN<T extends TABLE_JOIN_TYPE> = {
  type: T;
  models: JOIN_MODEL[];
};
type SELF_JOIN<T extends TABLE_JOIN_TYPE> = {
  type: T;
  alias?: string;
  on: JOIN_COND;
};
type CROSS_JOIN<T extends TABLE_JOIN_TYPE> = {
  type: T;
  alias?: string;
  tableName: string;
};

type TABLE_JOIN<T extends TABLE_JOIN_TYPE = TABLE_JOIN_TYPE> =
  T extends 'selfJoin'
    ? SELF_JOIN<T>
    : T extends 'crossJoin'
      ? CROSS_JOIN<T>
      : OTHER_JOIN<T>;

type ORDER_BY =
  | { [key in string]: ORDER_OPTION }
  | { key: string; order: ORDER_OPTION; nullOption?: NULL_OPTION }[];

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

export const OP = {
  eq: '=',
  neq: '!=',
  lte: '<=',
  lt: '<',
  gte: '>=',
  gt: '>',
  like: 'LIKE',
  iLike: 'ILIKE',
  in: 'IN',
  between: 'BETWEEN',
  isNull: 'IS',
  notNull: 'IS NOT',
  notLike: 'NOT LIKE',
  notILike: 'NOT ILIKE',
  notIn: 'NOT IN',
  notBetween: 'NOT BETWEEN',
  startsWith: 'LIKE',
  endsWith: 'LIKE',
  substring: 'LIKE',
  and: 'AND',
  or: 'OR',
} as const;

type OP_KEYS = keyof typeof OP;

type OP_KEYS_WITHOUT_AND_OR = Exclude<OP_KEYS, 'and' | 'or'>;

type ARRAY_VALUE_FILTER<Key extends OP_KEYS> = {
  column: string;
  op: Key;
  value: any[];
};
type SINGLE_VALUE_FILTER<Key extends OP_KEYS> = {
  column: string;
  op: Key;
  value: any;
};

type PRESET_VALUE_FILTER<Key extends OP_KEYS> = {
  column: string;
  op: Key;
};

type NORMAL_FILTER<Key extends OP_KEYS> = Key extends 'in'
  ? ARRAY_VALUE_FILTER<Key>
  : Key extends 'between'
    ? ARRAY_VALUE_FILTER<Key>
    : Key extends 'isNull'
      ? PRESET_VALUE_FILTER<Key>
      : Key extends 'notNull'
        ? PRESET_VALUE_FILTER<Key>
        : SINGLE_VALUE_FILTER<Key>;

type WHERE_FILTER<
  Key extends OP_KEYS = OP_KEYS,
  Key2 extends OP_KEYS_WITHOUT_AND_OR = OP_KEYS_WITHOUT_AND_OR,
> = Key extends 'and'
  ? { value1: NORMAL_FILTER<Key2>; op: Key; value2: NORMAL_FILTER<Key2> }
  : Key extends 'or'
    ? { value1: NORMAL_FILTER<Key2>; op: Key; value2: NORMAL_FILTER<Key2> }
    : NORMAL_FILTER<Key>;

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
  null: 'NULL',
  where: 'WHERE',
  select: 'SELECT',
  from: 'FROM',
  insertInto: 'INSERT INTO',
  values: 'VALUES',
  returning: 'RETURNING',
  limit: 'LIMIT',
  offset: 'OFFSET',
  as: 'AS',
  on: 'ON',
} as const;

export const dbDefaultValue = {
  currentDate: 'CURRENT_DATE',
  currentTimestamp: 'CURRENT_TIMESTAMP',
  currentTime: 'CURRENT_TIME',
  uuidV4: 'gen_random_uuid()',
};

type Reference = {
  parentTable: string;
  parentColumn: string | string[];
  column: string | string[];
  constraintName?: string;
  onDelete?: ForeignKeyActions;
  onUpdate?: ForeignKeyActions;
};
type ExtraOptions = {
  tableName: string;
  references?: Reference[];
};

type QueryAttributes = string | { column: string; alias: string }; // {columnName:aliasName}
type QueryParams = {
  attributes?: QueryAttributes[];
  isDistinct?: boolean;
  orderBy?: ORDER_BY;
  filters?: WHERE_FILTER[];
  limit?: PAGINATION;
  tableAlias?: string;
  include?: TABLE_JOIN;
};

export class DBQuery {
  static tableName: string = '';
  static tableColumns: Set<string> = new Set();
  static async findAll(queryParams?: QueryParams) {
    const {
      attributes,
      isDistinct,
      orderBy,
      filters,
      limit,
      tableAlias,
      include,
    } = queryParams || {};
    const distinctMaybe = isDistinct ? `${dbKeywords.distinct} ` : '';
    const allowedFields = this.tableColumns;
    const colStr = DBQuery.#getSelectColumns(allowedFields, attributes);
    const orderStr = DBQuery.#prepareOrderByStatement(allowedFields, orderBy);
    const { statement: whereStatement, values } =
      DBQuery.#prepareWhereStatement(allowedFields, filters);
    const limitStr = DBQuery.#preparePaginationStatement(limit);
    const joinStr = DBQuery.#prepareTableJoin(this.tableName, include);
    const variableQry = DBQuery.#prepareVariableQry(
      whereStatement,
      orderStr,
      limitStr,
      joinStr,
    );
    const modelAlias = tableAlias ? ` ${dbKeywords.as} ${tableAlias}` : '';
    const rawQry = `${dbKeywords.select} ${distinctMaybe}${colStr} ${dbKeywords.from} "${this.tableName}"${modelAlias}${variableQry}`;
    const findAllQuery = `${rawQry.trimEnd()};`;
    const result = await query(findAllQuery, values);
    return { rows: result.rows, count: result.rowCount };
  }
  static async create(
    fields: Record<string, any>,
    returnOnly?: QueryAttributes[],
  ) {
    const keys: string[] = [];
    const values: any[] = [];
    const valuePlaceholder: string[] = [];
    const allowedFields = this.tableColumns;
    const returnStr = DBQuery.#getSelectColumns(allowedFields, returnOnly);
    Object.entries(fields).forEach((entry, index) => {
      const [key, value] = entry;
      keys.push(DBQuery.#FieldQuote(allowedFields, key));
      values.push(value);
      valuePlaceholder.push(DBQuery.#createPlaceholder(index + 1));
    });
    const columns = keys.toString();
    const valuePlaceholders = valuePlaceholder.toString();
    const rawQry = `${dbKeywords.insertInto} "${this.tableName}"(${columns}) ${dbKeywords.values}(${valuePlaceholders}) ${dbKeywords.returning} ${returnStr}`;
    const createQry = `${rawQry.trimEnd()};`;
    const result = await query(createQry, values);
    return { rows: result.rows, count: result.rowCount };
  }

  static async createBulk(
    columns: Array<string>,
    values: Array<Array<any>>,
    returnOnly?: QueryAttributes[],
  ) {
    const flatedValues: any[] = [];
    const allowedFields = this.tableColumns;
    const returnStr = DBQuery.#getSelectColumns(allowedFields, returnOnly);
    let incrementBy = 1;
    const valuePlaceholder = values.map((val, pIndex) => {
      if (pIndex > 0) {
        incrementBy += val.length - 1;
      }
      flatedValues.push(...val);
      const placeholder = val
        .map((_, cIndex) =>
          DBQuery.#createPlaceholder(pIndex + cIndex + incrementBy),
        )
        .join(',');
      return `(${placeholder})`;
    });
    const colStr = columns.toString();
    const valuePlaceholders = valuePlaceholder.toString();
    const rawQry = `${dbKeywords.insertInto} "${this.tableName}"(${colStr}) ${dbKeywords.values}${valuePlaceholders} ${dbKeywords.returning} ${returnStr}`;
    const createQry = `${rawQry.trimEnd()};`;
    const result = await query(createQry, flatedValues);
    return { rows: result.rows, count: result.rowCount };
  }

  // Private Methods
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
        return `${DBQuery.#FieldQuote(allowedFields, attr.column)} ${
          dbKeywords.as
        } ${DBQuery.#quote(attr.alias)}`;
      })
      .join(', ');
  }
  static #quote = (str: string) => `${String(str).replace(/"/g, '""')}`;

  static #validateField(field: string, allowed: Set<string>) {
    if (field.includes('.')) {
      field = field.split('.').slice(-1)[0];
    }
    if (!allowed.has(field)) {
      throw new Error(
        `Invalid column name ${field}. Allowed Column names are: ${Array.from(
          allowed,
        ).join(', ')}.`,
      );
    }
  }

  static #FieldQuote = (allowedFields: Set<string>, str: string) => {
    DBQuery.#validateField(str, allowedFields);
    return DBQuery.#quote(str);
  };

  static #prepareOrderByStatement(
    allowedFields: Set<string>,
    orderBy?: ORDER_BY,
  ) {
    if (!orderBy || (Array.isArray(orderBy) && orderBy.length <= 0)) return '';
    let orderByStatemnt = dbKeywords.orderBy + ' ';
    if (Array.isArray(orderBy)) {
      orderByStatemnt += orderBy
        .map((ord) =>
          `${DBQuery.#FieldQuote(allowedFields, ord.key)} ${ord.order} ${
            ord.nullOption || ''
          }`.trimEnd(),
        )
        .join(', ');
    } else {
      orderByStatemnt += Object.entries(orderBy)
        .map(
          ([key, val]) => `${DBQuery.#FieldQuote(allowedFields, key)} ${val}`,
        )
        .join(', ');
    }

    return orderByStatemnt;
  }

  static #prepareWhereStatement(
    allowedFields: Set<string>,
    filters?: WHERE_FILTER[],
  ) {
    if (!filters || (Array.isArray(filters) && filters.length === 0))
      return { statement: '', values: [] };
    let queryStatement = dbKeywords.where + ' ';
    const values: any[] = [];
    const indexAndIncremntBy = { index: 0, incremntBy: 1 };
    queryStatement += filters
      .map((filter, indx) => {
        indexAndIncremntBy.index = indx;
        return DBQuery.#getQueryStatement(
          allowedFields,
          filter,
          indexAndIncremntBy,
          values,
        );
      })
      .join(` ${OP.and} `);
    return { statement: queryStatement, values };
  }

  static #getQueryStatement(
    allowedFields: Set<string>,
    singleQry: WHERE_FILTER,
    index: {
      index: number;
      incremntBy: number;
    },
    valuesArr: any[],
  ): string {
    const { op } = singleQry;
    const operation = OP[op];
    if (!operation) {
      throw new Error(
        `Invalid operator "${op}". Please use following operators: ${Object.keys(
          OP,
        ).join(', ')}. `,
      );
    }
    const preparePlachldrForArray = (values: any[]) => {
      const placeholderArr = values.map((val, i) => {
        if (i > 0) {
          index.incremntBy += 1;
        }
        valuesArr.push(val);
        return DBQuery.#createPlaceholder(index.index + index.incremntBy);
      });
      return placeholderArr;
    };
    const valPlaceholder = DBQuery.#createPlaceholder(
      index.index + index.incremntBy,
    );
    switch (op) {
      case 'eq':
      case 'neq':
      case 'gt':
      case 'gte':
      case 'lt':
      case 'lte':
      case 'like':
      case 'iLike':
      case 'notLike':
      case 'notILike': {
        valuesArr.push(singleQry.value);
        return `${DBQuery.#FieldQuote(
          allowedFields,
          singleQry.column,
        )} ${operation} ${valPlaceholder}`;
      }
      case 'notNull':
      case 'isNull': {
        index.incremntBy -= 1;
        return `${DBQuery.#FieldQuote(
          allowedFields,
          singleQry.column,
        )} ${operation} ${dbKeywords.null}`;
      }
      case 'startsWith': {
        valuesArr.push(`${singleQry.value}%`);
        return `${DBQuery.#FieldQuote(
          allowedFields,
          singleQry.column,
        )} ${operation} ${valPlaceholder}`;
      }
      case 'endsWith': {
        valuesArr.push(`%${singleQry.value}`);
        return `${DBQuery.#FieldQuote(
          allowedFields,
          singleQry.column,
        )} ${operation} ${valPlaceholder}`;
      }
      case 'substring': {
        valuesArr.push(`%${singleQry.value}%`);
        return `${DBQuery.#FieldQuote(
          allowedFields,
          singleQry.column,
        )} ${operation} ${valPlaceholder}`;
      }
      case 'in':
      case 'notIn': {
        if (!Array.isArray(singleQry.value)) {
          throw new Error(`For operator "${op}" value should be array.`);
        }
        const placeholders = preparePlachldrForArray(singleQry.value);
        return `${DBQuery.#FieldQuote(
          allowedFields,
          singleQry.column,
        )} ${operation} (${placeholders.toString()})`;
      }
      case 'between':
      case 'notBetween': {
        if (!Array.isArray(singleQry.value)) {
          throw new Error(`For operator "${op}" value should be array.`);
        }
        if (singleQry.value.length !== 2) {
          throw new Error(`Operator "${op}" requires exactly 2 values.`);
        }
        const placeholders = preparePlachldrForArray(singleQry.value);
        return `${DBQuery.#FieldQuote(
          allowedFields,
          singleQry.column,
        )} ${operation} ${placeholders[0]} ${OP.and} ${placeholders[1]}`;
      }
      case 'and':
      case 'or': {
        if (!singleQry.value1 || !singleQry.value2) {
          throw new Error(
            `For operator "${op}" value1 and value2 field is required.`,
          );
        }
        const column1Str = DBQuery.#getQueryStatement(
          allowedFields,
          singleQry.value1,
          index,
          valuesArr,
        );
        index.incremntBy += 1;
        const column2Str = DBQuery.#getQueryStatement(
          allowedFields,
          singleQry.value2,
          index,
          valuesArr,
        );
        return `${column1Str} ${operation} ${column2Str}`;
      }
      default:
        throw new Error(
          `Invalid operator "${op}". Please use following operators: ${Object.keys(
            OP,
          ).join(', ')}. `,
        );
    }
  }

  static #preparePaginationStatement(limit?: PAGINATION) {
    if (!limit || typeof limit.limit !== 'number') {
      return '';
    }
    const { limit: l, offset: o } = limit;
    let limitStatement = `${dbKeywords.limit} ${l}`;
    if (o) {
      limitStatement += ` ${dbKeywords.offset} ${o}`;
    }
    return limitStatement;
  }
  static #prepareVariableQry(
    whereQry?: string,
    orderbyQry?: string,
    limitQry?: string,
    joinStr?: string,
  ) {
    let variableQry = '';
    if (joinStr) {
      variableQry += ' ' + joinStr;
    }
    if (whereQry) {
      variableQry += ' ' + whereQry;
    }
    if (orderbyQry) {
      variableQry += ' ' + orderbyQry;
    }
    if (limitQry) {
      variableQry += ' ' + limitQry;
    }
    return variableQry;
  }

  static #prepareTableJoin(selfModelName: string, include?: TABLE_JOIN) {
    if (!include || !include.type) {
      return '';
    }
    switch (include.type) {
      case 'selfJoin': {
        const { type, on, alias } = include;
        const updatedInclude = {
          type,
          models: [{ tableName: selfModelName, on, alias }],
        };
        return DBQuery.#prepareJoinStr(updatedInclude);
      }
      case 'innerJoin':
      case 'fullOuterJoin':
      case 'leftJoin':
      case 'rightJoin':
        return DBQuery.#prepareJoinStr(include);
      case 'crossJoin': {
        const { type, tableName, alias } = include;
        const updatedInclude = {
          type,
          models: [{ tableName, on: [], alias }],
        };
        return DBQuery.#prepareJoinStr(updatedInclude);
      }
      default:
        throw new Error(
          `Invalid join type:"${(include as any).type}". Valid join types:${Object.keys(tableJoin).toString()}.`,
        );
    }
  }
  static #prepareJoinStr<T extends TABLE_JOIN_TYPE>(joinType: OTHER_JOIN<T>) {
    const { type, models } = joinType;
    const joinName = tableJoin[type];
    if (!joinName) {
      throw new Error(
        `Invalid join type:"${type}". Valid join types:${Object.keys(tableJoin).toString()}.`,
      );
    }
    const joinStr = models.map((m) => {
      const { on, tableName, alias } = m;
      const onStr =
        type === 'crossJoin'
          ? 'true'
          : on
              .map(
                ({ baseColumn, joinColumn }) =>
                  `${baseColumn} ${OP.eq} ${joinColumn}`,
              )
              .join(` ${OP.and} `);
      const aliasMaybe = alias
        ? ` ${dbKeywords.as} ${alias} ${dbKeywords.on} `
        : ` ${dbKeywords.on} `;
      return `${joinName} ${tableName}${aliasMaybe}${onStr}`;
    });

    return joinStr.join(' ');
  }
  static #createPlaceholder(val: number) {
    return `$${val}`;
  }
}

export class DBModel extends DBQuery {
  static init(modelObj: DbTable, option: ExtraOptions) {
    const { tableName, references = [] } = option;
    this.tableName = tableName;
    const primaryKeys: string[] = [];
    const columns: string[] = [];
    const enums: string[] = [];
    const tableColumns: Set<string> = new Set();
    Object.entries(modelObj).forEach((entry) => {
      const [key, value] = entry;
      tableColumns.add(key);
      columns.push(DBModel.#createColumn(key, value, primaryKeys, enums));
    });
    this.tableColumns = tableColumns;
    if (primaryKeys.length <= 0) {
      throw new Error(
        `At least one primary key column is required in table ${tableName}.`,
      );
    }
    columns.push(DBModel.#createPrimaryColumn(primaryKeys));
    references.forEach((ref) => {
      columns.push(DBModel.#createForeignColumn(ref));
    });
    const createEnumQryPromise = Promise.all(enums.map((e) => query(e)));
    const createTableQry = `CREATE TABLE IF NOT EXISTS "${tableName}" (${columns.toString()});`;
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
            const enumQry = `${enumQryPrefix} ${colUpr} ${dbKeywords.as} ${keyVale}; ${enumQrySuffix}`;
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
      parentTable,
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
    valueStr += `${dbKeywords.references} "${parentTable}" (${parentColStr}) `;
    if (onDelete) {
      valueStr += `${dbKeywords.onDelete} ${onDelete} `;
    }
    if (onUpdate) {
      valueStr += `${dbKeywords.onUpdate} ${onUpdate} `;
    }
    return valueStr.trimEnd();
  }
}
