import { query } from './db.config';

//============================================= CONSTANTS ===================================================//
const enumQryPrefix = `DO $$ BEGIN CREATE TYPE`;
const enumQrySuffix = `EXCEPTION WHEN duplicate_object THEN null; END $$;`;

const tableJoin = {
  INNER: 'INNER JOIN',
  LEFT: 'LEFT JOIN',
  RIGHT: 'RIGHT JOIN',
  FULLOUTER: 'FULL OUTER JOIN',
  SELF: 'INNER JOIN',
  CROSS: 'INNER JOIN',
} as const;

const fieldFunctionName = {
  MIN: 'MIN',
  MAX: 'MAX',
  COUNT: 'COUNT',
  AVG: 'AVG',
  SUM: 'SUM',
} as const;

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
  groupBy: 'GROUP BY',
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
  having: 'HAVING',
} as const;

export const dbDefaultValue = {
  currentDate: 'CURRENT_DATE',
  currentTimestamp: 'CURRENT_TIMESTAMP',
  currentTime: 'CURRENT_TIME',
  uuidV4: 'gen_random_uuid()',
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
  iStartsWith: 'ILIKE',
  iEndsWith: 'ILIKE',
  iSubstring: 'ILIKE',
  $and: 'AND',
  $or: 'OR',
} as const;
const validOperations = Object.keys(OP).join(', ');

export const foreignKeyActions = {
  noAction: 'NO ACTION',
  cascade: 'CASCADE',
  restrict: 'RESTRICT',
  null: 'SET NULL',
  default: 'SET DEFAULT',
} as const;

const fnJoiner = {
  joinFnAndColumn: (fn: FieldFunctionType, column: string) => `${column},${fn}`,
  sepFnAndColumn: (fnAndCol: string) => fnAndCol.split(','),
};

export const aggregateFn = Object.freeze({
  [fieldFunctionName.COUNT]: (column: string) => fieldFunc('COUNT', column),
  [fieldFunctionName.AVG]: (column: string) => fieldFunc('AVG', column),
  [fieldFunctionName.MAX]: (column: string) => fieldFunc('MAX', column),
  [fieldFunctionName.MIN]: (column: string) => fieldFunc('MIN', column),
  [fieldFunctionName.SUM]: (column: string) => fieldFunc('SUM', column),
});

//============================================= CONSTANTS ===================================================//

//============================================= TYPES ======================================================//

type FieldFunctionType = keyof typeof fieldFunctionName;

type TABLE_JOIN_TYPE = keyof typeof tableJoin;
type Primitive = string | number | boolean | null;

type PlaceholderRef = {
  index: number;
  incremntBy: number;
};

type ORDER_OPTION = 'ASC' | 'DESC';
type NULL_OPTION = 'NULLS FIRST' | 'NULLS LAST';
type PAGINATION = { limit: number; offset?: number };
type ColumnRef = `${string}.${string}`;
type BaseColumn = ColumnRef;
type TargetColumn = ColumnRef;
type JOIN_COND = Record<BaseColumn, TargetColumn>;
type OTHER_JOIN<T extends TABLE_JOIN_TYPE> = {
  type: T;
  model: DBModel;
  /**
   * {baseColumn:joinColumn} or {baseColumn:joinColumn, baseColumn2:joinColumn2}
   */
  on: JOIN_COND;
  alias?: string;
};
type JOIN<T extends TABLE_JOIN_TYPE> = {
  type: T;
  tableName?: string;
  model?: DBModel;
  on: JOIN_COND;
  alias?: string;
};
type SELF_JOIN<T extends TABLE_JOIN_TYPE> = {
  type: T;
  alias?: string;
  on: JOIN_COND;
};
type CROSS_JOIN<T extends TABLE_JOIN_TYPE> = {
  type: T;
  alias?: string;
  model: DBModel;
};

type TABLE_JOIN<T extends TABLE_JOIN_TYPE = TABLE_JOIN_TYPE> = T extends 'SELF'
  ? SELF_JOIN<T>
  : T extends 'CROSS'
    ? CROSS_JOIN<T>
    : OTHER_JOIN<T>;

type ORDER_BY = Record<
  string,
  | ORDER_OPTION
  | { order: ORDER_OPTION; nullOption?: NULL_OPTION; fn?: FieldFunctionType }
>;

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

type OP_KEYS = keyof typeof OP;

type OP_KEYS_WITHOUT_AND_OR = Exclude<OP_KEYS, '$and' | '$or'>;

type Condition<Key extends OP_KEYS_WITHOUT_AND_OR = OP_KEYS_WITHOUT_AND_OR> =
  Key extends 'in'
    ? { in: Primitive[] }
    : Key extends 'between'
      ? { between: Primitive[] }
      : Key extends 'isNull'
        ? { isNull: null }
        : Key extends 'notNull'
          ? { notNull: null }
          :
              | {
                  [key in Exclude<
                    OP_KEYS_WITHOUT_AND_OR,
                    'in' | 'between' | 'isNull' | 'notNull'
                  >]?: Primitive;
                }
              | Primitive;

type WhereClause =
  | {
      [column: string]: Condition;
    }
  | {
      $and: WhereClause[];
    }
  | {
      $or: WhereClause[];
    };

type WhereClauseKeys = '$and' | '$or' | string;

type ForeignKeyActions =
  (typeof foreignKeyActions)[keyof typeof foreignKeyActions];

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

/**
 * Different Flavours
 * 1. {columnName:null} - return column name as define in columnKey
 * 2.{columnName:aliasName} - return column name as define in columnValue
 */
type FindQueryAttributes = Record<string, null | string>;

type QueryParams = {
  attributes?: FindQueryAttributes;
  isDistinct?: boolean;
  orderBy?: ORDER_BY;
  groupBy?: string[];
  where?: WhereClause;
  limit?: PAGINATION;
  alias?: string;
  join?: TABLE_JOIN[];
  having?: WhereClause;
};

//============================================= TYPES ======================================================//

//============================================= HELPERS ===================================================//

const isPrimitiveValue = (value: Primitive) => {
  return (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  );
};

const errorHandler = (query: string, error: Error) => {
  const msg = `Error executing query: "${query}". Error: ${error.message}`;
  const err = new Error(msg);
  throw err;
};

const fieldFunctionCreator = (
  field: string,
  functionName: FieldFunctionType,
  alias?: string,
) => {
  const func = fieldFunctionName[functionName];
  if (!func) {
    throw new Error(
      `Invalid function name "${functionName}". Valid functions are: ${Object.keys(fieldFunctionName).join(', ')}.`,
    );
  }
  const aliasMaybe = alias ? ` ${alias}` : '';
  return `${func}(${field})${aliasMaybe}`;
};

const fieldFunc = (fn: FieldFunctionType, column: string) => {
  const func = fieldFunctionName[fn];
  if (!func) {
    throw new Error(
      `Invalid function name "${fn}". Valid functions are: ${Object.keys(fieldFunctionName).join(', ')}.`,
    );
  }
  return fnJoiner.joinFnAndColumn(func, column);
};

const quote = (str: string) => `${String(str).replace(/"/g, '""')}`;

const validateField = (field: string, allowed: Set<string>) => {
  if (!allowed.has(field)) {
    throw new Error(
      `Invalid column name ${field}. Allowed Column names are: ${Array.from(
        allowed,
      ).join(', ')}.`,
    );
  }
};

const FieldQuote = (allowedFields: Set<string>, str: string) => {
  validateField(str, allowedFields);
  return quote(str);
};

const aliasFieldNames = (names: Set<string>, alias?: string) => {
  if (!alias) return [];
  return Array.from(names).map((name) => `${alias}.${name}`);
};

const addJoinModelFields = <T extends TABLE_JOIN_TYPE>(
  joinType: OTHER_JOIN<T>,
  modelFields: string[],
) => {
  const { model, alias } = joinType;
  const tableNames = (model as any).tableColumns;
  const aliasTableNames = aliasFieldNames(tableNames, alias);
  modelFields.push(...tableNames, ...aliasTableNames);
};

const createPlaceholder = (val: number) => {
  return `$${val}`;
};

const joinTableCond = (cond: JOIN_COND, allowedFields: Set<string>) =>
  Object.entries(cond)
    .map(
      ([baseColumn, joinColumn]) =>
        `${FieldQuote(allowedFields, baseColumn)} ${OP.eq} ${FieldQuote(allowedFields, joinColumn)}`,
    )
    .join(` ${OP.$and} `);

//============================================= HELPERS ===================================================//

//============================================= DBQuery ===================================================//

export class DBQuery {
  static tableName: string = '';
  static tableColumns: Set<string> = new Set();
  static #groupByFields: Set<string> = new Set();

  static async findAll(queryParams?: QueryParams) {
    const {
      attributes,
      isDistinct,
      orderBy,
      where,
      limit,
      alias,
      join,
      groupBy,
      having,
    } = queryParams || {};
    const distinctMaybe = isDistinct ? `${dbKeywords.distinct} ` : '';
    const allowedFields = DBQuery.#getAllowedFields(
      this.tableColumns,
      alias,
      join,
    );
    const colStr = DBQuery.#getSelectColumns(allowedFields, attributes);
    const { statement: whereStatement, values: whereValues } =
      DBQuery.#prepareFilterStatement(allowedFields, where);
    const orderStr = DBQuery.#prepareOrderByStatement(allowedFields, orderBy);
    const limitStr = DBQuery.#preparePaginationStatement(limit);
    const joinStr = DBQuery.#prepareTableJoin(
      this.tableName,
      allowedFields,
      join,
    );
    const groupByStr = DBQuery.#prepareGroupByStatement(allowedFields, groupBy);
    const { statement: havingStatement, values: havingValues } =
      DBQuery.#prepareFilterStatement(allowedFields, having, {
        initialIndex: whereValues.length,
        isHavingFilter: true,
      });
    const variableQry = DBQuery.#prepareVariableQry(
      whereStatement,
      orderStr,
      limitStr,
      joinStr,
      groupByStr,
      havingStatement,
    );
    const tableAlias = alias ? ` ${dbKeywords.as} ${alias}` : '';
    const rawQry = `${dbKeywords.select} ${distinctMaybe}${colStr} ${dbKeywords.from} "${this.tableName}"${tableAlias}${variableQry}`;
    const findAllQuery = `${rawQry.trimEnd()};`;
    try {
      const values = [...whereValues, ...havingValues];
      const result = await query(findAllQuery, values);
      return { rows: result.rows, count: result.rowCount };
    } catch (error) {
      return errorHandler(findAllQuery, error as Error);
    }
  }

  static async create(
    fields: Record<string, Primitive>,
    returnOnly?: FindQueryAttributes,
  ) {
    const keys: string[] = [];
    const values: Primitive[] = [];
    const valuePlaceholder: string[] = [];
    const allowedFields = DBQuery.#getAllowedFields(this.tableColumns);
    const returnStr = DBQuery.#getSelectColumns(
      allowedFields,
      returnOnly,
      false,
    );
    Object.entries(fields).forEach((entry, index) => {
      const [key, value] = entry;
      keys.push(FieldQuote(allowedFields, key));
      values.push(value);
      valuePlaceholder.push(createPlaceholder(index + 1));
    });
    const columns = keys.toString();
    const valuePlaceholders = valuePlaceholder.toString();
    const rawQry = `${dbKeywords.insertInto} "${this.tableName}"(${columns}) ${dbKeywords.values}(${valuePlaceholders}) ${dbKeywords.returning} ${returnStr}`;
    const createQry = `${rawQry.trimEnd()};`;
    try {
      const result = await query(createQry, values);
      return { rows: result.rows, count: result.rowCount };
    } catch (error) {
      return errorHandler(createQry, error as Error);
    }
  }

  static async createBulk(
    columns: Array<string>,
    values: Array<Array<Primitive>>,
    returnOnly?: FindQueryAttributes,
  ) {
    const flatedValues: Primitive[] = [];
    const allowedFields = DBQuery.#getAllowedFields(this.tableColumns);
    const returnStr = DBQuery.#getSelectColumns(
      allowedFields,
      returnOnly,
      false,
    );
    let incrementBy = 1;
    const valuePlaceholder = values.map((val, pIndex) => {
      if (val.length !== columns.length) {
        throw new Error(
          `Invalid value length at index ${pIndex}. Expected ${columns.length} values, but got ${val.length}.`,
        );
      }
      if (pIndex > 0) {
        incrementBy += val.length - 1;
      }
      flatedValues.push(...val);
      const placeholder = val
        .map((_, cIndex) => createPlaceholder(pIndex + cIndex + incrementBy))
        .join(',');
      return `(${placeholder})`;
    });
    const colStr = columns.toString();
    const valuePlaceholders = valuePlaceholder.toString();
    const rawQry = `${dbKeywords.insertInto} "${this.tableName}"(${colStr}) ${dbKeywords.values}${valuePlaceholders} ${dbKeywords.returning} ${returnStr}`;
    const createQry = `${rawQry.trimEnd()};`;
    try {
      const result = await query(createQry, flatedValues);
      return { rows: result.rows, count: result.rowCount };
    } catch (error) {
      return errorHandler(createQry, error as Error);
    }
  }

  // Private Methods
  static #prepareVariableQry(
    whereQry?: string,
    orderbyQry?: string,
    limitQry?: string,
    joinStr?: string,
    groupByStr?: string,
    havingStatement?: string,
  ) {
    let variableQry = '';
    if (joinStr) {
      variableQry += ' ' + joinStr;
    }
    if (whereQry) {
      variableQry += ' ' + whereQry;
    }
    if (groupByStr) {
      variableQry += ' ' + groupByStr;
    }
    if (havingStatement) {
      variableQry += ' ' + havingStatement;
    }
    if (orderbyQry) {
      variableQry += ' ' + orderbyQry;
    }
    if (limitQry) {
      variableQry += ' ' + limitQry;
    }
    return variableQry;
  }
  static #getSelectColumns(
    allowedFields: Set<string>,
    attributes?: FindQueryAttributes,
    isAggregateAllowed = true,
  ) {
    if (!attributes || Object.keys(attributes).length < 1) return '*';
    return Object.entries(attributes)
      .map((attr) => {
        const [col, value] = attr;
        const [column, fn] = fnJoiner.sepFnAndColumn(col);
        let validCol = FieldQuote(allowedFields, column);
        if (!isAggregateAllowed && fn) {
          throw new Error(
            `Aggregate functions are not allowed in this context. Found "${fn}" for column "${column}".`,
          );
        }
        if (fn) {
          validCol = fieldFunctionCreator(validCol, fn as FieldFunctionType);
        }
        if (value === null) {
          return validCol;
        } else if (typeof value === 'string') {
          allowedFields.add(value);
          return `${validCol} ${dbKeywords.as} ${quote(value)}`;
        }
        return null;
      })
      .filter(Boolean)
      .join(', ');
  }
  static #prepareOrderByStatement(
    allowedFields: Set<string>,
    orderBy?: ORDER_BY,
  ) {
    if (!orderBy || Object.keys(orderBy).length < 1) return '';
    let orderByStatemnt = dbKeywords.orderBy + ' ';
    orderByStatemnt += Object.entries(orderBy)
      .map(([key, val]) => {
        const validKey = FieldQuote(allowedFields, key);
        if (typeof val === 'string') {
          return `${validKey} ${val}`;
        }
        if (typeof val === 'object' && val !== null) {
          const { order, nullOption, fn } = val;
          if (!order) {
            throw new Error(`Order option is required for column "${key}".`);
          }
          let orderStr = `${validKey} ${order}`;
          if (fn) {
            orderStr = fieldFunctionCreator(validKey, fn) + ` ${order}`;
          }
          if (nullOption) {
            orderStr += ` ${nullOption}`;
          }
          return orderStr;
        }
      })
      .filter(Boolean)
      .join(', ');

    return orderByStatemnt;
  }

  static #prepareGroupByStatement(
    allowedFields: Set<string>,
    groupBy?: string[],
  ) {
    DBQuery.#groupByFields.clear();
    if (!groupBy || (Array.isArray(groupBy) && groupBy.length < 1)) return '';
    let groupByStatemnt = dbKeywords.groupBy + ' ';
    groupByStatemnt += groupBy
      .map((key) => {
        const validKey = FieldQuote(allowedFields, key);
        DBQuery.#groupByFields.add(validKey);
        return validKey;
      })
      .join(', ');
    return groupByStatemnt;
  }

  static #prepareFilterStatement(
    allowedFields: Set<string>,
    filter?: WhereClause,
    options?: { initialIndex?: number; isHavingFilter?: boolean },
  ) {
    if (!filter) return { statement: '', values: [] };
    const { initialIndex = 0, isHavingFilter = false } = options || {};
    let queryStatement =
      (isHavingFilter ? dbKeywords.having : dbKeywords.where) + ' ';
    const values: Primitive[] = [];
    const indexAndIncremntBy = { index: 0, incremntBy: 1 };
    queryStatement += Object.entries(filter)
      .map((filter, indx) => {
        indexAndIncremntBy.index = indx + initialIndex;
        return DBQuery.#getQueryStatement(
          allowedFields,
          filter,
          indexAndIncremntBy,
          values,
          isHavingFilter,
        );
      })
      .join(` ${OP.$and} `);
    return { statement: queryStatement, values };
  }

  static #getQueryStatement(
    allowedFields: Set<string>,
    singleQry: [WhereClauseKeys, any],
    index: PlaceholderRef,
    valuesArr: Primitive[],
    isHavingFilter: boolean,
  ): string {
    const key = singleQry[0];
    let value = singleQry[1];
    if (isPrimitiveValue(value)) {
      value = { eq: value };
    }
    if (key === '$and' || key === '$or') {
      if (!Array.isArray(value)) {
        throw new Error(
          `For operator "${key}" value should be an array of conditions.`,
        );
      }
      if (value.length < 2) {
        throw new Error(
          `For operator "${key}" at least 2 conditions are required.`,
        );
      }
      const cond = value
        .map((v, i) => {
          const entries = Object.entries(v);
          if (i > 0) {
            index.incremntBy += 1;
          }
          return entries.map((filter) => {
            return DBQuery.#getQueryStatement(
              allowedFields,
              filter,
              index,
              valuesArr,
              isHavingFilter,
            );
          });
        })
        .join(` ${OP[key]} `);
      return cond ? `(${cond})` : '';
    } else {
      return DBQuery.#buildCondition(
        key,
        value,
        valuesArr,
        allowedFields,
        index,
        isHavingFilter,
      );
    }
  }

  static #buildCondition(
    key: string,
    value: Record<OP_KEYS_WITHOUT_AND_OR, Primitive>,
    valuesArr: Primitive[],
    allowedFields: Set<string>,
    index: PlaceholderRef,
    isHavingFilter: boolean,
  ) {
    let validKey: string;
    if (isHavingFilter) {
      const [k, fn] = fnJoiner.sepFnAndColumn(key);
      if (!fn && !DBQuery.#groupByFields.has(k)) {
        throw new Error(
          `Invalid column "${k}" for HAVING clause. Column should be part of GROUP BY or an aggregate function.`,
        );
      }
      validKey = FieldQuote(allowedFields, k);
      if (fn) {
        validKey = fieldFunctionCreator(validKey, fn as FieldFunctionType);
      }
    } else {
      validKey = FieldQuote(allowedFields, key);
    }
    const preparePlachldrForArray = (values: Primitive[]) => {
      const placeholderArr = values.map((val, i) => {
        if (i > 0) {
          index.incremntBy += 1;
        }
        valuesArr.push(val);
        return createPlaceholder(index.index + index.incremntBy);
      });
      return placeholderArr;
    };
    const prepareQry = (entry: [string, Primitive], i: number) => {
      const [op, val] = entry as [OP_KEYS_WITHOUT_AND_OR, Primitive];
      const operation = OP[op];
      if (!operation) {
        throw new Error(
          `Invalid operator "${op}". Please use following operators: ${validOperations}. `,
        );
      }
      if (i > 0) {
        index.incremntBy += 1;
      }
      const valPlaceholder = createPlaceholder(index.index + index.incremntBy);
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
          valuesArr.push(val);
          return `${validKey} ${operation} ${valPlaceholder}`;
        }
        case 'notNull':
        case 'isNull': {
          index.incremntBy -= 1;
          return `${key} ${operation} ${dbKeywords.null}`;
        }
        case 'startsWith':
        case 'iStartsWith': {
          valuesArr.push(`${val}%`);
          return `${validKey} ${operation} ${valPlaceholder}`;
        }
        case 'endsWith':
        case 'iEndsWith': {
          valuesArr.push(`%${val}`);
          return `${validKey} ${operation} ${valPlaceholder}`;
        }
        case 'substring':
        case 'iSubstring': {
          valuesArr.push(`%${val}%`);
          return `${validKey} ${operation} ${valPlaceholder}`;
        }
        case 'in':
        case 'notIn': {
          if (!Array.isArray(val)) {
            throw new Error(`For operator "${op}" value should be array.`);
          }
          if (val.length < 1) {
            throw new Error(`Operator "${op}" requires at least 1 value.`);
          }
          const placeholders = preparePlachldrForArray(val);
          return `${validKey} ${operation} (${placeholders.toString()})`;
        }
        case 'between':
        case 'notBetween': {
          if (!Array.isArray(val)) {
            throw new Error(`For operator "${op}" value should be array.`);
          }
          if (val.length !== 2) {
            throw new Error(`Operator "${op}" requires exactly 2 values.`);
          }
          const placeholders = preparePlachldrForArray(val);
          return `${validKey} ${operation} ${placeholders[0]} ${OP.$and} ${placeholders[1]}`;
        }
        default:
          throw new Error(
            `Invalid operator "${op}". Please use following operators: ${validOperations}. `,
          );
      }
    };
    const cond = Object.entries(value).map(prepareQry).join(` ${OP.$and} `);
    return cond ? `(${cond})` : '';
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

  static #prepareTableJoin(
    selfModelName: string,
    allowedFields: Set<string>,
    include?: TABLE_JOIN[],
  ) {
    if (!include || include.length < 1) {
      return '';
    }
    const joins = include.map((joinType) => {
      switch (joinType.type) {
        case 'SELF': {
          const { type, on, alias } = joinType;
          const updatedInclude = {
            type,
            tableName: selfModelName,
            on,
            alias,
          };
          return DBQuery.#prepareJoinStr(allowedFields, updatedInclude);
        }
        case 'INNER':
        case 'FULLOUTER':
        case 'LEFT':
        case 'RIGHT':
          return DBQuery.#prepareJoinStr(allowedFields, joinType);
        case 'CROSS': {
          const { type, model, alias } = joinType;
          const updatedInclude = {
            type,
            model,
            on: {},
            alias,
          };
          return DBQuery.#prepareJoinStr(allowedFields, updatedInclude);
        }
        default:
          throw new Error(
            `Invalid join type:"${(include as any).type}". Valid join types:${Object.keys(tableJoin).toString()}.`,
          );
      }
    });
    return joins.join(' ');
  }
  static #prepareJoinStr<T extends TABLE_JOIN_TYPE>(
    allowedFields: Set<string>,
    joinType: JOIN<T>,
  ) {
    const { type, model, on, tableName: name, alias } = joinType;
    const joinName = tableJoin[type];
    if (!joinName) {
      throw new Error(
        `Invalid join type:"${type}". Valid join types:${Object.keys(tableJoin).toString()}.`,
      );
    }
    if (!name && !model) {
      throw new Error('DBModel child is required for join.');
    }
    const tableName = name || (model as any).tableName;
    const onStr = type === 'CROSS' ? 'true' : joinTableCond(on, allowedFields);
    const aliasMaybe = alias
      ? ` ${dbKeywords.as} ${alias} ${dbKeywords.on} `
      : ` ${dbKeywords.on} `;
    return `${joinName} ${tableName}${aliasMaybe}${onStr}`;
  }

  static #getAllowedFields(
    selfAllowedFields: Set<string>,
    alias?: string,
    include?: TABLE_JOIN[],
  ) {
    const modelFields: string[] = [
      ...selfAllowedFields,
      ...aliasFieldNames(selfAllowedFields, alias),
    ];
    if (include && Array.isArray(include) && include.length > 0) {
      include.forEach((joinType) => {
        const { type } = joinType;
        switch (type) {
          case 'INNER':
          case 'LEFT':
          case 'RIGHT':
          case 'FULLOUTER': {
            addJoinModelFields(joinType, modelFields);
            break;
          }
          case 'CROSS': {
            const { model, alias } = joinType;
            addJoinModelFields({ model, on: {}, alias, type }, modelFields);
            break;
          }
        }
      });
    }
    return new Set(modelFields);
  }
}

//============================================= DBQuery ===================================================//

//============================================= DBModel ===================================================//

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
