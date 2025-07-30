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

const setOperation = {
  UNION: 'UNION',
  UNION_ALL: 'UNION ALL',
  INTERSECT: 'INTERSECT',
  EXCEPT: 'EXCEPT',
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
  text: 'TEXT',
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
    const valueStr = attachArrayWithComaSep(values.map((v) => `'${v}'`));
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
  any: 'ANY',
  array: 'ARRAY',
  all: 'ALL',
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
  $exists: 'EXISTS',
  $notExists: 'NOT EXISTS',
  $and: 'AND',
  $or: 'OR',
} as const;
const validOperations = Object.keys(OP).join(', ');

const conditionalOperator = new Set(['$or', '$and'] as const);
const subqueryOperator = new Set(['$exists', '$notExists'] as const);

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

type Primitive = string | number | boolean | null;
type PreparedValues = { index: number; values: Primitive[] };
type SubqueryWhereReq = 'WhereReq' | 'WhereNotReq';

type FieldFunctionType = keyof typeof fieldFunctionName;

type TABLE_JOIN_TYPE = keyof typeof tableJoin;

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

type SIMPLE_OP_KEYS = Exclude<
  OP_KEYS,
  '$and' | '$or' | '$exists' | '$notExists'
>;

type SubQueryFilterKey = (typeof dbKeywords)['any' | 'all'];
type SubQueryFilterRecord = {
  [key in SubQueryFilterKey]?: Array<Primitive> | SubQueryFilter;
};
type FilterColumnValue = Primitive | SubQueryFilterRecord;

type ExistsFilter<T extends SubqueryWhereReq = 'WhereNotReq'> = {
  model: DBQuery;
  alias?: string;
} & Subquery<T>;
type SubQueryFilter<T extends SubqueryWhereReq = 'WhereNotReq'> =
  ExistsFilter<T> & {
    orderBy?: ORDER_BY;
    column: string;
    isDistinct?: boolean;
  };

type InOperationSubQuery = SubQueryFilter & {
  isDistinct?: boolean;
};

type ConditionMap = {
  in: Primitive[] | InOperationSubQuery;
  notIn: Primitive[] | InOperationSubQuery;
  between: Primitive[];
  notBetween: Primitive[];
  isNull: null;
  notNull: null;
};

type NormalOperators =
  | {
      [key in Exclude<SIMPLE_OP_KEYS, keyof ConditionMap>]?: FilterColumnValue;
    }
  | FilterColumnValue;

type Condition<Key extends SIMPLE_OP_KEYS = SIMPLE_OP_KEYS> =
  Key extends keyof ConditionMap
    ? { [K in Key]: ConditionMap[K] }
    : NormalOperators;

type WhereClause =
  | {
      [column: string]: Condition;
    }
  | {
      $and: WhereClause[];
    }
  | {
      $or: WhereClause[];
    }
  | { $exists: ExistsFilter<'WhereReq'> }
  | { $notExists: ExistsFilter<'WhereReq'> };

type Subquery<T extends SubqueryWhereReq = 'WhereNotReq'> =
  (T extends 'WhereReq'
    ? { where: WhereClause }
    : {
        where?: WhereClause;
      }) & {
    groupBy?: string[];
    limit?: PAGINATION['limit'];
    offset?: PAGINATION['offset'];
    join?: TABLE_JOIN[];
    having?: WhereClause;
  };

type SelectQuery = {
  columns?: FindQueryAttributes;
  isDistinct?: boolean;
  alias?: AliasSubType;
};
type SetQuery = { type: SetOperationType } & SetOperationFilter;
type AliasSubType = string | ({ as: string } & SetOperationFilter);
type SetOperationFilter = {
  model: DBQuery;
  alias?: AliasSubType;
  columns?: FindQueryAttributes;
  orderBy?: ORDER_BY;
  set?: SetQuery;
} & Subquery<'WhereNotReq'>;

type SetOperationType = keyof typeof setOperation;
type WhereClauseKeys = '$and' | '$or' | string;

type ForeignKeyActions =
  (typeof foreignKeyActions)[keyof typeof foreignKeyActions];

type ReferenceTable = {
  parentColumn: string | string[];
  column: string | string[];
  constraintName?: string;
  onDelete?: ForeignKeyActions;
  onUpdate?: ForeignKeyActions;
};

type Reference = {
  [parentTable in string]: ReferenceTable;
};
type ExtraOptions = {
  tableName: string;
  reference?: Reference;
};

/**
 * Different Flavours
 * 1. {columnName:null} - return column name as define in columnKey
 * 2.{columnName:aliasName} - return column name as define in columnValue
 */
type FindQueryAttributes = Record<string, null | string>;

type QueryParams = SelectQuery &
  Subquery & {
    orderBy?: ORDER_BY;
    set?: SetQuery;
  };

//============================================= TYPES ======================================================//

//============================================= HELPERS ===================================================//

const isPrimitiveValue = (value: Primitive) => {
  return (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    value === null
  );
};

const attachArrayWithSep = (array: Array<Primitive>, sep: string) =>
  array.join(sep);

const attachArrayWithSpaceSep = (array: Array<Primitive>) =>
  attachArrayWithSep(array, ' ');

const attachArrayWithComaSep = (array: Array<Primitive>) =>
  attachArrayWithSep(array, ',');

const attachArrayWithAndSep = (array: Array<Primitive>) =>
  attachArrayWithSep(array, ` ${OP.$and} `);

const attachArrayWithComaAndSpaceSep = (array: Array<Primitive>) =>
  attachArrayWithSep(array, ', ');

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
      `Invalid function name "${functionName}". Valid functions are: ${attachArrayWithComaAndSpaceSep(Object.keys(fieldFunctionName))}.`,
    );
  }
  const aliasMaybe = alias ? ` ${alias}` : '';
  return `${func}(${field})${aliasMaybe}`;
};

const fieldFunc = (fn: FieldFunctionType, column: string) => {
  const func = fieldFunctionName[fn];
  if (!func) {
    throw new Error(
      `Invalid function name "${fn}". Valid functions are: ${attachArrayWithComaAndSpaceSep(Object.keys(fieldFunctionName))}.`,
    );
  }
  return fnJoiner.joinFnAndColumn(func, column);
};

const quote = (str: string) => `${String(str).replace(/"/g, '""')}`;

const validateField = (field: string, allowed: Set<string>) => {
  if (!allowed.has(field)) {
    throw new Error(
      `Invalid column name ${field}. Allowed Column names are: ${attachArrayWithComaAndSpaceSep(
        Array.from(allowed),
      )}.`,
    );
  }
};

const FieldQuote = (allowedFields: Set<string>, str: string) => {
  validateField(str, allowedFields);
  return quote(str);
};

const aliasFieldNames = (names: Set<string>, alias?: AliasSubType) => {
  alias =
    typeof alias === 'object' && alias !== null && alias.as ? alias.as : alias;
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
  attachArrayWithAndSep(
    Object.entries(cond).map(
      ([baseColumn, joinColumn]) =>
        `${FieldQuote(allowedFields, baseColumn)} ${OP.eq} ${FieldQuote(allowedFields, joinColumn)}`,
    ),
  );

const getPreparedValues = (
  preparedValues: PreparedValues,
  value: Primitive,
) => {
  const placeholder = createPlaceholder(preparedValues.index + 1);
  preparedValues.values[preparedValues.index] = value;
  preparedValues.index++;
  return placeholder;
};

const prepareColumnForHavingClause = (
  key: string,
  groupByFields: Set<string>,
  allowedFields: Set<string>,
  isHavingFilter: boolean,
) => {
  let validKey: string;
  if (isHavingFilter) {
    const [k, fn] = fnJoiner.sepFnAndColumn(key);
    if (!fn && !groupByFields.has(k)) {
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
  return validKey;
};
const getArrayDataType = (value: Primitive[]) => {
  const firstValue = value[0];
  if (typeof firstValue === 'number') {
    return DataTypes.int;
  } else if (typeof firstValue === 'string') {
    return DataTypes.text;
  } else if (typeof firstValue === 'boolean') {
    return DataTypes.boolean;
  } else {
    throw new Error(`Unsupported data type for array: ${typeof firstValue}`);
  }
};

const getAnyAndAllFilterValue = (val: any, op: string) => {
  if (typeof val !== 'object' || val === null) {
    throw new Error(
      `For operator "${op}" with ANY/ALL, value must be an object containing "${dbKeywords.any}" or "${dbKeywords.all}" property.`,
    );
  }
  const hasAny = (val as any).hasOwnProperty(dbKeywords.any);
  const hasAll = (val as any).hasOwnProperty(dbKeywords.all);
  if (!hasAny && !hasAll) {
    throw new Error(
      `For subquery operations, value must contain "${dbKeywords.any}" or "${dbKeywords.all}" property`,
    );
  }
  const subqueryKeyword = hasAll ? dbKeywords.all : dbKeywords.any;
  const subqueryVal: Array<Primitive> | SubQueryFilter = (val as any)[
    subqueryKeyword
  ];

  return { key: subqueryKeyword, value: subqueryVal };
};

const checkPrimitiveValueForOp = (op: string, value: Primitive) => {
  if (!isPrimitiveValue(value)) {
    throw new Error(`For operator "${op}" value should be a primitive type.`);
  }
};

const preparePlachldrForArray = (
  values: Primitive[],
  preparedValues: PreparedValues,
) => {
  const placeholderArr = values.map((val) => {
    const placeholder = getPreparedValues(preparedValues, val);
    return placeholder;
  });
  return placeholderArr;
};

const prepareQryForPrimitiveOp = (
  preparedValues: PreparedValues,
  key: string,
  operation: string,
  value: Primitive,
) => {
  const valPlaceholder = getPreparedValues(preparedValues, value);
  return `${key} ${operation} ${valPlaceholder}`;
};

const prepareFinalFindQry = (
  selectQry: string,
  setQry?: string,
  subQry?: string,
) => {
  const rowQueries: string[] = [];
  if (setQry && subQry) {
    const firstQry = `${dbKeywords.select} * ${dbKeywords.from} (${selectQry} ${setQry}) ${dbKeywords.as} results`;
    rowQueries.push(firstQry);
    rowQueries.push(subQry);
  } else if (setQry && !subQry) {
    rowQueries.push(selectQry);
    rowQueries.push(setQry);
  } else if (!setQry && subQry) {
    rowQueries.push(selectQry);
    rowQueries.push(subQry);
  }
  return attachArrayWithSpaceSep(rowQueries);
};

//============================================= HELPERS ===================================================//

//============================================= DBQuery ===================================================//

export class DBQuery {
  static tableName: string = '';
  static tableColumns: Set<string> = new Set();
  static #groupByFields: Set<string> = new Set();

  static async findAll(queryParams?: QueryParams) {
    const { columns, isDistinct, orderBy, alias, set, ...rest } =
      queryParams || {};
    const preparedValues: PreparedValues = { index: 0, values: [] };
    const allowedFields = DBQuery.#getAllowedFields(
      this.tableColumns,
      alias,
      rest.join,
    );
    const selectQry = DBQuery.#prepareSelectQuery(
      this.tableName,
      allowedFields,
      {
        columns,
        isDistinct,
        alias,
      },
    );
    const subQry = DBQuery.#prepareSubquery(
      allowedFields,
      preparedValues,
      rest,
      orderBy,
    );
    const setQry = DBQuery.#prepareSetQuery(preparedValues, set);
    const findAllQuery = prepareFinalFindQry(selectQry, setQry, subQry);
    try {
      const result = await query(findAllQuery, preparedValues.values);
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
    const valuePlaceholder: string[] = [];
    const allowedFields = DBQuery.#getAllowedFields(this.tableColumns);
    const returnStr = DBQuery.#getSelectColumns(
      allowedFields,
      returnOnly,
      false,
    );
    const preparedValues: PreparedValues = { index: 0, values: [] };
    Object.entries(fields).forEach((entry) => {
      const [key, value] = entry;
      keys.push(FieldQuote(allowedFields, key));
      const placeholder = getPreparedValues(preparedValues, value);
      valuePlaceholder.push(placeholder);
    });
    const columns = attachArrayWithComaSep(keys);
    const valuePlaceholders = attachArrayWithComaSep(valuePlaceholder);
    const insertClause = `${dbKeywords.insertInto} "${this.tableName}"(${columns})`;
    const valuesClause = `${dbKeywords.values}${valuePlaceholders}`;
    const returningClause = `${dbKeywords.returning} ${returnStr}`;
    const createQry = attachArrayWithSpaceSep([
      insertClause,
      valuesClause,
      returningClause,
    ]);
    try {
      const result = await query(createQry, preparedValues.values);
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
      const placeholder = attachArrayWithComaSep(
        val.map((_, cIndex) =>
          createPlaceholder(pIndex + cIndex + incrementBy),
        ),
      );

      return `(${placeholder})`;
    });
    const colStr = attachArrayWithComaSep(columns);
    const valuePlaceholders = attachArrayWithComaSep(valuePlaceholder);
    const insertClause = `${dbKeywords.insertInto} "${this.tableName}"(${colStr})`;
    const valuesClause = `${dbKeywords.values}${valuePlaceholders}`;
    const returningClause = `${dbKeywords.returning} ${returnStr}`;
    const createQry = attachArrayWithSpaceSep([
      insertClause,
      valuesClause,
      returningClause,
    ]);
    try {
      const result = await query(createQry, flatedValues);
      return { rows: result.rows, count: result.rowCount };
    } catch (error) {
      return errorHandler(createQry, error as Error);
    }
  }

  // Private Methods
  static #prepareVariableQry(params: {
    whereQry?: string;
    orderbyQry?: string;
    limitQry?: string;
    joinQry?: string;
    groupByQry?: string;
    havingQry?: string;
  }) {
    const { whereQry, orderbyQry, limitQry, joinQry, groupByQry, havingQry } =
      params;
    const variableQry: string[] = [];
    if (joinQry) {
      variableQry.push(joinQry);
    }
    if (whereQry) {
      variableQry.push(whereQry);
    }
    if (groupByQry) {
      variableQry.push(groupByQry);
    }
    if (havingQry) {
      variableQry.push(havingQry);
    }
    if (orderbyQry) {
      variableQry.push(orderbyQry);
    }
    if (limitQry) {
      variableQry.push(limitQry);
    }
    return attachArrayWithSpaceSep(variableQry);
  }

  static #prepareSubquery(
    allowedFields: Set<string>,
    preparedValues: PreparedValues,
    subQuery: Subquery,
    orderBy?: ORDER_BY,
  ) {
    const { where, groupBy, limit, offset, join, having } = subQuery || {};
    const whereStatement = DBQuery.#prepareFilterStatement(
      allowedFields,
      preparedValues,
      where,
    );
    const limitStr = DBQuery.#preparePaginationStatement(
      preparedValues,
      limit,
      offset,
    );
    const joinStr = DBQuery.#prepareTableJoin(
      this.tableName,
      allowedFields,
      join,
    );
    const groupByStr = DBQuery.#prepareGroupByStatement(allowedFields, groupBy);
    const orderStr = DBQuery.#prepareOrderByStatement(allowedFields, orderBy);
    const havingStatement = DBQuery.#prepareFilterStatement(
      allowedFields,
      preparedValues,
      having,
      {
        isHavingFilter: true,
      },
    );
    const finalSubQry = DBQuery.#prepareVariableQry({
      whereQry: whereStatement,
      limitQry: limitStr,
      joinQry: joinStr,
      groupByQry: groupByStr,
      havingQry: havingStatement,
      orderbyQry: orderStr,
    });
    return finalSubQry;
  }

  static #prepareSelectQuery(
    tableName: string,
    allowedFields: Set<string>,
    selectQuery: SelectQuery,
  ) {
    const { isDistinct, columns, alias } = selectQuery;
    const tableAlias = alias ? ` ${dbKeywords.as} ${alias}` : '';
    const distinctMaybe = isDistinct ? `${dbKeywords.distinct} ` : '';
    const colStr = DBQuery.#getSelectColumns(allowedFields, columns);
    const selectQry = `${dbKeywords.select} ${distinctMaybe}${colStr} ${dbKeywords.from} "${tableName}"${tableAlias}`;
    return selectQry;
  }

  static #prepareSetQuery(preparedValues: PreparedValues, setQry?: SetQuery) {
    if (!setQry) {
      return '';
    }
    if (typeof setQry !== 'object' || setQry === null) {
      throw new Error(`For Set Query Operation, value must be object.`);
    }
    if (!setQry.type || !setQry.model) {
      throw new Error(
        `Set Query Operation must contain at least "type", "model", and "columns" keys.`,
      );
    }
    const { type, columns, model, orderBy, alias, set, ...rest } = setQry;
    const queries: string[] = [setOperation[type]];
    const tableName = (model as any).tableName;
    const tableColumns = (model as any).tableColumns;
    const allowedFields = DBQuery.#getAllowedFields(
      tableColumns,
      alias,
      rest.join,
    );
    const selectQry = DBQuery.#prepareSelectQuery(tableName, allowedFields, {
      columns,
      alias,
    });
    const subQry = DBQuery.#prepareSubquery(
      allowedFields,
      preparedValues,
      rest,
      orderBy,
    );
    const setSubqry = DBQuery.#prepareSetQuery(preparedValues, set);
    const rawQries = [selectQry, subQry, setSubqry].filter(Boolean);
    let q;
    if (rawQries.length > 1) {
      q = `(${attachArrayWithSpaceSep(rawQries)})`;
    } else {
      q = attachArrayWithSpaceSep(rawQries);
    }
    queries.push(q);
    return attachArrayWithSpaceSep(queries);
  }

  static #getSelectColumns(
    allowedFields: Set<string>,
    columns?: FindQueryAttributes,
    isAggregateAllowed = true,
  ) {
    if (!columns || Object.keys(columns).length < 1) return '*';
    return Object.entries(columns)
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
    const orderStatement: string[] = [dbKeywords.orderBy];
    const qry = Object.entries(orderBy)
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
    orderStatement.push(qry);
    return attachArrayWithSpaceSep(orderStatement);
  }

  static #prepareGroupByStatement(
    allowedFields: Set<string>,
    groupBy?: string[],
  ) {
    DBQuery.#groupByFields.clear();
    if (!groupBy || (Array.isArray(groupBy) && groupBy.length < 1)) return '';
    const groupStatements: string[] = [dbKeywords.groupBy];
    const qry = attachArrayWithComaSep(
      groupBy.map((key) => {
        const validKey = FieldQuote(allowedFields, key);
        DBQuery.#groupByFields.add(validKey);
        return validKey;
      }),
    );
    groupStatements.push(qry);
    return attachArrayWithSpaceSep(groupStatements);
  }

  static #prepareFilterStatement(
    allowedFields: Set<string>,
    preparedValues: PreparedValues,
    filter?: WhereClause,
    options?: { isHavingFilter?: boolean },
  ) {
    if (!filter) return '';
    const { isHavingFilter = false } = options || {};
    const filterStatements: string[] = [];
    if (isHavingFilter) {
      filterStatements.push(dbKeywords.having);
    } else {
      filterStatements.push(dbKeywords.where);
    }
    const qry = attachArrayWithAndSep(
      Object.entries(filter)
        .map((filter) => {
          return DBQuery.#getQueryStatement(
            allowedFields,
            filter,
            preparedValues,
            isHavingFilter,
          );
        })
        .filter(Boolean),
    );
    if (qry) {
      filterStatements.push(qry);
    }
    return filterStatements.length > 1
      ? attachArrayWithSpaceSep(filterStatements)
      : '';
  }

  static #otherModelSubqueryBuilder<T extends InOperationSubQuery>(
    key: string,
    preparedValues: PreparedValues,
    value: T,
    isExistsFilter: boolean = true,
  ) {
    const { model, alias, column, orderBy, isDistinct, ...rest } =
      value as InOperationSubQuery;
    if (!model) {
      throw new Error(
        `DBQuery Model is required for subquery operator "${key}".`,
      );
    }
    if (!rest.where && isExistsFilter) {
      throw new Error(
        `Where clause is required for subquery operator "${key}".`,
      );
    }
    const tableName = (model as any).tableName;
    const tableColumns = new Set((model as any).tableColumns) as Set<string>;
    if (isExistsFilter) {
      tableColumns.add('1');
    }
    const selectQuery = isExistsFilter
      ? { columns: { '1': null }, alias, isDistinct }
      : { columns: { [column]: null }, alias, isDistinct };
    const subQryAllowedFields = DBQuery.#getAllowedFields(
      tableColumns,
      alias,
      rest.join,
    );
    const selectQry = DBQuery.#prepareSelectQuery(
      tableName,
      subQryAllowedFields,
      selectQuery,
    );
    const subquery = DBQuery.#prepareSubquery(
      subQryAllowedFields,
      preparedValues,
      rest,
    );
    const operator = isExistsFilter ? OP[key as OP_KEYS] : key;
    const subQryArr: string[] = operator ? [operator] : [];
    subQryArr.push(`(${selectQry} ${subquery})`);
    return attachArrayWithSpaceSep(subQryArr);
  }

  static #andOrFilterBuilder(
    key: OP_KEYS,
    allowedFields: Set<string>,
    preparedValues: PreparedValues,
    value: any,
    isHavingFilter: boolean,
  ) {
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
    const sep = ` ${OP[key]} `;
    const cond = value
      .map((v) => {
        const entries = Object.entries(v);
        return entries.map((filter) => {
          return DBQuery.#getQueryStatement(
            allowedFields,
            filter,
            preparedValues,
            isHavingFilter,
          );
        });
      })
      .join(sep);
    return cond ? `(${cond})` : '';
  }

  static #getQueryStatement(
    allowedFields: Set<string>,
    singleQry: [WhereClauseKeys, any],
    preparedValues: PreparedValues,
    isHavingFilter: boolean,
  ): string {
    const key = singleQry[0] as OP_KEYS;
    let value = singleQry[1];
    if (isPrimitiveValue(value)) {
      value = { eq: value };
    }
    if (conditionalOperator.has(key as any)) {
      const cond = DBQuery.#andOrFilterBuilder(
        key,
        allowedFields,
        preparedValues,
        value,
        isHavingFilter,
      );
      return cond;
    } else if (subqueryOperator.has(key as any)) {
      const finalSubQuery = DBQuery.#otherModelSubqueryBuilder(
        key,
        preparedValues,
        value,
        true,
      );
      return finalSubQuery;
    } else {
      return DBQuery.#buildCondition(
        key,
        value,
        allowedFields,
        preparedValues,
        isHavingFilter,
      );
    }
  }

  static #buildQueryForSubQryOperator(
    key: string,
    baseOperation: string,
    subQryOperation: string,
    preparedValues: PreparedValues,
    value: any,
    isArrayKeywordReq: boolean = false,
  ) {
    if (Array.isArray(value)) {
      if (value.length < 1) {
        throw new Error(
          `Operator "${baseOperation}" requires at least 1 value.`,
        );
      }
      const arrayKeyword = dbKeywords.array;
      const placeholders = preparePlachldrForArray(value, preparedValues);
      const dataType = getArrayDataType(value);
      const arrayQry = isArrayKeywordReq
        ? ` (${arrayKeyword}[${attachArrayWithComaSep(placeholders)}]::${dataType}[])`
        : `(${attachArrayWithComaSep(placeholders)})`;
      return `${key} ${baseOperation} ${subQryOperation}${arrayQry}`;
    }
    if (typeof value !== 'object' || value === null) {
      throw new Error(
        `For operator "${baseOperation}", value must be an object.`,
      );
    }
    const subQry = DBQuery.#otherModelSubqueryBuilder(
      subQryOperation,
      preparedValues,
      value,
      false,
    );
    return `${key} ${baseOperation} ${subQry}`;
  }

  static #buildCondition(
    key: string,
    value: Record<SIMPLE_OP_KEYS, Primitive>,
    allowedFields: Set<string>,
    preparedValues: PreparedValues,
    isHavingFilter: boolean,
  ) {
    const validKey = prepareColumnForHavingClause(
      key,
      DBQuery.#groupByFields,
      allowedFields,
      isHavingFilter,
    );

    const prepareQry = (entry: [string, FilterColumnValue]) => {
      const [op, val] = entry as [SIMPLE_OP_KEYS, FilterColumnValue];
      const operation = OP[op];
      if (!operation) {
        throw new Error(
          `Invalid operator "${op}". Please use following operators: ${validOperations}. `,
        );
      }

      switch (op) {
        case 'eq':
        case 'neq':
        case 'gt':
        case 'gte':
        case 'lt':
        case 'lte': {
          if (isPrimitiveValue(val as any)) {
            return prepareQryForPrimitiveOp(
              preparedValues,
              validKey,
              operation,
              val as Primitive,
            );
          }
          const { key, value } = getAnyAndAllFilterValue(val, op);
          const subQry = DBQuery.#buildQueryForSubQryOperator(
            validKey,
            operation,
            key,
            preparedValues,
            value,
            true,
          );
          return subQry;
        }
        case 'like':
        case 'iLike':
        case 'notLike':
        case 'notILike': {
          checkPrimitiveValueForOp(op, val as any);
          return prepareQryForPrimitiveOp(
            preparedValues,
            validKey,
            operation,
            val as Primitive,
          );
        }
        case 'notNull':
        case 'isNull': {
          return `${key} ${operation} ${dbKeywords.null}`;
        }
        case 'startsWith':
        case 'iStartsWith': {
          checkPrimitiveValueForOp(op, val as any);
          const valStr = `${val}%`;
          return prepareQryForPrimitiveOp(
            preparedValues,
            validKey,
            operation,
            valStr,
          );
        }
        case 'endsWith':
        case 'iEndsWith': {
          checkPrimitiveValueForOp(op, val as any);
          const valStr = `%${val}`;
          return prepareQryForPrimitiveOp(
            preparedValues,
            validKey,
            operation,
            valStr,
          );
        }
        case 'substring':
        case 'iSubstring': {
          checkPrimitiveValueForOp(op, val as any);
          const valStr = `%${val}%`;
          return prepareQryForPrimitiveOp(
            preparedValues,
            validKey,
            operation,
            valStr,
          );
        }
        case 'in':
        case 'notIn': {
          const subQry = DBQuery.#buildQueryForSubQryOperator(
            validKey,
            operation,
            '',
            preparedValues,
            val,
          );
          return subQry;
        }
        case 'between':
        case 'notBetween': {
          if (!Array.isArray(val)) {
            throw new Error(`For operator "${op}" value should be array.`);
          }
          if (val.length !== 2) {
            throw new Error(`Operator "${op}" requires exactly 2 values.`);
          }
          const placeholders = preparePlachldrForArray(val, preparedValues);
          return `${validKey} ${operation} ${placeholders[0]} ${OP.$and} ${placeholders[1]}`;
        }
        default:
          throw new Error(
            `Invalid operator "${op}". Please use following operators: ${validOperations}. `,
          );
      }
    };
    const cond = attachArrayWithAndSep(Object.entries(value).map(prepareQry));
    return cond ? `(${cond})` : '';
  }

  static #preparePaginationStatement(
    preparedValues: PreparedValues,
    limit?: PAGINATION['limit'],
    offset?: PAGINATION['offset'],
  ) {
    if (!limit || typeof limit !== 'number') {
      return '';
    }

    const limitPlaceholder = getPreparedValues(preparedValues, limit);
    const limitStatements = [`${dbKeywords.limit} ${limitPlaceholder}`];
    if (offset && typeof offset === 'number') {
      const offsetPlaceholder = getPreparedValues(preparedValues, offset);
      limitStatements.push(`${dbKeywords.offset} ${offsetPlaceholder}`);
    }
    return attachArrayWithSpaceSep(limitStatements);
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
            `Invalid join type:"${(include as any).type}". Valid join types:${attachArrayWithComaSep(Object.keys(tableJoin))}.`,
          );
      }
    });
    return attachArrayWithSpaceSep(joins);
  }
  static #prepareJoinStr<T extends TABLE_JOIN_TYPE>(
    allowedFields: Set<string>,
    joinType: JOIN<T>,
  ) {
    const { type, model, on, tableName: name, alias } = joinType;
    const joinName = tableJoin[type];
    if (!joinName) {
      throw new Error(
        `Invalid join type:"${type}". Valid join types:${attachArrayWithComaSep(Object.keys(tableJoin))}.`,
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
    alias?: AliasSubType,
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
    const { tableName, reference = {} } = option;
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
    Object.entries(reference).forEach(([key, ref]) => {
      columns.push(DBModel.#createForeignColumn(key, ref));
    });
    const createEnumQryPromise = Promise.all(enums.map((e) => query(e)));
    const createTableQry = `CREATE TABLE IF NOT EXISTS "${tableName}" (${attachArrayWithComaSep(columns)});`;
    createEnumQryPromise.then(() => query(createTableQry));
  }

  static #createColumn(
    columnName: string,
    value: DbTable[keyof DbTable],
    primaryKeys: string[],
    enums: string[],
  ) {
    const values: (string | boolean)[] = [columnName];
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
            values.push(colUpr);
          } else {
            values.push(keyVale);
          }
          break;
        }
        case 'isPrimary':
          primaryKeys.push(columnName);
          break;
        case 'defaultValue':
          values.push(`${dbKeywords.default} ${keyVale}`);
          break;
        case 'unique':
          values.push(dbKeywords.unique);
          break;
        case 'notNull':
          values.push(dbKeywords.notNull);
          break;
        case 'customDefaultValue':
          values.push(`${dbKeywords.default} '${keyVale}'`);
          break;
        case 'check':
          values.push(`${dbKeywords.check} (${keyVale})`);
          break;
      }
    });
    return attachArrayWithSpaceSep(values);
  }
  static #createPrimaryColumn(primaryKeys: string[]) {
    return `${dbKeywords.primaryKey} (${attachArrayWithComaSep(primaryKeys)})`;
  }
  static #createForeignColumn(parentTable: string, ref: ReferenceTable) {
    const { parentColumn, column, constraintName, onDelete, onUpdate } = ref;
    const colStr = Array.isArray(column)
      ? attachArrayWithComaSep(column)
      : column;
    const parentColStr = Array.isArray(parentColumn)
      ? attachArrayWithComaSep(parentColumn)
      : parentColumn;
    const values: string[] = [];
    if (constraintName) {
      values.push(`${dbKeywords.constraint} ${constraintName}`);
    }
    values.push(`${dbKeywords.foreignKey} (${colStr})`);
    values.push(`${dbKeywords.references} "${parentTable}" (${parentColStr})`);
    if (onDelete) {
      values.push(`${dbKeywords.onDelete} ${onDelete}`);
    }
    if (onUpdate) {
      values.push(`${dbKeywords.onUpdate} ${onUpdate}`);
    }
    return attachArrayWithSpaceSep(values);
  }
}
