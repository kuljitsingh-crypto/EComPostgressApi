import { query } from '../models/db.config';
import { PG_DATA_TYPE } from './constants/dataTypes';
import { DB_KEYWORDS, DB_KEYWORDS_TYPE } from './constants/dbkeywords';
import { FieldFunctionType } from './constants/fieldFunctions';
import { ReferenceTable } from './constants/foreignkeyActions';
import {
  conditionalOperator,
  OP,
  OP_KEYS,
  SIMPLE_OP_KEYS,
  subqueryOperator,
  validOperations,
} from './constants/operators';
import { setOperation } from './constants/setOperations';
import {
  OTHER_JOIN,
  TABLE_JOIN_COND,
  TABLE_JOIN_TYPE,
} from './constants/tableJoin';
import { Primitive, Table, TableValues } from './globalTypes';
import {
  AliasFilter,
  AliasSubType,
  ExtraOptions,
  FilterColumnValue,
  FindQueryAttributes,
  InOperationSubQuery,
  ORDER_BY,
  PAGINATION,
  PreparedValues,
  QueryParams,
  SelectQuery,
  SetQuery,
  Subquery,
  SubQueryFilter,
  WhereClause,
  WhereClauseKeys,
} from './internalTypes';
import { errorHandler, throwError } from './methods/errorHelper';
import {
  attachArrayWith,
  fieldFunctionCreator,
  FieldQuote,
  fnJoiner,
  prepareColumnForHavingClause,
  quote,
} from './methods/helperFunction';
import { TableJoin } from './methods/tableJoin';

//============================================= CONSTANTS ===================================================//
const enumQryPrefix = `DO $$ BEGIN CREATE TYPE`;
const enumQrySuffix = `EXCEPTION WHEN duplicate_object THEN null; END $$;`;

//============================================= CONSTANTS ===================================================//

//============================================= TYPES ======================================================//

//============================================= TYPES ======================================================//

//============================================= HELPERS ===================================================//

const isPrimitiveValue = (value: Primitive | undefined) => {
  return (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    typeof value === 'undefined' ||
    value === null
  );
};

const getAliasName = <Model>(alias?: AliasSubType<Model>): string | null => {
  const aliasStr =
    typeof alias === 'object' && alias !== null && alias.as
      ? alias.as
      : typeof alias === 'string'
        ? alias
        : null;
  return aliasStr;
};

const getAliasNames = <Model>(
  aliasNames: string[],
  alias?: AliasSubType<Model>,
) => {
  if (!alias) {
    return aliasNames;
  }
  const aliasStr = getAliasName(alias);
  if (!aliasStr) {
    return aliasNames;
  }
  aliasNames.push(aliasStr);
  if (typeof alias === 'string') {
    return aliasNames;
  }
  if (alias.query && alias.query.alias) {
    return getAliasNames(aliasNames, alias.query.alias);
  }
  return aliasNames;
};

const aliasFieldNames = <Model>(
  names: Set<string>,
  alias?: AliasSubType<Model>,
) => {
  const aliasNames = getAliasNames([], alias);
  if (!aliasNames || aliasNames.length < 1) return [];
  const nameArr = Array.from(names);
  const allowedNames = aliasNames.reduce((prev, alias) => {
    prev.push(...nameArr.map((name) => `${alias}.${name}`));
    return prev;
  }, [] as string[]);
  return allowedNames;
};

const addJoinModelFields = <T extends TABLE_JOIN_TYPE>(
  joinType: OTHER_JOIN<T, DBModel>,
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

const getPreparedValues = (
  preparedValues: PreparedValues,
  value: Primitive,
) => {
  const placeholder = createPlaceholder(preparedValues.index + 1);
  preparedValues.values[preparedValues.index] = value;
  preparedValues.index++;
  return placeholder;
};

const getArrayDataType = (value: Primitive[]) => {
  const firstValue = value[0];
  if (typeof firstValue === 'number') {
    return PG_DATA_TYPE.int;
  } else if (typeof firstValue === 'string') {
    return PG_DATA_TYPE.text;
  } else if (typeof firstValue === 'boolean') {
    return PG_DATA_TYPE.boolean;
  } else {
    return throwError.invalidDataType(firstValue);
  }
};

const getAnyAndAllFilterValue = (val: any, op: string) => {
  if (typeof val !== 'object' || val === null) {
    return throwError.invalidAnyAllOpType(op);
  }
  const hasAny = (val as any).hasOwnProperty(DB_KEYWORDS.any);
  const hasAll = (val as any).hasOwnProperty(DB_KEYWORDS.all);
  if (!hasAny && !hasAll) {
    return throwError.invalidAnySubQType();
  }
  const subqueryKeyword = hasAll ? DB_KEYWORDS.all : DB_KEYWORDS.any;
  const subqueryVal: Array<Primitive> | SubQueryFilter = (val as any)[
    subqueryKeyword
  ];

  return { key: subqueryKeyword, value: subqueryVal };
};

const checkPrimitiveValueForOp = (op: string, value: Primitive) => {
  if (!isPrimitiveValue(value)) {
    return throwError.invalidOPDataType(op);
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
    const firstQry = `${DB_KEYWORDS.select} * ${DB_KEYWORDS.from} (${selectQry} ${setQry}) ${DB_KEYWORDS.as} results`;
    rowQueries.push(firstQry);
    rowQueries.push(subQry);
  } else if (setQry && !subQry) {
    rowQueries.push(selectQry);
    rowQueries.push(setQry);
  } else if (!setQry && subQry) {
    rowQueries.push(selectQry);
    rowQueries.push(subQry);
  } else {
    rowQueries.push(selectQry);
  }
  return attachArrayWith.space(rowQueries);
};
const getAliasSubqueryModel = <Model>(
  alias?: AliasSubType<Model>,
): { tableColumns: Set<string>; tableName: string } => {
  if (
    typeof alias === 'string' ||
    typeof alias === 'undefined' ||
    alias === null
  ) {
    return throwError.invalidAliasType();
  }
  if (!alias.query) {
    return throwError.invalidAliasType(true);
  }
  if (alias.query && alias.query.alias) {
    return getAliasSubqueryModel(alias.query.alias);
  }
  if (!alias.query.model) {
    throwError.invalidModelType();
  }
  return alias.query.model as any;
};

const initializeModelFields = <Model>(
  refAllowedFields: Set<string>,
  alias?: AliasSubType<Model>,
) => {
  if (typeof alias === 'object') {
    const model = getAliasSubqueryModel(alias);
    refAllowedFields = model.tableColumns;
  }
  return [...refAllowedFields, ...aliasFieldNames(refAllowedFields, alias)];
};

const getAllowedFields = <Model>(
  selfAllowedFields: Set<string>,
  alias?: AliasSubType<Model>,
  include?: TABLE_JOIN_COND<DBModel>[],
) => {
  const modelFields = initializeModelFields(selfAllowedFields, alias);
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
};

//============================================= HELPERS ===================================================//

//============================================= DBQuery ===================================================//

export class DBQuery {
  static tableName: string = '';
  static tableColumns: Set<string> = new Set();
  static #groupByFields: Set<string> = new Set();

  static async findAll<Model>(queryParams?: QueryParams<Model>) {
    const preparedValues: PreparedValues = { index: 0, values: [] };
    const findAllQuery = DBQuery.#prepareQuery(
      preparedValues,
      this.tableColumns,
      this.tableName,
      queryParams || {},
    );
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
    const allowedFields = getAllowedFields(this.tableColumns);
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
    const columns = attachArrayWith.coma(keys);
    const valuePlaceholders = attachArrayWith.coma(valuePlaceholder);
    const insertClause = `${DB_KEYWORDS.insertInto} "${this.tableName}"(${columns})`;
    const valuesClause = `${DB_KEYWORDS.values}${valuePlaceholders}`;
    const returningClause = `${DB_KEYWORDS.returning} ${returnStr}`;
    const createQry = attachArrayWith.space([
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
    const allowedFields = getAllowedFields(this.tableColumns);
    const returnStr = DBQuery.#getSelectColumns(
      allowedFields,
      returnOnly,
      false,
    );
    let incrementBy = 1;
    const valuePlaceholder = values.map((val, pIndex) => {
      if (val.length !== columns.length) {
        return throwError.invalidColumnLenType(
          pIndex,
          columns.length,
          val.length,
        );
      }
      if (pIndex > 0) {
        incrementBy += val.length - 1;
      }
      flatedValues.push(...val);
      const placeholder = attachArrayWith.coma(
        val.map((_, cIndex) =>
          createPlaceholder(pIndex + cIndex + incrementBy),
        ),
      );

      return `(${placeholder})`;
    });
    const colStr = attachArrayWith.coma(columns);
    const valuePlaceholders = attachArrayWith.coma(valuePlaceholder);
    const insertClause = `${DB_KEYWORDS.insertInto} "${this.tableName}"(${colStr})`;
    const valuesClause = `${DB_KEYWORDS.values}${valuePlaceholders}`;
    const returningClause = `${DB_KEYWORDS.returning} ${returnStr}`;
    const createQry = attachArrayWith.coma([
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
  static #prepareQuery<Model>(
    preparedValues: PreparedValues,
    refAllowedFields: Set<string>,
    tableName: string,
    qry: QueryParams<Model>,
    useOnlyRefAllowedFields = false,
  ) {
    const { columns, isDistinct, orderBy, alias, set, ...rest } = qry;
    const allowedFields = useOnlyRefAllowedFields
      ? refAllowedFields
      : getAllowedFields(refAllowedFields, alias, rest.join as any);
    const selectQry = DBQuery.#prepareSelectQuery(
      tableName,
      allowedFields,
      preparedValues,
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
    const query = prepareFinalFindQry(selectQry, setQry, subQry);
    return query;
  }
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
    return attachArrayWith.space(variableQry);
  }

  static #prepareSubquery<Model>(
    allowedFields: Set<string>,
    preparedValues: PreparedValues,
    subQuery: Subquery<Model>,
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
    const joinStr = TableJoin.prepareTableJoin(
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

  static #prepareSelectQuery<Model>(
    tableName: string,
    allowedFields: Set<string>,
    preparedValues: PreparedValues,
    selectQuery: SelectQuery<Model>,
  ) {
    const { isDistinct, columns, alias } = selectQuery;
    const distinctMaybe = isDistinct ? `${DB_KEYWORDS.distinct}` : '';
    const colStr = DBQuery.#getSelectColumns(allowedFields, columns);
    const tableAlias = DBQuery.#prepareAliasSubQuery(
      tableName,
      preparedValues,
      allowedFields,
      alias,
    );
    const queries = [
      DB_KEYWORDS.select,
      distinctMaybe,
      colStr,
      DB_KEYWORDS.from,
      tableAlias,
    ].filter(Boolean);
    const selectQry = attachArrayWith.space(queries);
    return selectQry;
  }

  static #prepareAliasSubQuery<Model extends any = any>(
    tableName: string,
    preparedValues: PreparedValues,
    allowedFields: Set<string>,
    alias?: AliasSubType<Model>,
  ): string {
    const isPrimitiveAlias =
      typeof alias === 'string' || typeof alias == 'undefined';
    if (isPrimitiveAlias) {
      if (!alias) {
        return tableName;
      }
      return attachArrayWith.space([tableName, DB_KEYWORDS.as, alias]);
    }
    if (!alias?.query) {
      return throwError.invalidAliasType(true);
    }
    const { model: m, ...rest } = (alias as any).query as AliasFilter<Model>;
    const model = getAliasSubqueryModel(alias);
    const tablName = model.tableName;
    const query = DBQuery.#prepareQuery(
      preparedValues,
      allowedFields,
      tablName,
      rest as any,
      true,
    );
    const findAllQuery = `(${query})`;
    const queries = [findAllQuery];
    const aliasStr = getAliasName(alias);
    if (aliasStr) {
      queries.push(DB_KEYWORDS.as, aliasStr);
    }
    return attachArrayWith.space(queries);
  }

  static #prepareSetQuery<Model>(
    preparedValues: PreparedValues,
    setQry?: SetQuery<Model>,
  ) {
    if (!setQry) {
      return '';
    }
    if (typeof setQry !== 'object' || setQry === null) {
      return throwError.invalidSetQueryType();
    }
    if (!setQry.type || !setQry.model) {
      return throwError.invalidSetQueryType(true);
    }
    const { type, columns, model, orderBy, alias, set, ...rest } = setQry;
    const queries: string[] = [setOperation[type]];
    const tableName = (model as any).tableName;
    const tableColumns = (model as any).tableColumns;
    const allowedFields = getAllowedFields(tableColumns, alias, rest.join);
    const selectQry = DBQuery.#prepareSelectQuery(
      tableName,
      allowedFields,
      preparedValues,
      {
        columns,
        alias,
      },
    );
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
      q = `(${attachArrayWith.space(rawQries)})`;
    } else {
      q = attachArrayWith.space(rawQries);
    }
    queries.push(q);
    return attachArrayWith.space(queries);
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
          return throwError.invalidAggFuncPlaceType(fn, column);
        }
        if (fn) {
          validCol = fieldFunctionCreator(validCol, fn as FieldFunctionType);
        }
        if (value === null) {
          return validCol;
        } else if (typeof value === 'string') {
          allowedFields.add(value);
          return `${validCol} ${DB_KEYWORDS.as} ${quote(value)}`;
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
    const orderStatement: string[] = [DB_KEYWORDS.orderBy];
    const qry = Object.entries(orderBy)
      .map(([key, val]) => {
        const validKey = FieldQuote(allowedFields, key);
        if (typeof val === 'string') {
          return `${validKey} ${val}`;
        }
        if (typeof val === 'object' && val !== null) {
          const { order, nullOption, fn } = val;
          if (!order) {
            return throwError.invalidOrderOptionType(key);
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
    return attachArrayWith.space(orderStatement);
  }

  static #prepareGroupByStatement(
    allowedFields: Set<string>,
    groupBy?: string[],
  ) {
    DBQuery.#groupByFields.clear();
    if (!groupBy || (Array.isArray(groupBy) && groupBy.length < 1)) return '';
    const groupStatements: string[] = [DB_KEYWORDS.groupBy];
    const qry = attachArrayWith.coma(
      groupBy.map((key) => {
        const validKey = FieldQuote(allowedFields, key);
        DBQuery.#groupByFields.add(validKey);
        return validKey;
      }),
    );
    groupStatements.push(qry);
    return attachArrayWith.space(groupStatements);
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
      filterStatements.push(DB_KEYWORDS.having);
    } else {
      filterStatements.push(DB_KEYWORDS.where);
    }
    const qry = attachArrayWith.space(
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
      ? attachArrayWith.space(filterStatements)
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
      return throwError.invalidModelType();
    }
    if (!rest.where && isExistsFilter) {
      return throwError.invalidWhereClauseType(key);
    }
    const tableName = (model as any).tableName;
    const tableColumns = new Set((model as any).tableColumns) as Set<string>;
    if (isExistsFilter) {
      tableColumns.add('1');
    }
    const selectQuery = isExistsFilter
      ? { columns: { '1': null }, alias, isDistinct }
      : { columns: { [column]: null }, alias, isDistinct };
    const subQryAllowedFields = getAllowedFields(
      tableColumns,
      alias,
      rest.join,
    );
    const selectQry = DBQuery.#prepareSelectQuery(
      tableName,
      subQryAllowedFields,
      preparedValues,
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
    return attachArrayWith.space(subQryArr);
  }

  static #andOrFilterBuilder(
    key: OP_KEYS,
    allowedFields: Set<string>,
    preparedValues: PreparedValues,
    value: any,
    isHavingFilter: boolean,
  ) {
    if (!Array.isArray(value)) {
      return throwError.invalidArrayOPType(key);
    }
    if (value.length < 2) {
      return throwError.invalidArrayOPType(key, { min: 2 });
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
        return throwError.invalidArrayOPType(baseOperation, { min: 1 });
      }
      const arrayKeyword = DB_KEYWORDS.array;
      const placeholders = preparePlachldrForArray(value, preparedValues);
      const dataType = getArrayDataType(value);
      const arrayQry = isArrayKeywordReq
        ? ` (${arrayKeyword}[${attachArrayWith.coma(
            placeholders,
          )}]::${dataType}[])`
        : `(${attachArrayWith.coma(placeholders)})`;
      return `${key} ${baseOperation} ${subQryOperation}${arrayQry}`;
    }
    if (typeof value !== 'object' || value === null) {
      return throwError.invalidObjectOPType(baseOperation);
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
        return throwError.invalidOperatorType(op, validOperations);
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
          return `${key} ${operation} ${DB_KEYWORDS.null}`;
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
            return throwError.invalidArrayOPType(op);
          }
          if (val.length !== 2) {
            return throwError.invalidArrayOPType(op, { exact: 2 });
          }
          const placeholders = preparePlachldrForArray(val, preparedValues);
          return `${validKey} ${operation} ${placeholders[0]} ${OP.$and} ${placeholders[1]}`;
        }
        default:
          return throwError.invalidOperatorType(op, validOperations);
      }
    };
    const cond = attachArrayWith.space(Object.entries(value).map(prepareQry));
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
    const limitStatements = [`${DB_KEYWORDS.limit} ${limitPlaceholder}`];
    if (offset && typeof offset === 'number') {
      const offsetPlaceholder = getPreparedValues(preparedValues, offset);
      limitStatements.push(`${DB_KEYWORDS.offset} ${offsetPlaceholder}`);
    }
    return attachArrayWith.space(limitStatements);
  }
}

//============================================= DBQuery ===================================================//

//============================================= DBModel ===================================================//

export class DBModel extends DBQuery {
  static init(modelObj: Table, option: ExtraOptions) {
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
      throwError.invalidPrimaryColType(tableName);
    }
    columns.push(DBModel.#createPrimaryColumn(primaryKeys));
    Object.entries(reference).forEach(([key, ref]) => {
      columns.push(DBModel.#createForeignColumn(key, ref));
    });
    const createEnumQryPromise = Promise.all(enums.map((e) => query(e)));
    const createTableQry = `CREATE TABLE IF NOT EXISTS "${tableName}" (${attachArrayWith.coma(
      columns,
    )});`;
    createEnumQryPromise.then(() => query(createTableQry));
  }

  static #createColumn(
    columnName: string,
    value: TableValues,
    primaryKeys: string[],
    enums: string[],
  ) {
    const values: (string | boolean)[] = [columnName];
    const colUpr = columnName.toUpperCase();
    Object.entries(value).forEach((entry) => {
      const [key, keyVale] = entry as [keyof TableValues, string | boolean];
      switch (key) {
        case 'type': {
          if ((keyVale as any).startsWith('ENUM')) {
            const enumQry = `${enumQryPrefix} ${colUpr} ${DB_KEYWORDS.as} ${keyVale}; ${enumQrySuffix}`;
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
          values.push(`${DB_KEYWORDS.default} ${keyVale}`);
          break;
        case 'unique':
          values.push(DB_KEYWORDS.unique);
          break;
        case 'notNull':
          values.push(DB_KEYWORDS.notNull);
          break;
        case 'customDefaultValue':
          values.push(`${DB_KEYWORDS.default} '${keyVale}'`);
          break;
        case 'check':
          values.push(`${DB_KEYWORDS.check} (${keyVale})`);
          break;
      }
    });
    return attachArrayWith.space(values);
  }
  static #createPrimaryColumn(primaryKeys: string[]) {
    return `${DB_KEYWORDS.primaryKey} (${attachArrayWith.coma(primaryKeys)})`;
  }
  static #createForeignColumn(parentTable: string, ref: ReferenceTable) {
    const { parentColumn, column, constraintName, onDelete, onUpdate } = ref;
    const colStr = Array.isArray(column)
      ? attachArrayWith.coma(column)
      : column;
    const parentColStr = Array.isArray(parentColumn)
      ? attachArrayWith.coma(parentColumn)
      : parentColumn;
    const values: string[] = [];
    if (constraintName) {
      values.push(`${DB_KEYWORDS.constraint} ${constraintName}`);
    }
    values.push(`${DB_KEYWORDS.foreignKey} (${colStr})`);
    values.push(`${DB_KEYWORDS.references} "${parentTable}" (${parentColStr})`);
    if (onDelete) {
      values.push(`${DB_KEYWORDS.onDelete} ${onDelete}`);
    }
    if (onUpdate) {
      values.push(`${DB_KEYWORDS.onUpdate} ${onUpdate}`);
    }
    return attachArrayWith.space(values);
  }
}
