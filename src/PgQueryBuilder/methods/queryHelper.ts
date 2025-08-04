import { PG_DATA_TYPE } from '../constants/dataTypes';
import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  conditionalOperator,
  OP,
  OP_KEYS,
  SIMPLE_OP_KEYS,
  subqueryOperator,
  validOperations,
} from '../constants/operators';
import { setOperation } from '../constants/setOperations';
import { Primitive } from '../globalTypes';
import {
  AliasFilter,
  AliasSubType,
  FilterColumnValue,
  InOperationSubQuery,
  ORDER_BY,
  PreparedValues,
  QueryParams,
  SelectQuery,
  SetQuery,
  Subquery,
  SubQueryFilter,
  WhereClause,
  WhereClauseKeys,
} from '../internalTypes';
import { ColumnHelper } from './columnHelper';
import { throwError } from './errorHelper';
import { FieldHelper } from './fieldHelper';
import {
  attachArrayWith,
  fieldFunctionCreator,
  FieldQuote,
  getPreparedValues,
  isPrimitiveValue,
  prepareColumnForHavingClause,
} from './helperFunction';
import { PaginationQuery } from './paginationQuery';
import { TableJoin } from './tableJoin';

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

export class QueryHelper {
  static #prepareSubQry(params: {
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

  static prepareGroupByQuery(
    allowedFields: Set<string>,
    groupByFields: Set<string>,
    groupBy?: string[],
  ) {
    groupByFields.clear();
    if (!groupBy || (Array.isArray(groupBy) && groupBy.length < 1)) return '';
    const groupStatements: string[] = [DB_KEYWORDS.groupBy];
    const qry = attachArrayWith.coma(
      groupBy.map((key) => {
        const validKey = FieldQuote(allowedFields, key);
        groupByFields.add(validKey);
        return validKey;
      }),
    );
    groupStatements.push(qry);
    return attachArrayWith.space(groupStatements);
  }

  static prepareOrderByQuery(allowedFields: Set<string>, orderBy?: ORDER_BY) {
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

  // Private Methods
  static prepareQuery<Model>(
    preparedValues: PreparedValues,
    refAllowedFields: Set<string>,
    groupByFields: Set<string>,
    tableName: string,
    qry: QueryParams<Model>,
    useOnlyRefAllowedFields = false,
  ) {
    const { columns, isDistinct, orderBy, alias, set, ...rest } = qry;
    const allowedFields = useOnlyRefAllowedFields
      ? refAllowedFields
      : FieldHelper.getAllowedFields(refAllowedFields, alias, rest.join as any);
    const selectQry = QueryHelper.#prepareSelectQuery(
      tableName,
      allowedFields,
      groupByFields,
      preparedValues,
      {
        columns,
        isDistinct,
        alias,
      },
    );
    const subQry = QueryHelper.#prepareSubquery(
      tableName,
      allowedFields,
      groupByFields,
      preparedValues,
      rest,
      orderBy,
    );
    const setQry = QueryHelper.#prepareSetQuery(
      preparedValues,
      groupByFields,
      set,
    );
    const query = prepareFinalFindQry(selectQry, setQry, subQry);
    return query;
  }

  static #prepareSubquery<Model>(
    tableName: string,
    allowedFields: Set<string>,
    groupByFields: Set<string>,
    preparedValues: PreparedValues,
    subQuery: Subquery<Model>,
    orderBy?: ORDER_BY,
  ) {
    const { where, groupBy, limit, offset, join, having } = subQuery || {};
    const whereStatement = QueryHelper.#prepareFilterStatement(
      allowedFields,
      groupByFields,
      preparedValues,
      where,
    );
    const limitStr = PaginationQuery.preparePaginationStatement(
      preparedValues,
      limit,
      offset,
    );
    const joinStr = TableJoin.prepareTableJoin(tableName, allowedFields, join);
    const groupByStr = QueryHelper.prepareGroupByQuery(
      allowedFields,
      groupByFields,
      groupBy,
    );
    const orderStr = QueryHelper.prepareOrderByQuery(allowedFields, orderBy);
    const havingStatement = QueryHelper.#prepareFilterStatement(
      allowedFields,
      groupByFields,
      preparedValues,
      having,
      {
        isHavingFilter: true,
      },
    );
    const finalSubQry = QueryHelper.#prepareSubQry({
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
    groupByFields: Set<string>,
    preparedValues: PreparedValues,
    selectQuery: SelectQuery<Model>,
  ) {
    const { isDistinct, columns, alias } = selectQuery;
    const distinctMaybe = isDistinct ? `${DB_KEYWORDS.distinct}` : '';
    const colStr = ColumnHelper.getSelectColumns(allowedFields, columns);
    const tableAlias = QueryHelper.#prepareAliasSubQuery(
      tableName,
      preparedValues,
      allowedFields,
      groupByFields,
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
    groupByFields: Set<string>,
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
    const model = FieldHelper.getAliasSubqueryModel(alias);
    const tablName = model.tableName;
    const query = QueryHelper.prepareQuery(
      preparedValues,
      allowedFields,
      groupByFields,
      tablName,
      rest as any,
      true,
    );
    const findAllQuery = `(${query})`;
    const queries = [findAllQuery];
    const aliasStr = FieldHelper.getAliasName(alias);
    if (aliasStr) {
      queries.push(DB_KEYWORDS.as, aliasStr);
    }
    return attachArrayWith.space(queries);
  }

  static #prepareSetQuery<Model>(
    preparedValues: PreparedValues,
    groupByFields: Set<string>,
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
    const allowedFields = FieldHelper.getAllowedFields(
      tableColumns,
      alias,
      rest.join as any,
    );
    const selectQry = QueryHelper.#prepareSelectQuery(
      tableName,
      allowedFields,
      groupByFields,
      preparedValues,
      {
        columns,
        alias,
      },
    );
    const subQry = QueryHelper.#prepareSubquery(
      tableName,
      allowedFields,
      groupByFields,
      preparedValues,
      rest,
      orderBy,
    );
    const setSubqry = QueryHelper.#prepareSetQuery(
      preparedValues,
      groupByFields,
      set,
    );
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

  static #prepareFilterStatement<Model>(
    allowedFields: Set<string>,
    groupByFields: Set<string>,
    preparedValues: PreparedValues,
    filter?: WhereClause<Model>,
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
          return QueryHelper.#getQueryStatement(
            allowedFields,
            groupByFields,
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
    groupByFields: Set<string>,
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
      ? { columns: { 1: null }, alias, isDistinct }
      : { columns: { [column]: null }, alias, isDistinct };
    const subQryAllowedFields = FieldHelper.getAllowedFields(
      tableColumns,
      alias,
      rest.join,
    );
    const selectQry = QueryHelper.#prepareSelectQuery(
      tableName,
      subQryAllowedFields,
      groupByFields,
      preparedValues,
      selectQuery,
    );
    const subquery = QueryHelper.#prepareSubquery(
      tableName,
      subQryAllowedFields,
      groupByFields,
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
    groupByFields: Set<string>,
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
          return QueryHelper.#getQueryStatement(
            allowedFields,
            groupByFields,
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
    groupByFields: Set<string>,
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
      const cond = QueryHelper.#andOrFilterBuilder(
        key,
        allowedFields,
        groupByFields,
        preparedValues,
        value,
        isHavingFilter,
      );
      return cond;
    } else if (subqueryOperator.has(key as any)) {
      const finalSubQuery = QueryHelper.#otherModelSubqueryBuilder(
        key,
        preparedValues,
        groupByFields,
        value,
        true,
      );
      return finalSubQuery;
    } else {
      return QueryHelper.#buildCondition(
        key,
        value,
        allowedFields,
        groupByFields,
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
    groupByFields: Set<string>,
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
    const subQry = QueryHelper.#otherModelSubqueryBuilder(
      subQryOperation,
      preparedValues,
      groupByFields,
      value,
      false,
    );
    return `${key} ${baseOperation} ${subQry}`;
  }

  static #buildCondition(
    key: string,
    value: Record<SIMPLE_OP_KEYS, Primitive>,
    allowedFields: Set<string>,
    groupByFields: Set<string>,
    preparedValues: PreparedValues,
    isHavingFilter: boolean,
  ) {
    const validKey = prepareColumnForHavingClause(
      key,
      groupByFields,
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
          const subQry = QueryHelper.#buildQueryForSubQryOperator(
            validKey,
            operation,
            key,
            preparedValues,
            groupByFields,
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
          const subQry = QueryHelper.#buildQueryForSubQryOperator(
            validKey,
            operation,
            '',
            preparedValues,
            groupByFields,
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
    const cond = attachArrayWith.and(Object.entries(value).map(prepareQry));
    return cond ? `(${cond})` : '';
  }
}
