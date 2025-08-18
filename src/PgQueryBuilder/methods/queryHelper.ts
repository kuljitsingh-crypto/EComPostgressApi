import { DB_KEYWORDS } from '../constants/dbkeywords';
import { OP, OP_KEYS } from '../constants/operators';
import { setOperation } from '../constants/setOperations';
import { Primitive } from '../globalTypes';
import {
  AliasFilter,
  AliasSubType,
  AllowedFields,
  GroupByFields,
  InOperationSubQuery,
  ORDER_BY,
  PreparedValues,
  QueryParams,
  SelectQuery,
  SetQuery,
  Subquery,
} from '../internalTypes';
import { ColumnHelper } from './columnHelper';
import { throwError } from './errorHelper';
import { FieldHelper } from './fieldHelper';
import { TableFilter } from './filterHelper';
import {
  attachArrayWith,
  fieldQuote,
  isValidModel,
  getAggregatedColumn,
} from './helperFunction';
import { PaginationQuery } from './paginationQuery';
import { TableJoin } from './tableJoin';

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

export class QueryHelper {
  static prepareQuery<Model>(
    preparedValues: PreparedValues,
    refAllowedFields: AllowedFields,
    groupByFields: GroupByFields,
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

  static otherModelSubqueryBuilder<T extends InOperationSubQuery<Model>, Model>(
    key: string,
    preparedValues: PreparedValues,
    groupByFields: GroupByFields,
    value: T,
    isExistsFilter: boolean = true,
  ) {
    const { model, alias, column, orderBy, isDistinct, ...rest } =
      value as InOperationSubQuery<Model>;
    if (!isValidModel(model)) {
      return throwError.invalidModelType();
    }
    if (!rest.where && isExistsFilter) {
      return throwError.invalidWhereClauseType(key);
    }
    const tableName = (model as any).tableName;
    const tableColumns = new Set((model as any).tableColumns) as AllowedFields;
    if (isExistsFilter) {
      tableColumns.add('1');
    }
    const selectQuery = isExistsFilter
      ? { columns: ['1'], alias, isDistinct }
      : { columns: [column], alias, isDistinct };
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
      rest as any,
    );
    const operator = isExistsFilter ? OP[key as OP_KEYS] : key;
    const subQryArr: string[] = operator ? [operator] : [];
    const q = attachArrayWith.space([selectQry, subquery]);
    subQryArr.push(`(${q})`);
    return attachArrayWith.space(subQryArr);
  }

  // Private Methods
  static #prepareSelectQuery<Model>(
    tableName: string,
    allowedFields: AllowedFields,
    groupByFields: GroupByFields,
    preparedValues: PreparedValues,
    selectQuery: SelectQuery<Model>,
  ) {
    const { isDistinct, columns, alias } = selectQuery;
    const distinctMaybe = isDistinct ? `${DB_KEYWORDS.distinct}` : '';
    const colStr = ColumnHelper.getSelectColumns(allowedFields, columns, {
      preparedValues,
      groupByFields,
    });
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

  static #prepareSetQuery<Model>(
    preparedValues: PreparedValues,
    groupByFields: GroupByFields,
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

  static #prepareGroupByQuery(
    allowedFields: AllowedFields,
    groupByFields: GroupByFields,
    groupBy?: string[],
  ) {
    groupByFields.clear();
    if (!groupBy || (Array.isArray(groupBy) && groupBy.length < 1)) return '';
    const groupStatements: string[] = [DB_KEYWORDS.groupBy];
    const qry = attachArrayWith.coma(
      groupBy.map((key) => {
        const validKey = fieldQuote(allowedFields, key);
        groupByFields.add(validKey);
        return validKey;
      }),
    );
    groupStatements.push(qry);
    return attachArrayWith.space(groupStatements);
  }

  static #prepareOrderByQuery(
    allowedFields: AllowedFields,
    orderBy?: ORDER_BY,
  ) {
    if (!orderBy || Object.keys(orderBy).length < 1) return '';
    const orderStatement: string[] = [DB_KEYWORDS.orderBy];
    const qry = Object.entries(orderBy)
      .map(([key, val]) => {
        const validKey = fieldQuote(allowedFields, key);
        if (typeof val === 'string') {
          return `${validKey} ${val}`;
        }
        if (typeof val === 'object' && val !== null) {
          const { order, nullOption, fn } = val;
          if (!order) {
            return throwError.invalidOrderOptionType(key);
          }
          const aggregatedColumn = getAggregatedColumn({
            column: validKey,
            allowedFields,
            shouldSkipFieldValidation: true,
          });
          const orderStr = attachArrayWith.space([
            aggregatedColumn,
            order,
            nullOption as any,
          ]);
          return orderStr;
        }
      })
      .filter(Boolean)
      .join(', ');
    orderStatement.push(qry);
    return attachArrayWith.space(orderStatement);
  }

  static #prepareSubquery<Model>(
    tableName: string,
    allowedFields: AllowedFields,
    groupByFields: GroupByFields,
    preparedValues: PreparedValues,
    subQuery: Subquery<Model>,
    orderBy?: ORDER_BY,
  ) {
    const { where, groupBy, limit, offset, join, having } = subQuery || {};
    const whereStatement = TableFilter.prepareFilterStatement(
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
    const groupByStr = QueryHelper.#prepareGroupByQuery(
      allowedFields,
      groupByFields,
      groupBy,
    );
    const orderStr = QueryHelper.#prepareOrderByQuery(allowedFields, orderBy);
    const havingStatement = TableFilter.prepareFilterStatement(
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

  static #prepareAliasSubQuery<Model extends any = any>(
    tableName: string,
    preparedValues: PreparedValues,
    allowedFields: AllowedFields,
    groupByFields: GroupByFields,
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
}
