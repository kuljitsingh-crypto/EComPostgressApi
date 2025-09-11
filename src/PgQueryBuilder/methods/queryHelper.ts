import { DB_KEYWORDS } from '../constants/dbkeywords';
import { OP, OP_KEYS } from '../constants/operators';
import { setOperation } from '../constants/setOperations';
import { TableJoinType } from '../constants/tableJoin';
import {
  AliasSubType,
  AllowedFields,
  DerivedModel,
  GroupByFields,
  InOperationSubQuery,
  JoinQuery,
  ORDER_BY,
  PreparedValues,
  QueryParams,
  SelectQuery,
  SetQuery,
  Subquery,
  SubqueryMultiColFlag,
} from '../internalTypes';
import { ColumnHelper } from './columnHelper';
import { throwError } from './errorHelper';
import { FieldHelper } from './fieldHelper';
import { TableFilter } from './filterHelper';
import {
  attachArrayWith,
  fieldQuote,
  getJoinSubqueryFields,
  isNonEmptyString,
  isNonNullableValue,
  isValidDerivedModel,
  isValidObject,
  isValidSubQuery,
  isValidWhereQuery,
} from './helperFunction';
import { OrderByQuery } from './orderBy';
import { PaginationQuery } from './paginationQuery';
import { TableJoin } from './tableJoin';

const prepareFinalFindQry = (
  selectQry: string,
  setQry?: string,
  subQry?: string,
) => {
  const rowQueries: string[] = [];
  if (setQry && subQry) {
    const firstQry = attachArrayWith.space([
      DB_KEYWORDS.select,
      '*',
      DB_KEYWORDS.from,
      `(${selectQry} ${setQry})`,
      DB_KEYWORDS.as,
      'results',
    ]);
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
    options?: { useOnlyRefAllowedFields?: boolean; modelRef?: Model },
  ) {
    if (isNonNullableValue((qry as any).model)) {
      qry.derivedModel = (qry as any).model;
      delete (qry as any).model;
    }
    const { useOnlyRefAllowedFields = false, modelRef } = options || {};
    const { columns, isDistinct, orderBy, alias, derivedModel, set, ...rest } =
      qry;
    const join = getJoinSubqueryFields(rest);
    const allowedFields = useOnlyRefAllowedFields
      ? refAllowedFields
      : FieldHelper.getAllowedFields(refAllowedFields, {
          alias,
          join,
          derivedModel,
        });
    const selectQury = {
      columns,
      isDistinct,
      alias,
      derivedModel,
    };
    const selectQry = QueryHelper.#prepareSelectQuery(
      tableName,
      allowedFields,
      groupByFields,
      preparedValues,
      selectQury,
    );
    const subQry = QueryHelper.#prepareSubquery(
      tableName,
      allowedFields,
      groupByFields,
      preparedValues,
      rest,
      join,
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

  static otherModelSubqueryBuilder<
    M extends SubqueryMultiColFlag,
    T extends InOperationSubQuery<Model, 'WhereNotReq', M>,
    Model,
  >(
    existFilterKey: string,
    preparedValues: PreparedValues,
    groupByFields: GroupByFields,
    value: T,
    options: { isExistsFilter?: boolean; refAllowedFields?: AllowedFields },
  ) {
    const { isExistsFilter = true, refAllowedFields } = options || {};
    const {
      model: m,
      alias,
      orderBy,
      subquery: modelSubquery,
      column,
      columns,
      isDistinct,
      ...rest
    } = value as any;
    const join = getJoinSubqueryFields(rest);
    const validSubquery = isExistsFilter
      ? isValidModelSubquery(value)
      : isValidSubQuery(value);
    if (!validSubquery) {
      return throwError.invalidModelType();
    }
    if (isExistsFilter && !isValidWhereQuery(rest.where)) {
      return throwError.invalidWhereClauseType(existFilterKey);
    }
    const model = FieldHelper.getSubqueryModel(value);
    const tableName = model.tableName;
    const tableColumns: AllowedFields = new Set(model.tableColumns);
    if (isExistsFilter) {
      tableColumns.add('1');
    }
    const customAllowFields = isExistsFilter ? ['1'] : [];
    const columnArr = isExistsFilter
      ? ['1']
      : column
        ? [column]
        : columns
          ? [...columns]
          : [];
    const selectQuery =
      columnArr.length > 0
        ? { columns: columnArr, alias, isDistinct, subquery: modelSubquery }
        : { alias, isDistinct, subquery: modelSubquery };

    const subQryAllowedFields = FieldHelper.getAllowedFields(tableColumns, {
      alias,
      join,
      refAllowedFields,
      subquery: modelSubquery,
    });
    const selectQry = QueryHelper.#prepareSelectQuery(
      tableName,
      subQryAllowedFields,
      groupByFields,
      preparedValues,
      selectQuery,
      { customAllowFields },
    );
    const subquery = QueryHelper.#prepareSubquery(
      tableName,
      subQryAllowedFields,
      groupByFields,
      preparedValues,
      rest,
      join,
    );
    const operator = isExistsFilter
      ? OP[existFilterKey as OP_KEYS]
      : existFilterKey;
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
    options?: { customAllowFields: string[] },
  ) {
    const { customAllowFields = [] } = options || {};
    const { isDistinct, columns, alias, derivedModel } = selectQuery;
    const distinctMaybe = isDistinct ? `${DB_KEYWORDS.distinct}` : '';
    const colStr = ColumnHelper.getSelectColumns(allowedFields, columns, {
      preparedValues,
      groupByFields,
      customAllowFields,
    });

    const tableAlias = QueryHelper.#prepareDerivedModelSubquery(
      tableName,
      preparedValues,
      allowedFields,
      groupByFields,
      { alias, derivedModel },
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
    if (!isValidObject(setQry)) {
      return throwError.invalidSetQueryType();
    }
    if (!setQry.type || !isValidModelSubquery(setQry)) {
      return throwError.invalidSetQueryType(true);
    }
    const {
      type,
      columns,
      model: m,
      subquery,
      orderBy,
      alias,
      set,
      ...rest
    } = setQry;
    const model = FieldHelper.getSubqueryModel(setQry);
    const join = getJoinSubqueryFields(rest);
    const queries: string[] = [setOperation[type]];
    const tableName = model.tableName;
    const tableColumns = model.tableColumns;
    const allowedFields = FieldHelper.getAllowedFields(tableColumns, {
      alias,
      join,
      subquery,
    });
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
      join,
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
    const variableQry = Object.values(params).filter(isNonEmptyString);
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

  static #prepareSubquery<Model>(
    tableName: string,
    allowedFields: AllowedFields,
    groupByFields: GroupByFields,
    preparedValues: PreparedValues,
    subQuery: Subquery<Model>,
    join: Record<TableJoinType, JoinQuery<TableJoinType, Model>>,
    orderBy?: ORDER_BY<Model>,
  ) {
    const { where, groupBy, limit, offset, having } = subQuery || {};
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
    const joinStr = TableJoin.prepareTableJoin(
      tableName,
      allowedFields,
      preparedValues,
      groupByFields,
      join,
    );
    const groupByStr = QueryHelper.#prepareGroupByQuery(
      allowedFields,
      groupByFields,
      groupBy,
    );
    const orderStr = OrderByQuery.prepareOrderByQuery(
      allowedFields,
      preparedValues,
      groupByFields,
      orderBy,
    );
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

  static #prepareDerivedModelSubquery<Model extends any = any>(
    tableName: string,
    preparedValues: PreparedValues,
    allowedFields: AllowedFields,
    groupByFields: GroupByFields,
    options?: { alias?: AliasSubType; derivedModel?: DerivedModel<Model> },
  ): string {
    const { alias, derivedModel } = options || {};
    const aliasStr = isNonEmptyString(alias) ? alias : '';
    if (!isValidDerivedModel(derivedModel)) {
      return aliasStr
        ? attachArrayWith.space([tableName, DB_KEYWORDS.as, aliasStr])
        : tableName;
    }
    const model = FieldHelper.getDerivedModel(derivedModel);
    const rest;
    const tablName = model.tableName;
    const query = QueryHelper.prepareQuery(
      preparedValues,
      allowedFields,
      groupByFields,
      tablName,
      rest,
      true,
    );
    const findAllQuery = `(${query})`;
    const queries = [findAllQuery];
    if (aliasStr) {
      queries.push(DB_KEYWORDS.as, aliasStr);
    }
    return attachArrayWith.space(queries);
  }
}
