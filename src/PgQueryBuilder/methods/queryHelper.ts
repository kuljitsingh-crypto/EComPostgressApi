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
  isNullableValue,
  isValidArray,
  isValidDerivedModel,
  isValidSimpleModel,
  isValidObject,
  isValidSubQuery,
  isValidWhereQuery,
  createNewObj,
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

const getRestQueryFrDerivedModel = <Model>(
  derivedModel: DerivedModel<Model>,
) => {
  if (isNullableValue(derivedModel)) {
    return throwError.invalidModelType();
  }
  if (isValidSimpleModel(derivedModel)) {
    return {};
  }
  if (isValidSimpleModel((derivedModel as any).model)) {
    const { model, ...rest } = derivedModel as any;
    return rest;
  }
  return derivedModel;
};

const getTableWithAliasName = (tableName: string, alias?: string) => {
  return alias
    ? attachArrayWith.space([tableName, DB_KEYWORDS.as, alias])
    : tableName;
};

export class QueryHelper {
  static prepareQuery<Model>(
    preparedValues: PreparedValues,
    refAllowedFields: AllowedFields,
    groupByFields: GroupByFields,
    tableName: string,
    qry: QueryParams<Model>,
    memorizeOption?: {
      useOnlyRefAllowedFields?: boolean;
      derivedModelRef?: Model;
    },
  ) {
    if (isNonNullableValue((qry as any).model)) {
      const { model, ...rest } = qry as any;
      qry = createNewObj(rest, { derivedModel: model });
    }
    const { useOnlyRefAllowedFields = false, derivedModelRef } =
      memorizeOption || {};
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
      { derivedModelRef },
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
    options: {
      isExistsFilter?: boolean;
      refAllowedFields?: AllowedFields;
      isColumnReq?: boolean;
    },
  ) {
    const {
      isExistsFilter = true,
      isColumnReq = true,
      refAllowedFields,
    } = options || {};
    const {
      alias,
      orderBy,
      model: m,
      column,
      columns,
      isDistinct,
      ...rest
    } = value as any;
    const join = getJoinSubqueryFields(rest);
    const validSubquery =
      isExistsFilter || !isColumnReq
        ? isValidDerivedModel(m)
        : isValidSubQuery(value);
    if (!validSubquery) {
      return throwError.invalidModelType();
    }
    if (isExistsFilter && !isValidWhereQuery(rest.where)) {
      return throwError.invalidWhereClauseType(existFilterKey);
    }
    const model = FieldHelper.getDerivedModel(m);
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
        ? { columns: columnArr, alias, isDistinct, derivedModel: m }
        : { alias, isDistinct, derivedModel: m };
    const subQryAllowedFields = FieldHelper.getAllowedFields(tableColumns, {
      alias,
      join,
      refAllowedFields,
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
    options?: { customAllowFields?: string[]; derivedModelRef?: Model },
  ) {
    const { customAllowFields = [], derivedModelRef } = options || {};
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
      { alias, derivedModel, derivedModelRef },
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
    if (!setQry.type || !isValidDerivedModel(setQry.model)) {
      return throwError.invalidSetQueryType(true);
    }
    const { type, columns, model: m, orderBy, alias, set, ...rest } = setQry;
    const model = FieldHelper.getDerivedModel(setQry.model);
    const join = getJoinSubqueryFields(rest);
    const queries: string[] = [setOperation[type]];
    const tableName = model.tableName;
    const tableColumns = model.tableColumns;
    const allowedFields = FieldHelper.getAllowedFields(tableColumns, {
      alias,
      join,
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
    if (!isValidArray(groupBy)) return '';
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
    options?: {
      alias?: AliasSubType;
      derivedModel?: DerivedModel<Model>;
      derivedModelRef?: Model;
    },
  ): string {
    const { alias, derivedModel, derivedModelRef } = options || {};
    const aliasStr = isNonEmptyString(alias) ? alias : '';
    if (!isValidDerivedModel(derivedModel)) {
      return getTableWithAliasName(tableName, aliasStr);
    }

    const model = FieldHelper.getDerivedModel(derivedModelRef ?? derivedModel);
    const tablName = model.tableName;
    if (isValidSimpleModel<Model>(derivedModel)) {
      return getTableWithAliasName(tablName);
    }
    const rest = getRestQueryFrDerivedModel(derivedModel);
    const query = QueryHelper.prepareQuery(
      preparedValues,
      allowedFields,
      groupByFields,
      tablName,
      rest,
      { useOnlyRefAllowedFields: true, derivedModelRef: model },
    );
    const findAllQuery = `(${query})`;
    const queries = [findAllQuery];
    if (aliasStr) {
      queries.push(DB_KEYWORDS.as, aliasStr);
    }
    return attachArrayWith.space(queries);
  }
}
