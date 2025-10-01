import {
  DB_KEYWORDS,
  DEFAULT_ALIAS,
  WHERE_KEYWORD,
} from '../constants/dbkeywords';
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
  SetQueryArrField,
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
  createNewObj,
  getSetSubqueryFields,
  repeatValInArrUpto,
} from './helperFunction';
import { OrderByQuery } from './orderBy';
import { PaginationQuery } from './paginationQuery';
import { TableJoin } from './tableJoin';
import {
  isEmptyArray,
  isNonEmptyString,
  isNonNullableValue,
  isNullableValue,
  isValidArray,
  isValidDerivedModel,
  isValidSimpleModel,
  isValidSubQuery,
  isValidWhereQuery,
} from './util';

const prepareFinalFindQry = (
  selectQry: string,
  setQry?: string[],
  subQry?: string,
  options?: { isMainQuery?: boolean; alias?: string; needWrapper?: boolean },
) => {
  const {
    isMainQuery = false,
    alias = DEFAULT_ALIAS,
    needWrapper: pNeedWrapper = true,
  } = options || {};
  subQry = subQry ?? '';
  if (!isValidArray(setQry)) {
    const q = attachArrayWith.space([selectQry, subQry]);
    return isMainQuery ? q : `(${q})`;
  }
  const totalSetQueries = setQry.length;
  const needsWrapper = pNeedWrapper && (subQry || totalSetQueries > 1);
  const totalBrackets = needsWrapper ? totalSetQueries : totalSetQueries - 1;
  const selectUpdatedQries = repeatValInArrUpto<string>('(', totalBrackets);
  selectUpdatedQries.push(selectQry);
  if (subQry) selectUpdatedQries.push(' ', subQry);
  for (let i = 0; i < totalSetQueries; i++) {
    selectUpdatedQries.push(' ', setQry[i], ')');
  }
  if (!needsWrapper) selectUpdatedQries.pop();
  const finalSubquery = attachArrayWith.noSpace(selectUpdatedQries, false);
  const rawQueries = needsWrapper
    ? [
        DB_KEYWORDS.select,
        '*',
        DB_KEYWORDS.from,
        finalSubquery,
        DB_KEYWORDS.as,
        alias,
      ]
    : [finalSubquery];
  const q = attachArrayWith.space(rawQueries);
  return isMainQuery ? q : `(${q})`;
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

const prepareCols = (
  isExistsFilter: boolean,
  tableColumns: AllowedFields,
  col?: any,
  cols?: any[],
) => {
  const columns = isExistsFilter ? ['1'] : col ? [col] : cols ? [...cols] : [];
  tableColumns = new Set(tableColumns);
  if (isExistsFilter) {
    tableColumns.add('1');
  }
  const customAllowFields = isExistsFilter ? ['1'] : [];
  return { columns, tableColumns, customAllowFields };
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
      needWrapperForSetQry?: boolean;
    },
  ) {
    if (isNonNullableValue((qry as any).model)) {
      const { model, ...rest } = qry as any;
      qry = createNewObj(rest, { derivedModel: model });
    }
    const {
      useOnlyRefAllowedFields = false,
      derivedModelRef,
      needWrapperForSetQry = true,
    } = memorizeOption || {};
    const { columns, isDistinct, orderBy, alias, derivedModel, ...rest } = qry;
    const set = getSetSubqueryFields(rest);
    const join = getJoinSubqueryFields(rest);
    const allowedFields = useOnlyRefAllowedFields
      ? refAllowedFields
      : FieldHelper.getAllowedFields(refAllowedFields, {
          alias,
          join,
          derivedModel,
        });
    const updatedAlias =
      (isNonNullableValue(derivedModel) && (alias || DEFAULT_ALIAS)) || alias;

    const selectQury = {
      columns,
      isDistinct,
      alias: updatedAlias,
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
    const query = prepareFinalFindQry(selectQry, setQry, subQry, {
      isMainQuery: true,
      needWrapper: needWrapperForSetQry,
    });
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
      column: col,
      columns: cols,
      isDistinct,
      ...rest
    } = value as any;
    const validSubquery =
      isExistsFilter || !isColumnReq
        ? isValidDerivedModel(m)
        : isValidSubQuery(value);
    if (!validSubquery) {
      return throwError.invalidModelType();
    }
    if (
      isExistsFilter &&
      !isValidWhereQuery(WHERE_KEYWORD, rest.where, {
        treatSimpleObjAsWhereSubQry: true,
      })
    ) {
      return throwError.invalidWhereClauseType(existFilterKey);
    }
    const join = getJoinSubqueryFields(rest);
    const set = getSetSubqueryFields(rest);
    const model = FieldHelper.getDerivedModel(m);
    const tableName = model.tableName;
    const { columns, tableColumns, customAllowFields } = prepareCols(
      isExistsFilter,
      model.tableColumns,
      col,
      cols,
    );
    const updatedAlias =
      (!isValidSimpleModel(m) && (alias || DEFAULT_ALIAS)) || alias;
    const selectQuery =
      columns.length > 0
        ? { columns, alias: updatedAlias, isDistinct, derivedModel: m }
        : { alias: updatedAlias, isDistinct, derivedModel: m };
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
    const setQry = QueryHelper.#prepareSetQuery(
      preparedValues,
      groupByFields,
      set,
      { isExistsFilter },
    );
    const operator = isExistsFilter
      ? OP[existFilterKey as OP_KEYS]
      : existFilterKey;
    const subQryArr: string[] = operator ? [operator] : [];
    const q = prepareFinalFindQry(selectQry, setQry, subquery);
    subQryArr.push(q);
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
    setQry?: SetQueryArrField<Model>[],
    options?: { isExistsFilter?: boolean },
  ): string[] {
    if (
      isNullableValue(setQry) ||
      isEmptyArray<SetQueryArrField<Model>>(setQry)
    ) {
      return [];
    }
    if (!isValidArray<SetQueryArrField<Model>>(setQry)) {
      return throwError.invalidSetQueryType();
    }
    setQry = setQry as SetQueryArrField<Model>[];
    const setQueries = setQry.map((qry) =>
      QueryHelper.#prepareSingleSetOperation(
        qry,
        preparedValues,
        groupByFields,
        options,
      ),
    );
    return setQueries;
  }
  static #prepareSingleSetOperation = <Model>(
    setQry: SetQueryArrField<Model>,
    preparedValues: PreparedValues,
    groupByFields: GroupByFields,
    options?: { isExistsFilter?: boolean },
  ) => {
    if (!setQry.type || !isValidDerivedModel(setQry.model)) {
      return throwError.invalidSetQueryType(true);
    }
    const { isExistsFilter = false } = options || {};
    const { type, columns: cols, model: m, orderBy, alias, ...rest } = setQry;
    const model = FieldHelper.getDerivedModel(setQry.model);
    const set = getSetSubqueryFields(rest);
    const join = getJoinSubqueryFields(rest);
    const queries: string[] = [setOperation[type]];
    const tableName = model.tableName;
    const { columns, tableColumns, customAllowFields } = prepareCols(
      isExistsFilter,
      model.tableColumns,
      null,
      cols,
    );
    const allowedFields = FieldHelper.getAllowedFields(tableColumns, {
      alias,
      join,
    });
    const selectQ = {
      columns,
      alias,
    };
    const selectQry = QueryHelper.#prepareSelectQuery(
      tableName,
      allowedFields,
      groupByFields,
      preparedValues,
      selectQ,
      { customAllowFields },
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
    queries.push(
      prepareFinalFindQry(selectQry, setSubqry, subQry, { isMainQuery: true }),
    );
    return attachArrayWith.space(queries);
  };
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
    preparedValues: PreparedValues,
    groupBy?: string[],
  ) {
    groupByFields.clear();
    if (!isValidArray(groupBy)) return '';
    const groupStatements: string[] = [DB_KEYWORDS.groupBy];
    const qry = attachArrayWith.coma(
      groupBy.map((key) => {
        const validKey = fieldQuote(allowedFields, preparedValues, key);
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
      preparedValues,
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
      return getTableWithAliasName(tablName, aliasStr);
    }
    const rest = getRestQueryFrDerivedModel(derivedModel);
    const query = QueryHelper.prepareQuery(
      preparedValues,
      allowedFields,
      groupByFields,
      tablName,
      rest,
      {
        useOnlyRefAllowedFields: true,
        derivedModelRef: model,
        needWrapperForSetQry: false,
      },
    );
    const findAllQuery = `(${query})`;
    const queries = [findAllQuery];
    if (aliasStr) {
      queries.push(DB_KEYWORDS.as, aliasStr);
    }
    return attachArrayWith.space(queries);
  }
}
