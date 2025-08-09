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
import { Primitive } from '../globalTypes';
import {
  FilterColumnValue,
  InOperationSubQuery,
  ORDER_BY,
  PreparedValues,
  SelectQuery,
  Subquery,
  SubQueryFilter,
  WhereClause,
  WhereClauseKeys,
} from '../internalTypes';
import { throwError } from './errorHelper';
import { FieldHelper } from './fieldHelper';
import {
  attachArrayWith,
  getPreparedValues,
  isPrimitiveValue,
  prepareColumnForHavingClause,
} from './helperFunction';

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

const prepareArrayData = (arr: Primitive[], type: string) => {
  const arrayKeyword = DB_KEYWORDS.array;
  return `(${arrayKeyword}[${attachArrayWith.coma(arr)}]::${type}[])`;
};

type SelectQueryBuilder<Model> = (
  tableName: string,
  allowedFields: Set<string>,
  groupByFields: Set<string>,
  preparedValues: PreparedValues,
  selectQuery: SelectQuery<Model>,
) => string;

type SubQueryBuilder<Model> = (
  tableName: string,
  allowedFields: Set<string>,
  groupByFields: Set<string>,
  preparedValues: PreparedValues,
  subQuery: Subquery<Model>,
  orderBy?: ORDER_BY,
) => string;

export class TableFilter {
  static prepareFilterStatement<Model>(
    allowedFields: Set<string>,
    groupByFields: Set<string>,
    preparedValues: PreparedValues,
    selectQueryBuilder: SelectQueryBuilder<Model>,
    subQueryBuilder: SubQueryBuilder<Model>,
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
          return TableFilter.#getQueryStatement(
            allowedFields,
            groupByFields,
            filter,
            preparedValues,
            isHavingFilter,
            selectQueryBuilder,
            subQueryBuilder,
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

  static #otherModelSubqueryBuilder<T extends InOperationSubQuery, Model>(
    key: string,
    preparedValues: PreparedValues,
    groupByFields: Set<string>,
    value: T,
    selectQueryBuilder: SelectQueryBuilder<Model>,
    subQueryBuilder: SubQueryBuilder<Model>,
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
      ? { columns: ['1'], alias, isDistinct }
      : { columns: [column], alias, isDistinct };
    const subQryAllowedFields = FieldHelper.getAllowedFields(
      tableColumns,
      alias,
      rest.join,
    );
    const selectQry = selectQueryBuilder(
      tableName,
      subQryAllowedFields,
      groupByFields,
      preparedValues,
      selectQuery,
    );
    const subquery = subQueryBuilder(
      tableName,
      subQryAllowedFields,
      groupByFields,
      preparedValues,
      rest as any,
    );
    const operator = isExistsFilter ? OP[key as OP_KEYS] : key;
    const subQryArr: string[] = operator ? [operator] : [];
    subQryArr.push(`(${selectQry} ${subquery})`);
    return attachArrayWith.space(subQryArr);
  }

  static #andOrFilterBuilder<Model>(
    key: OP_KEYS,
    allowedFields: Set<string>,
    groupByFields: Set<string>,
    preparedValues: PreparedValues,
    value: any,
    isHavingFilter: boolean,
    selectQueryBuilder: SelectQueryBuilder<Model>,
    subQueryBuilder: SubQueryBuilder<Model>,
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
          return TableFilter.#getQueryStatement(
            allowedFields,
            groupByFields,
            filter,
            preparedValues,
            isHavingFilter,
            selectQueryBuilder,
            subQueryBuilder,
          );
        });
      })
      .join(sep);
    return cond ? `(${cond})` : '';
  }

  static #getQueryStatement<Model>(
    allowedFields: Set<string>,
    groupByFields: Set<string>,
    singleQry: [WhereClauseKeys, any],
    preparedValues: PreparedValues,
    isHavingFilter: boolean,
    selectQueryBuilder: SelectQueryBuilder<Model>,
    subQueryBuilder: SubQueryBuilder<Model>,
  ): string {
    const key = singleQry[0] as OP_KEYS;
    let value = singleQry[1];
    if (isPrimitiveValue(value)) {
      value = { eq: value };
    }
    if (conditionalOperator.has(key as any)) {
      const cond = TableFilter.#andOrFilterBuilder(
        key,
        allowedFields,
        groupByFields,
        preparedValues,
        value,
        isHavingFilter,
        selectQueryBuilder,
        subQueryBuilder,
      );
      return cond;
    } else if (subqueryOperator.has(key as any)) {
      const finalSubQuery = TableFilter.#otherModelSubqueryBuilder(
        key,
        preparedValues,
        groupByFields,
        value,
        selectQueryBuilder,
        subQueryBuilder,
        true,
      );
      return finalSubQuery;
    } else {
      return TableFilter.#buildCondition(
        key,
        value,
        allowedFields,
        groupByFields,
        preparedValues,
        isHavingFilter,
        selectQueryBuilder,
        subQueryBuilder,
      );
    }
  }

  static #buildQueryForSubQryOperator<Model>(
    key: string,
    baseOperation: string,
    subQryOperation: string,
    preparedValues: PreparedValues,
    groupByFields: Set<string>,
    value: any,
    selectQueryBuilder: SelectQueryBuilder<Model>,
    subQueryBuilder: SubQueryBuilder<Model>,
    isArrayKeywordReq: boolean = false,
  ) {
    if (Array.isArray(value)) {
      if (value.length < 1) {
        return throwError.invalidArrayOPType(baseOperation, { min: 1 });
      }
      const placeholders = preparePlachldrForArray(value, preparedValues);
      const dataType = getArrayDataType(value);
      const arrayQry = isArrayKeywordReq
        ? prepareArrayData(placeholders, dataType)
        : `(${attachArrayWith.coma(placeholders)})`;
      return `${key} ${baseOperation} ${subQryOperation}${arrayQry}`;
    }
    if (typeof value !== 'object' || value === null) {
      return throwError.invalidObjectOPType(baseOperation);
    }
    const subQry = TableFilter.#otherModelSubqueryBuilder(
      subQryOperation,
      preparedValues,
      groupByFields,
      value,
      selectQueryBuilder,
      subQueryBuilder,
      false,
    );
    return `${key} ${baseOperation} ${subQry}`;
  }

  static #buildCondition<Model>(
    key: string,
    value: Record<SIMPLE_OP_KEYS, Primitive>,
    allowedFields: Set<string>,
    groupByFields: Set<string>,
    preparedValues: PreparedValues,
    isHavingFilter: boolean,
    selectQueryBuilder: SelectQueryBuilder<Model>,
    subQueryBuilder: SubQueryBuilder<Model>,
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
          const subQry = TableFilter.#buildQueryForSubQryOperator(
            validKey,
            operation,
            key,
            preparedValues,
            groupByFields,
            value,
            selectQueryBuilder,
            subQueryBuilder,
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
          const subQry = TableFilter.#buildQueryForSubQryOperator(
            validKey,
            operation,
            '',
            preparedValues,
            groupByFields,
            val,
            selectQueryBuilder,
            subQueryBuilder,
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
