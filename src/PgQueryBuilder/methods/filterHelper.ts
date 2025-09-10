import { PG_DATA_TYPE } from '../constants/dataTypes';
import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  conditionalOperator,
  matchQueryOperator,
  OP,
  OP_KEYS,
  SIMPLE_OP_KEYS,
  subqueryOperator,
  validOperations,
} from '../constants/operators';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  FilterColumnValue,
  GroupByFields,
  InOperationSubQuery,
  PreparedValues,
  SubQueryFilter,
  WhereClause,
  WhereClauseKeys,
} from '../internalTypes';
import { ColumnHelper } from './columnHelper';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  fieldQuote,
  getPreparedValues,
  isCallableColumn,
  isNonEmptyString,
  isPrimitiveValue,
  isValidSubQuery,
  validCallableColCtx,
} from './helperFunction';
import { QueryHelper } from './queryHelper';

const checkPrimitiveValueForOp = (op: string, value: Primitive) => {
  if (!isPrimitiveValue(value)) {
    return throwError.invalidOPDataType(op);
  }
};

const preparePlachldrForArray = <Model>(
  values: (Primitive | InOperationSubQuery<Model, 'WhereNotReq', 'single'>)[],
  preparedValues: PreparedValues,
  groupByFields: GroupByFields,
) => {
  const placeholderArr = values.map((val) => {
    const placeholder = isPrimitiveValue(val)
      ? getPreparedValues(preparedValues, val)
      : QueryHelper.otherModelSubqueryBuilder(
          '',
          preparedValues,
          groupByFields,
          val as any,
          { isExistsFilter: false },
        );
    return placeholder;
  });
  return placeholderArr;
};

const prepareQryForPrimitiveOp = (
  preparedValues: PreparedValues,
  key: string,
  operation: string,
  value: Primitive,
  isPlaceholderReq = true,
) => {
  const valPlaceholder = isPlaceholderReq
    ? getPreparedValues(preparedValues, value)
    : value;
  return attachArrayWith.space([key, operation, valPlaceholder]);
};

const getArrayDataType = (value: Primitive[]) => {
  const firstValue = value[0];
  if (typeof firstValue === 'number') {
    return PG_DATA_TYPE.int;
  } else if (isNonEmptyString(firstValue)) {
    return PG_DATA_TYPE.text;
  } else if (typeof firstValue === 'boolean') {
    return PG_DATA_TYPE.boolean;
  } else {
    return throwError.invalidDataType(firstValue);
  }
};

const getAnyAndAllFilterValue = <Model>(val: any, op: string) => {
  if (typeof val !== 'object' || val === null) {
    return throwError.invalidAnyAllOpType(op);
  }
  const hasAny = (val as any).hasOwnProperty(DB_KEYWORDS.any);
  const hasAll = (val as any).hasOwnProperty(DB_KEYWORDS.all);
  if (!hasAny && !hasAll) {
    return throwError.invalidAnySubQType();
  }
  const subqueryKeyword = hasAll ? DB_KEYWORDS.all : DB_KEYWORDS.any;
  const subqueryVal: Array<Primitive> | SubQueryFilter<Model> = (val as any)[
    subqueryKeyword
  ];

  return { key: subqueryKeyword, value: subqueryVal };
};

const prepareArrayData = (arr: Primitive[], type: string) => {
  const arrayKeyword = DB_KEYWORDS.array;
  return `(${arrayKeyword}[${attachArrayWith.coma(arr)}]::${type}[])`;
};

export class TableFilter {
  static prepareFilterStatement<Model>(
    allowedFields: AllowedFields,
    groupByFields: GroupByFields,
    preparedValues: PreparedValues,
    filter?: WhereClause<Model>,
    options?: { isHavingFilter?: boolean; customKeyWord?: string },
  ) {
    if (!filter) return '';
    const { isHavingFilter = false, customKeyWord } = options || {};
    const filterStatements: string[] = [];
    if (isHavingFilter) {
      filterStatements.push(DB_KEYWORDS.having);
    } else if (isNonEmptyString(customKeyWord)) {
      filterStatements.push(customKeyWord);
    } else {
      filterStatements.push(DB_KEYWORDS.where);
    }
    const qry = attachArrayWith.and(
      Object.entries(filter)
        .map((filter) => {
          return TableFilter.#getQueryStatement(
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

  static #getQueryStatement(
    allowedFields: AllowedFields,
    groupByFields: GroupByFields,
    singleQry: [WhereClauseKeys, any],
    preparedValues: PreparedValues,
    isHavingFilter: boolean,
    shouldSkipFieldValidation = false,
  ): string {
    const key = singleQry[0] as OP_KEYS;
    let value = singleQry[1];
    if (isPrimitiveValue(value) || isCallableColumn(value)) {
      value = { eq: value };
    }
    if (conditionalOperator.has(key as any)) {
      return TableFilter.#andOrFilterBuilder(
        key,
        allowedFields,
        groupByFields,
        preparedValues,
        value,
        isHavingFilter,
      );
    } else if (subqueryOperator.has(key as any)) {
      return QueryHelper.otherModelSubqueryBuilder(
        key,
        preparedValues,
        groupByFields,
        value,
        { isExistsFilter: true, refAllowedFields: allowedFields },
      );
    } else if (matchQueryOperator.has(key as any)) {
      return TableFilter.#matchQueryOperator(
        key,
        value,
        allowedFields,
        groupByFields,
        preparedValues,
        isHavingFilter,
      );
    } else {
      return TableFilter.#buildCondition(
        key,
        value,
        allowedFields,
        groupByFields,
        preparedValues,
        isHavingFilter,
        false,
        shouldSkipFieldValidation,
      );
    }
  }

  static #matchQueryOperator = (
    key: OP_KEYS,
    value: any,
    allowedFields: AllowedFields,
    groupByFields: GroupByFields,
    preparedValues: PreparedValues,
    isHavingFilter: boolean,
  ) => {
    if (!Array.isArray(value)) {
      return throwError.invalidArrayOPType(key);
    }
    if (value.length < 1) {
      return throwError.invalidArrayOPType(key, { min: 1 });
    }

    const validMatches = value.filter(Boolean).map((val) => {
      val = Array.isArray(val) ? val : [val];
      if (val.length < 1) {
        return throwError.invalidArrayOPType(key, { min: 1 });
      }
      const column = ColumnHelper.getSelectColumns(allowedFields, [val[0]], {
        preparedValues,
        groupByFields,
        isAggregateAllowed: isHavingFilter,
      });
      if (val.length === 1) {
        return column;
      }
      return TableFilter.#getQueryStatement(
        allowedFields,
        groupByFields,
        [column, val[1]],
        preparedValues,
        isHavingFilter,
        true,
      );
    });
    return attachArrayWith.and(validMatches);
  };

  static #andOrFilterBuilder(
    key: OP_KEYS,
    allowedFields: AllowedFields,
    groupByFields: GroupByFields,
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
          return TableFilter.#getQueryStatement(
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

  static #buildQueryForSubQryOperator(
    key: string,
    baseOperation: string,
    subQryOperation: string,
    preparedValues: PreparedValues,
    groupByFields: GroupByFields,
    value: any,
    isArrayKeywordReq: boolean = false,
    minArrayLenReq = 1,
    sep = 'coma' as 'coma' | 'and',
  ) {
    if (Array.isArray(value)) {
      if (value.length < minArrayLenReq) {
        return throwError.invalidArrayOPType(baseOperation, {
          min: minArrayLenReq,
        });
      }
      const placeholders = preparePlachldrForArray(
        value,
        preparedValues,
        groupByFields,
      );
      const dataType = getArrayDataType(value);
      const arrayQry = isArrayKeywordReq
        ? prepareArrayData(placeholders, dataType)
        : sep === 'and'
          ? attachArrayWith.and(placeholders)
          : `(${attachArrayWith.coma(placeholders)})`;
      return attachArrayWith.space([
        key,
        baseOperation,
        subQryOperation,
        arrayQry,
      ]);
    }
    if (typeof value !== 'object' || value === null) {
      return throwError.invalidObjectOPType(baseOperation);
    }
    const subQry = QueryHelper.otherModelSubqueryBuilder(
      subQryOperation,
      preparedValues,
      groupByFields,
      value,
      { isExistsFilter: false },
    );
    return attachArrayWith.space([key, baseOperation, subQry]);
  }

  static #buildCondition<Model>(
    key: string,
    value: Record<SIMPLE_OP_KEYS, Primitive>,
    allowedFields: AllowedFields,
    groupByFields: GroupByFields,
    preparedValues: PreparedValues,
    isHavingFilter: boolean,
    returnRaw = false,
    shouldSkipFieldValidation = false,
  ) {
    const validKey = shouldSkipFieldValidation
      ? key
      : fieldQuote(allowedFields, key);
    const prepareQry = (entry: [string, FilterColumnValue<Model>]) => {
      const [op, val] = entry as [SIMPLE_OP_KEYS, FilterColumnValue<Model>];
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
          if (isPrimitiveValue(val)) {
            return prepareQryForPrimitiveOp(
              preparedValues,
              validKey,
              operation,
              val,
            );
          } else if (isCallableColumn(val)) {
            const { col: value } = validCallableColCtx(val, {
              allowedFields,
              groupByFields,
              preparedValues,
              isAggregateAllowed: false,
            });
            return prepareQryForPrimitiveOp(
              preparedValues,
              validKey,
              operation,
              value,
              false,
            );
          }
          const { key, value } = isValidSubQuery(val)
            ? { value: val, key: '' }
            : getAnyAndAllFilterValue(val, op);
          const subQry = TableFilter.#buildQueryForSubQryOperator(
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
        case 'ALL':
        case 'ANY': {
          return TableFilter.#buildCondition(
            key,
            { eq: { [op]: val } } as any,
            allowedFields,
            groupByFields,
            preparedValues,
            isHavingFilter,
            true,
            shouldSkipFieldValidation,
          );
        }
        case 'like':
        case 'iLike':
        case 'notLike':
        case 'notILike':
        case 'match':
        case 'iMatch':
        case 'notMatch':
        case 'iNotMatch': {
          if (isPrimitiveValue(val)) {
            return prepareQryForPrimitiveOp(
              preparedValues,
              validKey,
              operation,
              val,
            );
          }
          const updatedVal = Array.isArray(val)
            ? { [DB_KEYWORDS.any]: val }
            : val;
          const { key, value } = getAnyAndAllFilterValue(updatedVal, op);
          const subQry = TableFilter.#buildQueryForSubQryOperator(
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
        case 'notNull':
        case 'isNull': {
          return attachArrayWith.space([key, operation, DB_KEYWORDS.null]);
        }
        case 'isTrue':
        case 'notTrue':
        case 'isFalse':
        case 'notFalse':
        case 'isUnknown':
        case 'notUnknown':
          return attachArrayWith.space([key, operation]);

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
          );
          return subQry;
        }
        case 'arrayContainBy':
        case 'arrayContains':
        case 'arrayOverlap': {
          const subQuery = TableFilter.#buildQueryForSubQryOperator(
            validKey,
            operation,
            '',
            preparedValues,
            groupByFields,
            val,
            true,
            1,
          );
          return subQuery;
        }
        case 'between':
        case 'notBetween': {
          const subQry = TableFilter.#buildQueryForSubQryOperator(
            validKey,
            operation,
            '',
            preparedValues,
            groupByFields,
            val,
            false,
            2,
            'and',
          );
          return subQry;
        }
        default:
          return throwError.invalidOperatorType(op, validOperations);
      }
    };
    const cond = attachArrayWith.and(Object.entries(value).map(prepareQry));
    return cond ? (returnRaw ? cond : `(${cond})`) : '';
  }
}
