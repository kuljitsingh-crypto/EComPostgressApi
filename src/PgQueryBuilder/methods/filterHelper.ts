import { PgDataType } from '../constants/dataTypes';
import { DB_KEYWORDS } from '../constants/dbkeywords';
import { MULTIPLE_FIELD_OP } from '../constants/fieldFunctions';
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
  covertJSDataToSQLData,
  getAllEntries,
  getPreparedValues,
  prepareSQLDataType,
  validateColumn,
  validCallableColCtx,
} from './helperFunction';
import { QueryHelper } from './queryHelper';
import {
  ensureArray,
  isCallableColumn,
  isNonEmptyObject,
  isNonEmptyString,
  isPrimitiveValue,
  isValidArray,
  isValidSubQuery,
} from './util';

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

const preparePlachldrForObject = (
  values: Record<string, Primitive>,
  preparedValues: PreparedValues,
) => {
  const flattenArray: string[] = [];
  let key, val, keyType, valType;
  for (key in values) {
    val = values[key];
    keyType = prepareSQLDataType(key);
    valType = prepareSQLDataType(val);
    flattenArray.push(
      getPreparedValues(preparedValues, key, { type: keyType }),
    );
    flattenArray.push(
      getPreparedValues(preparedValues, val, { type: valType }),
    );
  }
  return flattenArray;
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

const getArrayDataType = (value: Primitive[]) =>
  covertJSDataToSQLData(value[0]);

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
    options?: {
      isHavingFilter?: boolean;
      customKeyWord?: string;
      isWhereKeywordReq?: boolean;
    },
  ) {
    if (!filter) return '';
    const {
      isHavingFilter = false,
      customKeyWord,
      isWhereKeywordReq = true,
    } = options || {};
    const filterStatements: string[] = [];
    if (isHavingFilter) {
      filterStatements.push(DB_KEYWORDS.having);
    } else if (isNonEmptyString(customKeyWord)) {
      filterStatements.push(customKeyWord);
    } else if (isWhereKeywordReq) {
      filterStatements.push(DB_KEYWORDS.where);
    }

    const qry = attachArrayWith.and(
      getAllEntries(filter)
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
    const minLength = isWhereKeywordReq ? 1 : 0;
    return filterStatements.length > minLength
      ? attachArrayWith.space(filterStatements)
      : '';
  }

  static #getQueryStatement(
    allowedFields: AllowedFields,
    groupByFields: GroupByFields,
    singleQry: [WhereClauseKeys | symbol, any],
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
    if (!isValidArray<any>(value)) {
      return throwError.invalidArrayOPType(key);
    }
    if (value.length < 1) {
      return throwError.invalidArrayOPType(key, { min: 1 });
    }

    const validMatches = value.filter(Boolean).map((val) => {
      val = ensureArray(val);
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
    if (!isValidArray<any>(value)) {
      return throwError.invalidArrayOPType(key);
    }
    if (value.length < 2) {
      return throwError.invalidArrayOPType(key, { min: 2 });
    }
    const sep = ` ${OP[key]} `;
    const cond = value
      .map((v) => {
        const entries = getAllEntries(v);
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

  static #buildJsonbQryOperator = (
    key: string,
    baseOperation: string,
    preparedValues: PreparedValues,
    value: any,
  ) => {
    if (isNonEmptyObject(value)) {
      const flattenArray = preparePlachldrForObject(
        value as any,
        preparedValues,
      );
      const placeholder = attachArrayWith.noSpace([
        MULTIPLE_FIELD_OP.jsonbBuildObject,
        '(',
        attachArrayWith.coma(flattenArray),
        ')',
      ]);
      return attachArrayWith.space([key, baseOperation, placeholder]);
    }
    return throwError.invalidObjectOPType(baseOperation);
  };

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
    if (isValidArray<any>(value)) {
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
    } else if (isValidSubQuery(value)) {
      const subQry = QueryHelper.otherModelSubqueryBuilder(
        subQryOperation,
        preparedValues,
        groupByFields,
        value as any,
        { isExistsFilter: false },
      );
      return attachArrayWith.space([key, baseOperation, subQry]);
    }
    return throwError.invalidObjectOPType(baseOperation);
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
    const validKey = validateColumn(key, { shouldSkipFieldValidation })({
      preparedValues,
      allowedFields,
      groupByFields,
    });
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
          const updatedVal = isValidArray(val)
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
        case 'arrayContainsBy':
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

        case 'jsonbContainsBy':
        case 'jsonbContains':
          const subqry = TableFilter.#buildJsonbQryOperator(
            validKey,
            operation,
            preparedValues,
            val,
          );
          return subqry;
        case 'jsonbHasKey':
        case 'jsonbExists':
        case 'jsonbMatch':
          if (isPrimitiveValue(val)) {
            return prepareQryForPrimitiveOp(
              preparedValues,
              validKey,
              operation,
              val,
            );
          }
        case 'jsonbHasAll':
        case 'jsonbHasAny':
          const subQry = TableFilter.#buildQueryForSubQryOperator(
            validKey,
            operation,
            '',
            preparedValues,
            groupByFields,
            val,
            true,
            1,
          );
          return subQry;
        default:
          return throwError.invalidOperatorType(op, validOperations);
      }
    };
    const cond = attachArrayWith.and(Object.entries(value).map(prepareQry));
    return cond ? (returnRaw ? cond : `(${cond})`) : '';
  }
}
