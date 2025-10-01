import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  aggregateFunctionName,
  AggregateFunctionType,
  doubleParamAggrFunctionNames,
} from '../constants/fieldFunctions';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  CallableField,
  CallableFieldParam,
  GroupByFields,
  InOperationSubQuery,
  ORDER_BY,
  PreparedValues,
} from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import { getFieldValue } from './fieldFunc';
import {
  attachArrayWith,
  getPreparedValues,
  getValidCallableFieldValues,
} from './helperFunction';
import { OrderByQuery } from './orderBy';
import { isNonEmptyString, isNonNullableValue, isValidArray } from './util';

type Options<Model> = {
  isDistinct?: boolean;
  orderBy?: ORDER_BY<Model>;
  separator?: string;
};

type AggrCol<Model> =
  | Primitive
  | CallableField
  | InOperationSubQuery<Model, 'WhereNotReq', 'single'>;

type RequiredColumn<Model> = (
  col: AggrCol<Model>,
  options?: Options<Model>,
) => CallableField;

type OptionalColumn<Model> = (
  col?: AggrCol<Model>,
  options?: Options<Model>,
) => CallableField;

type SingleColumn<Model> = (col: AggrCol<Model>) => CallableField;
type DoubleColumn<Model> = (
  col1: AggrCol<Model>,
  col2: AggrCol<Model>,
) => CallableField;

type SingleOperationKeys = Extract<
  AggregateFunctionType,
  | 'max'
  | 'min'
  | 'boolOr'
  | 'boolAnd'
  | 'stdDev'
  | 'variance'
  | 'varPop'
  | 'varSamp'
  | 'stddevPop'
  | 'stddevSamp'
>;

type DoubleOperationKeys = Extract<
  AggregateFunctionType,
  | 'corr'
  | 'covarPop'
  | 'covarSamp'
  | 'regrSlope'
  | 'regrIntercept'
  | 'regrCount'
  | 'regrR2'
  | 'regrAvgX'
  | 'regrAvgY'
  | 'regrSxx'
  | 'regrSyy'
  | 'regrSxy'
>;

type Func<Model extends unknown = unknown> = {
  [Key in AggregateFunctionType]: Key extends SingleOperationKeys
    ? SingleColumn<Model>
    : Key extends DoubleOperationKeys
      ? DoubleColumn<Model>
      : Key extends 'count'
        ? OptionalColumn<Model>
        : RequiredColumn<Model>;
};

interface AggregateFunction extends Func {}

const distinctColFn: Partial<AggregateFunctionType>[] = ['avg', 'count', 'sum'];
const orderByColFn: Partial<AggregateFunctionType>[] = [
  'arrayAgg',
  'stringAgg',
];
const separatorColFn: Partial<AggregateFunctionType>[] = ['stringAgg'];

const prepareAggFn = <Model>(
  col: string[],
  fn: AggregateFunctionType,
  fieldOptions: CallableFieldParam,
  options: Options<Model>,
) => {
  const { preparedValues, groupByFields, allowedFields, isAggregateAllowed } =
    getValidCallableFieldValues(
      fieldOptions,
      'allowedFields',
      'groupByFields',
      'preparedValues',
      'isAggregateAllowed',
    );
  if (!isAggregateAllowed && fn) {
    return throwError.invalidAggFuncPlaceType(fn, (col || 'null').toString());
  }
  if (!aggregateFunctionName[fn]) {
    return throwError.invalidAggFuncPlaceType(fn, (col || 'null').toString());
  }
  const { isDistinct, orderBy, separator } = options;
  const distinctMayBe =
    distinctColFn.includes(fn) && isDistinct ? DB_KEYWORDS.distinct : '';
  const isValidOrderByField =
    orderByColFn.includes(fn) && isValidArray(orderBy);

  const isValidSeparator =
    separatorColFn.includes(fn) && isNonEmptyString(separator);

  const orderByMaybe = isValidOrderByField
    ? OrderByQuery.prepareOrderByQuery(
        allowedFields,
        preparedValues,
        groupByFields,
        orderBy,
      )
    : '';
  const separatorMaybe = isValidSeparator
    ? `,${getPreparedValues(preparedValues, `${separator}`)}`
    : '';
  const colStr = attachArrayWith.coma(col);
  const finalCol = attachArrayWith.space([
    distinctMayBe,
    colStr,
    separatorMaybe,
    orderByMaybe,
  ]);
  const funcUpr = aggregateFunctionName[fn].toUpperCase();
  return `${funcUpr}(${finalCol})`;
};

const getUpdatedColumnAndCustomAllowedFields = <Model>(
  column: AggrCol<Model>,
  fn: AggregateFunctionType,
) => {
  const isStartAllowed = ['count'].includes(fn);
  column =
    isStartAllowed && (typeof column === 'undefined' || column === null)
      ? '*'
      : column;
  const customAllowedFields = isStartAllowed ? ['*'] : [];
  return { column, customAllowedFields };
};

const prepareAggrCol =
  <Model>(
    fn: AggregateFunctionType,
    preparedValues: PreparedValues,
    groupByFields: GroupByFields,
    allowedFields: AllowedFields,
  ) =>
  (col: AggrCol<Model>) => {
    const updatedCol = getUpdatedColumnAndCustomAllowedFields(col, fn);
    col = updatedCol.column;
    const val = getFieldValue(
      fn,
      col,
      preparedValues,
      groupByFields,
      allowedFields,
      {
        customAllowedFields: updatedCol.customAllowedFields,
        refAllowedFields: allowedFields,
      },
    );
    return val;
  };

class AggregateFunction {
  static #instance: null | AggregateFunction = null;

  constructor() {
    if (AggregateFunction.#instance === null) {
      AggregateFunction.#instance = this;
      this.#attachMethods();
    }
    return AggregateFunction.#instance;
  }
  #functionCreator<Model>(
    column: AggrCol<Model>[],
    fn: AggregateFunctionType,
    options: Options<Model>,
  ) {
    return (fieldOptions: CallableFieldParam) => {
      const { preparedValues, groupByFields, allowedFields } =
        getValidCallableFieldValues(
          fieldOptions,
          'allowedFields',
          'preparedValues',
          'groupByFields',
        );
      const vals = column
        .map(prepareAggrCol(fn, preparedValues, groupByFields, allowedFields))
        .filter(isNonNullableValue);
      if (vals.length < 1) {
        return throwError.invalidAggFuncPlaceType(fn, 'null');
      }
      return {
        col: prepareAggFn(vals, fn, fieldOptions, options),
        alias: null,
        ctx: getInternalContext(),
      };
    };
  }

  #aggregateFunc<Model>(fn: AggregateFunctionType) {
    if (doubleParamAggrFunctionNames.has(fn)) {
      return (
        col1: AggrCol<Model>,
        col2: AggrCol<Model>,
        options: Options<Model> = {},
      ) => {
        return this.#functionCreator([col1, col2], fn, options);
      };
    }
    return (col: AggrCol<Model>, options: Options<Model> = {}) => {
      return this.#functionCreator([col], fn, options);
    };
  }

  #attachMethods = () => {
    for (let k in aggregateFunctionName) {
      // @ts-ignore
      this[k] = this.#aggregateFunc(k);
    }
  };
}

export const aggregateFn = new AggregateFunction();
