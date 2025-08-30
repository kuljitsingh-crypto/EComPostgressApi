import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  aggregateFunctionName,
  AggregateFunctionType,
} from '../constants/fieldFunctions';
import { Primitive } from '../globalTypes';
import { CallableField, CallableFieldParam, ORDER_BY } from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  fieldQuote,
  getPreparedValues,
  getValidCallableFieldValues,
  isCallableColumn,
  isPrimitiveValue,
  validCallableColCtx,
} from './helperFunction';
import { OrderByQuery } from './orderBy';

type Options<Model> = {
  isDistinct?: boolean;
  orderBy?: ORDER_BY<Model>;
  separator?: string;
};

type RequiredColumn<Model> = (
  col: Primitive | CallableField,
  options?: Options<Model>,
) => CallableField;

type OptionalColumn<Model> = (
  col?: Primitive | CallableField,
  options?: Options<Model>,
) => CallableField;

type SingleColumn = (col: Primitive | CallableField) => CallableField;

type SingleOperationKeys = Extract<
  AggregateFunctionType,
  'max' | 'min' | 'boolOr' | 'boolAnd' | 'stdDev' | 'variance'
>;

type Func<Model extends unknown = unknown> = {
  [Key in AggregateFunctionType]: Key extends SingleOperationKeys
    ? SingleColumn
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
  col: string,
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
    return throwError.invalidAggFuncPlaceType(fn, col || 'null');
  }
  if (!aggregateFunctionName[fn]) {
    return throwError.invalidAggFuncPlaceType(fn, col || 'null');
  }
  const { isDistinct, orderBy, separator } = options;
  const distinctMayBe =
    distinctColFn.includes(fn) && isDistinct ? DB_KEYWORDS.distinct : '';

  const isValidOrderByField =
    orderByColFn.includes(fn) && Array.isArray(orderBy);

  const isValidSeparator =
    separatorColFn.includes(fn) && typeof separator === 'string';

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

  col = attachArrayWith.space([
    distinctMayBe,
    col,
    separatorMaybe,
    orderByMaybe,
  ]);
  const funcUpr = aggregateFunctionName[fn].toUpperCase();
  return `${funcUpr}(${col})`;
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
    column: Primitive | CallableField,
    fn: AggregateFunctionType,
    options: Options<Model>,
  ) {
    return (fieldOptions: CallableFieldParam) => {
      const { preparedValues } = getValidCallableFieldValues(
        fieldOptions,
        'allowedFields',
        'preparedValues',
      );
      const isStartAllowed = ['count'].includes(fn);
      column =
        isStartAllowed && (typeof column === 'undefined' || column === null)
          ? '*'
          : column;
      const customAllowedFields = isStartAllowed ? ['*'] : [];
      if (isPrimitiveValue(column)) {
        let field = getPreparedValues(preparedValues, column);
        return {
          col: prepareAggFn(field, fn, fieldOptions, options),
          alias: null,
          ctx: getInternalContext(),
        };
      } else if (isCallableColumn(column)) {
        const col = validCallableColCtx(column, {
          ...fieldOptions,
          customAllowedFields,
        });
        col.col = prepareAggFn(col.col, fn, fieldOptions, options);
        return { col: col.col, alias: col.alias, ctx: getInternalContext() };
      }
      return throwError.invalidAggFuncPlaceType(fn, 'null');
    };
  }

  #aggregateFunc<Model>(fn: AggregateFunctionType) {
    return (col: Primitive | CallableField, options: Options<Model> = {}) => {
      return this.#functionCreator(col, fn, options);
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
