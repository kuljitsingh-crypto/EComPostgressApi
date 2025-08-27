import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  aggregateFunctionName,
  AggregateFunctionType,
} from '../constants/fieldFunctions';
import {
  AllowedFields,
  CallableField,
  GroupByFields,
  ORDER_BY,
  PreparedValues,
  SubQueryColumnAttribute,
} from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  fieldQuote,
  getPreparedValues,
  validCallableColCtx,
} from './helperFunction';
import { OrderByQuery } from './orderBy';

type Options<Model> = {
  isDistinct?: boolean;
  orderBy?: ORDER_BY<Model>;
  separator?: string;
};

type RequiredColumn<Model> = (
  col: SubQueryColumnAttribute,
  options?: Options<Model>,
) => CallableField;

type OptionalColumn<Model> = (
  col?: SubQueryColumnAttribute,
  options?: Options<Model>,
) => CallableField;

type SingleColumn = (col: SubQueryColumnAttribute) => CallableField;

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
    column: SubQueryColumnAttribute,
    fn: AggregateFunctionType,
    options: Options<Model>,
  ) {
    return (
      preparedValues: PreparedValues,
      groupByFields: GroupByFields,
      allowedFields: AllowedFields,
      isAggregateAllowed: boolean,
    ) => {
      const prepareAggFn = (col: string, fn: AggregateFunctionType) => {
        if (!isAggregateAllowed && fn) {
          return throwError.invalidAggFuncPlaceType(fn, col || 'null');
        }
        if (!aggregateFunctionName[fn]) {
          return throwError.invalidAggFuncPlaceType(fn, col || 'null');
        }
        const { isDistinct, orderBy, separator } = options;
        const distinctMayBe =
          distinctColFn.includes(fn) && isDistinct ? DB_KEYWORDS.distinct : '';
        const orderByMaybe =
          orderByColFn.includes(fn) && Array.isArray(orderBy)
            ? OrderByQuery.prepareOrderByQuery(
                allowedFields,
                preparedValues,
                groupByFields,
                orderBy,
              )
            : '';
        const separatorMaybe =
          separatorColFn.includes(fn) && typeof separator === 'string'
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
      const isStartAllowed = ['count'].includes(fn);
      column =
        isStartAllowed && (typeof column === 'undefined' || column === null)
          ? '*'
          : column;
      const customAllowFields = isStartAllowed ? ['*'] : [];
      if (typeof column === 'string') {
        let field = fieldQuote(allowedFields, column, { customAllowFields });
        return {
          col: prepareAggFn(field, fn),
          alias: null,
          ctx: getInternalContext(),
        };
      } else if (typeof column === 'function') {
        const col = validCallableColCtx(
          column,
          allowedFields,
          isAggregateAllowed,
          preparedValues,
          groupByFields,
        );
        col.col = prepareAggFn(col.col, fn);
        return { col: col.col, alias: col.alias, ctx: getInternalContext() };
      }
      return throwError.invalidAggFuncPlaceType(fn, 'null');
    };
  }

  #aggregateFunc<Model>(fn: AggregateFunctionType) {
    return (col: SubQueryColumnAttribute, options: Options<Model> = {}) => {
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
