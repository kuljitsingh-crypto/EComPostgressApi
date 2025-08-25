import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  aggregateFunctionName,
  AggregateFunctionType,
} from '../constants/fieldFunctions';
import {
  AllowedFields,
  GroupByFields,
  PreparedValues,
  SubQueryColumnAttribute,
} from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import { fieldQuote, validCallableColCtx } from './helperFunction';

type Options = {
  isDistinct?: boolean;
};

class AggregateFunction {
  #functionCreator =
    (
      column: SubQueryColumnAttribute,
      fn: AggregateFunctionType,
      options: Options,
    ) =>
    (
      preparedValues: PreparedValues,
      groupByFields: GroupByFields,
      allowedFields: AllowedFields,
      isAggregateAllowed: boolean,
    ) => {
      const prepareAggFn = (col: string, fn: AggregateFunctionType) => {
        if (!isAggregateAllowed && fn) {
          return throwError.invalidAggFuncPlaceType(fn, col || 'null');
        }
        const { isDistinct } = options;
        if (isDistinct) {
          col = `${DB_KEYWORDS.distinct} ${col}`;
        }
        const funcUpr = fn.toUpperCase();
        return `${funcUpr}(${col})`;
      };

      if (typeof column === 'string') {
        let field = fieldQuote(allowedFields, column);
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
        return col;
      }
      return throwError.invalidAggFuncPlaceType(fn, 'null');
    };

  avg(col: SubQueryColumnAttribute, options: Options = {}) {
    return this.#functionCreator(col, aggregateFunctionName.avg, options);
  }
}

export const aggregateFn = new AggregateFunction();
