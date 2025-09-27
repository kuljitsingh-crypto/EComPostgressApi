import { aggregateFn } from './aggregateFunctionHelper';
import { colFn } from './columnFunctionHelepr';
import { jsonPathFn } from './jsonPathHelper';
import { typeCastFn } from './typeCastHelper';
import { windowFn } from './windowFunctionHelper';

type AggrKeys = keyof typeof aggregateFn;

type Aggr = { [k in AggrKeys]: (typeof aggregateFn)[k] };

interface GlobalFunction extends Aggr {
  cast: typeof typeCastFn;
  window: typeof windowFn;
}

console.log(Object.keys(aggregateFn));
class GlobalFunction {
  static #instance: GlobalFunction | null = null;

  constructor() {
    if (GlobalFunction.#instance === null) {
      GlobalFunction.#instance = this;
      this.cast = typeCastFn;
      this.window = windowFn;
      this.#attachAggregateFunctions();
    }
    return GlobalFunction.#instance;
  }
  #attachAggregateFunctions() {
    for (let key in aggregateFn) {
      //@ts-ignore
      this[key] = aggregateFn[key];
    }
  }

  jPath(...args: Parameters<typeof jsonPathFn>): ReturnType<typeof jsonPathFn> {
    return jsonPathFn(...args);
  }
  col(...args: Parameters<typeof colFn>): ReturnType<typeof colFn> {
    return colFn(...args);
  }
}

export const fn = new GlobalFunction();
