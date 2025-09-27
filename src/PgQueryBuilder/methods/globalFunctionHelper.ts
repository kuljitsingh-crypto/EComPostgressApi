import { aggregateFn } from './aggregateFunctionHelper';
import { colFn } from './columnFunctionHelepr';
import { fieldFn } from './fieldFunctionHelper';
import { jsonPathFn } from './jsonPathHelper';
import { typeCastFn } from './typeCastHelper';
import { frameFn, windowFn } from './windowFunctionHelper';

type AggrKeys = keyof typeof aggregateFn;
type FrameKeys = keyof typeof frameFn;
type fieldKeys = keyof typeof fieldFn;

type Func = { [k in AggrKeys]: (typeof aggregateFn)[k] } & {
  [k in FrameKeys]: (typeof frameFn)[k];
} & {
  [k in fieldKeys]: (typeof fieldFn)[k];
};

interface GlobalFunction extends Func {
  cast: typeof typeCastFn;
  window: typeof windowFn;
}

class GlobalFunction {
  static #instance: GlobalFunction | null = null;

  constructor() {
    if (GlobalFunction.#instance === null) {
      GlobalFunction.#instance = this;
      this.cast = typeCastFn;
      this.window = windowFn;
      this.#attachAggregateFunctions();
      this.#attachFrameFunctions();
      this.#attachFieldFunctions();
    }
    return GlobalFunction.#instance;
  }
  #attachAggregateFunctions() {
    for (let key in aggregateFn) {
      //@ts-ignore
      this[key] = aggregateFn[key];
    }
  }

  #attachFrameFunctions() {
    for (let key in frameFn) {
      //@ts-ignore
      this[key] = frameFn[key];
    }
  }

  #attachFieldFunctions() {
    for (let key in fieldFn) {
      //@ts-ignore
      this[key] = fieldFn[key];
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
