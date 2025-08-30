class UtilityFunctionHelper {
  static #instance: UtilityFunctionHelper | null = null;
  constructor() {
    if (UtilityFunctionHelper.#instance === null) {
      UtilityFunctionHelper.#instance = this;
    }
    return UtilityFunctionHelper.#instance;
  }
}

export const utilFn = new UtilityFunctionHelper();
