import { CallableField, CallableFieldParam } from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import { fieldQuote, isValidAllowedFields } from './helperFunction';

class UtilityFunctionHelper {
  static #instance: UtilityFunctionHelper | null = null;
  constructor() {
    if (UtilityFunctionHelper.#instance === null) {
      UtilityFunctionHelper.#instance = this;
    }
    return UtilityFunctionHelper.#instance;
  }

  col(col: string): CallableField {
    return (options: CallableFieldParam) => {
      const { allowedFields } = options || {};
      const hasValidRequiredFields = isValidAllowedFields(allowedFields);

      if (!hasValidRequiredFields) {
        return throwError.invalidFieldFuncCallType();
      }
      const column = fieldQuote(allowedFields, col);
      return {
        col: column,
        alias: null,
        ctx: getInternalContext(),
      };
    };
  }
}

export const utilFn = new UtilityFunctionHelper();
