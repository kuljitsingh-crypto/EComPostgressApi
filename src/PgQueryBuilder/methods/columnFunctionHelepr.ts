import { CallableField, CallableFieldParam } from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import { fieldQuote, isValidAllowedFields } from './helperFunction';

export function colFn(col: string): CallableField {
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
