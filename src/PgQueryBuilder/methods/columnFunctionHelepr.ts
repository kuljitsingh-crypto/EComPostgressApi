import { CallableField, CallableFieldParam } from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import { fieldQuote, getValidCallableFieldValues } from './helperFunction';

export function colFn(col: string): CallableField {
  return (options: CallableFieldParam) => {
    const { allowedFields } = getValidCallableFieldValues(
      options,
      'allowedFields',
    );
    const column = fieldQuote(allowedFields, col);
    return {
      col: column,
      alias: null,
      ctx: getInternalContext(),
    };
  };
}
