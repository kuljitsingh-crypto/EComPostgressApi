import {
  CallableField,
  CallableFieldParam,
  FieldMetadata,
} from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import {
  attachMethodToSymbolRegistry,
  fieldQuote,
  getValidCallableFieldValues,
} from './helperFunction';

export function colFn(col: string, colOptions?: { asJson?: boolean }): any {
  const callable = (options: CallableFieldParam) => {
    const { asJson } = colOptions || {};
    const { allowedFields, preparedValues } = getValidCallableFieldValues(
      options,
      'allowedFields',
      'preparedValues',
    );
    const metadata = {} as FieldMetadata;
    const customAllowFields = options?.customAllowedFields || [];
    const column = fieldQuote(allowedFields, preparedValues, col, {
      customAllowFields,
      metadata,
      asJson,
    });
    return {
      col: metadata.isJSONField ? `(${column})` : column,
      alias: null,
      ctx: getInternalContext(),
    };
  };
  attachMethodToSymbolRegistry(callable, 'colFn');
  return callable;
}
