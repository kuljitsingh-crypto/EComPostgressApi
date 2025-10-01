import { Primitive } from '../globalTypes';
import { CallableFieldParam } from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  attachMethodToSymbolRegistry,
  dynamicFieldQuote,
  getPreparedValues,
  getValidCallableFieldValues,
  prepareFieldForJson,
} from './helperFunction';
import {
  isNonEmptyString,
  isValidArray,
  isValidBoolean,
  isValidNumber,
} from './util';

const filterOutValidData = (d: unknown) =>
  isNonEmptyString(d) || isValidBoolean(d) || isValidNumber(d);

export function jsonPathFn(
  path: string | Primitive[],
  colOptions?: { asJson?: boolean },
): any {
  const callable = (options: CallableFieldParam) => {
    const { asJson = false } = colOptions || {};
    const { preparedValues } = getValidCallableFieldValues(
      options,
      'preparedValues',
    );

    if (isValidArray(path)) {
      path = path.filter(filterOutValidData);
      // .map((p) => getPreparedValues(preparedValues, p));
      if (path.length < 1) {
        return throwError.invalidArrayDataType();
      }
      path = `{${attachArrayWith.coma(path)}}`;
      const d = getPreparedValues(preparedValues, path);
      return {
        col: d,
        alias: null,
        ctx: getInternalContext(),
      };
    } else if (isNonEmptyString(path)) {
      path = path.split('.').filter((p) => dynamicFieldQuote(p, []));
      return {
        col: prepareFieldForJson(path as string[], preparedValues, 1, asJson),
        alias: null,
        ctx: getInternalContext(),
      };
    }
    return throwError.invalidJsonPathType(path);
  };
  attachMethodToSymbolRegistry(callable, 'jsonPathFn');
  return callable;
}
