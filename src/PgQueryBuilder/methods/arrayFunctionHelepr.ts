import { PgDataType } from '../constants/dataTypes';
import { Primitive } from '../globalTypes';
import { CallableFieldParam } from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { ArrayArg, getFieldValue } from './fieldFunc';
import {
  attachMethodToSymbolRegistry,
  getValidCallableFieldValues,
} from './helperFunction';

type AllowedKeys = Exclude<
  keyof typeof PgDataType,
  'string' | 'numeric' | 'enum'
>;

export function arrayFn<Model, P extends Primitive = Primitive>(
  arg: ArrayArg<P, Model>[],
  arrOptions?: {
    type?: (typeof PgDataType)[AllowedKeys];
  },
): any {
  const callable = (options: CallableFieldParam) => {
    const { type } = arrOptions || {};
    const { allowedFields, preparedValues, groupByFields } =
      getValidCallableFieldValues(
        options,
        'allowedFields',
        'preparedValues',
        'groupByFields',
      );
    const value = getFieldValue(
      null,
      arg,
      preparedValues,
      groupByFields,
      allowedFields,
      { customArrayType: type },
    );
    return {
      col: value,
      alias: null,
      ctx: getInternalContext(),
    };
  };
  attachMethodToSymbolRegistry(callable, 'arrayFn');
  return callable;
}
