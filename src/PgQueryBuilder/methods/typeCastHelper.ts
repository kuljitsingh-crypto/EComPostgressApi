import {
  allowedIntervalFields,
  AllowedIntervalFields,
  fieldsParamTypeCast,
  intervalFieldsWithPrecision,
  lengthParamTypeCast,
  noParamTypeCast,
  NoParamTypeCast,
  precisionAndScaleParamTypeCast,
  precisionParamTypeCast,
  TypeCastKeys,
} from '../constants/typeCast';
import { Primitive } from '../globalTypes';
import { CallableField, CallableFieldParam } from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import { getFieldValue } from './fieldFunc';
import {
  attachArrayWith,
  attachMethodToSymbolRegistry,
  getValidCallableFieldValues,
} from './helperFunction';
import { isNullableValue, isValidArray } from './util';

type ParamValue = {
  length: number;
  precision: number;
  scale: number;
  field: AllowedIntervalFields;
};

type TypeCastRefVal = { value: string; paramAllowed: (keyof ParamValue)[] };

type NoParamFunc = (value: Primitive | CallableField) => any;

type ParamFunc = (
  value: Primitive | CallableField,
  options?: Partial<ParamValue>,
) => any;

type TypeCastFunc = {
  [Key in TypeCastKeys]: Key extends NoParamTypeCast ? NoParamFunc : ParamFunc;
};

const castOptionType: Record<keyof ParamValue, any> = {
  length: 'number',
  precision: 'number',
  scale: 'number',
  field: allowedIntervalFields,
};

const prepareCastObj = <T extends TypeCastKeys>(
  ref: Record<T, string>,
  ...paramAllowed: (keyof ParamValue)[]
) => {
  return Object.entries(ref).reduce(
    (pre, acc) => {
      const [key, value] = acc as [string, string];
      (pre as any)[key] = { value, paramAllowed };
      return pre;
    },
    {} as Record<T, TypeCastRefVal>,
  );
};

const getValidTypeValue = (
  paramAllowed: (keyof ParamValue)[],
  castOptions?: ParamValue,
) => {
  castOptions = (castOptions || {}) as ParamValue;
  const getPrimitiveValidValue = (val: unknown, allowedType: string): string =>
    typeof val === allowedType ? (val as string) : '';
  let wrapFieldParamWithBracket = true,
    i = 0;
  const arr: string[] = [];
  for (; i < paramAllowed.length; i++) {
    const acc = paramAllowed[i];
    const val = (castOptions as any)[acc];
    const nextVal = (castOptions as any)[paramAllowed[i + 1]];
    const allowedType = castOptionType[acc];
    if (!allowedType) continue;
    if (isValidArray(allowedType) && allowedType.includes(val)) {
      wrapFieldParamWithBracket = false;
      const precision =
        intervalFieldsWithPrecision.includes(val) &&
        getPrimitiveValidValue(nextVal, castOptionType['precision']);
      arr.push(`${val}${precision ? '(' + precision + ')' : ''}`);
      break;
    } else if (typeof val === allowedType) {
      arr.push(getPrimitiveValidValue(val, allowedType));
    }
  }
  const v = attachArrayWith.coma(arr);
  if (!v) return '';
  if (wrapFieldParamWithBracket) return `(${v})`;
  return ` ${v}`;
};

interface TypeCast extends TypeCastFunc {}

class TypeCast {
  static #instance: TypeCast | null = null;

  constructor() {
    if (TypeCast.#instance === null) {
      TypeCast.#instance = this;
      const methodKeyRef = {
        ...prepareCastObj(noParamTypeCast),
        ...prepareCastObj(lengthParamTypeCast, 'length'),
        ...prepareCastObj(precisionParamTypeCast, 'precision'),
        ...prepareCastObj(precisionAndScaleParamTypeCast, 'precision', 'scale'),
        ...prepareCastObj(fieldsParamTypeCast, 'field', 'precision'),
      };
      this.#attachMethods(methodKeyRef);
    }
    return TypeCast.#instance;
  }

  #prepareMethodForTypeCast(refVal: TypeCastRefVal, key: string) {
    return (value: Primitive | CallableField, castOptions?: ParamValue) => {
      const callable = (options: CallableFieldParam) => {
        const { value: fnName, paramAllowed = [] } = refVal;
        const { allowedFields, groupByFields, preparedValues } =
          getValidCallableFieldValues(
            options,
            'allowedFields',
            'groupByFields',
            'preparedValues',
          );

        const prepareType = () => {
          const v = getValidTypeValue(paramAllowed, castOptions);
          return attachArrayWith.customSep(['::', fnName, v], '');
        };
        const type = prepareType();
        let col: string | null = null;
        col = getFieldValue(
          key,
          value,
          preparedValues,
          groupByFields,
          allowedFields,
        );
        if (isNullableValue(col)) {
          return throwError.invalidColumnNameType('null', allowedFields);
        }
        col += type;
        return { col, alias: null, ctx: getInternalContext() };
      };

      attachMethodToSymbolRegistry(callable, 'castFn', refVal.value);
      return callable;
    };
  }

  #attachMethods<T extends TypeCastKeys>(
    methodKeyRef: Record<T, TypeCastRefVal>,
  ) {
    for (let key in methodKeyRef) {
      //@ts-ignore
      this[key] = this.#prepareMethodForTypeCast(methodKeyRef[key], key);
    }
  }
}

export const typeCastFn = new TypeCast();
