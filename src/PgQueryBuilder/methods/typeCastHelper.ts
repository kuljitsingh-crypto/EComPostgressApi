import {
  NoParamTypeCast,
  TypeCastKeys,
  TypeCastValue,
} from '../constants/typeCast';
import { Primitive } from '../globalTypes';

type ParamValue = {
  length: number;
  precision: number;
  scale: number;
  fields: Primitive[];
};

type NoParamFunc = (
  value: Primitive,
) => <K extends TypeCastKeys>(
  key: K,
) => { value: Primitive; type: TypeCastValue<K> };

type ParamFunc = (
  value: Primitive,
  options?: Partial<ParamValue>,
) => <K extends TypeCastKeys>(
  key: K,
) => { value: Primitive; type: TypeCastValue<K>; params: Primitive[] };

type TypeCastFunc = {
  [Key in TypeCastKeys]: Key extends NoParamTypeCast ? NoParamFunc : ParamFunc;
};

interface TypeCast extends TypeCastFunc {}
class TypeCast {
  static #instance: TypeCast | null = null;
  constructor() {
    if (TypeCast.#instance === null) {
      TypeCast.#instance = this;
    }
    return TypeCast.#instance;
  }
}

export const typeCastFn = new TypeCast();
