import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  AllowedFields,
  CallableField,
  CallableFieldParam,
  GroupByFields,
  PreparedValues,
} from '../internalTypes';
import { throwError } from './errorHelper';
import { FieldOperand, getFieldValue } from './fieldFunc';
import {
  attachArrayWith,
  getValidCallableFieldValues,
  isNonEmptyString,
} from './helperFunction';

const frameFunction = {
  rows: 'ROWS',
  groups: 'GROUPS',
  range: 'RANGE',
};

const currentRow = 'CURRENT ROW';
const unbounded = 'UNBOUNDED';
const precedingKey = 'PRECEDING';
const followingKey = 'FOLLOWING';

const allowedFuncParams = new Set([unbounded, currentRow]);

type FrameFunctionKeys = keyof typeof frameFunction;
type Suffix = typeof precedingKey | typeof followingKey;

type Func<Model> = {
  [key in FrameFunctionKeys]: (
    preceding: typeof unbounded | FieldOperand<Model, number>,
    following:
      | typeof unbounded
      | typeof currentRow
      | FieldOperand<Model, number>,
  ) => CallableField;
};

interface FrameFunction<>extends Func<any> {}

class FrameFunction {
  static #instance: FrameFunction | null = null;
  constructor() {
    if (FrameFunction.#instance === null) {
      FrameFunction.#instance = this;
      this.#initializeMethods();
    }
    return FrameFunction.#instance;
  }

  #getValidParam = (
    methodName: string,
    param: 'UNBOUNDED' | 'CURRENT ROW' | number,
    preparedValues: PreparedValues,
    groupByFields: GroupByFields,
    allowedFields: AllowedFields,
    suffix: Suffix | '' = '',
  ) => {
    if (isNonEmptyString(param) && allowedFuncParams.has(param)) {
      return attachArrayWith.space([param, suffix]);
    }
    const val = getFieldValue(
      param,
      preparedValues,
      groupByFields,
      allowedFields,
    );
    if (!isNonEmptyString(val)) {
      return throwError.invalidFrameFunction(methodName);
    }
    return attachArrayWith.space([val, suffix]);
  };

  #attachMethods = <T extends FrameFunctionKeys>(methodName: T) => {
    return (
        preceding: 'UNBOUNDED' | number,
        following: 'UNBOUNDED' | 'CURRENT ROW' | number,
      ) =>
      (options: CallableFieldParam) => {
        const method = frameFunction[methodName];
        if (!method) {
          return throwError.invalidFrameFunction(methodName);
        }
        const { preparedValues, allowedFields, groupByFields } =
          getValidCallableFieldValues(
            options,
            'preparedValues',
            'allowedFields',
            'groupByFields',
          );
        const validPreceding = this.#getValidParam(
          methodName,
          preceding,
          preparedValues,
          groupByFields,
          allowedFields,
          precedingKey,
        );
        const validFollowing = this.#getValidParam(
          methodName,
          following,
          preparedValues,
          groupByFields,
          allowedFields,
          following === currentRow ? '' : followingKey,
        );

        return attachArrayWith.space([
          method,
          DB_KEYWORDS.between,
          validPreceding,
          DB_KEYWORDS.and,
          validFollowing,
        ]);
      };
  };

  #initializeMethods() {
    for (let key in frameFunction) {
      //@ts-ignore
      this[key] = this.#attachMethods(key);
    }
  }
}

class WindowFunction {
  static #instance: WindowFunction | null = null;

  constructor() {
    if (WindowFunction.#instance === null) {
      WindowFunction.#instance = this;
      this.#initializeMethods();
    }
    return WindowFunction.#instance;
  }

  #initializeMethods() {}
}

export const windowFn = new WindowFunction();
export const frameFn = new FrameFunction();
