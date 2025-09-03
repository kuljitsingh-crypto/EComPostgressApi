import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  CallableField,
  CallableFieldParam,
  PreparedValues,
} from '../internalTypes';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  getPreparedValues,
  getValidCallableFieldValues,
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

type Func = {
  [key in FrameFunctionKeys]: (
    preceding: typeof unbounded | number,
    following: typeof unbounded | typeof currentRow | number,
  ) => CallableField;
};

interface FrameFunction extends Func {}

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
    suffix: Suffix | '' = '',
  ) => {
    if (typeof param === 'string' && allowedFuncParams.has(param)) {
      return attachArrayWith.space([param, suffix]);
    } else if (typeof param === 'number') {
      return attachArrayWith.space([
        getPreparedValues(preparedValues, param),
        suffix,
      ]);
    }
    return throwError.invalidFrameFunction(methodName);
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
        const { preparedValues } = getValidCallableFieldValues(
          options,
          'preparedValues',
        );
        const validPreceding = this.#getValidParam(
          methodName,
          preceding,
          preparedValues,
          precedingKey,
        );
        const validFollowing = this.#getValidParam(
          methodName,
          following,
          preparedValues,
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
