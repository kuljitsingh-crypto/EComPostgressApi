import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  doubleExprWindowFns,
  exprWithExtraWindowFns,
  noArgWindowFns,
  singleExprWindowFns,
  SingleParamWindowFunction,
  windowFunctionNames,
  WindowFunctions,
  ZeroParamWindowFunction,
} from '../constants/fieldFunctions';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  CallableField,
  CallableFieldParam,
  DBField,
  GroupByFields,
  ORDER_BY,
  PreparedValues,
} from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import { Arg, getFieldValue } from './fieldFunc';
import {
  attachArrayWith,
  attachMethodToSymbolRegistry,
  getValidCallableFieldValues,
  prepareMultipleValues,
} from './helperFunction';
import { OrderByQuery } from './orderBy';
import { isNonEmptyObject, isNonEmptyString, isNullableValue } from './util';

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

type FrameFunc<Model> = {
  [key in FrameFunctionKeys]: (
    preceding: typeof unbounded | Arg<Model, number>,
    following: typeof unbounded | typeof currentRow | Arg<Model, number>,
  ) => CallableField;
};

type WindowFunctionOptions<Model> = {
  orderBy?: ORDER_BY<Model>;
  offset?: number;
  defaultValue?: number;
  n?: number;
  frameOption?: CallableField;
  partitionBy?: DBField<Model>;
};

type NoArgWindowFunction<Model> = (
  options?: WindowFunctionOptions<Model>,
) => CallableField;

type SingleArgWindowFunction<Model> = (
  arg: Primitive,
  options?: WindowFunctionOptions<Model>,
) => CallableField;

type DoubleArgWindowFunction<Model> = (
  arg1: Primitive,
  arg2: Primitive,
  options?: WindowFunctionOptions<Model>,
) => CallableField;

type WindowFunc<Model> = {
  [key in WindowFunctions]: key extends ZeroParamWindowFunction
    ? NoArgWindowFunction<Model>
    : key extends SingleParamWindowFunction
      ? SingleArgWindowFunction<Model>
      : DoubleArgWindowFunction<Model>;
};

interface FrameFunction<>extends FrameFunc<any> {}
interface WindowFunction<>extends WindowFunc<any> {}

const prepareArg = <Model>(
  methodName: WindowFunctions,
  ...args: unknown[]
): {
  arg1?: Primitive;
  arg2?: Primitive;
  options: WindowFunctionOptions<Model>;
} => {
  const getValidOption = (options: unknown) => {
    const isValidOptions =
      isNullableValue(options) || isNonEmptyObject(options);
    if (!isValidOptions) {
      return throwError.invalidWindowFuncOpt(methodName);
    }
    return options;
  };
  const result = { arg1: undefined, arg2: undefined, options: {} };
  if (methodName in noArgWindowFns) {
    const options = getValidOption(args[0]);
    result.options = options || {};
  } else if (methodName in singleExprWindowFns) {
    const options = getValidOption(args[1]);
    (result as any).arg1 = args[0] as Primitive;
    result.options = options || {};
  } else if (methodName in doubleExprWindowFns) {
    const options = getValidOption(args[2]);
    (result as any).arg1 = args[0] as Primitive;
    (result as any).arg2 = args[1] as Primitive;
    result.options = options || {};
  }
  return result;
};

class FrameFunction {
  static #instance: FrameFunction | null = null;
  constructor() {
    if (FrameFunction.#instance === null) {
      FrameFunction.#instance = this;
      this.#initializeMethods();
    }
    return FrameFunction.#instance;
  }

  static #getValidParam = (
    methodName: string,
    param: 'UNBOUNDED' | 'CURRENT ROW' | number | CallableField,
    preparedValues: PreparedValues,
    groupByFields: GroupByFields,
    allowedFields: AllowedFields,
    suffix: Suffix | '' = '',
  ) => {
    if (isNonEmptyString(param) && allowedFuncParams.has(param)) {
      return attachArrayWith.space([param, suffix]);
    }
    const val = getFieldValue(
      methodName,
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
        preceding: 'UNBOUNDED' | number | CallableField,
        following: 'UNBOUNDED' | 'CURRENT ROW' | number | CallableField,
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
        const validPreceding = FrameFunction.#getValidParam(
          methodName,
          preceding,
          preparedValues,
          groupByFields,
          allowedFields,
          precedingKey,
        );
        const validFollowing = FrameFunction.#getValidParam(
          methodName,
          following,
          preparedValues,
          groupByFields,
          allowedFields,
          following === currentRow ? '' : followingKey,
        );

        const col = attachArrayWith.space([
          method,
          DB_KEYWORDS.between,
          validPreceding,
          DB_KEYWORDS.and,
          validFollowing,
        ]);
        return { col, alias: null, ctx: getInternalContext() };
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

  #attachMethod(methodName: WindowFunctions) {
    return (...args: unknown[]) => {
      const callable = (callableParams: CallableFieldParam) => {
        const { arg1, arg2, options } = prepareArg(methodName, ...args);
        const { frameOption, offset, defaultValue, n, orderBy, partitionBy } =
          options || {};
        const { allowedFields, groupByFields, preparedValues } =
          getValidCallableFieldValues(
            callableParams,
            'allowedFields',
            'groupByFields',
            'preparedValues',
          );
        const frameOptionMaybe =
          getFieldValue(
            methodName,
            frameOption,
            preparedValues,
            groupByFields,
            allowedFields,
          ) ?? '';
        const partitionByMaybe = getFieldValue(
          methodName,
          partitionBy,
          preparedValues,
          groupByFields,
          allowedFields,
        );
        const orderByMaybe = OrderByQuery.prepareOrderByQuery(
          allowedFields,
          preparedValues,
          groupByFields,
          orderBy,
        );
        const {
          offset: o,
          defaultValue: deValue,
          n: num,
          arg1: param1,
          arg2: param2,
        } = prepareMultipleValues(preparedValues, {
          offset: { type: 'number', val: offset },
          defaultValue: { type: 'number', val: defaultValue },
          n: { type: 'number', val: n },
          arg1: { type: 'primitive', val: arg1 },
          arg2: { type: 'primitive', val: arg2 },
        });
        const namesArr = [windowFunctionNames[methodName], '('];
        if (methodName === 'nthValue') {
          namesArr.push(param1, num);
        } else if (methodName in exprWithExtraWindowFns) {
          namesArr.push(param1, o, deValue);
        } else if (methodName in singleExprWindowFns) {
          namesArr.push(param1);
        } else if (methodName in doubleExprWindowFns) {
          namesArr.push(param1, param2);
        }
        namesArr.push(')');
        const functionName = attachArrayWith.noSpace(namesArr);
        const overArr = [DB_KEYWORDS.over, ' ', '('];
        const partitions = [];
        if (partitionByMaybe) {
          partitions.push(DB_KEYWORDS.partitionBy, partitionByMaybe);
        }
        if (orderByMaybe) {
          partitions.push(orderByMaybe);
        }
        partitions.push(frameOptionMaybe);
        overArr.push(attachArrayWith.space(partitions), ')');
        const over = attachArrayWith.noSpace(overArr, false);
        const col = attachArrayWith.space([functionName, over]);
        return { col: col, alias: null, ctx: getInternalContext() };
      };
      attachMethodToSymbolRegistry(callable, 'windowFn', methodName);
      return callable;
    };
  }

  #initializeMethods() {
    for (let key in windowFunctionNames) {
      //@ts-ignore
      this[key] = this.#attachMethod(key);
    }
  }
}

export const windowFn = new WindowFunction();
export const frameFn = new FrameFunction();
