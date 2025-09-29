import { Primitive } from '../globalTypes';
import { throwError } from './errorHelper';
import { attachArrayWith } from './helperFunction';
import { toJsonStr } from './jsonFunctionHelepr';

const funcs = {
  type: 'type',
  size: 'size',
  boolean: 'boolean',
  string: 'string',
  double: 'double',
  ceiling: 'ceiling',
  floor: 'floor',
  abs: 'abs',
  bigint: 'bigint',
  decimal: 'decimal',
  integer: 'integer',
  number: 'number',
  datetime: 'datetime',
  date: 'date',
  time: 'time',
  timeTz: 'time_tz',
  timestamp: 'timestamp',
  timestampTz: 'timestamp_tz',
  keyvalue: 'keyvalue',
  exists: 'exists',
  cardinality: 'cardinality',
};

const operator = {
  eq: '==',
  neq: '!=',
  lte: '<=',
  lt: '<',
  gte: '>=',
  gt: '>',
};

const constant = {
  startBracket: '(',
  endBracket: ')',
  key: 'key',
  wildcard: '*',
  recursive: '**',
  val: 'value',
  and: '&&',
  or: '||',
  not: '!',
  base: '$',
  likeRegex: 'like_regex',
  is: 'is',
  contextBase: '@',
  context: '?',
};

const keysPrefixWithBase = new Set([constant.wildcard, constant.recursive]);

const keyToInitializePrefixFlag = new Set([
  constant.endBracket,
  constant.startBracket,
  constant.and,
  constant.or,
]);

type PrivateData = {
  queryStringArr: Primitive[];
  base: string;
  hasPrefixedWithBase: boolean;
  insideCtx: boolean;
};

const privates = new WeakMap<JPathBuilder, PrivateData>();

const functionHelperObj = {
  startCtx(caller: JPathBuilder) {
    const ref = this.getValidRef(caller);
    ref.insideCtx = true;
  },
  endCtx(caller: JPathBuilder) {
    const ref = this.getValidRef(caller);
    ref.insideCtx = false;
  },
  changeBase(caller: JPathBuilder, base: string) {
    const ref = this.getValidRef(caller);
    ref.base = base;
  },
  base(caller: JPathBuilder) {
    const ref = this.getValidRef(caller);
    return ref.base;
  },
  isInsideCtx(caller: JPathBuilder) {
    const ref = this.getValidRef(caller);
    return ref.insideCtx;
  },
  getValidRef(caller: JPathBuilder) {
    const isJQueryBuildInstance = caller instanceof JPathBuilder;
    const privateRef = privates.get(caller as any);
    if (!isJQueryBuildInstance || !privateRef) {
      return throwError.invalidJsonQueryBuilderType();
    }
    return privateRef;
  },

  addBase(caller: JPathBuilder) {
    const ref = this.getValidRef(caller);
    if (ref.hasPrefixedWithBase) return;
    ref.hasPrefixedWithBase = true;
    ref.queryStringArr.push(ref.base);
  },

  addProperty(caller: JPathBuilder, key: Primitive, addDot = true) {
    const ref = this.getValidRef(caller);
    if (keysPrefixWithBase.has(key as any)) {
      this.addBase(caller);
    }
    if (keyToInitializePrefixFlag.has(key as any)) {
      ref.hasPrefixedWithBase = false;
    }
    key = addDot ? `.${key}` : key;
    ref.queryStringArr.push(key as any);
    return caller;
  },

  addMultiProperties(
    caller: JPathBuilder,
    addDot: boolean,
    ...params: Primitive[]
  ) {
    if (params.length < 0) {
      return throwError.invalidJsonQueryBuilderType();
    }
    this.getValidRef(caller);
    //@ts-ignore
    let ref: JPathBuilder = caller;
    for (let param of params) {
      ref = this.addProperty(caller, param, addDot);
    }
    return ref;
  },
};

function prepareQueryFunction(name: keyof typeof funcs) {
  return function (...params: Primitive[]) {
    let val = funcs[name];
    val = `${val}(${attachArrayWith.coma(params)})`;
    //@ts-ignore
    functionHelperObj.addBase(this);
    //@ts-ignore
    return functionHelperObj.addMultiProperties(this, true, val);
  };
}

function prepareQueryOperator(name: keyof typeof operator) {
  return function (param: Primitive) {
    const val = operator[name];
    param = toJsonStr(param);
    return functionHelperObj.addMultiProperties(
      //@ts-ignore
      this,
      false,
      ' ',
      val,
      ' ',
      param,
    );
  };
}

type Funcs = {
  [key in keyof typeof funcs]: (...params: Primitive[]) => JPathBuilder;
} & {
  [key in keyof typeof operator]: (param: Primitive) => JPathBuilder;
};

export interface JPathBuilder extends Funcs {}

export class JPathBuilder {
  constructor(base?: string) {
    base = base || constant.base;
    privates.set(this, {
      queryStringArr: [],
      base,
      hasPrefixedWithBase: false,
      insideCtx: false,
    });
  }

  #addMultiProperties(addDot: boolean, ...params: Primitive[]) {
    //@ts-ignore
    return functionHelperObj.addMultiProperties(this, addDot, ...params);
  }

  grpStart() {
    return this.#addMultiProperties(false, constant.startBracket);
  }

  grpEnd() {
    return this.#addMultiProperties(false, constant.endBracket);
  }
  ctxStart(key: Primitive) {
    this.key(key);
    functionHelperObj.changeBase(this, constant.contextBase);
    functionHelperObj.startCtx(this);
    return this.#addMultiProperties(
      false,
      ' ',
      constant.context,
      ' ',
      constant.startBracket,
      constant.contextBase,
    );
  }

  ctxEnd() {
    functionHelperObj.endCtx(this);
    functionHelperObj.changeBase(this, constant.base);
    return this.#addMultiProperties(false, constant.endBracket);
  }

  wildcard() {
    return this.#addMultiProperties(true, constant.wildcard);
  }

  recursive() {
    return this.#addMultiProperties(true, constant.recursive);
  }

  key(key: Primitive): JPathBuilder {
    const shouldAppendBase = key !== functionHelperObj.base(this);
    key = typeof key === 'string' && shouldAppendBase ? toJsonStr(key) : key;
    if (shouldAppendBase && !functionHelperObj.isInsideCtx(this)) {
      functionHelperObj.addBase(this);
    }
    return this.#addMultiProperties(shouldAppendBase, key);
  }

  asKey() {
    return this.#addMultiProperties(true, constant.key);
  }

  asVal() {
    return this.#addMultiProperties(true, constant.val);
  }
  not() {
    return this.#addMultiProperties(false, constant.not);
  }

  at(index?: number) {
    let strIndex = typeof index === 'number' ? index : '*';
    strIndex = `[${strIndex}]`;
    functionHelperObj.addBase(this);
    return this.#addMultiProperties(false, strIndex);
  }
  likeRegex(regex: string) {
    regex = toJsonStr(regex);
    return this.#addMultiProperties(false, ' ', constant.likeRegex, ' ', regex);
  }

  is(value: string) {
    return this.#addMultiProperties(false, ' ', constant.is, ' ', value);
  }

  and() {
    const ctxMaybe = functionHelperObj.isInsideCtx(this)
      ? constant.contextBase
      : '';
    return this.#addMultiProperties(false, ' ', constant.and, ' ', ctxMaybe);
  }

  or() {
    const ctxMaybe = functionHelperObj.isInsideCtx(this)
      ? constant.contextBase
      : '';
    return this.#addMultiProperties(false, ' ', constant.or, ' ', ctxMaybe);
  }

  build() {
    const ref = functionHelperObj.getValidRef(this);
    console.log(attachArrayWith.noSpace(ref.queryStringArr, false));
    if (ref.queryStringArr.length < 1) {
      return constant.base;
    }
    return attachArrayWith.noSpace(ref.queryStringArr, false);
  }
}

(function () {
  Object.keys(funcs).forEach((key) => {
    //@ts-ignore
    JPathBuilder.prototype[key] = prepareQueryFunction(key);
  });
  Object.keys(operator).forEach((key) => {
    //@ts-ignore
    JPathBuilder.prototype[key] = prepareQueryOperator(key);
  });
})();
