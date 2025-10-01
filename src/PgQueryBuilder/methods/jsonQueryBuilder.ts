import { Primitive } from '../globalTypes';
import { throwError } from './errorHelper';
import { attachArrayWith } from './helperFunction';
import { toJsonStr } from './jsonFunctionHelepr';
import { isNonNullableValue, isNullableValue } from './util';

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
} as const;

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

const funcHelper = {
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

  addProperty(
    caller: JPathBuilder,
    key: Primitive,
    addDot = true,
    isReplace = false,
  ) {
    const ref = this.getValidRef(caller);
    if (keysPrefixWithBase.has(key as any) && !funcHelper.isInsideCtx(caller)) {
      this.addBase(caller);
    }
    if (keyToInitializePrefixFlag.has(key as any)) {
      ref.hasPrefixedWithBase = false;
    }
    key = addDot ? `.${key}` : key;
    if (isReplace) {
      ref.queryStringArr[ref.queryStringArr.length - 1] = key;
    } else {
      ref.queryStringArr.push(key);
    }
    return caller;
  },

  addMultiProperties(
    caller: JPathBuilder,
    addDot: boolean,
    isReplace: boolean,
    ...params: Primitive[]
  ) {
    if (params.length < 0) {
      return throwError.invalidJsonQueryBuilderType();
    }
    this.getValidRef(caller);
    //@ts-ignore
    let ref: JPathBuilder = caller;
    for (let param of params) {
      ref = this.addProperty(caller, param, addDot, isReplace);
    }
    return ref;
  },
};

function prepareQueryFunction(name: keyof typeof funcs) {
  return function (...params: Primitive[]) {
    let val = funcs[name];
    val = `${val}(${attachArrayWith.coma(params)})`;
    //@ts-ignore
    funcHelper.addBase(this);
    //@ts-ignore
    return funcHelper.addMultiProperties(this, true, false, val);
  };
}

function prepareQueryOperator(name: keyof typeof operator) {
  return function (param: Primitive) {
    const val = operator[name];
    param = toJsonStr(param);
    return funcHelper.addMultiProperties(
      //@ts-ignore
      this,
      false,
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

  #addMultiProperties(
    addDot: boolean,
    isReplace: boolean,
    ...params: Primitive[]
  ) {
    return funcHelper.addMultiProperties(this, addDot, isReplace, ...params);
  }

  grpStart(key: string | number | null) {
    let ref: JPathBuilder = this;
    if (funcHelper.isInsideCtx(this)) {
      funcHelper.addProperty(this, constant.startBracket, false, true);
      ref = this.#addMultiProperties(false, false, constant.contextBase);
    } else {
      ref = this.#addMultiProperties(false, false, constant.startBracket);
    }
    if (isNonNullableValue(key)) {
      this.key(key);
    }
    return ref;
  }

  grpEnd() {
    return this.#addMultiProperties(false, false, constant.endBracket);
  }
  ctxStart(key: string | number, at?: number | (typeof constant)['wildcard']) {
    this.key(key);
    if (typeof at !== 'undefined') {
      this.at(at as any);
    }
    funcHelper.changeBase(this, constant.contextBase);
    funcHelper.startCtx(this);
    return this.#addMultiProperties(
      false,
      false,
      ' ',
      constant.context,
      ' ',
      constant.startBracket,
      constant.contextBase,
    );
  }

  ctxEnd() {
    funcHelper.endCtx(this);
    funcHelper.changeBase(this, constant.base);
    return this.#addMultiProperties(false, false, constant.endBracket);
  }

  wildcard() {
    return this.#addMultiProperties(true, false, constant.wildcard);
  }

  recursive() {
    return this.#addMultiProperties(true, false, constant.recursive);
  }

  key(key: string | number): JPathBuilder {
    if (isNullableValue(key)) {
      return this;
    }
    const shouldAppendBase = key !== funcHelper.base(this);
    key = typeof key === 'string' && shouldAppendBase ? toJsonStr(key) : key;
    if (shouldAppendBase && !funcHelper.isInsideCtx(this)) {
      funcHelper.addBase(this);
    }
    return this.#addMultiProperties(shouldAppendBase, false, key);
  }

  asKey() {
    return this.#addMultiProperties(true, false, constant.key);
  }

  asVal() {
    return this.#addMultiProperties(true, false, constant.val);
  }
  not() {
    return this.#addMultiProperties(false, false, constant.not);
  }

  at(index?: number) {
    let strIndex = typeof index === 'number' ? index : '*';
    strIndex = `[${strIndex}]`;
    if (!funcHelper.isInsideCtx(this)) {
      funcHelper.addBase(this);
    }
    return this.#addMultiProperties(false, false, strIndex);
  }
  likeRegex(regex: string) {
    regex = toJsonStr(regex);
    return this.#addMultiProperties(
      false,
      false,
      ' ',
      constant.likeRegex,
      ' ',
      regex,
    );
  }

  is(value: string) {
    return this.#addMultiProperties(false, false, ' ', constant.is, ' ', value);
  }

  and() {
    if (funcHelper.isInsideCtx(this)) {
      return this.#addMultiProperties(
        false,
        false,
        ' ',
        constant.and,
        ' ',
        constant.contextBase,
      );
    }
    return this.#addMultiProperties(false, false, ' ', constant.and, ' ');
  }

  or() {
    if (funcHelper.isInsideCtx(this)) {
      return this.#addMultiProperties(
        false,
        false,
        ' ',
        constant.or,
        ' ',
        constant.contextBase,
      );
    }
    return this.#addMultiProperties(false, false, ' ', constant.or, ' ');
  }

  build() {
    const ref = funcHelper.getValidRef(this);
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
