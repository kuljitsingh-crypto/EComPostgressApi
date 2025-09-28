import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  DOUBLE_FIELD_OP,
  SINGLE_FIELD_OP,
  STR_FIELD_OP,
  DoubleFieldOpKeys,
  SingleFieldOpKeys,
  MATH_FIELD_OP,
  STR_IN_FIELD_OP,
  MultipleFieldOpKeys,
  TripleFieldOpKeys,
  TRIPLE_FIELD_OP,
  MULTIPLE_FIELD_OP,
  SUBSTRING_FIELD_OP,
  TRIM_FIELD_OP,
  DATE_EXTRACT_FIELD_OP,
  dateExtractFieldMapping,
  NoPramFieldOpKeys,
  NO_PRAM_FIELD_OP,
  CURRENT_DATE_FIELD_OP,
  CaseOpKeys,
  CASE_FIELD_OP,
} from '../constants/fieldFunctions';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  CallableField,
  CallableFieldParam,
  CaseSubquery,
  GroupByFields,
  PreparedValues,
} from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import { Arg, getFieldValue } from './fieldFunc';
import {
  attachArrayWith,
  attachMethodToSymbolRegistry,
  getValidCallableFieldValues,
  isNonEmptyString,
  isNonNullableValue,
  isValidArray,
  isValidObject,
} from './helperFunction';

type CaseFieldOp = <Model>(...query: CaseSubquery<Model>[]) => CallableField;

type NoFieldOpCb = <Model>() => CallableField;

type DoubleFieldOpCb = <Model>(a: Arg<Model>, b: Arg<Model>) => CallableField;

type SingleFieldOpCb = <Model>(b: Arg<Model>) => CallableField;
type TripleFieldOpCb = <Model>(
  a: Arg<Model>,
  b: Arg<Model>,
  c: Arg<Model>,
) => CallableField;

type MultipleFieldOpCb = <Model>(...args: Arg<Model>[]) => CallableField;

type Ops =
  | DoubleFieldOpKeys
  | SingleFieldOpKeys
  | MultipleFieldOpKeys
  | TripleFieldOpKeys
  | NoPramFieldOpKeys;

type Func = {
  [key in Ops]: key extends NoPramFieldOpKeys
    ? NoFieldOpCb
    : key extends SingleFieldOpKeys
      ? SingleFieldOpCb
      : key extends DoubleFieldOpKeys
        ? DoubleFieldOpCb
        : key extends TripleFieldOpKeys
          ? TripleFieldOpCb
          : key extends CaseOpKeys
            ? CaseFieldOp
            : MultipleFieldOpCb;
};
type OperandType = 'single' | 'double' | 'multiple' | 'triple' | 'noParam';
type AttachType = 'opInBtw' | 'default' | 'custom';

type CommonParamForOpGroup = {
  attachBy: AttachType;
  attachCond?: string[];
  suffixAllowed?: boolean;
  prefixAllowed?: boolean;
  prefixRef?: Record<string, string>;
  suffixRef?: Record<string, string>;
  zeroArgAllowed?: boolean;
  isOpCallable?: boolean;
};

type OpGroup = CommonParamForOpGroup & {
  type: OperandType;
  set: Partial<Record<Ops, string>>;
};

type PrepareCb<Model> = {
  colAndOperands: Arg<Model>[];
  operator: Ops;
  preparedValues: PreparedValues;
  groupByFields: GroupByFields;
  allowedFields: AllowedFields;
  operatorRef: Record<string, string>;
  isNullColAllowed: boolean;
} & CommonParamForOpGroup;

type MultiOperatorFieldCb = {
  operandType: OperandType;
  op: Ops;
  operatorRef: Record<string, string>;
} & CommonParamForOpGroup;

type FieldOperatorCb<Model> = {
  colAndOperands: Arg<Model>[];
  op: Ops;
  operatorRef: Record<string, string>;
  isNullColAllowed: boolean;
} & CommonParamForOpGroup;

interface FieldFunction extends Func {}

// function isCombinedFn(fn: CallableField | CombinedFun): fn is CombinedFun {
//   return fn.length === 0;
// }

const attachOperator = (
  isOpCallable: boolean,
  op: string,
  ...values: Primitive[]
) =>
  isOpCallable
    ? attachArrayWith.customSep([op, `(${attachArrayWith.coma(values)})`], '')
    : attachArrayWith.space([op, attachArrayWith.coma(values)]);

const attachOpInBtwOperator = (
  isOpCallable: boolean,
  op: string,
  ...values: Primitive[]
) => attachArrayWith.space([values[0], op, values[1]]);

const customAttach =
  (attachCond: string[]) =>
  (isOpCallable: boolean, op: string, ...values: Primitive[]) => {
    const valuesLen = values.length;
    const lastAttachStr = attachCond[attachCond.length - 1] ?? '';
    const attachedVal: Primitive[] = [values[0] ?? ''];
    for (let i = 1; i < valuesLen; i++) {
      const attachType = attachCond[i - 1] ?? lastAttachStr;
      attachedVal.push(attachType, values[i]);
    }
    return attachOperator(isOpCallable, op, attachArrayWith.space(attachedVal));
  };

const attachOp = (
  zeroArgAllowed: boolean,
  isOpCallable: boolean,
  op: string,
  attachBy: AttachType,
  attachCond: string[],
  ...values: Primitive[]
) => {
  values = values.filter(isNonNullableValue);
  if (values.length < 1 && !zeroArgAllowed) {
    return throwError.invalidOpDataType(op);
  }
  let opCb: (
    isOpCallable: boolean,
    op: string,
    ...values: Primitive[]
  ) => string = attachOperator;
  switch (attachBy) {
    case 'opInBtw':
      opCb = attachOpInBtwOperator;
      break;
    case 'custom':
      if (isValidArray(attachCond)) {
        opCb = customAttach(attachCond);
      }
      break;
  }

  return opCb(isOpCallable, op, ...values);
};

const resolveOperand = <Model>(
  op: string,
  colAndOperands: Arg<Model>[],
  allowedFields: AllowedFields,
  preparedValues: PreparedValues,
  groupByFields: GroupByFields,
  isNullColAllowed: boolean,
  prefixValue: string | null,
  suffixValue: string | null,
) => {
  const isValidPrefixValue = isNonEmptyString(prefixValue);
  const isValidSuffixValue = isNonEmptyString(suffixValue);
  const operandsRef: Primitive[] = isValidPrefixValue ? [prefixValue] : [];
  colAndOperands.forEach((arg) => {
    const value = getFieldValue(
      op,
      arg,
      preparedValues,
      groupByFields,
      allowedFields,
    );
    if (value === null && !isNullColAllowed) {
      throwError.invalidColumnNameType('null', allowedFields);
    }
    operandsRef.push(value);
  });
  if (isValidSuffixValue) {
    operandsRef.push(suffixValue);
  }
  return operandsRef;
};

const prepareFields = <Model>(params: PrepareCb<Model>) => {
  const {
    colAndOperands,
    operator,
    attachBy,
    operatorRef,
    preparedValues,
    groupByFields,
    allowedFields,
    attachCond = [],
    isNullColAllowed = false,
    prefixAllowed = false,
    prefixRef = {},
    suffixAllowed = false,
    suffixRef = {},
    zeroArgAllowed = false,
    isOpCallable = true,
  } = params;
  const op = operatorRef[operator];
  if (!op) {
    return throwError.invalidColumnOpType(op, Object.keys(operatorRef));
  }
  const validPrefixRef = prefixAllowed && isValidObject(prefixRef);
  const validSuffixRef = suffixAllowed && isValidObject(suffixRef);
  const prefixValue = (validPrefixRef && prefixRef[operator]) || null;
  const suffixValue = (validSuffixRef && suffixRef[operator]) || null;
  const operands = resolveOperand(
    op,
    colAndOperands,
    allowedFields,
    preparedValues,
    groupByFields,
    isNullColAllowed,
    prefixValue,
    suffixValue,
  );
  return attachOp(
    zeroArgAllowed,
    isOpCallable,
    op,
    attachBy,
    attachCond,
    ...operands,
  );
};

const getColAndOperands = <Model>(
  type: OperandType,
  ...operands: Arg<Model>[]
): Arg<Model>[] => {
  switch (type) {
    case 'noParam':
      return [];
    case 'single':
      return [operands[0]];
    case 'double':
      return operands.slice(0, 2);
    case 'triple':
      return operands.slice(0, 3);
    case 'multiple':
      return operands;
    default:
      return throwError.invalidOperandType();
  }
};

const opGroups: OpGroup[] = [
  {
    set: NO_PRAM_FIELD_OP,
    type: 'noParam',
    attachBy: 'default',
    zeroArgAllowed: true,
  },
  {
    set: CURRENT_DATE_FIELD_OP,
    type: 'noParam',
    attachBy: 'default',
    zeroArgAllowed: true,
    isOpCallable: false,
  },
  {
    set: SINGLE_FIELD_OP,
    type: 'single',
    attachBy: 'default',
  },

  {
    set: DATE_EXTRACT_FIELD_OP,
    type: 'single',
    attachBy: 'custom',
    attachCond: [DB_KEYWORDS.from],
    prefixAllowed: true,
    prefixRef: dateExtractFieldMapping,
  },
  {
    set: TRIM_FIELD_OP,
    type: 'double',
    attachBy: 'custom',
    attachCond: [DB_KEYWORDS.from],
  },
  {
    set: MATH_FIELD_OP,
    type: 'double',
    attachBy: 'opInBtw',
  },
  {
    set: DOUBLE_FIELD_OP,
    type: 'double',
    attachBy: 'default',
  },
  {
    set: STR_FIELD_OP,
    type: 'double',
    attachBy: 'default',
  },
  {
    set: STR_IN_FIELD_OP,
    type: 'double',
    attachBy: 'custom',
    attachCond: [DB_KEYWORDS.in],
  },
  {
    set: TRIPLE_FIELD_OP,
    type: 'triple',
    attachBy: 'default',
  },
  {
    set: SUBSTRING_FIELD_OP,
    type: 'triple',
    attachBy: 'custom',
    attachCond: [DB_KEYWORDS.from, DB_KEYWORDS.for],
  },
  {
    set: MULTIPLE_FIELD_OP,
    type: 'multiple',
    attachBy: 'default',
  },
  {
    set: CASE_FIELD_OP,
    type: 'multiple',
    attachBy: 'custom',
    attachCond: [''],
    isOpCallable: false,
    suffixAllowed: true,
    suffixRef: { case: 'END' },
  },
] as const;

class FieldFunction {
  #fieldFunc = false;
  static #instance: FieldFunction | null = null;

  constructor() {
    if (FieldFunction.#instance === null) {
      FieldFunction.#instance = this;
      this.#fieldFunc = true;
      this.#attachFieldMethods();
    }
    return FieldFunction.#instance;
  }

  #attachFieldMethods() {
    let op: Ops;
    opGroups.forEach(({ set, type, ...rest }) => {
      for (const op in set) {
        // @ts-ignore
        this[op] = this.#multiFieldOperator({
          operandType: type,
          op: op as Ops,
          operatorRef: set,
          ...rest,
        });
      }
    });
  }

  #multiFieldOperator = (args: MultiOperatorFieldCb) => {
    const { operandType, op, operatorRef, ...rest } = args;
    return <Model>(...ops: Arg<Model>[]) => {
      const colAndOperands = getColAndOperands(operandType, ...ops);
      return this.#operateOnFields({
        colAndOperands,
        op,
        isNullColAllowed: false,
        operatorRef,
        ...rest,
      });
    };
  };

  #checkFunctionExecutionState() {
    if (!this.#fieldFunc) {
      return throwError.invalidFieldFuncCallType();
    }
  }

  #operateOnFields<Model>(args: FieldOperatorCb<Model>) {
    const { colAndOperands, op, operatorRef, isNullColAllowed, ...rest } = args;
    const callable = (options: CallableFieldParam) => {
      const { preparedValues, groupByFields, allowedFields } =
        getValidCallableFieldValues(
          options,
          'allowedFields',
          'groupByFields',
          'preparedValues',
        );
      this.#checkFunctionExecutionState();
      const value = prepareFields<Model>({
        colAndOperands,
        operator: op,
        preparedValues,
        groupByFields,
        allowedFields,
        operatorRef,
        isNullColAllowed,
        ...rest,
      });
      return {
        col: value,
        alias: null,
        ctx: getInternalContext(),
      };
    };
    attachMethodToSymbolRegistry(callable, 'fieldFn', op);
    return callable;
  }
}

export const fieldFn = new FieldFunction();
