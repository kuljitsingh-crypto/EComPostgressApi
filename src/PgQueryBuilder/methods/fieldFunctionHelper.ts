import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  DOUBLE_FIELD_OP,
  SINGLE_FIELD_OP,
  STR_FIELD_OP,
  DoubleFieldOpKeys,
  SingleFieldOpKeys,
  SYMBOL_FIELD_OP,
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
  CustomFunctionKeys,
  CUSTOM_FIELD_OP,
  NOT_FIELD_OP,
  ARRAY_INDEX_OP,
  ARRAY_SLICE_OP,
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
} from './helperFunction';
import {
  isNonEmptyString,
  isNonNullableValue,
  isValidArray,
  isValidObject,
} from './util';

type CustomFieldOptions = {
  name: string;
  callable?: boolean;
  suffix?: string;
  prefix?: string;
  attachMode?: AttachMode;
  conditions?: string[];
};

type CaseFieldOp = <Model>(...query: CaseSubquery<Model>[]) => CallableField;

type CustomFieldOp = <Model>(
  options: CustomFieldOptions,
  ...args: Arg<Model>[]
) => CallableField;

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
            : key extends CustomFunctionKeys
              ? CustomFieldOp
              : MultipleFieldOpCb;
};
type OperandType = 'single' | 'double' | 'multiple' | 'triple' | 'noParam';
type AttachMode = 'operatorInBetween' | 'default' | 'custom' | 'arrayOperator';

type CommonParamForOpGroup = {
  attachMode: AttachMode;
  attachCond?: string[];
  suffixAllowed?: boolean;
  prefixAllowed?: boolean;
  prefixRef?: Record<string, string>;
  suffixRef?: Record<string, string>;
  zeroArgAllowed?: boolean;
  callable?: boolean;
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

const attachOperator = (
  callable: boolean,
  op: string,
  ...values: Primitive[]
) =>
  callable
    ? attachArrayWith.customSep([op, `(${attachArrayWith.coma(values)})`], '')
    : attachArrayWith.space([op, attachArrayWith.coma(values)]);

const attachOpInBtwOperator = (
  callable: boolean,
  op: string,
  ...values: Primitive[]
) => {
  const arrWithOp = values.reduce((acc, val, index) => {
    if (index > 0) {
      acc.push(op);
    }
    acc.push(val);
    return acc;
  }, [] as Primitive[]);
  return attachArrayWith.space(arrWithOp);
};

const customAttach =
  (attachCond: string[]) =>
  (callable: boolean, op: string, ...values: Primitive[]) => {
    const valuesLen = values.length;
    const lastAttachStr = attachCond[attachCond.length - 1] ?? '';
    const attachedVal: Primitive[] = [values[0] ?? ''];
    for (let i = 1; i < valuesLen; i++) {
      const attachType = attachCond[i - 1] ?? lastAttachStr;
      attachedVal.push(attachType, values[i]);
    }
    return attachOperator(callable, op, attachArrayWith.space(attachedVal));
  };

const arrayAttach = (callable: boolean, op: string, ...values: Primitive[]) => {
  const [first, ...rest] = values;
  const valuesStr = attachArrayWith.customSep(rest, ':');
  return attachArrayWith.noSpace([first, '[', valuesStr, ']']);
};

const attachOp = (
  zeroArgAllowed: boolean,
  callable: boolean,
  op: string,
  attachMode: AttachMode,
  attachCond: string[],
  ...values: Primitive[]
) => {
  values = values.filter(isNonNullableValue);
  if (values.length < 1 && !zeroArgAllowed) {
    return throwError.invalidOpDataType(op);
  }
  let opCb: (callable: boolean, op: string, ...values: Primitive[]) => string =
    attachOperator;
  switch (attachMode) {
    case 'operatorInBetween':
      opCb = attachOpInBtwOperator;
      break;
    case 'custom':
      if (isValidArray(attachCond)) {
        opCb = customAttach(attachCond);
      }
      break;
    case 'arrayOperator':
      opCb = arrayAttach;
      break;
  }

  return opCb(callable, op, ...values);
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
    attachMode,
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
    callable = true,
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
    callable,
    op,
    attachMode,
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
    attachMode: 'default',
    zeroArgAllowed: true,
  },
  {
    set: CURRENT_DATE_FIELD_OP,
    type: 'noParam',
    attachMode: 'default',
    zeroArgAllowed: true,
    callable: false,
  },
  {
    set: SINGLE_FIELD_OP,
    type: 'single',
    attachMode: 'default',
  },

  {
    set: DATE_EXTRACT_FIELD_OP,
    type: 'single',
    attachMode: 'custom',
    attachCond: [DB_KEYWORDS.from],
    prefixAllowed: true,
    prefixRef: dateExtractFieldMapping,
  },
  {
    set: NOT_FIELD_OP,
    type: 'single',
    attachMode: 'custom',
    attachCond: [DB_KEYWORDS.not],
    callable: false,
  },

  {
    set: TRIM_FIELD_OP,
    type: 'double',
    attachMode: 'custom',
    attachCond: [DB_KEYWORDS.from],
  },
  {
    set: SYMBOL_FIELD_OP,
    type: 'double',
    attachMode: 'operatorInBetween',
  },
  {
    set: DOUBLE_FIELD_OP,
    type: 'double',
    attachMode: 'default',
  },
  {
    set: STR_FIELD_OP,
    type: 'double',
    attachMode: 'default',
  },
  {
    set: STR_IN_FIELD_OP,
    type: 'double',
    attachMode: 'custom',
    attachCond: [DB_KEYWORDS.in],
  },
  {
    set: STR_IN_FIELD_OP,
    type: 'double',
    attachMode: 'custom',
    attachCond: [DB_KEYWORDS.in],
  },
  {
    set: STR_IN_FIELD_OP,
    type: 'double',
    attachMode: 'custom',
    attachCond: [DB_KEYWORDS.in],
  },
  {
    set: ARRAY_INDEX_OP,
    type: 'double',
    attachMode: 'arrayOperator',
  },
  {
    set: TRIPLE_FIELD_OP,
    type: 'triple',
    attachMode: 'default',
  },
  {
    set: SUBSTRING_FIELD_OP,
    type: 'triple',
    attachMode: 'custom',
    attachCond: [DB_KEYWORDS.from, DB_KEYWORDS.for],
  },
  {
    set: ARRAY_SLICE_OP,
    type: 'triple',
    attachMode: 'arrayOperator',
  },
  {
    set: MULTIPLE_FIELD_OP,
    type: 'multiple',
    attachMode: 'default',
  },
  {
    set: CASE_FIELD_OP,
    type: 'multiple',
    attachMode: 'custom',
    attachCond: [''],
    callable: false,
    suffixAllowed: true,
    suffixRef: { case: 'END' },
  },
  {
    set: CUSTOM_FIELD_OP,
    type: 'multiple',
    attachMode: 'custom',
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

  #prepareArgs<Model>(
    operatorRef: Record<string, string>,
    op: Ops,
    fieldOptions: CommonParamForOpGroup,
    ...operands: Arg<Model>[]
  ): {
    operator: Ops;
    operatorRef: Record<string, string>;
    operands: Arg<Model>[];
    fieldOptions: CommonParamForOpGroup;
  } {
    if (op !== CUSTOM_FIELD_OP.custom) {
      return { operator: op, operands, fieldOptions, operatorRef };
    }
    const [options, ...rest] = operands as [CustomFieldOptions, Arg<Model>];
    const {
      name,
      suffix,
      prefix,
      callable = true,
      attachMode = 'default',
      conditions,
    } = options;
    fieldOptions = {
      ...fieldOptions,
      attachMode,
      attachCond: conditions ?? (callable ? [] : [name]),
      ...(isNonEmptyString(suffix)
        ? { suffixAllowed: true, suffixRef: { [name]: suffix } }
        : {}),
      ...(isNonEmptyString(prefix)
        ? { prefixAllowed: true, prefixRef: { [name]: prefix } }
        : {}),
      callable,
    };
    return {
      operator: name as Ops,
      operands: rest,
      fieldOptions,
      operatorRef: { [name]: name },
    };
  }

  #operateOnFields<Model>(args: FieldOperatorCb<Model>) {
    const {
      colAndOperands,
      op,
      operatorRef: ref,
      isNullColAllowed,
      ...rest
    } = args;
    const { operands, operator, operatorRef, fieldOptions } = this.#prepareArgs(
      ref,
      op,
      rest,
      ...colAndOperands,
    );
    const callable = (options: CallableFieldParam) => {
      const { preparedValues, groupByFields, allowedFields } =
        getValidCallableFieldValues(
          options,
          'allowedFields',
          'groupByFields',
          'preparedValues',
        );

      const value = prepareFields<Model>({
        colAndOperands: operands,
        operator: operator,
        preparedValues,
        groupByFields,
        allowedFields,
        operatorRef,
        isNullColAllowed,
        ...fieldOptions,
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
