import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  DOUBLE_FIELD_OP,
  SINGLE_FIELD_OP,
  STR_FIELD_OP,
  DoubleFieldOpKeys,
  SingleFieldOpKeys,
  StrFieldOpKeys,
  MATH_FIELD_OP,
  SimpleMathOpKeys,
  STR_IN_FIELD_OP,
  MultipleFieldOpKeys,
  TripleFieldOpKeys,
  TRIPLE_FIELD_OP,
  MULTIPLE_FIELD_OP,
  SUBSTRING_FIELD_OP,
  SubstringFieldOpKeys,
  TrimFieldOpKeys,
  TRIM_FIELD_OP,
} from '../constants/fieldFunctions';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  CallableField,
  CallableFieldParam,
  GroupByFields,
  InOperationSubQuery,
  PreparedValues,
} from '../internalTypes';
import { getInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  getPreparedValues,
  getValidCallableFieldValues,
  isCallableColumn,
  isNotNullPrimitiveValue,
  isValidSubQuery,
  validCallableColCtx,
} from './helperFunction';
import { QueryHelper } from './queryHelper';

// type ColFunc = () => {
//   colName: string;
//   isCol: boolean;
//   ctx: symbol;
// };

// type ValFunc = () => {
//   value: Primitive;
//   isVal: boolean;
//   ctx: symbol;
// };

type CombinedFun = () => {
  value?: Primitive;
  isVal?: boolean;
  colName?: string;
  isCol?: boolean;
  ctx: symbol;
};
type FieldOperand<Model> =
  | string
  | number
  | boolean
  | InOperationSubQuery<Model, 'WhereNotReq', 'single'>
  // | CombinedFun
  | CallableField;

type FieldOperandInternal<Model> = FieldOperand<Model> | null;

type DoubleFieldOpCb = <Model>(
  a: FieldOperand<Model>,
  b: FieldOperand<Model>,
) => CallableField;

type SingleFieldOpCb = <Model>(b: FieldOperand<Model>) => CallableField;
type TripleFieldOpCb = <Model>(
  a: FieldOperand<Model>,
  b: FieldOperand<Model>,
  c: FieldOperand<Model>,
) => CallableField;

type MultipleFieldOpCb = <Model>(
  ...args: FieldOperand<Model>[]
) => CallableField;

type Ops =
  | SimpleMathOpKeys
  | DoubleFieldOpKeys
  | SingleFieldOpKeys
  | StrFieldOpKeys
  | MultipleFieldOpKeys
  | TripleFieldOpKeys
  | SubstringFieldOpKeys
  | TrimFieldOpKeys;

type Func = {
  [key in Ops]: key extends SingleFieldOpKeys
    ? SingleFieldOpCb
    : key extends DoubleFieldOpKeys | TrimFieldOpKeys | StrFieldOpKeys
      ? DoubleFieldOpCb
      : key extends TripleFieldOpKeys | SubstringFieldOpKeys
        ? TripleFieldOpCb
        : MultipleFieldOpCb;
};
type OperandType = 'single' | 'double' | 'multiple' | 'triple';
type AttachType = 'inBtw' | 'default' | 'custom';

type PrepareCb<Model> = {
  colAndOperands: FieldOperandInternal<Model>[];
  attachBy: AttachType;
  operator: Ops;
  preparedValues: PreparedValues;
  groupByFields: GroupByFields;
  allowedFields: AllowedFields;
  operatorRef: Record<string, string>;
  isNullColAllowed: boolean;
  attachCond?: string[];
};

type MultiOperatorFieldCb = {
  operandType: OperandType;
  op: Ops;
  operatorRef: Record<string, string>;
  attachBy: AttachType;
  attachCond?: string[];
};

type FieldOperatorCb<Model> = {
  colAndOperands: FieldOperandInternal<Model>[];
  op: Ops;
  operatorRef: Record<string, string>;
  isNullColAllowed: boolean;
  attachBy: AttachType;
  attachCond?: string[];
};

interface FieldFunction extends Func {}

// function isCombinedFn(fn: CallableField | CombinedFun): fn is CombinedFun {
//   return fn.length === 0;
// }

const attachOperator = (op: string, ...values: Primitive[]) =>
  `${op}(${attachArrayWith.coma(values)})`;

const attachInBtwOperator = (op: string, ...values: Primitive[]) =>
  attachArrayWith.space([values[0], op, values[1]]);

const customAttach =
  (attachCond: string[]) =>
  (op: string, ...values: Primitive[]) => {
    const valuesLen = values.length;
    const lastAttachStr = attachCond[attachCond.length - 1] ?? '';
    const attachedVal: Primitive[] = [values[0] ?? ''];
    for (let i = 1; i < valuesLen; i++) {
      const attachType = attachCond[i - 1] ?? lastAttachStr;
      attachedVal.push(attachType, values[i]);
    }
    return attachOperator(op, attachArrayWith.space(attachedVal));
  };

const attachOp = (
  op: string,
  attachBy: AttachType,
  attachCond: string[],
  ...values: Primitive[]
) => {
  values = values.filter((v) => v !== null && v !== undefined);
  if (values.length < 1) {
    return throwError.invalidOpDataType(op);
  }
  let opCb: (op: string, ...values: Primitive[]) => string = attachOperator;
  switch (attachBy) {
    case 'inBtw':
      opCb = attachInBtwOperator;
      break;
    case 'custom':
      if (Array.isArray(attachCond) && attachCond.length > 0) {
        opCb = customAttach(attachCond);
      }
      break;
  }

  return opCb(op, ...values);
};

const getColOrValFrmCb = (
  value: CombinedFun | CallableField,
  preparedValues: PreparedValues,
  allowedFields: AllowedFields,
  groupByFields: GroupByFields,
  isNullColAllowed: boolean,
) => {
  if (isCallableColumn(value)) {
    const { col } = validCallableColCtx(value, {
      allowedFields,
      isAggregateAllowed: true,
      preparedValues,
      groupByFields,
    });
    return col;
  }
  // if (isCombinedFn(value)) {
  //   const { ctx, ...rest } = value();
  //   if (!isValidInternalContext(ctx)) {
  //     return throwError.invalidFieldFuncCallType();
  //   }
  //   if (isNotNullPrimitiveValue(rest.value || null) && rest.isVal) {
  //     return getPreparedValues(preparedValues, rest.value as Primitive);
  //   }
  //   if (typeof rest.colName === 'string' && rest.isCol) {
  //     return fieldQuote(allowedFields, rest.colName, { isNullColAllowed });
  //   }
  // }
  return null;
};

const getColValue = <Model>(
  value: FieldOperandInternal<Model>,
  preparedValues: PreparedValues,
  groupByFields: GroupByFields,
  allowedFields: AllowedFields,
  isNullColAllowed: boolean,
) => {
  if (isNotNullPrimitiveValue(value)) {
    return getPreparedValues(preparedValues, value as Primitive);
  } else if (typeof value === 'function') {
    return getColOrValFrmCb(
      value,
      preparedValues,
      allowedFields,
      groupByFields,
      isNullColAllowed,
    );
  } else if (isValidSubQuery(value)) {
    const query = QueryHelper.otherModelSubqueryBuilder(
      '',
      preparedValues,
      groupByFields,
      value,
      false,
    );
    return query;
  }
  return null;
};

const resolveOperand = <Model>(
  colAndOperands: FieldOperandInternal<Model>[],
  allowedFields: AllowedFields,
  preparedValues: PreparedValues,
  groupByFields: GroupByFields,
  isNullColAllowed: boolean,
) => {
  const operandsRef: Primitive[] = [];
  // For Now primitive data type treated as value , For Column use col(colNme)
  // const [col, ...operands] = colAndOperands;
  // if (col === null || typeof col === 'string') {
  //   operandsRef.push(fieldQuote(allowedFields, col, isNullColAllowed));
  // } else {
  //   operandsRef.push(
  //     getColValue(
  //       col,
  //       preparedValues,
  //       groupByFields,
  //       allowedFields,
  //       isNullColAllowed,
  //       operandAllowed,
  //     ),
  //   );
  // }
  colAndOperands.forEach((op) => {
    const value = getColValue(
      op,
      preparedValues,
      groupByFields,
      allowedFields,
      isNullColAllowed,
    );
    if (value === null && !isNullColAllowed) {
      throwError.invalidColumnNameType('null', allowedFields);
    }
    operandsRef.push(value);
  });
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
  } = params;
  const op = operatorRef[operator];
  if (!op) {
    return throwError.invalidColumnOpType(op, Object.keys(operatorRef));
  }
  const operands = resolveOperand(
    colAndOperands,
    allowedFields,
    preparedValues,
    groupByFields,
    isNullColAllowed,
  );
  console.log(attachBy, attachCond);
  return attachOp(op, attachBy, attachCond, ...operands);
};

const getColAndOperands = <Model>(
  type: OperandType,
  ...operands: FieldOperandInternal<Model>[]
): FieldOperandInternal<Model>[] => {
  switch (type) {
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

const opGroups: {
  type: OperandType;
  set: Partial<Record<Ops, string>>;
  attachBy: AttachType;
  attachCond?: string[];
}[] = [
  {
    set: MATH_FIELD_OP,
    type: 'double',
    attachBy: 'inBtw',
  },
  {
    set: SINGLE_FIELD_OP,
    type: 'single',
    attachBy: 'default',
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
    set: MULTIPLE_FIELD_OP,
    type: 'multiple',
    attachBy: 'default',
  },
  {
    set: SUBSTRING_FIELD_OP,
    type: 'triple',
    attachBy: 'custom',
    attachCond: [DB_KEYWORDS.from, DB_KEYWORDS.for],
  },
  {
    set: TRIM_FIELD_OP,
    type: 'double',
    attachBy: 'custom',
    attachCond: [DB_KEYWORDS.from],
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
    opGroups.forEach(({ set, type, attachBy, attachCond }) => {
      for (const op in set) {
        // @ts-ignore
        this[op] = this.#multiFieldOperator({
          operandType: type,
          op: op as Ops,
          operatorRef: set,
          attachBy,
          attachCond,
        });
      }
    });
  }

  #multiFieldOperator = (args: MultiOperatorFieldCb) => {
    const { operandType, op, operatorRef, attachBy, attachCond } = args;
    return <Model>(...ops: FieldOperandInternal<Model>[]) => {
      const colAndOperands = getColAndOperands(operandType, ...ops);
      return this.#operateOnFields({
        colAndOperands,
        op,
        isNullColAllowed: false,
        operatorRef,
        attachBy,
        attachCond,
      });
    };
  };

  #checkFunctionExecutionState() {
    if (!this.#fieldFunc) {
      return throwError.invalidFieldFuncCallType();
    }
  }

  #operateOnFields<Model>(args: FieldOperatorCb<Model>) {
    const {
      colAndOperands,
      op,
      operatorRef,
      isNullColAllowed,
      attachBy,
      attachCond,
    } = args;
    return (options: CallableFieldParam) => {
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
        attachBy,
        attachCond,
      });
      return {
        col: value,
        alias: null,
        ctx: getInternalContext(),
      };
    };
  }
}

export const fieldFn = new FieldFunction();
