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
} from '../constants/fieldFunctions';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  CallableField,
  GroupByFields,
  InOperationSubQuery,
  PreparedValues,
} from '../internalTypes';
import { getInternalContext, isValidInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  fieldQuote,
  getPreparedValues,
  isNotNullPrimitiveValue,
  isValidSubQuery,
} from './helperFunction';
import { QueryHelper } from './queryHelper';

type ColFunc = () => {
  colName: string;
  isCol: boolean;
  ctx: symbol;
};

type ValFunc = () => {
  value: Primitive;
  isVal: boolean;
  ctx: symbol;
};

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
  | InOperationSubQuery<Model>
  | CombinedFun;

type FieldOperandInternal<Model> = FieldOperand<Model> | null;

type DoubleFieldOpCb = <Model>(
  a: FieldOperand<Model>,
  b: FieldOperand<Model>,
) => CallableField;

type SingleFieldOpCb = <Model>(b: FieldOperand<Model>) => CallableField;
type TripleFieldOpCb = <Model>(b: FieldOperand<Model>) => CallableField;

type Ops =
  | SimpleMathOpKeys
  | DoubleFieldOpKeys
  | SingleFieldOpKeys
  | StrFieldOpKeys;

type Func = {
  [key in Ops]: key extends SingleFieldOpKeys
    ? SingleFieldOpCb
    : DoubleFieldOpCb;
};
type AllowedOperand = 'number' | 'string' | 'all';
type OperandType = 'single' | 'double' | 'multiple' | 'triple';
type AttachType = 'in' | 'inBtw' | 'default';

type PrepareCb<Model> = {
  col: FieldOperandInternal<Model>;
  operand: FieldOperandInternal<Model>;
  attachBy: AttachType;
  operator: Ops;
  preparedValues: PreparedValues;
  groupByFields: GroupByFields;
  allowedFields: AllowedFields;
  operandAllowed: AllowedOperand;
  operatorRef: Record<string, string>;
  isNullColAllowed: boolean;
};

type MultiOperatorFieldCb = {
  operandType: OperandType;
  op: Ops;
  operandAllowed: AllowedOperand;
  operatorRef: Record<string, string>;
  attachBy: AttachType;
};

type FieldOperatorCb<Model> = {
  colName: FieldOperandInternal<Model>;
  operand: FieldOperandInternal<Model>;
  op: Ops;
  operatorRef: Record<string, string>;
  operandAllowed: AllowedOperand;
  isNullColAllowed: boolean;
  attachBy: AttachType;
};

interface FieldFunction extends Func {}

const attachOperator = (op: string, ...values: Primitive[]) =>
  `${op}(${attachArrayWith.coma(values)})`;

const attachInBtwOperator = (op: string, ...values: Primitive[]) =>
  attachArrayWith.space([values[0], op, values[1]]);

const attachInOperator = (op: string, ...values: Primitive[]) =>
  `${op}(${attachArrayWith.customSep(values, ` ${DB_KEYWORDS.in} `)})`;

const attachOp = (op: string, attachBy: AttachType, ...values: Primitive[]) => {
  values = values.filter((v) => v !== null && v !== undefined);
  let opCb: (op: string, ...values: Primitive[]) => string = attachOperator;
  switch (attachBy) {
    case 'in': {
      values.reverse();
      opCb = attachInOperator;
      break;
    }
    case 'inBtw':
      opCb = attachInBtwOperator;
      break;
  }

  return opCb(op, ...values);
};

const getColName = <Model>(
  col: FieldOperandInternal<Model>,
  allowedFields: AllowedFields,
  isStringCheckReq = false,
) => {
  if (typeof col === 'function') {
    const { colName, isCol, ctx } = col();
    const isValidCol =
      typeof colName === 'string' &&
      colName &&
      isCol &&
      isValidInternalContext(ctx);
    if (!isValidCol) {
      return throwError.invalidFieldFuncCallType();
    }
    return fieldQuote(allowedFields, colName);
  } else if (isStringCheckReq && typeof col == 'string') {
    return fieldQuote(allowedFields, col);
  }
  return null;
};
const getColOrValFrmCb = (
  value: CombinedFun,
  preparedValues: PreparedValues,
  allowedFields: AllowedFields,
) => {
  const { ctx, ...rest } = value();
  if (!isValidInternalContext(ctx)) {
    return throwError.invalidFieldFuncCallType();
  }
  if (isNotNullPrimitiveValue(rest.value || null) && rest.isVal) {
    return getPreparedValues(preparedValues, rest.value as Primitive);
  }
  if (typeof rest.colName === 'string' && rest.isCol) {
    return fieldQuote(allowedFields, rest.colName);
  }
  return null;
};

const isSubquery = <Model>(
  value: FieldOperandInternal<Model>,
): value is InOperationSubQuery<Model> =>
  typeof value === 'object' && value !== null && isValidSubQuery(value);

const getColValue = <Model>(
  value: FieldOperandInternal<Model>,
  preparedValues: PreparedValues,
  groupByFields: GroupByFields,
  allowedFields: AllowedFields,
) => {
  if (['number', 'boolean', 'string'].includes(typeof value)) {
    const placeholder = getPreparedValues(preparedValues, value as Primitive);
    return placeholder;
  } else if (typeof value === 'function') {
    return getColOrValFrmCb(value, preparedValues, allowedFields);
  } else if (isSubquery(value)) {
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

const prepareFields = <Model>(params: PrepareCb<Model>) => {
  const {
    col,
    operand,
    operator,
    attachBy,
    operandAllowed,
    operatorRef,
    preparedValues,
    groupByFields,
    allowedFields,
    isNullColAllowed = false,
  } = params;
  const validCol =
    col === null
      ? col
      : typeof col === 'string'
        ? fieldQuote(allowedFields, col)
        : getColName(operand, allowedFields);
  if (validCol === null && !isNullColAllowed) {
    return throwError.invalidColumnNameType('null', allowedFields);
  }
  const op = operatorRef[operator];
  if (!op) {
    return throwError.invalidColumnOpType(op, Object.keys(operatorRef));
  }
  if (operandAllowed !== 'all' && typeof operand !== operandAllowed) {
    return throwError.invalidOperandType();
  }
  const operandValue =
    typeof col === 'function' || isSubquery(col)
      ? getColValue(col, preparedValues, groupByFields, allowedFields)
      : getColValue(operand, preparedValues, groupByFields, allowedFields);
  console.log(validCol, operand);
  if (validCol === null && operandValue === null) {
    return throwError.invalidOpDataType(op);
  }
  return attachOp(op, attachBy, validCol, operandValue);
};

const opGroups: {
  type: OperandType;
  allowed: AllowedOperand;
  set: Partial<Record<Ops, string>>;
  attachBy: AttachType;
}[] = [
  {
    set: MATH_FIELD_OP,
    type: 'double',
    allowed: 'all',
    attachBy: 'inBtw',
  },
  {
    set: SINGLE_FIELD_OP,
    type: 'single',
    allowed: 'all',
    attachBy: 'default',
  },
  {
    set: DOUBLE_FIELD_OP,
    type: 'double',
    allowed: 'all',
    attachBy: 'default',
  },
  {
    set: STR_FIELD_OP,
    type: 'double',
    allowed: 'string',
    attachBy: 'default',
  },
  {
    set: STR_IN_FIELD_OP,
    type: 'double',
    allowed: 'string',
    attachBy: 'in',
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

  column(col: string): ColFunc {
    return () => {
      this.#checkFunctionExecutionState();
      return { colName: col, isCol: true, ctx: getInternalContext() };
    };
  }
  value(val: Primitive): ValFunc {
    return () => {
      this.#checkFunctionExecutionState();
      return { value: val, isVal: true, ctx: getInternalContext() };
    };
  }

  #attachFieldMethods<Model>() {
    let op: Ops;
    opGroups.forEach(({ set, allowed, type, attachBy }) => {
      for (const op in set) {
        // @ts-ignore
        this[op] = this.#multiFieldOperator({
          operandType: type,
          op: op as Ops,
          operandAllowed: allowed,
          operatorRef: set,
          attachBy,
        });
      }
    });
  }

  #multiFieldOperator = (args: MultiOperatorFieldCb) => {
    const { operandType, operandAllowed, op, operatorRef, attachBy } = args;
    switch (operandType) {
      case 'single':
        return <Model>(operand: FieldOperandInternal<Model>) =>
          this.#operateOnFields({
            colName: null,
            operand,
            op,
            operandAllowed,
            isNullColAllowed: true,
            operatorRef,
            attachBy,
          });
      case 'double':
        return <Model>(
          col: FieldOperandInternal<Model>,
          operand: FieldOperandInternal<Model>,
        ) => {
          return this.#operateOnFields({
            colName: col,
            operand,
            op,
            operandAllowed,
            isNullColAllowed: false,
            operatorRef,
            attachBy,
          });
        };
    }
  };

  #checkFunctionExecutionState() {
    if (!this.#fieldFunc) {
      return throwError.invalidFieldFuncCallType();
    }
  }

  #operateOnFields<Model>(args: FieldOperatorCb<Model>) {
    const {
      colName,
      operand,
      operandAllowed,
      op,
      operatorRef,
      isNullColAllowed,
      attachBy,
    } = args;
    return (
      preparedValues: PreparedValues,
      groupByFields: GroupByFields,
      allowedFields: AllowedFields,
    ) => {
      this.#checkFunctionExecutionState();
      const value = prepareFields<Model>({
        col: colName,
        operand,
        operator: op,
        preparedValues,
        groupByFields,
        allowedFields,
        operatorRef,
        operandAllowed,
        isNullColAllowed,
        attachBy,
      });
      return {
        col: value,
        value: null,
        shouldSkipFieldValidation: true,
        ctx: getInternalContext(),
      };
    };
  }
}

export const fieldFn = new FieldFunction();
