import { DB_KEYWORDS } from '../constants/dbkeywords';
import {
  ADVANCE_DOUBLE_FIELD_OP,
  ADVANCE_SINGLE_FIELD_OP,
  ADVANCE_STR_DOUBLE_FIELD_OP,
  AdvanceDoubleFieldOpKeys,
  AdvanceSingleFieldOpKeys,
  AdvanceStrDoubleFieldOpKeys,
  SIMPLE_MATH_FIELD_OP,
  SimpleMathOpKeys,
} from '../constants/fieldFunctions';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  CallableField,
  GroupByFields,
  InOperationSubQuery,
  PreparedValues,
} from '../internalTypes';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  fieldQuote,
  getPreparedValues,
  isValidSubQuery,
} from './helperFunction';
import { QueryHelper } from './queryHelper';

type FieldOperand<Model> = string | number | InOperationSubQuery<Model>;
type StringFieldOperand = string;
type DoubleFieldOpCb = <Model>(
  a: string,
  b: FieldOperand<Model>,
) => CallableField;

type SingleFieldOpCb = <Model>(b: FieldOperand<Model>) => CallableField;
type Ops =
  | SimpleMathOpKeys
  | AdvanceDoubleFieldOpKeys
  | AdvanceSingleFieldOpKeys
  | AdvanceStrDoubleFieldOpKeys;

type Func = {
  [key in Ops]: key extends AdvanceSingleFieldOpKeys
    ? SingleFieldOpCb
    : DoubleFieldOpCb;
};
type AllowedOperand = 'number' | 'string' | 'all';
type OperandType = 'single' | 'double' | 'multiple' | 'triple';
type PrepareCb<Model> = {
  col: string | null;
  operand: FieldOperand<Model>;
  operator: Ops;
  preparedValues: PreparedValues;
  groupByFields: GroupByFields;
  allowedFields: AllowedFields;
  operandAllowed: AllowedOperand;
  operatorRef: Record<string, string>;
  isNullColAllowed: boolean;
  shouldValidateStringOperandAsCol: boolean;
};

interface FieldFunction extends Func {}

const inOperator = new Set([ADVANCE_STR_DOUBLE_FIELD_OP.position]);
const inBtwOperator: Set<string> = new Set(Object.values(SIMPLE_MATH_FIELD_OP));
const stringColOp: Set<string> = new Set(
  Object.keys(ADVANCE_STR_DOUBLE_FIELD_OP),
);

const attachOperator = (op: string, ...values: Primitive[]) =>
  `${op}(${attachArrayWith.coma(values)})`;

const attachInBtwOperator = (op: string, ...values: Primitive[]) =>
  attachArrayWith.space([values[0], op, values[1]]);

const attachInOperator = (op: string, ...values: Primitive[]) =>
  `${op}(${attachArrayWith.customSep(values, ` ${DB_KEYWORDS.in} `)})`;

const attachOp = (op: string, ...values: Primitive[]) => {
  values = values.filter((v) => v !== null && v !== undefined);
  if (inOperator.has(op as any)) {
    values.reverse();
  }
  const opCb = inOperator.has(op as any)
    ? attachInOperator
    : inBtwOperator.has(op)
      ? attachInBtwOperator
      : attachOperator;

  return opCb(op, ...values);
};

const prepareFields = <Model>(params: PrepareCb<Model>) => {
  const {
    col,
    operand,
    operator,
    operandAllowed,
    operatorRef,
    preparedValues,
    groupByFields,
    allowedFields,
    isNullColAllowed = false,
    shouldValidateStringOperandAsCol = true,
  } = params;
  const validCol =
    isNullColAllowed && col === null ? col : fieldQuote(allowedFields, col);
  const op = operatorRef[operator];
  if (!op) {
    return throwError.invalidColumnOpType(op, Object.keys(operatorRef));
  }
  if (operandAllowed !== 'all' && typeof operand !== operandAllowed) {
    return throwError.invalidOperandType();
  }
  if (typeof operand === 'number') {
    const placeholder = getPreparedValues(preparedValues, operand);
    return attachOp(op, validCol, placeholder);
  } else if (typeof operand === 'string') {
    if (shouldValidateStringOperandAsCol) {
      const validOperand = fieldQuote(allowedFields, operand);
      return attachOp(op, col, validOperand);
    } else {
      const placeholder = getPreparedValues(preparedValues, operand);
      return attachOp(op, col, placeholder);
    }
  } else if (isValidSubQuery(operand)) {
    const query = QueryHelper.otherModelSubqueryBuilder(
      '',
      preparedValues,
      groupByFields,
      operand,
      false,
    );
    return attachOp(op, col, query);
  }

  return throwError.invalidOpDataType(op);
};

const opGroups: {
  type: OperandType;
  allowed: AllowedOperand;
  set: Partial<Record<Ops, string>>;
}[] = [
  { set: SIMPLE_MATH_FIELD_OP, type: 'double', allowed: 'all' },
  { set: ADVANCE_SINGLE_FIELD_OP, type: 'single', allowed: 'all' },
  { set: ADVANCE_DOUBLE_FIELD_OP, type: 'double', allowed: 'all' },
  { set: ADVANCE_STR_DOUBLE_FIELD_OP, type: 'double', allowed: 'string' },
] as const;

class FieldFunction {
  #fieldFunc = true;
  static #instance: FieldFunction | null = null;

  constructor() {
    if (FieldFunction.#instance === null) {
      FieldFunction.#instance = this;
      this.#attachFieldMethods();
    }
    return FieldFunction.#instance;
  }

  #attachFieldMethods<Model>() {
    let op: Ops;
    opGroups.forEach(({ set, allowed, type }) => {
      for (const op in set) {
        // @ts-ignore
        this[op] = this.#multiFieldOperator({
          operandType: type,
          op: op as Ops,
          operandAllowed: allowed,
          operatorRef: set,
        });
      }
    });
  }

  #multiFieldOperator = ({
    operandType,
    operandAllowed,
    op,
    operatorRef,
  }: {
    operandType: OperandType;
    op: Ops;
    operandAllowed: AllowedOperand;
    operatorRef: Record<string, string>;
  }) => {
    switch (operandType) {
      case 'single':
        return <Model>(operand: FieldOperand<Model>) =>
          this.#operateOnFields({
            colName: null,
            operand,
            op,
            operandAllowed,
            shouldValidateStringOperandAsCol: true,
            isNullColAllowed: true,
            operatorRef,
          });
      case 'double':
        return <Model>(col: string, operand: FieldOperand<Model>) => {
          return this.#operateOnFields({
            colName: col,
            operand,
            op,
            operandAllowed,
            shouldValidateStringOperandAsCol: !stringColOp.has(op),
            isNullColAllowed: false,
            operatorRef,
          });
        };
    }
  };

  #checkFunctionExecutionState() {
    if (!this.#fieldFunc) {
      return throwError.invalidFieldFuncCallType();
    }
  }

  #operateOnFields<Model>({
    colName,
    operand,
    operandAllowed,
    op,
    operatorRef,
    isNullColAllowed,
    shouldValidateStringOperandAsCol,
  }: {
    colName: string | null;
    operand: FieldOperand<Model>;
    op: Ops;
    operatorRef: Record<string, string>;
    operandAllowed: AllowedOperand;
    isNullColAllowed: boolean;
    shouldValidateStringOperandAsCol: boolean;
  }) {
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
        shouldValidateStringOperandAsCol,
      });
      return { col: value, value: null, shouldSkipFieldValidation: true };
    };
  }
}

export const fieldFn = new FieldFunction();
