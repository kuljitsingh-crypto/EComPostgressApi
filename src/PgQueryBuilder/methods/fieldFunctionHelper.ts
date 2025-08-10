import {
  ADVANCE_DOUBLE_FIELD_OP,
  ADVANCE_SINGLE_FIELD_OP,
  AdvanceDoubleFieldOpKeys,
  AdvanceSingleFieldOpKeys,
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
import { attachArrayWith, fieldQuote } from './helperFunction';
import { QueryHelper } from './queryHelper';

type FieldOperand<Model> = string | number | InOperationSubQuery<Model>;
type DoubleFieldOpCb = <Model>(
  a: string,
  b: FieldOperand<Model>,
) => CallableField;

type SingleFieldOpCb = <Model>(b: FieldOperand<Model>) => CallableField;
type Ops =
  | SimpleMathOpKeys
  | AdvanceDoubleFieldOpKeys
  | AdvanceSingleFieldOpKeys;

type Func = {
  [key in Ops]: key extends AdvanceSingleFieldOpKeys
    ? SingleFieldOpCb
    : DoubleFieldOpCb;
};

interface FieldFunction extends Func {}

const attachOperator = (op: string, ...values: Primitive[]) =>
  `${op}(${attachArrayWith.coma(values)})`;

const prepareSimpleOperand = <Model>(
  colName: string,
  operand: FieldOperand<Model>,
  operator: SimpleMathOpKeys,
  preparedValues: PreparedValues,
  groupByFields: GroupByFields,
  allowedFields: AllowedFields,
) => {
  colName = fieldQuote(allowedFields, colName);
  const op = SIMPLE_MATH_FIELD_OP[operator];
  if (!op) {
    return throwError.invalidColumnOpType(
      op,
      Object.keys(SIMPLE_MATH_FIELD_OP),
    );
  }
  if (typeof operand === 'number') {
    return attachArrayWith.space([colName, op, operand]);
  } else if (typeof operand === 'string') {
    operand = fieldQuote(allowedFields, operand);
    return attachArrayWith.space([colName, op, operand]);
  } else {
    const query = QueryHelper.otherModelSubqueryBuilder(
      '',
      preparedValues,
      groupByFields,
      operand,
      false,
    );
    return attachArrayWith.space([colName, op, query]);
  }
};

const prepareAdvanceSingleOperand = <Model>(
  operand: FieldOperand<Model>,
  operator: AdvanceSingleFieldOpKeys,
  preparedValues: PreparedValues,
  groupByFields: GroupByFields,
  allowedFields: AllowedFields,
) => {
  const op = ADVANCE_SINGLE_FIELD_OP[operator];
  if (!op) {
    return throwError.invalidColumnOpType(
      op,
      Object.keys(ADVANCE_SINGLE_FIELD_OP),
    );
  }
  if (typeof operand === 'number') {
    return attachOperator(op, operand);
  } else if (typeof operand === 'string') {
    operand = fieldQuote(allowedFields, operand);
    return attachOperator(op, operand);
  } else {
    const query = QueryHelper.otherModelSubqueryBuilder(
      '',
      preparedValues,
      groupByFields,
      operand,
      false,
    );
    return attachOperator(op, query);
  }
};

const prepareAdvanceDoubleOperand = <Model>(
  colName: string,
  operand: FieldOperand<Model>,
  operator: AdvanceDoubleFieldOpKeys,
  preparedValues: PreparedValues,
  groupByFields: GroupByFields,
  allowedFields: AllowedFields,
) => {
  colName = fieldQuote(allowedFields, colName);
  const op = ADVANCE_DOUBLE_FIELD_OP[operator];
  if (!op) {
    return throwError.invalidColumnOpType(
      op,
      Object.keys(ADVANCE_DOUBLE_FIELD_OP),
    );
  }
  if (typeof operand === 'number') {
    return attachOperator(op, colName, operand);
  } else if (typeof operand === 'string') {
    operand = fieldQuote(allowedFields, operand);
    return attachOperator(op, colName, operand);
  } else {
    const query = QueryHelper.otherModelSubqueryBuilder(
      '',
      preparedValues,
      groupByFields,
      operand,
      false,
    );
    return attachOperator(op, colName, query);
  }
};

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
    for (let op in SIMPLE_MATH_FIELD_OP) {
      // @ts-ignore - dynamic assignment
      this[op] = this.#doubleFieldSimpleOpWrapper<Model>(op);
    }
    for (let op in ADVANCE_SINGLE_FIELD_OP) {
      // @ts-ignore - dynamic assignment
      this[op] = this.#SingleFieldAdvanceOpWrapper(op);
    }
    for (let op in ADVANCE_DOUBLE_FIELD_OP) {
      // @ts-ignore - dynamic assignment
      this[op] = this.#doubleFieldAdvanceOpWrapper(op);
    }
  }

  #doubleFieldSimpleOpWrapper<Model>(op: SimpleMathOpKeys) {
    return (a: string, b: FieldOperand<Model>) => {
      return this.#operateOnFields(a, b, op);
    };
  }
  #SingleFieldAdvanceOpWrapper<Model>(op: AdvanceSingleFieldOpKeys) {
    return (a: FieldOperand<Model>) => {
      return this.#operateOnAdvanceSingleFields(a, op);
    };
  }

  #doubleFieldAdvanceOpWrapper<Model>(op: AdvanceDoubleFieldOpKeys) {
    return (a: string, b: FieldOperand<Model>) => {
      return this.#operateOnAdvanceDoubleFields(a, b, op);
    };
  }

  #checkFunctionExecutionState() {
    if (!this.#fieldFunc) {
      return throwError.invalidFieldFuncCallType();
    }
  }

  #operateOnFields<Model>(
    colName: string,
    operand: FieldOperand<Model>,
    op: SimpleMathOpKeys,
  ) {
    return (
      preparedValues: PreparedValues,
      groupByFields: GroupByFields,
      allowedFields: AllowedFields,
    ) => {
      this.#checkFunctionExecutionState();
      const value = prepareSimpleOperand(
        colName,
        operand,
        op,
        preparedValues,
        groupByFields,
        allowedFields,
      );
      return { col: value, value: null, shouldSkipFieldValidation: true };
    };
  }

  #operateOnAdvanceSingleFields<Model>(
    operand: FieldOperand<Model>,
    op: AdvanceSingleFieldOpKeys,
  ) {
    return (
      preparedValues: PreparedValues,
      groupByFields: GroupByFields,
      allowedFields: AllowedFields,
    ) => {
      this.#checkFunctionExecutionState();
      const value = prepareAdvanceSingleOperand(
        operand,
        op,
        preparedValues,
        groupByFields,
        allowedFields,
      );
      return { col: value, value: null, shouldSkipFieldValidation: true };
    };
  }

  #operateOnAdvanceDoubleFields<Model>(
    colName: string,
    operand: FieldOperand<Model>,
    op: AdvanceDoubleFieldOpKeys,
  ) {
    return (
      preparedValues: PreparedValues,
      groupByFields: GroupByFields,
      allowedFields: AllowedFields,
    ) => {
      this.#checkFunctionExecutionState();
      const value = prepareAdvanceDoubleOperand(
        colName,
        operand,
        op,
        preparedValues,
        groupByFields,
        allowedFields,
      );
      return { col: value, value: null, shouldSkipFieldValidation: true };
    };
  }
}

export const fieldFn = new FieldFunction();
