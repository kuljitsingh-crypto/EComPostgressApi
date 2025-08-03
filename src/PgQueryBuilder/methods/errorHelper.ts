import { TABLE_JOIN } from '../constants/tableJoin';
import { attachArrayWith } from './helperFunction';

function throwInvalidJoinTypeError(type: string): never {
  throw new Error(
    `Invalid join type:"${type}". Valid join types:${attachArrayWith.coma(
      Object.keys(TABLE_JOIN),
    )}.`,
  );
}

function throwInvalidModelTypeError(): never {
  throw new Error(`Invalid model type. Model should be of Type DBModel.`);
}

function throwInvalidAggFuncTypeError(
  fn: string,
  allowedFunctions: string[],
): never {
  throw new Error(
    `Invalid function name "${fn}". Valid functions are: ${attachArrayWith.comaAndSpace(allowedFunctions)}.`,
  );
}

function throwInvalidDataTypeError(type: any): never {
  throw new Error(`Unsupported data type for array: ${typeof type}`);
}

export const throwError = {
  invalidJoinType: throwInvalidJoinTypeError,
  invalidModelType: throwInvalidModelTypeError,
  invalidAggFuncType: throwInvalidAggFuncTypeError,
  invalidDataType: throwInvalidDataTypeError,
};

export const errorHandler = (query: string, error: Error) => {
  const msg = `Error executing query: "${query}". Error: ${error.message}`;
  const err = new Error(msg);
  throw err;
};
