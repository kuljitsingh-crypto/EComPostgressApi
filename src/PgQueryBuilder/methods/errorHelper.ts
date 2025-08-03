import { TABLE_JOIN } from '../constants/tableJoin';
import { attachArrayWith } from './helperFunction';

function throwInvalidJoinTypeError(type: string) {
  throw new Error(
    `Invalid join type:"${type}". Valid join types:${attachArrayWith.coma(
      Object.keys(TABLE_JOIN),
    )}.`,
  );
}

function throwInvalidModelTypeError() {
  throw new Error(`Invalid model type. Model should be of Type DBModel.`);
}

function throwInvalidAggFuncTypeError(fn: string, allowedFunctions: string[]) {
  throw new Error(
    `Invalid function name "${fn}". Valid functions are: ${attachArrayWith.comaAndSpace(allowedFunctions)}.`,
  );
}

export const throwError = {
  invalidJoinType: throwInvalidJoinTypeError,
  invalidModelType: throwInvalidModelTypeError,
  invalidAggFuncType: throwInvalidAggFuncTypeError,
};
