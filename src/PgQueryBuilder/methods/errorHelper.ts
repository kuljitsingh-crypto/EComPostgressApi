import { DB_KEYWORDS } from '../constants/dbkeywords';
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

function throwInvalidAnyOpTypeError(op: string): never {
  throw new Error(
    `For operator "${op}" with ANY/ALL, value must be an object containing "${DB_KEYWORDS.any}" or "${DB_KEYWORDS.all}" property.`,
  );
}

function throwInvalidAnySubQTypeError(): never {
  throw new Error(
    `For subquery operations, value must contain "${DB_KEYWORDS.any}" or "${DB_KEYWORDS.all}" property`,
  );
}

function throwInvalidPrimitiveDataTypeError(op: string): never {
  throw new Error(`For operator "${op}" value should be a primitive type.`);
}

function throwInvalidAliasFormatError(invalidSubQuery = false): never {
  const msg = invalidSubQuery
    ? 'To use subquery in alias, alias must has "query" field with appropriate value.'
    : 'Alias must be object with appropriate fields.';
  throw new Error(msg);
}

function throwInvalidColumnLenError(
  index: number,
  requiredLen: number,
  givenLen: number,
): never {
  throw new Error(
    `Invalid value length at index ${index}. Expected ${requiredLen} values, but got ${givenLen}.`,
  );
}

function throwInvalidSetOpTypeError(invalidQuery = false): never {
  const msg = invalidQuery
    ? 'Set Query Operation must contain at least "type", "model", and "columns" keys.'
    : 'For Set Query Operation, value must be object.';
  throw new Error(msg);
}

function throwInvalidAggFuncPlaceError(fn: string, column: string): never {
  throw new Error(
    `Aggregate functions are not allowed in this context. Found "${fn}" for column "${column}".`,
  );
}

function throwInvalidOrderOptionError(key: string): never {
  throw new Error(`Order option is required for column "${key}".`);
}

function throwInvalidWhereClauseError(key: string): never {
  throw new Error(`Where clause is required for subquery operator "${key}".`);
}
function throwInvalidArrayOpTypeError(
  key: string,
  options?: {
    min?: number;
    max?: number;
    exact?: number;
  },
): never {
  const { min = 0, max = 0, exact = 0 } = options || {};
  if (min > 0) {
    throw new Error(
      `For operator "${key}" value should be array of minimum length ${min}.`,
    );
  } else if (max > 0) {
    throw new Error(
      `For operator "${key}" value should be array of maximum length ${max}.`,
    );
  } else if (exact > 0) {
    throw new Error(
      `For operator "${key}" value should be array of length ${exact}.`,
    );
  }
  throw new Error(`For operator "${key}" value should be array.`);
}

function throwInvalidObjectOpError(key: string): never {
  throw new Error(`For operator "${key}", value must be an object.`);
}

function throwInvalidOperatorTypeError(
  op: string,
  validOperations: string,
): never {
  throw new Error(
    `Invalid operator "${op}". Please use following operators: ${validOperations}. `,
  );
}

function throwInvalidPrimaryColumnError(tableName: string): never {
  throw new Error(
    `At least one primary key column is required in table ${tableName}.`,
  );
}

export const throwError = {
  invalidJoinType: throwInvalidJoinTypeError,
  invalidModelType: throwInvalidModelTypeError,
  invalidAggFuncType: throwInvalidAggFuncTypeError,
  invalidDataType: throwInvalidDataTypeError,
  invalidAnyAllOpType: throwInvalidAnyOpTypeError,
  invalidAnySubQType: throwInvalidAnySubQTypeError,
  invalidOPDataType: throwInvalidPrimitiveDataTypeError,
  invalidAliasType: throwInvalidAliasFormatError,
  invalidColumnLenType: throwInvalidColumnLenError,
  invalidSetQueryType: throwInvalidSetOpTypeError,
  invalidAggFuncPlaceType: throwInvalidAggFuncPlaceError,
  invalidOrderOptionType: throwInvalidOrderOptionError,
  invalidWhereClauseType: throwInvalidWhereClauseError,
  invalidArrayOPType: throwInvalidArrayOpTypeError,
  invalidObjectOPType: throwInvalidObjectOpError,
  invalidOperatorType: throwInvalidOperatorTypeError,
  invalidPrimaryColType: throwInvalidPrimaryColumnError,
};

export const errorHandler = (query: string, error: Error) => {
  const msg = `Error executing query: "${query}". Error: ${error.message}`;
  const err = new Error(msg);
  throw err;
};
