import { DB_KEYWORDS } from '../constants/dbkeywords';
import { TABLE_JOIN } from '../constants/tableJoin';
import { Primitive } from '../globalTypes';
import { AllowedFields } from '../internalTypes';
import { attachArrayWith } from './helperFunction';

function throwInvalidJson(methodName: string): never {
  throw new Error(`Invalid JSON data  provided to ${methodName} method.`);
}

function throwInvalidJsonPathSlicing(): never {
  throw new Error(
    'Invalid JSON path slicing format. Provide at least start or end index.',
  );
}

function throwInvalidJsonQuery(): never {
  throw new Error(`Invalid data  provided JSON Path query builder.`);
}

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

function throwInvalidJsonPathDataTypeError(type: any): never {
  throw new Error(`Unsupported data type jsonPath: ${typeof type}`);
}

function throwInvalidDataTypeError(type: any): never {
  throw new Error(`Unsupported data type for array: ${typeof type}`);
}

function throwInvalidOpDataTypeError(op: string): never {
  throw new Error(`Unsupported data type for operator: ${op}.`);
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

function throwInvalidSubquery(): never {
  const msg =
    'A subquery must include at least one of the fields: model or subquery';
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

function throwInvalidArrayDataTypeError(): never {
  throw new Error('Value should be an array.');
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

function throwInvalidColumnNameError(
  field: any,
  allowedNames: AllowedFields,
): never {
  const allowed = attachArrayWith.comaAndSpace(Array.from(allowedNames));
  field =
    field === null
      ? 'null'
      : field === undefined
        ? 'undefined'
        : field.toString();
  throw new Error(
    `Invalid column name ${field}. Allowed Column names are: ${allowed}.`,
  );
}

function throwInvalidColumnOperationError(
  field: string,
  allowedNames: string[],
): never {
  const allowed = attachArrayWith.comaAndSpace(allowedNames);
  throw new Error(
    `Invalid column operator ${field}. Allowed Column operations are: ${allowed}.`,
  );
}

function throwInvalidColumnNameLenError(
  field: string,
  options: { min: number; max: number },
): never {
  throw new Error(
    `Invalid column name ${field}. Column name should be within ${options.min} - ${options.max} characters.`,
  );
}

function throwInvalidColumnNameRegexError(field: string): never {
  throw new Error(
    `Invalid column name ${field}. Column name must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_) and must begin with a letter or underscore.`,
  );
}

function throwInvalidColumnNameForGrpError(field: string): never {
  throw new Error(
    `Invalid column "${field}" for HAVING clause. Column should be part of GROUP BY or an aggregate function.`,
  );
}

function throwInvalidFieldFuncCall(): never {
  throw new Error(
    'Invalid FieldFunction call: This method can only be executed within a valid query-building context.',
  );
}

function throwInvalidCol(): never {
  throw new Error('Invalid Column type provided.');
}
function throwInvalidOperand(): never {
  throw new Error('Invalid operand value provided.');
}

function throwInvalidCastFunction(): never {
  throw new Error('Invalid type cast function provided.');
}

function throwInvalidFrameFunction(methodName: string): never {
  throw new Error(`Invalid frame function option provided for ${methodName}.`);
}

function throwInvalidWindowFunctionOption(methodName: string): never {
  throw new Error(
    `Invalid option provided for  window function ${methodName}.`,
  );
}

function throwInvalidRawQueryOption(): never {
  throw new Error(
    'Invalid raw query option provided.Only string or object allowed.',
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
  invalidColumnNameType: throwInvalidColumnNameError,
  invalidGrpColumnNameType: throwInvalidColumnNameForGrpError,
  invalidColNameLenType: throwInvalidColumnNameLenError,
  invalidColumnNameRegexType: throwInvalidColumnNameRegexError,
  invalidColumnOpType: throwInvalidColumnOperationError,
  invalidFieldFuncCallType: throwInvalidFieldFuncCall,
  invalidOpDataType: throwInvalidOpDataTypeError,
  invalidColType: throwInvalidCol,
  invalidOperandType: throwInvalidOperand,
  invalidTypeCastFunc: throwInvalidCastFunction,
  invalidFrameFunction: throwInvalidFrameFunction,
  invalidModelSubquery: throwInvalidSubquery,
  invalidWindowFuncOpt: throwInvalidWindowFunctionOption,
  invalidArrayDataType: throwInvalidArrayDataTypeError,
  invalidJsonPathType: throwInvalidJsonPathDataTypeError,
  invalidRawQueryType: throwInvalidRawQueryOption,
  invalidJsonType: throwInvalidJson,
  invalidJsonSlicingType: throwInvalidJsonPathSlicing,
  invalidJsonQueryBuilderType: throwInvalidJsonQuery,
};

export const errorHandler = (
  query: string,
  flatedValues: Primitive[],
  error: Error,
) => {
  const strValues = attachArrayWith.coma(flatedValues);
  const msg = `Error executing query: "${query}" with params "(${strValues})". Error: ${error.message}`;
  const err = new Error(msg);
  throw err;
};
