import { PgDataType } from '../constants/dataTypes';
import { OP } from '../constants/operators';
import { setOperation } from '../constants/setOperations';
import { TABLE_JOIN, TableJoinType } from '../constants/tableJoin';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  CallableField,
  CallableFieldParam,
  FieldMetadata,
  GroupByFields,
  JoinQuery,
  PreparedValues,
  SetQueryArrField,
  Subquery,
} from '../internalTypes';
import { isValidInternalContext } from './ctxHelper';
import { throwError } from './errorHelper';
import { symbolFuncRegister } from './symbolHelper';
import {
  filterOutValidDbData,
  isNonEmptyString,
  isNullableValue,
  isPrimitiveValue,
  isValidAggregateValue,
  isValidAllowedFields,
  isValidArray,
  isValidBoolean,
  isValidCustomALlowedFields,
  isValidFunction,
  isValidGroupByFieldsFields,
  isValidNumber,
  isValidObject,
  isValidPreparedValues,
  isValidSymbol,
} from './util';

type FieldQuoteReturn<T extends boolean> = T extends false
  ? string
  : string | null;

type ValidOption = Exclude<
  CallableFieldParam[keyof CallableFieldParam],
  undefined
>;

const MIN_COLUMN_LENGTH = 1;
const MAX_COLUMN_LENGTH = 63;
const validColumnNameRegex = /^([a-zA-Z_][a-zA-Z0-9_$]*)(\.[a-zA-Z0-9_$]*)*$/;
const digitRegex = /^([0-9]+)$/;

const callableFieldValidator: Record<
  keyof CallableFieldParam,
  (val: unknown) => boolean
> = {
  preparedValues: isValidPreparedValues,
  allowedFields: isValidAllowedFields,
  groupByFields: isValidGroupByFieldsFields,
  isAggregateAllowed: isValidAggregateValue,
  customAllowedFields: isValidCustomALlowedFields,
};

const attachArrayWithSep = (
  array: Array<Primitive>,
  sep: string,
  shouldTrimStr?: boolean,
) => array.filter(filterOutValidDbData(shouldTrimStr)).join(sep);

const attachArrayWithDotSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, '.', shouldTrimStr);

const attachArrayWithSpaceSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, ' ', shouldTrimStr);

const attachArrayWithNoSpaceSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, '', shouldTrimStr);

const attachArrayWithComaSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, ',', shouldTrimStr);

const attachArrayWithAndSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, ` ${OP.$and} `, shouldTrimStr);

const attachArrayWithComaAndSpaceSep = (
  array: Array<Primitive>,
  shouldTrimStr?: boolean,
) => attachArrayWithSep(array, ', ', shouldTrimStr);

const isFieldAllowed =
  (allowed: AllowedFields, customAllowFields: string[]) => (field: string) =>
    allowed.has(field) || customAllowFields.includes(field);

const checkForJsonField =
  (
    allowed: AllowedFields,
    preparedValues: PreparedValues | null,
    customAllowFields: string[],
    asJson: boolean,
  ) =>
  (field: string) => {
    if (isNullableValue(preparedValues)) {
      return null;
    }
    const fieldArr = field.split('.');
    const simpleField = fieldArr[0];
    const aliasField = `${fieldArr[0]}.${fieldArr[1]}`;
    if (isFieldAllowed(allowed, customAllowFields)(simpleField)) {
      return prepareFieldForJson(fieldArr, preparedValues, 1, asJson);
    } else if (isFieldAllowed(allowed, customAllowFields)(aliasField)) {
      return prepareFieldForJson(fieldArr, preparedValues, 2, asJson);
    }
    return null;
  };

const validateField = (
  field: string,
  allowed: AllowedFields,
  preparedValues: PreparedValues | null,
  options?: {
    customAllowFields: string[];
    metadata?: FieldMetadata;
    asJson?: boolean;
  },
) => {
  const {
    customAllowFields = [],
    metadata = {},
    asJson = false,
  } = options || {};
  field = simpleFieldValidate(field, customAllowFields);
  const isAllowedField = isFieldAllowed(allowed, customAllowFields)(field);
  if (isAllowedField) {
    return field;
  }
  const jsonField = checkForJsonField(
    allowed,
    preparedValues,
    customAllowFields,
    asJson,
  )(field);
  if (isNonEmptyString(jsonField)) {
    metadata.isJSONField = true;
    return jsonField;
  }
  return throwError.invalidColumnNameType(field, allowed);
};

const callableCol = (col: CallableField, options: CallableFieldParam) => {
  const validOptions = Object.entries(options || {}).reduce(
    (pre, acc) => {
      const [key, val] = acc;
      if (typeof val !== 'undefined') {
        pre[key] = val;
      }
      return pre;
    },
    {} as Record<string, ValidOption>,
  );
  return col(validOptions);
};

const createSymbolMethodRef = (method: CallableField, ...keys: string[]) => {
  const symbolName = attachArrayWith.dot(keys);
  const symbol = Symbol(symbolName);
  symbolFuncRegister.add(symbol, method);
  return symbol;
};

//=================== export functions ======================//

export function prepareFieldForJson(
  fieldArr: string[],
  preparedValues: PreparedValues,
  startIndex: number,
  asJson: boolean,
) {
  const fieldName = attachArrayWith.dot(fieldArr.slice(0, startIndex)); //
  const placeholders = fieldArr
    .slice(startIndex)
    .map((val) =>
      getPreparedValues(preparedValues, val, { returnNumAsItIs: true }),
    );
  const lastIndex = placeholders.length - 1;
  const lastFieldKey = asJson ? '->' : '->>';
  if (placeholders.length < 2) {
    return attachArrayWith.noSpace([
      fieldName,
      lastFieldKey,
      placeholders[lastIndex],
    ]);
  }
  const middleFields = attachArrayWith.customSep(
    placeholders.slice(0, lastIndex),
    '->',
  );
  return attachArrayWith.noSpace([
    fieldName,
    '->',
    middleFields,
    lastFieldKey,
    placeholders[lastIndex],
  ]);
}

export const createPlaceholder = (index: number, type?: string) => {
  return type ? `$${index}${type}` : `$${index}`;
};
const prepareVal = (val: Primitive | Primitive[]) =>
  isValidArray(val)
    ? val
    : digitRegex.test((val as any) || '')
      ? Number(val)
      : val;

export const getPreparedValues = <T extends boolean = false>(
  preparedValues: PreparedValues,
  value: Primitive | Primitive[],
  options?: { type?: string; returnNumAsItIs?: T },
): T extends true ? string | number : string => {
  const { type, returnNumAsItIs = false } = options || {};
  const val = prepareVal(value);
  if (isValidNumber(val) && returnNumAsItIs) {
    return val as any;
  }
  const placeholder = createPlaceholder(preparedValues.index + 1, type);
  preparedValues.values[preparedValues.index] = val;
  preparedValues.index++;
  return placeholder;
};

export const simpleFieldValidate = (
  field: string | null,
  customAllowFields: string[],
) => {
  if (!isNonEmptyString(field)) {
    return throwError.invalidColType();
  }
  field = field.trim();
  if (field.length < MIN_COLUMN_LENGTH || field.length > MAX_COLUMN_LENGTH) {
    return throwError.invalidColNameLenType(field, {
      min: MIN_COLUMN_LENGTH,
      max: MAX_COLUMN_LENGTH,
    });
  }
  if (customAllowFields.includes(field)) {
    return field;
  }
  const isValidRegexField = validColumnNameRegex.test(field);
  if (!isValidRegexField) {
    return throwError.invalidColumnNameRegexType(field);
  }
  return field;
};

export const quote = (str: string) => `${String(str).replace(/"/g, '""')}`;

export const dynamicFieldQuote = (
  field: string,
  customAllowFields: string[] = [],
) => {
  field = simpleFieldValidate(field, customAllowFields);
  return quote(field);
};

export const fieldQuote = <T extends boolean = false>(
  allowedFields: AllowedFields,
  preparedValues: PreparedValues | null,
  str: string | null,
  options?: {
    isNullColAllowed?: T;
    customAllowFields?: string[];
    metadata?: FieldMetadata;
    asJson?: boolean;
  },
): FieldQuoteReturn<T> => {
  const {
    isNullColAllowed = false,
    customAllowFields = [],
    metadata,
    asJson,
  } = options || {};
  if (str === null && isNullColAllowed) {
    return str as any;
  }
  if (!isNonEmptyString(str)) {
    return throwError.invalidColumnNameType(str, allowedFields);
  }
  str = validateField(str, allowedFields, preparedValues, {
    customAllowFields,
    metadata,
    asJson,
  });
  return quote(str);
};

export const getJoinSubqueryFields = <Model>(subQuery: Subquery<Model>) => {
  return Object.entries(subQuery || {}).reduce(
    (pre, acc) => {
      const [key, value] = acc;
      if (key in TABLE_JOIN) {
        (pre as any)[key] = value;
      }
      return pre;
    },
    {} as Record<TableJoinType, JoinQuery<TableJoinType, Model>>,
  );
};

export const getSetSubqueryFields = <Model>(
  subQuery: Subquery<Model>,
): SetQueryArrField<Model>[] => {
  return Object.entries(subQuery || {}).reduce((pre, acc) => {
    const [key, value] = acc;
    if (key in setOperation) {
      pre.push(createNewObj(value as any, { type: key }));
    }
    return pre;
  }, [] as SetQueryArrField<Model>[]);
};

export const getValidCallableFieldValues = <T extends keyof CallableFieldParam>(
  options: CallableFieldParam,
  ...requiredValues: T[]
) => {
  options = options || {};
  const validOptions = {} as {
    [k in T]: Exclude<CallableFieldParam[k], undefined>;
  };
  requiredValues.forEach((key) => {
    const isRequiredValid =
      options.hasOwnProperty(key) && callableFieldValidator[key](options[key]);
    if (isRequiredValid) {
      (validOptions as any)[key] = options[key];
    } else {
      throwError.invalidFieldFuncCallType();
    }
  });
  return validOptions;
};

export const validCallableColCtx = (
  col: CallableField,
  options: CallableFieldParam,
) => {
  const { ctx, ...rest } = callableCol(col, options);
  if (!isValidInternalContext(ctx)) {
    return throwError.invalidFieldFuncCallType();
  }
  return rest;
};

export const prepareMultipleValues = <T extends string>(
  preparedValues: PreparedValues,
  arg: {
    [key in T]: {
      type: 'string' | 'number' | 'primitive' | 'boolean';
      val: unknown;
    };
  },
): Record<T, string> => {
  const finalObject = {} as Record<T, string>;
  return Object.entries(arg).reduce((acc, [key, value]) => {
    let isValidValue = false;
    const { type, val } = value as {
      type: 'string' | 'number' | 'primitive' | 'boolean';
      val: Primitive;
    };
    (acc as any)[key] = '';
    if (type === 'string') {
      isValidValue = isNonEmptyString(val);
    } else if (type === 'number') {
      isValidValue = isValidNumber(val);
    } else if (type === 'boolean') {
      isValidValue = isValidBoolean(val);
    } else if (type === 'primitive') {
      isValidValue = isPrimitiveValue(val);
    }
    if (isValidValue) {
      (acc as any)[key] = getPreparedValues(preparedValues, val);
    }
    return acc;
  }, finalObject);
};

export const getAllEntries = (obj: unknown): Array<[string | symbol, any]> => {
  if (!isValidObject(obj)) {
    return [];
  }
  const symbolKeys = Object.getOwnPropertySymbols(obj);
  const keyEntries = Object.entries(obj) as [string | symbol, any];
  for (let symbol of symbolKeys) {
    keyEntries.push([symbol, (obj as any)[symbol]]);
  }
  return keyEntries;
};

export const validateColumn =
  (
    col: string | symbol,
    colOptions?: {
      shouldSkipFieldValidation?: boolean;
      isNullColAllowed?: false | undefined;
      customAllowFields?: string[];
      isAggregateAllowed?: boolean | undefined;
    },
  ) =>
  (options: {
    preparedValues: PreparedValues;
    groupByFields: GroupByFields;
    allowedFields: AllowedFields;
  }) => {
    const { shouldSkipFieldValidation, isAggregateAllowed, ...rest } =
      colOptions || {};
    const { allowedFields, preparedValues } = options;
    if (isValidSymbol(col)) {
      const registry = symbolFuncRegister.get(col);
      if (!isValidFunction(registry)) {
        return throwError.invalidColumnNameRegexType(col.toString());
      }
      const { col: val } = registry({
        ...options,
        isAggregateAllowed,
        customAllowedFields: rest.customAllowFields,
      });
      symbolFuncRegister.delete(col);
      return val;
    }
    if (isNonEmptyString(col)) {
      if (shouldSkipFieldValidation) {
        return col;
      }
      return fieldQuote(allowedFields, preparedValues, col, rest);
    }
    return throwError.invalidColumnNameRegexType(
      ((col as any) || 'null').toString(),
    );
  };

export const attachMethodToSymbolRegistry = (
  method: any,
  ...keys: string[]
) => {
  method.toString = () => {
    const symbol = createSymbolMethodRef(
      method,
      ...keys,
      Date.now().toString(),
    );
    return symbol;
  };
};

export const isFloatVal = (val: unknown): val is number => {
  return isValidNumber(val) && !Number.isInteger(val);
};

export const isIntegerVal = (val: unknown): val is number => {
  return isValidNumber(val) && Number.isInteger(val);
};

export const covertJSDataToSQLData = (data: unknown): string => {
  if (data === null) {
    return PgDataType.null;
  } else if (typeof data === 'boolean') {
    return PgDataType.boolean;
  } else if (typeof data === 'string') {
    return PgDataType.text;
  } else if (typeof data === 'bigint') {
    return PgDataType.bigInt;
  } else if (isFloatVal(data)) {
    return PgDataType.double;
  } else if (isIntegerVal(data)) {
    return PgDataType.int;
  } else if (isValidArray(data)) {
    return `${covertJSDataToSQLData(data[0])}[]`;
  }
  return throwError.invalidDataType(data);
};

export const prepareSQLDataType = (data: Primitive) =>
  `::${covertJSDataToSQLData(data)}`;
//===================================== Object wrapped functions =======================//

export const attachArrayWith = {
  space: attachArrayWithSpaceSep,
  coma: attachArrayWithComaSep,
  and: attachArrayWithAndSep,
  dot: attachArrayWithDotSep,
  noSpace: attachArrayWithNoSpaceSep,
  comaAndSpace: attachArrayWithComaAndSpaceSep,
  customSep: attachArrayWithSep,
};

//==================================== Field helper depend on object ============================//
export const covertStrArrayToStr = (
  value: string | string[],
  options?: { by: keyof typeof attachArrayWith; sep?: string },
): string => {
  const { by = 'coma', sep } = options || {};
  return isValidArray<string>(value)
    ? by === 'customSep'
      ? attachArrayWith.customSep(value, sep ?? ',')
      : attachArrayWith[by](value)
    : value;
};

export const createNewObj = (...objs: object[]) => {
  return Object.assign({}, ...objs);
};

export const repeatValInArrUpto = <T extends Primitive>(
  ch: T,
  upto: number,
) => {
  const arr: T[] = [];
  for (let i = 0; i < upto; i++) {
    arr.push(ch);
  }
  return arr;
};
