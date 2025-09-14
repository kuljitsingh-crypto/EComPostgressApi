import { DB_KEYWORDS } from '../constants/dbkeywords';
import { Primitive } from '../globalTypes';
import {
  AllowedFields,
  CallableField,
  CaseSubquery,
  GroupByFields,
  InOperationSubQuery,
  PreparedValues,
} from '../internalTypes';
import { TableFilter } from './filterHelper';
import {
  attachArrayWith,
  fieldQuote,
  getPreparedValues,
  isCallableColumn,
  isColAliasNameArr,
  isNonEmptyString,
  isPrimitiveValue,
  isValidCaseQuery,
  isValidSubQuery,
  isValidWhereQuery,
  validCallableColCtx,
} from './helperFunction';
import { QueryHelper } from './queryHelper';

export type FieldOperand<Model, P extends Primitive = Primitive> =
  | P
  | InOperationSubQuery<Model, 'WhereNotReq', 'single'>
  | CallableField
  | CaseSubquery<Model>;

export const getFieldValue = <Model>(
  value: unknown,
  preparedValues: PreparedValues,
  groupByFields: GroupByFields,
  allowedFields: AllowedFields,
  options: {
    isAggregateAllowed?: boolean;
    customAllowedFields?: string[];
    isExistsFilter?: boolean;
    refAllowedFields?: AllowedFields;
    treatStrAsCol?: boolean;
    isFromCol?: boolean;
  } = {},
): string | null => {
  const {
    isExistsFilter = false,
    refAllowedFields,
    treatStrAsCol = false,
    isFromCol = false,
    ...callableOptions
  } = options;
  if (treatStrAsCol && isNonEmptyString(value)) {
    return fieldQuote(allowedFields, value, {
      customAllowFields: callableOptions.customAllowedFields,
    });
  } else if (isPrimitiveValue(value)) {
    return getPreparedValues(preparedValues, value as Primitive);
  } else if (isCallableColumn(value)) {
    const { col } = validCallableColCtx(value, {
      allowedFields,
      isAggregateAllowed: true,
      preparedValues,
      groupByFields,
      ...callableOptions,
    });
    return col;
  } else if (isValidCaseQuery(value)) {
    const v = value as any;
    if (typeof v.else !== 'undefined') {
      const elseVal = getFieldValue(
        v.else,
        preparedValues,
        groupByFields,
        allowedFields,
      );
      return attachArrayWith.space([DB_KEYWORDS.else, elseVal]);
    } else if (typeof v.when !== 'undefined' && typeof v.then !== 'undefined') {
      const query = TableFilter.prepareFilterStatement(
        allowedFields,
        groupByFields,
        preparedValues,
        v.when,
        { customKeyWord: '' },
      );
      const thenVal = getFieldValue(
        v.then,
        preparedValues,
        groupByFields,
        allowedFields,
      );
      return attachArrayWith.space([
        DB_KEYWORDS.when,
        query,
        DB_KEYWORDS.then,
        thenVal,
      ]);
    }
  } else if (isValidSubQuery(value)) {
    const query = QueryHelper.otherModelSubqueryBuilder(
      '',
      preparedValues,
      groupByFields,
      value,
      { isExistsFilter, refAllowedFields },
    );
    return query;
  } else if (isFromCol && isColAliasNameArr(value)) {
    return getFieldValue(
      value[0],
      preparedValues,
      groupByFields,
      allowedFields,
      options,
    );
  } else if (isValidWhereQuery(value)) {
    const query = TableFilter.prepareFilterStatement(
      allowedFields,
      groupByFields,
      preparedValues,
      value,
      { customKeyWord: '' },
    );
    return query;
  }
  return null;
};
