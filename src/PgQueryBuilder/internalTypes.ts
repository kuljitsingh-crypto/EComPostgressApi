import { DB_KEYWORDS_TYPE } from './constants/dbkeywords';
import { FieldFunctionType } from './constants/fieldFunctions';
import { Reference } from './constants/foreignkeyActions';
import { SIMPLE_OP_KEYS } from './constants/operators';
import { SetOperationType } from './constants/setOperations';
import { TABLE_JOIN_COND } from './constants/tableJoin';
import { Primitive } from './globalTypes';

type SubqueryWhereReq = 'WhereReq' | 'WhereNotReq';

export type ORDER_OPTION = 'ASC' | 'DESC';
export type NULL_OPTION = 'NULLS FIRST' | 'NULLS LAST';
export type PAGINATION = { limit: number; offset?: number };

export type ORDER_BY = Record<
  string,
  | ORDER_OPTION
  | { order: ORDER_OPTION; nullOption?: NULL_OPTION; fn?: FieldFunctionType }
>;

export type PreparedValues = { index: number; values: Primitive[] };

export type SubQueryFilterKey = DB_KEYWORDS_TYPE['any' | 'all'];
export type SubQueryFilterRecord = {
  [key in SubQueryFilterKey]?: Array<Primitive> | SubQueryFilter;
};
export type FilterColumnValue = Primitive | SubQueryFilterRecord;

export type ConditionMap = {
  in: Primitive[] | InOperationSubQuery;
  notIn: Primitive[] | InOperationSubQuery;
  between: Primitive[];
  notBetween: Primitive[];
  isNull: null;
  notNull: null;
};

export type NormalOperators =
  | {
      [key in Exclude<SIMPLE_OP_KEYS, keyof ConditionMap>]?: FilterColumnValue;
    }
  | FilterColumnValue;

export type Condition<Key extends SIMPLE_OP_KEYS = SIMPLE_OP_KEYS> =
  Key extends keyof ConditionMap
    ? { [K in Key]: ConditionMap[K] }
    : NormalOperators;

export type ExistsFilter<Model, T extends SubqueryWhereReq = 'WhereNotReq'> = {
  model: Model;
  alias?: string;
} & Subquery<T>;
export type SubQueryFilter<T extends SubqueryWhereReq = 'WhereNotReq'> =
  ExistsFilter<T> & {
    orderBy?: ORDER_BY;
    column: string;
    isDistinct?: boolean;
  };

export type InOperationSubQuery = SubQueryFilter & {
  isDistinct?: boolean;
};

export type WhereClause<Model> =
  | {
      [column: string]: Condition;
    }
  | {
      $and: WhereClause<Model>[];
    }
  | {
      $or: WhereClause<Model>[];
    }
  | { $exists: ExistsFilter<Model, 'WhereReq'> }
  | { $notExists: ExistsFilter<Model, 'WhereReq'> };

export type Subquery<
  Model,
  T extends SubqueryWhereReq = 'WhereNotReq',
> = (T extends 'WhereReq'
  ? { where: WhereClause<Model> }
  : {
      where?: WhereClause<Model>;
    }) & {
  groupBy?: string[];
  limit?: PAGINATION['limit'];
  offset?: PAGINATION['offset'];
  join?: TABLE_JOIN_COND<Model>[];
  having?: WhereClause<Model>;
};

export type SelectQuery<Model> = {
  columns?: FindQueryAttributes;
  isDistinct?: boolean;
  alias?: AliasSubType<Model>;
};
export type SetQuery<Model> = {
  type: SetOperationType;
} & SetOperationFilter<Model>;
export type AliasFilter<Model> = {
  model?: Model;
  alias?: AliasSubType<Model>;
  columns?: FindQueryAttributes;
  orderBy?: ORDER_BY;
  set?: SetQuery<Model>;
} & Subquery<'WhereNotReq'> & {
    isDistinct?: boolean;
  };
export type AliasSubType<Model> =
  | string
  | { as?: string; query: AliasFilter<Model> };
export type SetOperationFilter<Model> = {
  model: Model;
  alias?: AliasSubType<Model>;
  columns?: FindQueryAttributes;
  orderBy?: ORDER_BY;
  set?: SetQuery<Model>;
} & Subquery<'WhereNotReq'>;

export type WhereClauseKeys = '$and' | '$or' | string;

export type ExtraOptions = {
  tableName: string;
  reference?: Reference;
};

/**
 * Different Flavours
 * 1. {columnName:null} - return column name as define in columnKey
 * 2.{columnName:aliasName} - return column name as define in columnValue
 */
export type FindQueryAttribute = Record<string, null | string> | string;
export type FindQueryAttributes = FindQueryAttribute[];

export type QueryParams<Model> = SelectQuery<Model> &
  Subquery<Model> & {
    orderBy?: ORDER_BY;
    set?: SetQuery<Model>;
  };

export type RawQuery =
  | string
  | {
      columns?: string[];
      where?: string[];
      groupBy?: string[];
      orderBy?: string[];
      having?: string[];
      distinct?: boolean;
      limit?: number;
      offset?: number;
    };
