import { DB_KEYWORDS_TYPE } from './constants/dbkeywords';
import { FieldFunctionType } from './constants/fieldFunctions';
import { Reference } from './constants/foreignkeyActions';
import { SIMPLE_OP_KEYS } from './constants/operators';
import { SetOperationType } from './constants/setOperations';
import { TableJoinType } from './constants/tableJoin';
import { Primitive } from './globalTypes';

type ColumnRef = `${string}.${string}` | string;
type BaseColumn = ColumnRef;
type TargetColumn = ColumnRef;

export type SubqueryWhereReq = 'WhereReq' | 'WhereNotReq';
export type NonNullPrimitive = string | number | boolean;
export type ORDER_OPTION = 'ASC' | 'DESC';
export type NULL_OPTION = 'NULLS FIRST' | 'NULLS LAST';
export type PAGINATION = { limit: number; offset?: number };
export type GroupByFields = Set<string>;
export type AllowedFields = Set<string>;

export type ModelAndAlias<Model> = {
  model: Model;
  alias?: string;
};

export type ORDER_BY = Record<
  string,
  | ORDER_OPTION
  | { order: ORDER_OPTION; nullOption?: NULL_OPTION; fn?: FieldFunctionType }
>;

export type PreparedValues = { index: number; values: Primitive[] };

export type SubQueryFilterKey = DB_KEYWORDS_TYPE['any' | 'all'];
export type SubQueryFilterRecord<Model> = {
  [key in SubQueryFilterKey]?:
    | SubQueryFilter<Model, 'WhereNotReq'>
    | Array<Primitive>;
};
export type FilterColumnValue<Model> = Primitive | SubQueryFilterRecord<Model>;

export type ConditionMap<Model> = {
  in: Primitive[] | InOperationSubQuery<Model>;
  notIn: Primitive[] | InOperationSubQuery<Model>;
  between: Primitive[];
  notBetween: Primitive[];
  isNull: null;
  notNull: null;
};

export type NormalOperators<Model> =
  | {
      [key in Exclude<
        SIMPLE_OP_KEYS,
        keyof ConditionMap<Model>
      >]?: FilterColumnValue<Model>;
    }
  | FilterColumnValue<Model>;

export type Condition<
  Model,
  Key extends SIMPLE_OP_KEYS = SIMPLE_OP_KEYS,
> = Key extends keyof ConditionMap<Model>
  ? { [K in Key]: ConditionMap<Model>[K] }
  : NormalOperators<Model>;

export type ExistsFilter<
  Model,
  T extends SubqueryWhereReq = 'WhereNotReq',
> = ModelAndAlias<Model> & Subquery<Model, T>;

export type SubQueryFilter<
  Model,
  T extends SubqueryWhereReq = 'WhereNotReq',
> = ExistsFilter<Model, T> & {
  orderBy?: ORDER_BY;
  column: SubQueryColumnAttribute;
  isDistinct?: boolean;
};

export type InOperationSubQuery<Model> = SubQueryFilter<Model>;

export type SelfJoinSubQuery<Model> = Subquery<Model> & {
  orderBy?: ORDER_BY;
  column?: SubQueryColumnAttribute;
  isDistinct?: boolean;
  alias?: string;
};

export type JoinSubQuery<Model> = ModelAndAlias<Model> &
  Subquery<Model> & {
    orderBy?: ORDER_BY;
    column?: SubQueryColumnAttribute;
    isDistinct?: boolean;
  };

export type JoinCond<Model> = Record<
  BaseColumn,
  TargetColumn | InOperationSubQuery<Model>
>;

export type OtherJoin<Model extends any> = {
  on: JoinCond<Model>;
} & JoinSubQuery<Model>;

type SelfJoin<Model extends any> = {
  on: JoinCond<Model>;
} & SelfJoinSubQuery<Model>;

type CrossJoin<Model extends any> = JoinSubQuery<Model>;

export type TableJoin<T extends TableJoinType, Model> = T extends 'selfJoin'
  ? SelfJoin<Model>
  : T extends 'crossJoin'
    ? CrossJoin<Model>
    : OtherJoin<Model>;

export type JoinQuery<Type extends TableJoinType, Model> =
  | TableJoin<Type, Model>
  | TableJoin<Type, Model>[];

export type Join<Model extends any> = {
  [Type in TableJoinType]: JoinQuery<Type, Model>;
};

export type WhereClause<Model> =
  | {
      [column: string]: Condition<Model>;
    }
  | {
      $and: WhereClause<Model>[];
    }
  | {
      $or: WhereClause<Model>[];
    }
  | { $exists: ExistsFilter<Model, 'WhereReq'> }
  | { $notExists: ExistsFilter<Model, 'WhereReq'> };

export type WhereAndOtherSubQuery<
  Model,
  T extends SubqueryWhereReq,
> = (T extends 'WhereReq'
  ? { where: WhereClause<Model> }
  : {
      where?: WhereClause<Model>;
    }) & {
  groupBy?: string[];
  limit?: PAGINATION['limit'];
  offset?: PAGINATION['offset'];
  having?: WhereClause<Model>;
};

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
  having?: WhereClause<Model>;
} & Partial<Join<Model>>;

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
} & Subquery<Model, 'WhereNotReq'> & {
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
} & Subquery<Model, 'WhereNotReq'>;

export type WhereClauseKeys = '$and' | '$or' | string;

export type ExtraOptions = {
  tableName: string;
  reference?: Reference;
};

export type CallableField = (
  preparedValues: PreparedValues,
  groupByFields: GroupByFields,
  allowedFields: AllowedFields,
) => {
  col: string;
  value: string | null;
  shouldSkipFieldValidation?: boolean;
  ctx: symbol;
};

export type FindQueryAttribute =
  | [string | CallableField, null | string]
  | string
  | CallableField;

export type SubQueryColumnAttribute = string | CallableField;

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
