import { DB_KEYWORDS } from '../constants/dbkeywords';
import { OP } from '../constants/operators';
import { TABLE_JOIN, TableJoinType } from '../constants/tableJoin';
import {
  AllowedFields,
  TableJoin as JoinType,
  JoinCond,
  JoinQuery,
  SelfJoin,
  CrossJoin,
  OtherJoin,
  PreparedValues,
  GroupByFields,
} from '../internalTypes';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  fieldQuote,
  getJoinSubqueryFields,
  isEmptyObject,
  isValidModel,
  isValidSubQuery,
} from './helperFunction';
import { QueryHelper } from './queryHelper';

type UpdatedSelfJoin<Model> = SelfJoin<Model> & {
  type: 'selfJoin';
  name: string;
};
type UpdatedCrossJoin<Model> = CrossJoin<Model> & { type: 'crossJoin' };
type UpdatedOtherJoin<Model> = OtherJoin<Model> & {
  type: 'innerJoin' | 'leftJoin' | 'rightJoin' | 'fullJoin';
};
type UpdatedJoin<Model> =
  | UpdatedCrossJoin<Model>
  | UpdatedOtherJoin<Model>
  | UpdatedSelfJoin<Model>;

const isCrossJoin = <Model>(
  join: UpdatedJoin<Model>,
): join is UpdatedCrossJoin<Model> => join.type === 'crossJoin';

const isSelfJoin = <Model>(
  join: UpdatedJoin<Model>,
): join is UpdatedSelfJoin<Model> => join.type === 'selfJoin';

const joinTableCond = <Model>(
  cond: JoinCond<Model, 'WhereNotReq', 'single'>,
  allowedFields: AllowedFields,
  preparedValues: PreparedValues,
  groupByFields: GroupByFields,
) => {
  const onStr = attachArrayWith.and(
    Object.entries(cond).map(([baseColumn, joinColumn]) => {
      const value =
        typeof joinColumn === 'string'
          ? fieldQuote(allowedFields, joinColumn)
          : QueryHelper.otherModelSubqueryBuilder(
              '',
              preparedValues,
              groupByFields,
              joinColumn,
            );
      return attachArrayWith.space([
        fieldQuote(allowedFields, baseColumn),
        OP.eq,
        value,
      ]);
    }),
  );
  return onStr ? `(${onStr})` : '';
};
export class TableJoin {
  static prepareTableJoin<Model>(
    selfModelName: string,
    allowedFields: AllowedFields,
    preparedValues: PreparedValues,
    groupByFields: GroupByFields,
    include?: Record<TableJoinType, JoinQuery<TableJoinType, Model>>,
  ) {
    if (isEmptyObject(include)) {
      return '';
    }
    const join = getJoinSubqueryFields(include as any);
    const joinArr: string[] = [];
    Object.entries(join).forEach((j) => {
      const [key, value] = j as [
        TableJoinType,
        JoinQuery<TableJoinType, Model>,
      ];
      const valArr = Array.isArray(value) ? value : [value];
      TableJoin.#prepareMultiJoinStrs(
        selfModelName,
        key,
        allowedFields,
        preparedValues,
        groupByFields,
        joinArr,
        valArr,
      );
    });

    return attachArrayWith.space(joinArr);
  }

  static #getJoinModelName<Model>(join: UpdatedJoin<Model>): string {
    if (isSelfJoin(join)) {
      return join.name;
    }
    if (!isValidModel(join.model)) {
      return throwError.invalidModelType();
    }
    return (join.model as any).tableName;
  }

  static #prepareMultiJoinStrs<Model>(
    selfModelName: NamedCurve,
    type: TableJoinType,
    allowedFields: AllowedFields,
    preparedValues: PreparedValues,
    groupByFields: GroupByFields,
    joins: string[],
    joinQueries: (OtherJoin<Model> | SelfJoin<Model> | CrossJoin<Model>)[],
  ) {
    joinQueries.forEach((join: any) => {
      join.type = type;
      if (type === 'selfJoin') {
        join.name = selfModelName;
      }
      const joinQry = TableJoin.#prepareJoinStr(
        allowedFields,
        preparedValues,
        groupByFields,
        join,
      );
      joins.push(joinQry);
    });
  }

  static #prepareJoinStr<Model>(
    allowedFields: AllowedFields,
    preparedValues: PreparedValues,
    groupByFields: GroupByFields,
    join: UpdatedJoin<Model>,
  ) {
    const { type, alias, ...restJoin } = join;
    const joinName = TABLE_JOIN[type];
    if (!joinName) {
      return throwError.invalidJoinType(type);
    }
    const onQuery = isCrossJoin(join) ? null : join.on;
    const isSubquery = isValidSubQuery(restJoin as any);
    const table = isSubquery
      ? QueryHelper.otherModelSubqueryBuilder(
          '',
          preparedValues,
          groupByFields,
          restJoin as any,
        )
      : TableJoin.#getJoinModelName(join);
    const onStr =
      onQuery === null
        ? null
        : joinTableCond(onQuery, allowedFields, preparedValues, groupByFields);
    const joinArr: string[] = [joinName];
    joinArr.push(table);
    if (alias) {
      joinArr.push(`${DB_KEYWORDS.as} ${alias}`);
    }
    if (onStr) {
      joinArr.push(`${DB_KEYWORDS.on} ${onStr}`);
    }
    return attachArrayWith.space(joinArr);
  }
}
