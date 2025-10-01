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
import { FieldHelper } from './fieldHelper';
import {
  attachArrayWith,
  createNewObj,
  fieldQuote,
  getJoinSubqueryFields,
} from './helperFunction';
import { QueryHelper } from './queryHelper';
import {
  ensureArray,
  isEmptyObject,
  isNonEmptyObject,
  isNonEmptyString,
  isValidSimpleModel,
  isValidSubQuery,
} from './util';

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
      const value = isNonEmptyString(joinColumn)
        ? fieldQuote(allowedFields, preparedValues, joinColumn)
        : QueryHelper.otherModelSubqueryBuilder(
            '',
            preparedValues,
            groupByFields,
            joinColumn,
            { isExistsFilter: false },
          );
      return attachArrayWith.space([
        fieldQuote(allowedFields, preparedValues, baseColumn),
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
      const valArr = ensureArray(value);
      TableJoin.#prepareMultiJoinStars(
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

  static #getJoinModelName<Model>(
    join: UpdatedJoin<Model>,
    options: {
      allowedFields: AllowedFields;
      preparedValues: PreparedValues;
      groupByFields: GroupByFields;
    },
  ): string {
    if (isSelfJoin(join)) {
      return join.name;
    }
    if (isValidSimpleModel<Model>(join.model)) {
      return (join.model as any).tableName;
    } else if (isNonEmptyObject(join.model)) {
      return QueryHelper.otherModelSubqueryBuilder(
        '',
        options.preparedValues,
        options.groupByFields,
        join.model as any,
        { isExistsFilter: false, isColumnReq: false },
      );
    }
    return throwError.invalidModelType();
  }

  static #prepareMultiJoinStars<Model>(
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
    const { type, alias, modelAlias, ...restJoin } = join;
    const joinName = TABLE_JOIN[type];
    if (!joinName) {
      return throwError.invalidJoinType(type);
    }
    const onQuery = isCrossJoin(join) ? null : join.on;
    const isSubquery = isValidSubQuery(restJoin);
    const table = isSubquery
      ? QueryHelper.otherModelSubqueryBuilder(
          '',
          preparedValues,
          groupByFields,
          createNewObj(restJoin, { alias: modelAlias }),
          { isExistsFilter: false },
        )
      : TableJoin.#getJoinModelName(join, {
          preparedValues,
          groupByFields,
          allowedFields,
        });
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
