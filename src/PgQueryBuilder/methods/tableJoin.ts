import { DB_KEYWORDS } from '../constants/dbkeywords';
import { OP } from '../constants/operators';
import {
  JOIN,
  JOIN_COLUMN,
  TABLE_JOIN,
  TABLE_JOIN_COND,
  TABLE_JOIN_TYPE,
} from '../constants/tableJoin';
import { throwError } from './errorHelper';
import { attachArrayWith, FieldQuote } from './helperFunction';

const joinTableCond = (cond: JOIN_COLUMN, allowedFields: Set<string>) =>
  attachArrayWith.and(
    Object.entries(cond).map(([baseColumn, joinColumn]) =>
      attachArrayWith.space([
        FieldQuote(allowedFields, baseColumn),
        OP.eq,
        FieldQuote(allowedFields, joinColumn),
      ]),
    ),
  );

export class TableJoin {
  static prepareTableJoin<Model>(
    selfModelName: string,
    allowedFields: Set<string>,
    include?: TABLE_JOIN_COND<Model>[],
  ) {
    if (!include || include.length < 1) {
      return '';
    }
    const joins = include.map((joinType) => {
      switch (joinType.type) {
        case 'SELF': {
          const { type, on, alias } = joinType;
          const updatedInclude = {
            type,
            tableName: selfModelName,
            on,
            alias,
          };
          return TableJoin.#prepareJoinStr(allowedFields, updatedInclude);
        }
        case 'INNER':
        case 'FULLOUTER':
        case 'LEFT':
        case 'RIGHT':
          return TableJoin.#prepareJoinStr(allowedFields, joinType);
        case 'CROSS': {
          const { type, model, alias } = joinType;
          const updatedInclude = {
            type,
            model,
            on: {},
            alias,
          };
          return TableJoin.#prepareJoinStr(allowedFields, updatedInclude);
        }
        default:
          return throwError.invalidJoinType((include as any).type);
      }
    });
    return attachArrayWith.space(joins as string[]);
  }
  static #prepareJoinStr<T extends TABLE_JOIN_TYPE, Model>(
    allowedFields: Set<string>,
    joinType: JOIN<T, Model>,
  ) {
    const { type, model, on, tableName: name, alias } = joinType;
    const joinName = TABLE_JOIN[type];
    if (!joinName) {
      return throwError.invalidJoinType(type);
    }
    if (!name && !model) {
      return throwError.invalidModelType();
    }
    const tableName = name || (model as any).tableName;
    const onStr = type === 'CROSS' ? 'true' : joinTableCond(on, allowedFields);
    const aliasMaybe = alias
      ? ` ${DB_KEYWORDS.as} ${alias} ${DB_KEYWORDS.on}`
      : ` ${DB_KEYWORDS.on}`;
    return attachArrayWith.space([joinName, tableName, aliasMaybe, onStr]);
  }
}
