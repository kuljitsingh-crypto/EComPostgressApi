import { DB_KEYWORDS } from '../constants/dbkeywords';
import { OP } from '../constants/operators';
import { TABLE_JOIN, TableJoinType } from '../constants/tableJoin';
import { AllowedFields, Join, JoinCond, JoinQuery } from '../internalTypes';
import { throwError } from './errorHelper';
import {
  attachArrayWith,
  fieldQuote,
  isEmptyObject,
  isValidModel,
} from './helperFunction';

const joinTableCond = <Model>(
  cond: JoinCond<Model>,
  allowedFields: AllowedFields,
) => {
  const onStr = attachArrayWith.and(
    Object.entries(cond).map(([baseColumn, joinColumn]) =>
      attachArrayWith.space([
        fieldQuote(allowedFields, baseColumn),
        OP.eq,
        fieldQuote(allowedFields, joinColumn),
      ]),
    ),
  );
  return onStr ? `(${onStr})` : '';
};
export class TableJoin {
  static prepareTableJoin<Model>(
    selfModelName: string,
    allowedFields: AllowedFields,
    join?: Record<TableJoinType, JoinQuery<TableJoinType, Model>>,
  ) {
    if (isEmptyObject(join)) {
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
  static #prepareJoinStr<T extends TableJoinType, Model>(
    allowedFields: AllowedFields,
    joinType: Join<Model>,
  ) {
    const { type, model, on, tableName: name, alias } = joinType;
    const joinName = TABLE_JOIN[type];
    if (!joinName) {
      return throwError.invalidJoinType(type);
    }
    if (!name && !isValidModel(model)) {
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
