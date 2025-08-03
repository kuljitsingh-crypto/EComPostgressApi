import {
  OTHER_JOIN,
  TABLE_JOIN_COND,
  TABLE_JOIN_TYPE,
} from '../constants/tableJoin';
import { AliasSubType } from '../internalTypes';
import { throwError } from './errorHelper';

export class FieldHelper {
  static getAllowedFields<Model>(
    selfAllowedFields: Set<string>,
    alias?: AliasSubType<Model>,
    include?: TABLE_JOIN_COND<Model>[],
  ) {
    const modelFields = FieldHelper.#initializeModelFields(
      selfAllowedFields,
      alias,
    );
    if (include && Array.isArray(include) && include.length > 0) {
      include.forEach((joinType) => {
        const { type } = joinType;
        switch (type) {
          case 'INNER':
          case 'LEFT':
          case 'RIGHT':
          case 'FULLOUTER': {
            FieldHelper.#addJoinModelFields(joinType, modelFields);
            break;
          }
          case 'CROSS': {
            const { model, alias } = joinType;
            FieldHelper.#addJoinModelFields(
              { model, on: {}, alias, type },
              modelFields,
            );
            break;
          }
        }
      });
    }
    return new Set(modelFields);
  }

  static getAliasSubqueryModel<Model>(alias?: AliasSubType<Model>): {
    tableColumns: Set<string>;
    tableName: string;
  } {
    if (
      typeof alias === 'string' ||
      typeof alias === 'undefined' ||
      alias === null
    ) {
      return throwError.invalidAliasType();
    }
    if (!alias.query) {
      return throwError.invalidAliasType(true);
    }
    if (alias.query && alias.query.alias) {
      return FieldHelper.getAliasSubqueryModel(alias.query.alias);
    }
    if (!alias.query.model) {
      throwError.invalidModelType();
    }
    return alias.query.model as any;
  }
  static getAliasName<Model>(alias?: AliasSubType<Model>): string | null {
    const aliasStr =
      typeof alias === 'object' && alias !== null && alias.as
        ? alias.as
        : typeof alias === 'string'
          ? alias
          : null;
    return aliasStr;
  }

  static #getAliasNames<Model>(
    aliasNames: string[],
    alias?: AliasSubType<Model>,
  ): string[] {
    if (!alias) {
      return aliasNames;
    }
    const aliasStr = FieldHelper.getAliasName(alias);
    if (!aliasStr) {
      return aliasNames;
    }
    aliasNames.push(aliasStr);
    if (typeof alias === 'string') {
      return aliasNames;
    }
    if (alias.query && alias.query.alias) {
      return FieldHelper.#getAliasNames(aliasNames, alias.query.alias);
    }
    return aliasNames;
  }

  static #aliasFieldNames<Model>(
    names: Set<string>,
    alias?: AliasSubType<Model>,
  ) {
    const aliasNames = FieldHelper.#getAliasNames([], alias);
    if (!aliasNames || aliasNames.length < 1) return [];
    const nameArr = Array.from(names);
    const allowedNames = aliasNames.reduce((prev, alias) => {
      prev.push(...nameArr.map((name) => `${alias}.${name}`));
      return prev;
    }, [] as string[]);
    return allowedNames;
  }

  static #addJoinModelFields<T extends TABLE_JOIN_TYPE, Model>(
    joinType: OTHER_JOIN<T, Model>,
    modelFields: string[],
  ) {
    const { model, alias } = joinType;
    const tableNames = (model as any).tableColumns;
    const aliasTableNames = FieldHelper.#aliasFieldNames(tableNames, alias);
    modelFields.push(...tableNames, ...aliasTableNames);
  }

  static #initializeModelFields<Model>(
    refAllowedFields: Set<string>,
    alias?: AliasSubType<Model>,
  ) {
    if (typeof alias === 'object') {
      const model = FieldHelper.getAliasSubqueryModel(alias);
      refAllowedFields = model.tableColumns;
    }
    return [
      ...refAllowedFields,
      ...FieldHelper.#aliasFieldNames(refAllowedFields, alias),
    ];
  }
}
