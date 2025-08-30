import { TableJoinType } from '../constants/tableJoin';
import {
  AliasSubType,
  AllowedFields,
  FindQueryAttributes,
  Join,
  JoinQuery,
  ModelAndAlias,
  OtherJoin,
} from '../internalTypes';
import { throwError } from './errorHelper';
import { isNonEmptyObject, simpleFieldValidate } from './helperFunction';

export class FieldHelper {
  static getAllowedFields<Model>(
    selfAllowedFields: AllowedFields,
    alias?: AliasSubType<Model>,
    include?: Record<TableJoinType, JoinQuery<TableJoinType, Model>>,
  ) {
    const modelFields = FieldHelper.#initializeModelFields(
      selfAllowedFields,
      alias,
    );
    FieldHelper.#getJoinFieldNames(modelFields, include);
    return new Set(modelFields);
  }

  static getAliasSubqueryModel<Model>(alias?: AliasSubType<Model>): {
    tableColumns: AllowedFields;
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

  static #getJoinFieldNames = <Model>(
    modelFields: string[],
    include?: Record<TableJoinType, JoinQuery<TableJoinType, Model>>,
  ) => {
    if (isNonEmptyObject(include)) {
      Object.entries(include).forEach((joinType) => {
        const [type, join] = joinType;
        switch (type) {
          case 'leftJoin':
          case 'innerJoin':
          case 'rightJoin':
          case 'fullJoin':
          case 'crossJoin': {
            FieldHelper.#addJoinModelFields(
              join as OtherJoin<Model> | OtherJoin<Model>[],
              modelFields,
            );
            break;
          }
        }
      });
    }
  };

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

  static #getColumnsAliasNames = (
    columns: FindQueryAttributes = [],
    alias = '',
  ) => {
    columns = Array.isArray(columns) ? columns : [columns];
    return columns.reduce((pre, acc) => {
      if (
        Array.isArray(acc) &&
        acc.length === 2 &&
        typeof acc[1] === 'string' &&
        !!acc[1]
      ) {
        const validField = simpleFieldValidate(acc[1], []);
        if (typeof alias == 'string' && alias) {
          pre.push(`${alias}.${validField}`);
        }
        pre.push(validField);
      }
      return pre;
    }, [] as string[]);
  };

  static #addJoinModelFields<Model>(
    join: OtherJoin<Model> | OtherJoin<Model>[],
    modelFields: string[],
  ) {
    const joinArrays = Array.isArray(join) ? join : [join];
    joinArrays.forEach((joinType) => {
      const { model, alias } = joinType;
      const tableNames = (model as any).tableColumns;
      const aliasTableNames = FieldHelper.#aliasFieldNames(tableNames, alias);
      const columnAlias = FieldHelper.#getColumnsAliasNames(
        joinType.columns,
        alias,
      );
      modelFields.push(...tableNames, ...columnAlias, ...aliasTableNames);
    });
  }

  static #initializeModelFields<Model>(
    refAllowedFields: AllowedFields,
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
