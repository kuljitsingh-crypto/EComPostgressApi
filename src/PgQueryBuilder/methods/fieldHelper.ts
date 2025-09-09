import { TableJoinType } from '../constants/tableJoin';
import {
  AliasSubType,
  AllowedFields,
  FindQueryAttributes,
  JoinQuery,
  OtherJoin,
  SubModelQuery,
} from '../internalTypes';
import { throwError } from './errorHelper';
import {
  isNonEmptyObject,
  isNonEmptyString,
  isValidModel,
  isValidObject,
  simpleFieldValidate,
} from './helperFunction';

export class FieldHelper {
  static getAllowedFields<Model>(
    selfAllowedFields: AllowedFields,
    options?: {
      subquery: SubModelQuery<Model>;
      alias?: AliasSubType;
      join?: Record<TableJoinType, JoinQuery<TableJoinType, Model>>;
      refAllowedFields?: AllowedFields;
    },
  ): AllowedFields {
    const { alias, join, refAllowedFields, subquery } = options || {};
    const modelFields = FieldHelper.#initializeModelFields(
      selfAllowedFields,
      refAllowedFields,
      { alias, subquery },
    );
    FieldHelper.#getJoinFieldNames(modelFields, join);
    return new Set(modelFields);
  }

  static getSubqueryModel<Model>(subquery?: SubModelQuery<Model>): {
    tableColumns: AllowedFields;
    tableName: string;
  } {
    if (!isValidObject(subquery)) {
      return throwError.invalidModelSubquery();
    }
    if (isValidObject(subquery.subquery)) {
      return FieldHelper.getSubqueryModel(subquery.subquery);
    }
    if (!isValidModel(subquery.model)) {
      throwError.invalidModelType();
    }
    return subquery.model as any;
  }

  static getAliasName(alias?: AliasSubType): string | null {
    const aliasStr = isNonEmptyString(alias) ? alias : null;
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

  static #addSubqueryAliasName<Model>(
    aliasNames: string[],
    subQuery?: SubModelQuery<Model>,
  ): void {
    if (!isValidObject(subQuery)) {
      return;
    }
    if (isNonEmptyString(subQuery.alias)) {
      aliasNames.push(subQuery.alias);
    }
    return FieldHelper.#addSubqueryAliasName(aliasNames, subQuery.subquery);
  }

  static #getAliasNames<Model>(
    aliasNames: string[],
    alias?: AliasSubType,
    subQuery?: SubModelQuery<Model>,
  ): string[] {
    if (isNonEmptyString(alias)) {
      aliasNames.push(alias);
    }
    FieldHelper.#addSubqueryAliasName(aliasNames, subQuery);
    return aliasNames;
  }

  static #aliasFieldNames<Model>(
    names: Set<string>,
    options?: { alias?: AliasSubType; subquery?: SubModelQuery<Model> },
  ) {
    const { alias, subquery } = options || {};
    const aliasNames = FieldHelper.#getAliasNames([], alias, subquery);
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
  ): string[] => {
    columns = Array.isArray(columns) ? columns : [columns];
    return columns.reduce((pre, acc) => {
      if (Array.isArray(acc) && acc.length === 2 && isNonEmptyString(acc[1])) {
        const validField = simpleFieldValidate(acc[1], []);
        if (isNonEmptyString(alias)) {
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
      const model = FieldHelper.getSubqueryModel(joinType);
      const tableNames = model.tableColumns;
      const aliasTableNames = FieldHelper.#aliasFieldNames(
        tableNames,
        joinType,
      );
      const columnAlias = FieldHelper.#getColumnsAliasNames(
        joinType.columns,
        joinType.alias,
      );
      modelFields.push(...tableNames, ...columnAlias, ...aliasTableNames);
    });
  }

  static #initializeModelFields<Model>(
    selfAllowedFields: AllowedFields,
    refAllowedFields?: AllowedFields,
    options?: { alias?: AliasSubType; subquery?: SubModelQuery<Model> },
  ) {
    const { alias, subquery } = options || {};
    if (isValidObject(subquery)) {
      const model = FieldHelper.getSubqueryModel(subquery);
      selfAllowedFields = model.tableColumns;
    }
    refAllowedFields = (refAllowedFields ?? new Set()) as Set<string>;
    return [
      ...selfAllowedFields,
      ...refAllowedFields,
      ...FieldHelper.#aliasFieldNames(selfAllowedFields, { alias, subquery }),
    ];
  }
}
