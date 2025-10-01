import { TableJoinType } from '../constants/tableJoin';
import {
  AliasSubType,
  AllowedFields,
  DerivedModel,
  FindQueryAttributes,
  JoinQuery,
  OtherJoin,
} from '../internalTypes';
import { throwError } from './errorHelper';
import { simpleFieldValidate } from './helperFunction';
import {
  ensureArray,
  isNonEmptyObject,
  isNonEmptyString,
  isNonNullableValue,
  isNullableValue,
  isValidArray,
  isValidDerivedModel,
  isValidSimpleModel,
} from './util';

export class FieldHelper {
  static getAllowedFields<Model>(
    selfAllowedFields: AllowedFields,
    options?: {
      derivedModel?: DerivedModel<Model>;
      alias?: AliasSubType;
      join?: Record<TableJoinType, JoinQuery<TableJoinType, Model>>;
      refAllowedFields?: AllowedFields;
    },
  ): AllowedFields {
    const { alias, join, refAllowedFields, derivedModel } = options || {};
    const modelFields = FieldHelper.#initializeModelFields(
      selfAllowedFields,
      refAllowedFields,
      { alias, derivedModel },
    );
    FieldHelper.#getJoinFieldNames(modelFields, join);
    return new Set(modelFields);
  }

  static getDerivedModel<Model>(derivedModel?: DerivedModel<Model>): {
    tableColumns: AllowedFields;
    tableName: string;
  } {
    if (!isValidDerivedModel<Model>(derivedModel)) {
      return throwError.invalidModelType();
    }
    if (isValidSimpleModel(derivedModel)) {
      return derivedModel as any;
    }
    return FieldHelper.getDerivedModel((derivedModel as any).model);
  }

  static getAliasName(alias?: AliasSubType): string | null {
    const aliasStr = isNonEmptyString(alias) ? alias : null;
    return aliasStr;
  }

  static #getJoinFieldNames = <Model>(
    modelFields: string[],
    join?: Record<TableJoinType, JoinQuery<TableJoinType, Model>>,
  ) => {
    if (isNonEmptyObject(join)) {
      Object.entries(join).forEach((joinType) => {
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

  static #addDerivedModelAliasName<Model>(
    aliasNames: string[],
    derivedModel?: DerivedModel<Model>,
  ): void {
    if (isNullableValue(derivedModel) || isValidSimpleModel(derivedModel)) {
      return;
    }
    if (isNonEmptyString((derivedModel as any).alias)) {
      aliasNames.push((derivedModel as any).alias);
    }
    return FieldHelper.#addDerivedModelAliasName(
      aliasNames,
      (derivedModel as any).model,
    );
  }

  static #getAliasNames<Model>(
    aliasNames: string[],
    alias?: AliasSubType,
    derivedModel?: DerivedModel<Model>,
  ): string[] {
    if (isNonEmptyString(alias)) {
      aliasNames.push(alias);
    }
    FieldHelper.#addDerivedModelAliasName(aliasNames, derivedModel);
    return aliasNames;
  }

  static #aliasFieldNames<Model>(
    names: Set<string>,
    options?: { alias?: AliasSubType; derivedModel?: DerivedModel<Model> },
  ) {
    const { alias, derivedModel } = options || {};
    const aliasNames = FieldHelper.#getAliasNames([], alias, derivedModel);
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
    columns = ensureArray(columns);
    return columns.reduce((pre, acc) => {
      if (
        isValidArray(acc, 1) &&
        acc.length === 2 &&
        isNonEmptyString(acc[1])
      ) {
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
    const joinArrays = ensureArray(join);
    joinArrays.forEach((joinType) => {
      const model = FieldHelper.getDerivedModel(joinType.model);
      const tableNames = model.tableColumns;
      const aliasTableNames = FieldHelper.#aliasFieldNames<Model>(
        tableNames,
        joinType.model as any,
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
    options?: { alias?: AliasSubType; derivedModel?: DerivedModel<Model> },
  ) {
    const { alias, derivedModel } = options || {};
    if (isNonNullableValue(derivedModel)) {
      const model = FieldHelper.getDerivedModel(derivedModel);
      selfAllowedFields = model.tableColumns;
    }
    refAllowedFields = (refAllowedFields ?? new Set()) as Set<string>;
    return [
      ...selfAllowedFields,
      ...refAllowedFields,
      ...FieldHelper.#aliasFieldNames(selfAllowedFields, {
        alias,
        derivedModel,
      }),
    ];
  }
}
