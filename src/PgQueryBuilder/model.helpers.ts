import { query } from '../models/db.config';
import { DB_KEYWORDS } from './constants/dbkeywords';
import { ReferenceTable } from './constants/foreignkeyActions';
import { Primitive, Table, TableValues } from './globalTypes';
import {
  ExtraOptions,
  FindQueryAttributes,
  PreparedValues,
  QueryParams,
  RawQuery,
} from './internalTypes';
import { ColumnHelper } from './methods/columnHelper';
import { errorHandler, throwError } from './methods/errorHelper';
import { FieldHelper } from './methods/fieldHelper';
import {
  attachArrayWith,
  createPlaceholder,
  FieldQuote,
  getPreparedValues,
} from './methods/helperFunction';
import { QueryHelper } from './methods/queryHelper';
import { RawQueryHandler } from './methods/rawQueryHelper';

//============================================= CONSTANTS ===================================================//
const enumQryPrefix = `DO $$ BEGIN CREATE TYPE`;
const enumQrySuffix = `EXCEPTION WHEN duplicate_object THEN null; END $$;`;

//============================================= DBQuery ===================================================//

export class DBQuery {
  static tableName: string = '';
  static tableColumns: Set<string> = new Set();
  static #groupByFields: Set<string> = new Set();

  static async findAll<Model>(queryParams?: QueryParams<Model>) {
    const preparedValues: PreparedValues = { index: 0, values: [] };
    const findAllQuery = QueryHelper.prepareQuery(
      preparedValues,
      this.tableColumns,
      DBQuery.#groupByFields,
      this.tableName,
      queryParams || {},
    );
    try {
      const result = await query(findAllQuery, preparedValues.values);
      return { rows: result.rows, count: result.rowCount };
    } catch (error) {
      return errorHandler(findAllQuery, error as Error);
    }
  }

  static async queryRawSql(
    qry: RawQuery = {} as RawQuery,
    params: Primitive[] = [],
  ) {
    const tableName = this.tableName;
    const { query: rawQry, values } = RawQueryHandler.buildRawQuery(
      qry,
      tableName,
      params,
    );
    try {
      const result = await query(rawQry, values);
      return { rows: result.rows, count: result.rowCount };
    } catch (error) {
      return errorHandler(rawQry, error as Error);
    }
  }

  static async create(
    fields: Record<string, Primitive>,
    returnOnly?: FindQueryAttributes,
  ) {
    const keys: string[] = [];
    const valuePlaceholder: string[] = [];
    const allowedFields = FieldHelper.getAllowedFields(this.tableColumns);
    const returnStr = ColumnHelper.getSelectColumns(
      allowedFields,
      returnOnly,
      false,
    );
    const preparedValues: PreparedValues = { index: 0, values: [] };
    Object.entries(fields).forEach((entry) => {
      const [key, value] = entry;
      keys.push(FieldQuote(allowedFields, key));
      const placeholder = getPreparedValues(preparedValues, value);
      valuePlaceholder.push(placeholder);
    });
    const columns = attachArrayWith.coma(keys);
    const valuePlaceholders = attachArrayWith.coma(valuePlaceholder);
    const insertClause = `${DB_KEYWORDS.insertInto} "${this.tableName}"(${columns})`;
    const valuesClause = `${DB_KEYWORDS.values}${valuePlaceholders}`;
    const returningClause = `${DB_KEYWORDS.returning} ${returnStr}`;
    const createQry = attachArrayWith.space([
      insertClause,
      valuesClause,
      returningClause,
    ]);
    try {
      const result = await query(createQry, preparedValues.values);
      return { rows: result.rows, count: result.rowCount };
    } catch (error) {
      return errorHandler(createQry, error as Error);
    }
  }

  static async createBulk(
    columns: Array<string>,
    values: Array<Array<Primitive>>,
    returnOnly?: FindQueryAttributes,
  ) {
    const flatedValues: Primitive[] = [];
    const allowedFields = FieldHelper.getAllowedFields(this.tableColumns);
    const returnStr = ColumnHelper.getSelectColumns(
      allowedFields,
      returnOnly,
      false,
    );
    let incrementBy = 1;
    const valuePlaceholder = values.map((val, pIndex) => {
      if (val.length !== columns.length) {
        return throwError.invalidColumnLenType(
          pIndex,
          columns.length,
          val.length,
        );
      }
      if (pIndex > 0) {
        incrementBy += val.length - 1;
      }
      flatedValues.push(...val);
      const placeholder = attachArrayWith.coma(
        val.map((_, cIndex) =>
          createPlaceholder(pIndex + cIndex + incrementBy),
        ),
      );

      return `(${placeholder})`;
    });
    const colStr = attachArrayWith.coma(columns);
    const valuePlaceholders = attachArrayWith.coma(valuePlaceholder);
    const insertClause = `${DB_KEYWORDS.insertInto} "${this.tableName}"(${colStr})`;
    const valuesClause = `${DB_KEYWORDS.values}${valuePlaceholders}`;
    const returningClause = `${DB_KEYWORDS.returning} ${returnStr}`;
    const createQry = attachArrayWith.space([
      insertClause,
      valuesClause,
      returningClause,
    ]);
    try {
      const result = await query(createQry, flatedValues);
      return { rows: result.rows, count: result.rowCount };
    } catch (error) {
      return errorHandler(createQry, error as Error);
    }
  }
}

//============================================= DBQuery ===================================================//

//============================================= DBModel ===================================================//

export class DBModel extends DBQuery {
  static init<T extends string>(modelObj: Table<T>, option: ExtraOptions) {
    const { tableName, reference = {} } = option;
    this.tableName = tableName;
    const primaryKeys: string[] = [];
    const columns: string[] = [];
    const enums: string[] = [];
    const tableColumns: Set<string> = new Set();
    Object.entries(modelObj).forEach((entry) => {
      const [key, value] = entry as [T, TableValues];
      tableColumns.add(key);
      columns.push(DBModel.#createColumn(key, value, primaryKeys, enums));
    });
    this.tableColumns = tableColumns;
    if (primaryKeys.length <= 0) {
      throwError.invalidPrimaryColType(tableName);
    }
    columns.push(DBModel.#createPrimaryColumn(primaryKeys));
    Object.entries(reference).forEach(([key, ref]) => {
      columns.push(DBModel.#createForeignColumn(key, ref));
    });
    const createEnumQryPromise = Promise.all(enums.map((e) => query(e)));
    const createTableQry = `CREATE TABLE IF NOT EXISTS "${tableName}" (${attachArrayWith.coma(
      columns,
    )});`;
    createEnumQryPromise.then(() => query(createTableQry));
  }

  static #createColumn(
    columnName: string,
    value: TableValues,
    primaryKeys: string[],
    enums: string[],
  ) {
    const values: (string | boolean)[] = [columnName];
    const colUpr = columnName.toUpperCase();
    Object.entries(value).forEach((entry) => {
      const [key, keyVale] = entry as [keyof TableValues, string | boolean];
      switch (key) {
        case 'type': {
          if ((keyVale as any).startsWith('ENUM')) {
            const enumQry = `${enumQryPrefix} ${colUpr} ${DB_KEYWORDS.as} ${keyVale}; ${enumQrySuffix}`;
            enums.push(enumQry);
            values.push(colUpr);
          } else {
            values.push(keyVale);
          }
          break;
        }
        case 'isPrimary':
          primaryKeys.push(columnName);
          break;
        case 'defaultValue':
          values.push(`${DB_KEYWORDS.default} ${keyVale}`);
          break;
        case 'unique':
          values.push(DB_KEYWORDS.unique);
          break;
        case 'notNull':
          values.push(DB_KEYWORDS.notNull);
          break;
        case 'customDefaultValue':
          values.push(`${DB_KEYWORDS.default} '${keyVale}'`);
          break;
        case 'check':
          values.push(`${DB_KEYWORDS.check} (${keyVale})`);
          break;
      }
    });
    return attachArrayWith.space(values);
  }
  static #createPrimaryColumn(primaryKeys: string[]) {
    return `${DB_KEYWORDS.primaryKey} (${attachArrayWith.coma(primaryKeys)})`;
  }
  static #createForeignColumn(parentTable: string, ref: ReferenceTable) {
    const { parentColumn, column, constraintName, onDelete, onUpdate } = ref;
    const colStr = Array.isArray(column)
      ? attachArrayWith.coma(column)
      : column;
    const parentColStr = Array.isArray(parentColumn)
      ? attachArrayWith.coma(parentColumn)
      : parentColumn;
    const values: string[] = [];
    if (constraintName) {
      values.push(`${DB_KEYWORDS.constraint} ${constraintName}`);
    }
    values.push(`${DB_KEYWORDS.foreignKey} (${colStr})`);
    values.push(`${DB_KEYWORDS.references} "${parentTable}" (${parentColStr})`);
    if (onDelete) {
      values.push(`${DB_KEYWORDS.onDelete} ${onDelete}`);
    }
    if (onUpdate) {
      values.push(`${DB_KEYWORDS.onUpdate} ${onUpdate}`);
    }
    return attachArrayWith.space(values);
  }
}
