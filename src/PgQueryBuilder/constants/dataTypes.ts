import { Primitive } from '../globalTypes';

const filterOutValidDbData = (a: Primitive) => {
  if (a === null || typeof a === 'boolean' || typeof a === 'number') {
    return true;
  } else if (typeof a == 'string' && a.trim().length > 0) {
    return true;
  }
  return false;
};

export const PgDataType = {
  boolean: 'BOOLEAN',
  true: 'TRUE',
  false: 'FALSE',
  text: 'TEXT',
  real: 'REAL',
  smallInt: 'SMALLINT',
  int: 'INTEGER',
  bigInt: 'BIGINT',
  serial: 'SERIAL',
  date: 'DATE',
  timestamp: 'TIMESTAMP',
  timestamptz: 'TIMESTAMPTZ',
  time: 'TIME',
  json: 'JSON',
  jsonb: 'JSONB',
  uuid: 'UUID',
  null: 'NULL',
  bigint: 'BIGINT',
  smallint: 'SMALLINT',
  double: 'DOUBLE PRECISION',
  bigSerial: 'BIGSERIAL',
  smallSerial: 'SMALLSERIAL',
  bytea: 'BYTEA',
  inet: 'INET',
  cidr: 'CIDR',
  macaddr: 'MACADDR',
  macaddr8: 'MACADDR8',
  point: 'POINT',
  line: 'LINE',
  lseg: 'LINE SEGMENT',
  lineSegment: 'LINE SEGMENT',
  box: 'BOX',
  path: 'PATH',
  polygon: 'POLYGON',
  circle: 'CIRCLE',
  int4Range: 'INT4RANGE',
  int8Range: 'INT8RANGE',
  numRange: 'NUMRANGE',
  tsRange: 'TSRANGE',
  tstzRange: 'TSTZRANGE',
  dateRange: 'DATERANGE',
  string(n: number) {
    return `VARCHAR(${n})`;
  },
  numeric(precision: number, scale = 0) {
    return `NUMERIC(${precision}, ${scale})`;
  },
  enum(values: string[]) {
    const valueStr = values
      .filter(filterOutValidDbData)
      .map((v) => `'${v}'`)
      .join(',');
    return `ENUM(${valueStr})`;
  },
} as const;

export const PG_DEFAULT_VALUE = {
  currentDate: 'CURRENT_DATE',
  currentTimestamp: 'CURRENT_TIMESTAMP',
  currentTime: 'CURRENT_TIME',
  uuidV4: 'gen_random_uuid()',
};
