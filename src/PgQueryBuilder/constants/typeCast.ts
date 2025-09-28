export const noParamTypeCast = {
  int: 'INTEGER',
  bigint: 'BIGINT',
  smallint: 'SMALLINT',
  real: 'REAL',
  double: 'DOUBLE PRECISION',
  serial: 'SERIAL',
  bigSerial: 'BIGSERIAL',
  smallSerial: 'SMALLSERIAL',
  text: 'TEXT',
  date: 'DATE',
  boolean: 'BOOLEAN',
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
  json: 'JSON',
  jsonb: 'JSONB',
  uuid: 'UUID',
  int4Range: 'INT4RANGE',
  int8Range: 'INT8RANGE',
  numRange: 'NUMRANGE',
  tsRange: 'TSRANGE',
  tstzRange: 'TSTZRANGE',
  dateRange: 'DATERANGE',
} as const;

export const lengthParamTypeCast = {
  varchar: 'VARCHAR',
  characterVarying: 'CHARACTER VARYING',
  char: 'CHAR',
  character: 'CHARACTER',
  decimal: 'DECIMAL',
  bit: 'BIT',
  bitVarying: 'BIT VARYING',
  varbit: 'VARBIT',
  varcharArray: 'VARCHAR[]',
  charArray: 'CHAR[]',
} as const;

export const precisionParamTypeCast = {
  time: 'TIME',
  timetz: 'TIME WITH TIME ZONE',
  timeWithTimeZone: 'TIME WITH TIME ZONE',
  timestamp: 'TIMESTAMP',
  timestamptz: 'TIMESTAMP WITH TIME ZONE',
  timestampWithTimeZone: 'TIMESTAMP WITH TIME ZONE',
  float: 'FLOAT',
  timeArray: 'TIME[]',
  timestampArray: 'TIMESTAMP[]',
} as const;

export const precisionAndScaleParamTypeCast = {
  decimal: 'DECIMAL',
  numeric: 'NUMERIC',
  decimalArray: 'DECIMAL[]',
  numericArray: 'NUMERIC[]',
} as const;
export const fieldsParamTypeCast = { interval: 'INTERVAL' } as const;

export const allowedIntervalFields = [
  'YEAR',
  'MONTH',
  'DAY',
  'HOUR',
  'MINUTE',
  'SECOND',
  'YEAR TO MONTH',
  'DAY TO HOUR',
  'DAY TO MINUTE',
  'DAY TO SECOND',
  'HOUR TO MINUTE',
  'HOUR TO SECOND',
  'MINUTE TO SECOND',
] as const;

export const intervalFieldsWithPrecision = [
  'SECOND',
  'DAY TO SECOND',
  'HOUR TO SECOND',
  'MINUTE TO SECOND',
] as const;

export type NoParamTypeCast = keyof typeof noParamTypeCast;
export type LengthParamTypeCast = keyof typeof lengthParamTypeCast;
export type PrecisionParamTypeCast = keyof typeof precisionParamTypeCast;
export type PrecisionAndScaleParamTypeCast =
  keyof typeof precisionAndScaleParamTypeCast;
export type FieldsParamTypeCast = keyof typeof fieldsParamTypeCast;

export type TypeCastKeys =
  | NoParamTypeCast
  | LengthParamTypeCast
  | PrecisionParamTypeCast
  | PrecisionAndScaleParamTypeCast
  | FieldsParamTypeCast;

export type AllowedIntervalFields = (typeof allowedIntervalFields)[number];
