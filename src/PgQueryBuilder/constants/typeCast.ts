export const noParamTypeCast = {
  integer: 'INTEGER',
  int: 'INTEGER',
  int4: 'INTEGER',
  bigint: 'BIGINT',
  int8: 'BIGINT',
  smallint: 'SMALLINT',
  int2: 'SMALLINT',
  real: 'REAL',
  float4: 'REAL',
  doublePrecision: 'DOUBLE PRECISION',
  float8: 'DOUBLE PRECISION',
  serial: 'SERIAL',
  bigSerial: 'BIGSERIAL',
  smallSerial: 'SMALLSERIAL',
  text: 'TEXT',
  date: 'DATE',
  boolean: 'BOOLEAN',
  bool: 'BOOLEAN',
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
};

// {
//   "varchar": "VARCHAR({length})",
//   "characterVarying": "CHARACTER VARYING({length})",
//   "char": "CHAR({length})",
//   "character": "CHARACTER({length})",
//   "decimal": "DECIMAL({precision},{scale})",
//   "numeric": "NUMERIC({precision},{scale})",
//   "time": "TIME({precision})",
//   "timetz": "TIME({precision}) WITH TIME ZONE",
//   "timeWithTimeZone": "TIME({precision}) WITH TIME ZONE",
//   "timestamp": "TIMESTAMP({precision})",
//   "timestamptz": "TIMESTAMP({precision}) WITH TIME ZONE",
//   "timestampWithTimeZone": "TIMESTAMP({precision}) WITH TIME ZONE",
//   "interval": "INTERVAL {fields}",
//   "bit": "BIT({length})",
//   "bitVarying": "BIT VARYING({length})",
//   "varbit": "VARBIT({length})",
//   "float": "FLOAT({precision})",
//   "varcharArray": "VARCHAR({length})[]",
//   "charArray": "CHAR({length})[]",
//   "decimalArray": "DECIMAL({precision},{scale})[]",
//   "numericArray": "NUMERIC({precision},{scale})[]",
//   "timeArray": "TIME({precision})[]",
//   "timestampArray": "TIMESTAMP({precision})[]"
// }

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
};
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
};

export const precisionAndScaleParamTypeCast = {
  decimal: 'DECIMAL',
  numeric: 'NUMERIC',
  decimalArray: 'DECIMAL[]',
  numericArray: 'NUMERIC[]',
};
export const fieldsParamTypeCast = { interval: 'INTERVAL' };

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
