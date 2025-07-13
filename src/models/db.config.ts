import { Pool } from 'pg';

const DB_PORT = parseInt(process.env.POSTGRES_DB_PORT || '5432', 10);

const pool = new Pool({
  host: process.env.POSTGRES_DB_HOST,
  user: process.env.POSTGRES_DB_USER_NAME,
  password: process.env.POSTGRES_DB_PASSWORD,
  database: process.env.POSTGRES_DB_NAME,
  port: DB_PORT,
});

export default pool;

export const query = async (text: string, params?: unknown[]) => {
  const result = await pool.query(text, params);
  console.log('executed query', { query: text, rows: result.rowCount });
  return result;
};

export const getClient = () => {
  return pool.connect();
};

export const addUUIDExtension = async () => {
  const qry = 'CREATE EXTENSION IF NOT EXISTS "pgcrypto";';
  await query(qry);
};
