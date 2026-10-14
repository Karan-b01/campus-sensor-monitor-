import { Pool } from 'pg';

export const databasePool = new Pool({
  host: process.env.DB_HOST ?? 'localhost',
  port: Number(process.env.DB_PORT ?? 5433),
  user: process.env.DB_USER ?? 'postgres',
  password: process.env.DB_PASSWORD ?? 'supersecret',
  database: process.env.DB_NAME ?? 'campus_sensors'
});
