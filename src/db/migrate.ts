import { readFile } from 'node:fs/promises';
import { databasePool } from '../config/database.js';

const runMigration = async (): Promise<void> => {
  const migrationSql = await readFile(new URL('./init.sql', import.meta.url), 'utf8');

  try {
    await databasePool.query(migrationSql);
    console.log('Database migration completed successfully.');
  } finally {
    await databasePool.end();
  }
};

runMigration().catch((error: unknown) => {
  console.error('Database migration failed:', error);
  process.exitCode = 1;
});
