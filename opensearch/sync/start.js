import { incrementalSync } from './sync.js';
import { Client as PgClient } from 'pg';

// 1️⃣ Run initial full sync
console.log('🚀 Running initial incremental sync...');
await incrementalSync();
console.log('✅ Initial sync done.');

// 2️⃣ Start listening for real-time changes
const pgClient = new PgClient({
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: process.env.PG_PORT,                  // PostgreSQL port
});

await pgClient.connect();
console.log('👂 Listening for table changes...');

await pgClient.query('LISTEN table_changes');

pgClient.on('notification', async (msg) => {
  console.log('📣 Change detected, running incremental sync...');
  try {
    await incrementalSync();
    console.log('✅ Incremental sync completed.');
  } catch (err) {
    console.error('❌ Error during incremental sync:', err);
  }
});

process.on('SIGINT', async () => {
  console.log('Shutting down listener...');
  await pgClient.end();
  process.exit();
});
