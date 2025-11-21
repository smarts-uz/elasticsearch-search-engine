import { Client as PgClient } from 'pg';
import { incrementalSync } from './sync.js'; // your sync script


const pgClient = new PgClient({
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: process.env.PG_PORT,                // PostgreSQL port
});

await pgClient.connect();
console.log('👂 Listening for table changes...');

// Listen to the notification channel
await pgClient.query('LISTEN table_changes');

// When a notification is received, run incremental sync
pgClient.on('notification', async (msg) => {
  console.log('📣 Change detected, running incremental sync...');
  try {
    await incrementalSync();
    console.log('✅ Incremental sync completed.');
  } catch (err) {
    console.error('❌ Error during incremental sync:', err);
  }
});

// Optional: keep process alive
process.on('SIGINT', async () => {
  console.log('Shutting down listener...');
  await pgClient.end();
  process.exit();
});
