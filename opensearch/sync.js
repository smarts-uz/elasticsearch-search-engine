import { Client } from '@opensearch-project/opensearch';
import pkg from 'pg';
const { Pool } = pkg;

const pgPool = new Pool({
  user: process.env.PGUSER,
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  password: process.env.PGPASSWORD,
  port: process.env.PGPORT,
});

const osClient = new Client({
  node: process.env.OPENSEARCH_NODE || 'http://opensearch:9200',
});

const TABLES_TO_SYNC = ['player', 'club', 'team'];

// Oxirgi sync vaqtini saqlash (yoki fayl/db)
let lastSyncTime = new Date(0); // dastlab 1970

export async function incrementalSync() {
  try {
    for (const table of TABLES_TO_SYNC) {
      console.log(`🔄 ${table} jadvali incremental sync qilinmoqda...`);

      await osClient.indices.create({ index: table }, { ignore: [400] });

      // Faqat o‘zgargan yoki yangi yozuvlarni olamiz
      const { rows } = await pgPool.query(
        `SELECT * FROM ${table} WHERE updated_at > $1`,
        [lastSyncTime]
      );

      for (const row of rows) {
        await osClient.index({
          index: table,
          body: row,
        });
      }

      console.log(`✅ ${rows.length} yozuv ${table} dan OpenSearch ga yuklandi.`);
    }

    lastSyncTime = new Date(); // oxirgi sync vaqtini yangilaymiz
  } catch (err) {
    console.error('❌ Incremental sync xatolik:', err);
  }
}
