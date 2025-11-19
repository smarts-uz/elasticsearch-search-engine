import { Client } from '@opensearch-project/opensearch';
import pkg from 'pg';
import { normalizeRow, loadLastSyncTimes, saveLastSyncTimes } from './utils.js';

const { Pool } = pkg;

const pgPool = new Pool({
  user: 'postgres',
  host: '192.168.3.56',
  database: 'postgres',
  password: 'c21hcnRza29tb2x0ZWFt',
  port: 5444,
});

const osClient = new Client({
  node: process.env.OPENSEARCH_NODE || 'http://localhost:9200',
});

// Tables to sync
const TABLES_TO_SYNC = process.env.TABLES_TO_SYNC 
  ? process.env.TABLES_TO_SYNC.split(',').map(t => t.trim())
  : ["player", "club", "team", "user", "match", "player_result", "player_point"];

export async function incrementalSync() {
  const lastSyncTimes = loadLastSyncTimes();

  try {
    for (const table of TABLES_TO_SYNC) {
      console.log(`🔄 ${table} jadvali incremental sync qilinmoqda...`);

      await osClient.indices.create({
        index: table,
        body: {
          settings: {
            analysis: {
              analyzer: {
                translit_analyzer: {
                  tokenizer: 'standard',
                  filter: ['lowercase', 'russian_translit']
                }
              },
              filter: {
                russian_translit: {
                  type: 'icu_transform',
                  id: 'Any-Latin; Latin-Cyrillic'
                }
              }
            }
          },
          mappings: {
            properties: {
              name: {
                type: 'text',
                analyzer: 'translit_analyzer',
                search_analyzer: 'translit_analyzer'
              },
              geo: { type: 'geo_point' },
              agent: { type: 'object' }
            }
          }
        }
      }, { ignore: [400] });

      const { rows } = await pgPool.query(
        `SELECT * FROM "${table}" WHERE updated_at > $1`,
        [lastSyncTimes[table]]
      );

      if (rows.length === 0) {
        console.log(`ℹ  ${table} jadvalida yangi yozuv yo'q.`);
        continue;
      }

      const schema = {};
      const sample = rows[0];
      for (const key of Object.keys(sample)) {
        if (key === 'geo') schema[key] = 'geo_point';
        else if (key === 'agent') schema[key] = 'agent';
        else schema[key] = 'other';
      }

      for (const row of rows) {
        const normalizedRow = normalizeRow(row, schema);
        await osClient.index({
          index: table,
          id: row.id,
          body: normalizedRow,
        });
      }

      console.log(`✅ ${rows.length} yozuv ${table} dan OpenSearch ga yuklandi.`);

      lastSyncTimes[table] = new Date();
    }

    // 6️⃣ Faylga saqlash
    saveLastSyncTimes(lastSyncTimes);

  } catch (err) {
    console.error('❌ Incremental sync xatolik:', err);
  }
}
incrementalSync()