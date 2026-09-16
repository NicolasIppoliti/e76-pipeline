// Test-only process: real OS termination, with the pause installed by the test database.
import { readFileSync } from 'node:fs';
import { database } from '../src/db.js';
import { loadConfig, loadManifest } from '../src/config.js';
import { ingestBytes } from '../src/ingest.js';
const config = loadConfig();
const tenant = config.tenants[0]!;
const batch = loadManifest('fixtures', config).find(b => b.tenant === tenant.id && b.source === 'orders' && b.batch === 3)!;
const pool = database(process.env['TEST_WORKER_URL']!);
try { await ingestBytes(pool, tenant, batch, readFileSync(`fixtures/${batch.path}`)); }
finally { await pool.end(); }
