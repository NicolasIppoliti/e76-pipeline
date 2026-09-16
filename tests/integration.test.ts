import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { database, setup } from '../src/db.js';
import { loadConfig, loadManifest } from '../src/config.js';
import { ingestBytes } from '../src/ingest.js';

process.loadEnvFile?.('.env');
const adminUrl = process.env['TEST_DATABASE_URL'];
if (!adminUrl || !new URL(adminUrl).pathname.endsWith('_test')) throw new Error('TEST_DATABASE_URL must explicitly target a dedicated database ending in _test');
const config = loadConfig();
const manifest = loadManifest('fixtures', config);
const admin = database(adminUrl);
const urls: Record<string, string> = {};
for (const tenant of config.tenants) {
  const url = new URL(adminUrl); url.username = tenant.role; url.password = `local-${tenant.id}-only`;
  urls[tenant.databaseUrlEnv] = url.href;
}
const northwind = config.tenants[0]!;
const pool = database(urls[northwind.databaseUrlEnv]!);

test.before(async () => {
  await admin.query('DROP SCHEMA IF EXISTS reporting, staging, pipeline CASCADE');
  await setup(admin, config, manifest, urls);
});
test.after(async () => { await pool.end(); await admin.end(); });

test('a restricted tenant connection atomically ingests and exactly replays an order file', async () => {
  const batch = manifest.find(b => b.tenant === 'northwind' && b.source === 'orders' && b.batch === 1)!;
  const bytes = readFileSync(`fixtures/${batch.path}`);
  const first = await ingestBytes(pool, northwind, batch, bytes);
  const second = await ingestBytes(pool, northwind, batch, bytes);
  assert.equal(first.status, 'processed');
  assert.equal(second.status, 'replayed');
  const result = await pool.query('SELECT count(*)::int AS n FROM staging.orders');
  assert.equal(result.rows[0].n, first.rows);
  const files = await pool.query('SELECT count(*)::int AS n FROM pipeline.files');
  assert.equal(files.rows[0].n, 1);
});

test('overlapping order exports retain 680 unique orders and exact gross', async () => {
  for (const batch of manifest.filter(b => b.tenant === 'northwind' && b.source === 'orders')) {
    await ingestBytes(pool, northwind, batch, readFileSync(`fixtures/${batch.path}`));
  }
  const result = await pool.query('SELECT count(*)::int AS n, sum(gross)::text AS gross FROM staging.orders');
  assert.deepEqual(result.rows[0], { n: 680, gross: '79303.38' });
  const raw = await pool.query("SELECT count(*)::int AS n FROM pipeline.raw_records WHERE source='orders'");
  assert.equal(raw.rows[0].n, 694);
});

test('adapters publish both other sources without merging tenant business keys', async () => {
  const lumen = config.tenants[1]!;
  const other = database(urls[lumen.databaseUrlEnv]!);
  try {
    for (const tenant of config.tenants) {
      for (const batch of manifest.filter(b => b.tenant === tenant.id && b.source !== 'orders')) {
        if (batch.tenant === 'lumen' && batch.source === 'ad_spend' && batch.batch === 3) continue;
        await ingestBytes(tenant.id === northwind.id ? pool : other, tenant, batch, readFileSync(`fixtures/${batch.path}`));
      }
    }
    assert.equal((await admin.query('SELECT count(*)::int AS n FROM staging.ad_spend')).rows[0].n, 162);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM staging.email_events')).rows[0].n, 1507);
    assert.equal((await other.query('SELECT count(*)::int AS n FROM staging.email_events')).rows[0].n, 1473);
  } finally { await other.end(); }
});

test('daily reporting exposes the missing middle batch instead of zero ad spend', async () => {
  const lumen = config.tenants[1]!;
  const other = database(urls[lumen.databaseUrlEnv]!);
  try {
    const status = await other.query("SELECT batch, status FROM reporting.batch_status WHERE source='ad_spend' ORDER BY batch");
    assert.deepEqual(status.rows, [1, 2, 3, 4, 5].map(batch => ({ batch, status: batch === 3 ? 'pending' : 'processed' })));
    const gap = await other.query("SELECT date, is_complete, observed_records FROM reporting.daily_coverage WHERE source='ad_spend' AND date BETWEEN '2026-01-18' AND '2026-01-23' ORDER BY date");
    assert.equal(gap.rowCount, 6);
    assert.ok(gap.rows.every(row => row.is_complete === false && row.observed_records === null));
    const currencies = await other.query('SELECT DISTINCT currency FROM reporting.daily_ad_spend ORDER BY currency');
    assert.deepEqual(currencies.rows, [{ currency: 'UNKNOWN' }, { currency: 'USD' }]);
  } finally { await other.end(); }
});
