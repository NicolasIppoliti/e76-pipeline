import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';
import pg from 'pg';
import { database, setup } from '../src/db.js';
import { loadConfig, loadManifest } from '../src/config.js';
import { ingestBytes } from '../src/ingest.js';

if (existsSync('.env')) process.loadEnvFile('.env');
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
test.beforeEach(async () => { await admin.query('TRUNCATE pipeline.files, pipeline.ingest_attempts CASCADE'); });
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
    for (const batch of manifest.filter(b => b.tenant === 'lumen' && b.source === 'ad_spend' && b.batch !== 3)) {
      await ingestBytes(other, lumen, batch, readFileSync(`fixtures/${batch.path}`));
    }
    const status = await other.query("SELECT batch, status FROM reporting.batch_status WHERE source='ad_spend' ORDER BY batch");
    assert.deepEqual(status.rows, [1, 2, 3, 4, 5].map(batch => ({ batch, status: batch === 3 ? 'pending' : 'processed' })));
    const gap = await other.query("SELECT date, is_complete, observed_records FROM reporting.daily_coverage WHERE source='ad_spend' AND date BETWEEN '2026-01-18' AND '2026-01-23' ORDER BY date");
    assert.equal(gap.rowCount, 6);
    assert.ok(gap.rows.every(row => row.is_complete === false && row.observed_records === null));
    const currencies = await other.query('SELECT DISTINCT currency FROM reporting.daily_ad_spend ORDER BY currency');
    assert.deepEqual(currencies.rows, [{ currency: 'UNKNOWN' }, { currency: 'USD' }]);
  } finally { await other.end(); }
});

const fixtureBatch = (source: string, batch: number, tenant = 'northwind') => manifest.find(b => b.tenant === tenant && b.source === source && b.batch === batch)!;
const fixtureBytes = (source: string, batch: number, tenant = 'northwind') => readFileSync(`fixtures/${fixtureBatch(source, batch, tenant).path}`);

test('late events correct prior daily results, including reverse-order delivery', async () => {
  for (const batch of [1, 2, 3, 4]) await ingestBytes(pool, northwind, fixtureBatch('email_events', batch), fixtureBytes('email_events', batch));
  const historic = async () => (await pool.query("SELECT sum(event_count)::int AS n FROM reporting.daily_email WHERE date BETWEEN '2026-01-12' AND '2026-01-17'")).rows[0].n;
  assert.equal(await historic(), 297);
  await ingestBytes(pool, northwind, fixtureBatch('email_events', 5), fixtureBytes('email_events', 5));
  assert.equal(await historic(), 321);
  const forward = await pool.query('SELECT * FROM reporting.daily_email ORDER BY date,campaign_id,event_type');
  await admin.query('TRUNCATE pipeline.files, pipeline.ingest_attempts CASCADE');
  for (const batch of [5, 4, 3, 2, 1]) await ingestBytes(pool, northwind, fixtureBatch('email_events', batch), fixtureBytes('email_events', batch));
  const reverse = await pool.query('SELECT * FROM reporting.daily_email ORDER BY date,campaign_id,event_type');
  assert.deepEqual(reverse.rows, forward.rows);
});

test('same business key with changed payload rolls back all rows and records a visible failure', async () => {
  const original = fixtureBytes('orders', 1);
  await ingestBytes(pool, northwind, fixtureBatch('orders', 1), original);
  const lines = original.toString().split('\n');
  const existing = lines[1]!.split(',');
  const novel = [...existing]; novel[0] = 'NEW-BEFORE-CONFLICT';
  existing[3] = existing[3] === '1.00' ? '2.00' : '1.00';
  const corrupt = Buffer.from(`${lines[0]}\n${novel.join(',')}\n${existing.join(',')}\n`);
  await assert.rejects(ingestBytes(pool, northwind, fixtureBatch('orders', 2), corrupt), /RECORD_CONFLICT/);
  assert.equal((await pool.query("SELECT count(*)::int AS n FROM staging.orders WHERE order_id='NEW-BEFORE-CONFLICT'")).rows[0].n, 0);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM pipeline.files')).rows[0].n, 1);
  assert.equal((await pool.query("SELECT count(*)::int AS n FROM pipeline.batch_receipts WHERE batch=2")).rows[0].n, 0);
  const failure = await pool.query("SELECT status,error_message FROM pipeline.ingest_attempts WHERE batch=2");
  assert.equal(failure.rows[0].status, 'failed');
  assert.match(failure.rows[0].error_message, /RECORD_CONFLICT/);
  await assert.rejects(ingestBytes(pool, northwind, fixtureBatch('orders', 1), corrupt), /BATCH_CONFLICT/);
});

test('identical bytes cannot close another expected batch', async () => {
  const bytes = fixtureBytes('orders', 1);
  await ingestBytes(pool, northwind, fixtureBatch('orders', 1), bytes);
  await assert.rejects(ingestBytes(pool, northwind, fixtureBatch('orders', 2), bytes), /DUPLICATE_FILE/);
  const status = await pool.query("SELECT status,latest_attempt_status FROM reporting.batch_status WHERE source='orders' AND batch=2");
  assert.deepEqual(status.rows, [{ status: 'pending', latest_attempt_status: 'failed' }]);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM pipeline.files')).rows[0].n, 1);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM pipeline.batch_receipts')).rows[0].n, 1);
});

test('concurrent delivery serializes safely without duplicates', async () => {
  const results = await Promise.all([1, 2].map(() => ingestBytes(pool, northwind, fixtureBatch('orders', 1), fixtureBytes('orders', 1))));
  assert.deepEqual(results.map(r => r.status).sort(), ['processed', 'replayed']);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM pipeline.files')).rows[0].n, 1);
});

test('ingestion waits beyond session timeouts for tenant serialization and restores them', async () => {
  const worker = new pg.Pool({ connectionString: urls[northwind.databaseUrlEnv]!, max: 2,
    application_name: 'e76-lock-test', options: '-c lock_timeout=50 -c statement_timeout=150' });
  const holder = await admin.connect();
  try {
    await holder.query('BEGIN');
    await holder.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`e76-pipeline:${northwind.id}`]);
    const pending = ingestBytes(worker, northwind, fixtureBatch('orders', 1), fixtureBytes('orders', 1)).catch((error: unknown) => error);
    try {
      let waiting = false;
      for (let attempt = 0; attempt < 100; attempt++) {
        const state = await admin.query("SELECT 1 FROM pg_stat_activity WHERE application_name='e76-lock-test' AND wait_event='advisory'");
        if (state.rowCount) { waiting = true; break; }
        await delay(5);
      }
      assert.ok(waiting, 'ingestion must reach the held serialization lock');
      await delay(200);
    } finally { await holder.query('ROLLBACK'); }
    const result = await pending;
    if (result instanceof Error) throw result;
    assert.equal((result as { status: string }).status, 'processed');
    assert.deepEqual((await worker.query("SELECT status,latest_attempt_status FROM reporting.batch_status WHERE source='orders' AND batch=1")).rows, [{ status: 'processed', latest_attempt_status: 'processed' }]);
    assert.equal((await worker.query('SHOW lock_timeout')).rows[0].lock_timeout, '50ms');
    assert.equal((await worker.query('SHOW statement_timeout')).rows[0].statement_timeout, '150ms');
  } finally { await holder.query('ROLLBACK'); holder.release(); await worker.end(); }
});

test('tenant login policies isolate raw, canonical, control and reporting data without app filters', async () => {
  const lumen = config.tenants[1]!;
  const other = database(urls[lumen.databaseUrlEnv]!);
  try {
    const nwBatch = fixtureBatch('ad_spend', 1);
    const luBatch = fixtureBatch('ad_spend', 1, 'lumen');
    await ingestBytes(pool, northwind, nwBatch, fixtureBytes('ad_spend', 1));
    await ingestBytes(other, lumen, luBatch, fixtureBytes('ad_spend', 1, 'lumen'));
    const roles = await pool.query('SELECT rolsuper,rolbypassrls,rolcreaterole FROM pg_roles WHERE rolname=session_user');
    assert.deepEqual(roles.rows[0], { rolsuper: false, rolbypassrls: false, rolcreaterole: false });
    const tables = ['pipeline.tenants', 'pipeline.expected_batches', 'pipeline.files', 'pipeline.raw_records', 'pipeline.batch_receipts', 'pipeline.ingest_attempts', 'staging.ad_spend', 'reporting.daily_ad_spend', 'reporting.daily_coverage', 'reporting.batch_status'];
    for (const table of tables) {
      const visible = await pool.query(`SELECT DISTINCT tenant_id FROM ${table}`);
      assert.deepEqual(visible.rows, [{ tenant_id: 'northwind' }], table);
      const forbidden = await pool.query(`SELECT * FROM ${table} WHERE tenant_id='lumen'`);
      assert.equal(forbidden.rowCount, 0, table);
    }
    await pool.query("SET app.tenant_id='lumen'");
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM staging.ad_spend WHERE tenant_id='lumen'")).rows[0].n, 0);
    await assert.rejects(pool.query('SET ROLE pipeline_lumen'), /permission denied/);
    await assert.rejects(pool.query("SET SESSION AUTHORIZATION pipeline_lumen"), /permission denied/);
    await assert.rejects(pool.query("UPDATE pipeline.tenants SET tenant_id='lumen' WHERE tenant_id='northwind'"), /permission denied/);
    await assert.rejects(pool.query("UPDATE staging.ad_spend SET tenant_id='lumen'"), /permission denied/);
    await assert.rejects(pool.query('TRUNCATE staging.ad_spend'), /permission denied/);
    await assert.rejects(pool.query('ALTER TABLE staging.ad_spend DISABLE ROW LEVEL SECURITY'), /must be owner/);
    await assert.rejects(pool.query("INSERT INTO pipeline.ingest_attempts(attempt_id,tenant_id,source,batch,status) VALUES(gen_random_uuid(),'lumen','ad_spend',1,'running')"), /row-level security/);
    const nw = await pool.query("SELECT amount::text FROM staging.ad_spend WHERE date='2026-01-06' AND campaign_id='cmp_100'");
    const lu = await other.query("SELECT amount::text FROM staging.ad_spend WHERE date='2026-01-06' AND campaign_id='cmp_100'");
    assert.notEqual(nw.rows[0].amount, lu.rows[0].amount);
    assert.equal((await admin.query('SELECT count(*)::int AS n FROM staging.ad_spend')).rows[0].n, 36);
  } finally { await other.end(); }
});

test('setup rejects deleting a persisted expected batch without hiding its coverage', async () => {
  const removed = fixtureBatch('ad_spend', 3, 'lumen');
  await assert.rejects(setup(admin, config, manifest.filter(batch => batch !== removed), urls), /Manifest.*(removed|divergence)/);
  const status = await admin.query("SELECT status FROM reporting.batch_status WHERE tenant_id='lumen' AND source='ad_spend' AND batch=3");
  assert.deepEqual(status.rows, [{ status: 'pending' }]);
});

test('a third tenant is provisioned from configuration and reuses all three source models', async () => {
  const third = { ...northwind, id: 'third', role: 'pipeline_third', databaseUrlEnv: 'THIRD_DATABASE_URL',
    channels: { Affiliate: 'affiliate' }, platforms: { 'Partner Ads': 'meta' }, eventTypes: { SEEN: 'open' } };
  const thirdUrl = new URL(adminUrl!); thirdUrl.username = third.role; thirdUrl.password = 'local-third-only';
  const thirdBatches = ['orders', 'email_events', 'ad_spend'].map(source => ({ ...fixtureBatch(source, 1), tenant: 'third', path: `third/${source}/batch_01` }));
  await setup(admin, { tenants: [...config.tenants, third] }, [...manifest, ...thirdBatches], { ...urls, THIRD_DATABASE_URL: thirdUrl.href });
  const worker = database(thirdUrl.href);
  try {
    for (const source of ['orders', 'email_events', 'ad_spend']) await ingestBytes(pool, northwind, fixtureBatch(source, 1), fixtureBytes(source, 1));
    const orderFields = fixtureBytes('orders', 1).toString().split('\n')[1]!.split(',');
    const order = `order_id,created_at,channel,gross,currency,customer_email\n${orderFields[0]},2026-01-06T00:00:00Z,Affiliate,999.99,USD,a@example.invalid\n`;
    const originalEmail = JSON.parse(fixtureBytes('email_events', 1).toString().split('\n')[0]!);
    const email = JSON.stringify({ ...originalEmail, type: 'SEEN' });
    const ads = 'date,campaign_id,platform,cost_usd\n2026-01-06,cmp_100,Partner Ads,77.77\n';
    for (const [index, body] of [order, email, ads].entries()) await ingestBytes(worker, third, thirdBatches[index]!, Buffer.from(body));
    assert.deepEqual((await worker.query('SELECT tenant_id,channel,gross::text FROM staging.orders')).rows, [{ tenant_id: 'third', channel: 'affiliate', gross: '999.99' }]);
    assert.equal((await worker.query('SELECT count(*)::int AS n FROM staging.email_events')).rows[0].n, 1);
    assert.equal((await worker.query('SELECT count(*)::int AS n FROM staging.ad_spend')).rows[0].n, 1);
    assert.equal((await worker.query("SELECT count(*)::int AS n FROM reporting.daily_orders WHERE tenant_id='northwind'")).rows[0].n, 0);
  } finally { await worker.end(); }
});

test('SIGKILL one third through a file rolls back partial work; full retry equals a clean run', { timeout: 30000 }, async () => {
  for (const batch of [1, 2]) await ingestBytes(pool, northwind, fixtureBatch('orders', batch), fixtureBytes('orders', batch));
  const before = await pool.query('SELECT count(*)::int AS n FROM staging.orders');
  const lines = fixtureBytes('orders', 3).toString().trim().split('\n').slice(1);
  const stopKey = lines[Math.floor(lines.length / 3)]!.split(',')[0]!.replaceAll("'", "''");
  await admin.query(`CREATE FUNCTION pipeline.test_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.order_id='${stopKey}' THEN PERFORM pg_sleep(3); END IF; RETURN NEW; END $$; CREATE TRIGGER test_pause BEFORE INSERT ON staging.orders FOR EACH ROW EXECUTE FUNCTION pipeline.test_pause()`);
  const crashUrl = new URL(urls[northwind.databaseUrlEnv]!); crashUrl.searchParams.set('application_name', 'e76-crash-test');
  const child = spawn(process.execPath, ['--import', 'tsx', 'tests/crash-child.ts'], { env: { ...process.env, TEST_WORKER_URL: crashUrl.href }, stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = ''; child.stderr.on('data', chunk => { stderr += String(chunk); });
  const exited = new Promise<NodeJS.Signals | null>((resolve, reject) => { child.once('error', reject); child.once('exit', (_code, signal) => resolve(signal)); });
  try {
    let paused = false;
    for (let attempt = 0; attempt < 100; attempt++) {
      const state = await admin.query("SELECT 1 FROM pg_stat_activity WHERE application_name='e76-crash-test' AND wait_event='PgSleep'");
      if (state.rowCount) { paused = true; break; }
      if (child.exitCode !== null) break;
      await delay(30);
    }
    assert.ok(paused, `child did not reach partial-file database pause: ${stderr}`);
    assert.equal(child.kill('SIGKILL'), true);
    assert.equal(await exited, 'SIGKILL');
    assert.deepEqual((await pool.query('SELECT count(*)::int AS n FROM staging.orders')).rows, before.rows);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM pipeline.files')).rows[0].n, 2);
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM pipeline.batch_receipts')).rows[0].n, 2);
    assert.equal((await pool.query("SELECT count(*)::int AS n FROM pipeline.raw_records WHERE file_hash=$1", [(await import('../src/db.js')).sha256(fixtureBytes('orders', 3))])).rows[0].n, 0);
  } finally {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await admin.query('DROP TRIGGER test_pause ON staging.orders; DROP FUNCTION pipeline.test_pause()');
  }
  const ingestAll = async () => {
    for (const tenant of config.tenants) {
      const worker = tenant.id === northwind.id ? pool : database(urls[tenant.databaseUrlEnv]!);
      try {
        for (const batch of manifest.filter(b => b.tenant === tenant.id)) {
          const path = `fixtures/${batch.path}`;
          if (existsSync(path)) await ingestBytes(worker, tenant, batch, readFileSync(path));
        }
      } finally { if (worker !== pool) await worker.end(); }
    }
  };
  const snapshot = async () => {
    const result: Record<string, unknown> = {};
    for (const table of ['orders', 'email_events', 'ad_spend']) result[table] = (await admin.query(`SELECT tenant_id,canonical_payload FROM staging.${table} ORDER BY tenant_id,canonical_payload::text`)).rows;
    return result;
  };
  await ingestAll();
  const retried = await snapshot();
  assert.equal((await pool.query("SELECT count(*)::int AS n FROM pipeline.ingest_attempts WHERE status='abandoned'")).rows[0].n, 1);
  const totals = await admin.query('SELECT count(*)::int AS files, sum(row_count)::int AS rows FROM pipeline.files');
  assert.deepEqual(totals.rows[0], { files: 29, rows: 4498 });
  assert.equal((await admin.query('SELECT count(*)::int AS n FROM staging.orders')).rows[0].n, 1342);
  await admin.query('TRUNCATE pipeline.files, pipeline.ingest_attempts CASCADE');
  await ingestAll();
  assert.deepEqual(await snapshot(), retried);
});

test('invalid source schema fails visibly without a partial file or successful receipt', async () => {
  await assert.rejects(ingestBytes(pool, northwind, fixtureBatch('ad_spend', 1), Buffer.from('date,campaign_id,platform,mystery\n2026-01-06,c,facebook,1.00\n')), /Unsupported ad_spend schema/);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM pipeline.raw_records')).rows[0].n, 0);
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM pipeline.batch_receipts')).rows[0].n, 0);
  assert.equal((await pool.query('SELECT status FROM pipeline.ingest_attempts')).rows[0].status, 'failed');
});

test('runtime rejects administrative credentials before creating tenant metadata', async () => {
  await assert.rejects(ingestBytes(admin, northwind, fixtureBatch('orders', 1), fixtureBytes('orders', 1)), /Worker identity/);
  assert.equal((await admin.query('SELECT count(*)::int AS n FROM pipeline.ingest_attempts')).rows[0].n, 0);
});

test('runtime commands reject a deleted manifest entry even outside their selected source and batch', async () => {
  // Simulate an established expectation that the current on-disk manifest no longer contains.
  await admin.query("INSERT INTO pipeline.expected_batches SELECT tenant_id,source,99,'northwind/ad_spend/batch_99.csv',covers_from,covers_to FROM pipeline.expected_batches WHERE tenant_id='northwind' AND source='ad_spend' AND batch=1");
  for (const command of ['status', 'ingest', 'report']) {
    const filters = command === 'report' ? [] : ['--source', 'orders', '--batch', '1'];
    const child = spawn(process.execPath, ['--import', 'tsx', 'src/cli.ts', command, '--tenant', 'northwind', ...filters], { env: { ...process.env, ...urls }, stdio: ['ignore', 'pipe', 'pipe'] });
    let stderr = ''; let stdout = '';
    child.stderr.on('data', chunk => { stderr += String(chunk); });
    child.stdout.on('data', chunk => { stdout += String(chunk); });
    const code = await new Promise<number | null>((resolve, reject) => { child.once('error', reject); child.once('close', resolve); });
    assert.equal(code, 1, `${command}: ${stdout} ${stderr}`);
    assert.match(stderr, /Manifest divergence/, command);
    assert.equal(stdout, '', command);
  }
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM pipeline.ingest_attempts')).rows[0].n, 0);
  assert.deepEqual((await pool.query("SELECT status FROM reporting.batch_status WHERE source='ad_spend' AND batch=99")).rows, [{ status: 'pending' }]);
});
