import { existsSync, readFileSync } from 'node:fs';
import { parseArgs } from 'node:util';
import { loadConfig, loadManifest, safeFixturePath, SOURCES, type Batch, type Tenant } from './config.js';
import { assertTenantManifest, assertWorkerIdentity, database, setup } from './db.js';
import { ingestBytes } from './ingest.js';

const HELP = `Batch pipeline (PostgreSQL 15+, Node.js 22+)
  npm run db:setup
  npm run ingest -- --all
  npm run ingest -- --tenant northwind [--source orders --batch 3] [--reverse]
  npm run status -- --all
  npm run report -- --tenant northwind
Options: --config <path> (config/tenants.json), --fixtures <directory> (fixtures)
Exit codes: 0 complete; 1 invalid configuration/processing failure; 2 missing or unprocessed batches.
Reports contain current observed values plus separate manifest coverage; they never invent FX or missing spend.
`;
const emit = (value: unknown) => console.log(JSON.stringify(value));
const requiredEnv = (key: string): string => { const value = process.env[key]; if (!value) throw new Error(`Missing ${key}`); return value; };

async function main(): Promise<void> {
  const { values, positionals } = parseArgs({ options: {
    help: { type: 'boolean' }, all: { type: 'boolean' }, tenant: { type: 'string' },
    source: { type: 'string' }, batch: { type: 'string' }, reverse: { type: 'boolean' },
    config: { type: 'string' }, fixtures: { type: 'string' },
  }, allowPositionals: true, strict: true });
  if (values.help) { console.log(HELP); return; }
  const command = positionals[0];
  if (positionals.length !== 1 || !['setup', 'ingest', 'status', 'report'].includes(command ?? '')) throw new Error(`Choose a supported command.\n${HELP}`);
  if (existsSync('.env')) process.loadEnvFile('.env');
  const config = loadConfig(values.config);
  const root = values.fixtures ?? 'fixtures';
  const manifest = loadManifest(root, config);
  if (command === 'setup') {
    if (values.tenant || values.all || values.source || values.batch || values.reverse) throw new Error('setup provisions the whole configuration; filters are not supported');
    const admin = database(requiredEnv('DATABASE_URL'));
    try {
      const urls = Object.fromEntries(config.tenants.map(t => [t.databaseUrlEnv, requiredEnv(t.databaseUrlEnv)]));
      await setup(admin, config, manifest, urls);
      emit({ status: 'ready', tenants: config.tenants.map(t => t.id), expectedBatches: manifest.length });
    } finally { await admin.end(); }
    return;
  }
  if (Boolean(values.all) === Boolean(values.tenant)) throw new Error('Specify exactly one of --all or --tenant <id>');
  const tenants = config.tenants.filter(t => values.all || t.id === values.tenant);
  if (!tenants.length) throw new Error(`Unknown tenant: ${values.tenant}`);
  if (values.source && !Object.values(SOURCES).some(s => s === values.source)) throw new Error(`Unknown source: ${values.source}`);
  if (values.batch && (!/^\d+$/.test(values.batch) || !Number.isSafeInteger(Number(values.batch)) || Number(values.batch) < 1)) throw new Error('--batch must be a positive integer');
  if (values.batch && !values.source) throw new Error('--batch requires --source');
  if (values.reverse && command !== 'ingest') throw new Error('--reverse is only supported by ingest');
  if (command === 'report' && (values.all || values.source || values.batch)) throw new Error('report requires one --tenant and does not accept batch/source filters');
  for (const tenant of tenants) {
    let batches = manifest.filter(b => b.tenant === tenant.id && (!values.source || b.source === values.source) && (!values.batch || b.batch === Number(values.batch)));
    if (!batches.length) throw new Error(`No expected batches match ${tenant.id}`);
    if (values.reverse) batches = batches.reverse();
    await runTenant(command!, tenant, batches, root, manifest);
  }
}

async function runTenant(command: string, tenant: Tenant, batches: Batch[], root: string, manifest: Batch[]): Promise<void> {
  const worker = database(requiredEnv(tenant.databaseUrlEnv));
  try {
    await assertWorkerIdentity(worker, tenant);
    await assertTenantManifest(worker, tenant, manifest);
    if (command === 'ingest') {
      let missing = false;
      for (const batch of batches) {
        const path = safeFixturePath(root, batch.path);
        if (!existsSync(path)) {
          emit({ tenant: tenant.id, source: batch.source, batch: batch.batch, status: 'missing', path: batch.path, coversFrom: batch.covers_from, coversTo: batch.covers_to });
          missing = true;
          continue;
        }
        const result = await ingestBytes(worker, tenant, batch, readFileSync(path));
        emit({ tenant: tenant.id, source: batch.source, batch: batch.batch, ...result });
      }
      if (missing && process.exitCode !== 1) process.exitCode = 2;
    } else if (command === 'status') {
      const result = await worker.query('SELECT * FROM reporting.batch_status ORDER BY source,batch');
      const selected = result.rows.filter(row => batches.some(b => b.source === row.source && b.batch === row.batch));
      if (selected.length !== batches.length) throw new Error('Manifest batches are not provisioned; run db:setup first');
      const rows = selected.map(row => ({ ...row, file_present: existsSync(safeFixturePath(root, row.path as string)) }));
      emit({ tenant: tenant.id, expected: rows.length, processed: rows.filter(r => r.status === 'processed').length, batches: rows });
      if (rows.some(r => r.latest_attempt_status === 'failed')) process.exitCode = 1;
      else if (rows.some(r => r.status !== 'processed') && process.exitCode !== 1) process.exitCode = 2;
    } else {
      const client = await worker.connect();
      try {
        await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
        const orders = await client.query('SELECT * FROM reporting.daily_orders ORDER BY date,channel,currency');
        const email = await client.query('SELECT * FROM reporting.daily_email ORDER BY date,campaign_id,event_type');
        const adSpend = await client.query('SELECT * FROM reporting.daily_ad_spend ORDER BY date,platform,currency');
        const coverage = await client.query('SELECT * FROM reporting.daily_coverage ORDER BY date,source');
        await client.query('COMMIT');
        emit({ tenant: tenant.id, timezone: 'UTC', orders: orders.rows, email: email.rows, adSpend: adSpend.rows, coverage: coverage.rows });
        if (coverage.rows.some(row => !row.is_complete)) process.exitCode = 2;
      } catch (error) { await client.query('ROLLBACK'); throw error; }
      finally { client.release(); }
    }
  } finally { await worker.end(); }
}

main().catch((error: unknown) => {
  console.error(JSON.stringify({ status: 'failed', message: error instanceof Error ? error.message : 'Unknown failure' }));
  process.exitCode = 1;
});
