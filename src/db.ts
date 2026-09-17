import { createHash } from 'node:crypto';
import { readFileSync, readdirSync } from 'node:fs';
import { resolve } from 'node:path';
import pg from 'pg';
import type { Batch, Config, Tenant } from './config.js';

export function database(url: string): pg.Pool {
  return new pg.Pool({ connectionString: url, max: 3, connectionTimeoutMillis: 5000,
    options: '-c timezone=UTC -c statement_timeout=30000 -c lock_timeout=10000' });
}
export function sha256(bytes: Buffer | string): string { return createHash('sha256').update(bytes).digest('hex'); }
export function configHash(tenant: Tenant): string { return sha256(JSON.stringify(tenant)); }
export async function assertWorkerIdentity(pool: pg.Pool, tenant: Tenant): Promise<void> {
  const result = await pool.query('SELECT t.tenant_id,t.config_hash,r.rolsuper,r.rolbypassrls,r.rolcreaterole FROM pipeline.tenants t JOIN pg_roles r ON r.rolname=session_user WHERE t.role_name=session_user');
  const identity = result.rows[0];
  if (result.rowCount !== 1 || identity.tenant_id !== tenant.id || identity.config_hash !== configHash(tenant) || identity.rolsuper || identity.rolbypassrls || identity.rolcreaterole) {
    throw new Error('Worker identity or config does not match a restricted provisioned tenant');
  }
}

/** Compare the complete tenant manifest before applying command filters. */
export async function assertTenantManifest(pool: pg.Pool, tenant: Tenant, batches: Batch[]): Promise<void> {
  const persisted = await pool.query<Batch>('SELECT tenant_id AS tenant,source,batch,path,covers_from::text,covers_to::text FROM pipeline.expected_batches WHERE tenant_id=$1', [tenant.id]);
  const expected = batches.filter(batch => batch.tenant === tenant.id);
  if (persisted.rows.length !== expected.length || persisted.rows.some(row => !expected.some(batch =>
    batch.source === row.source && batch.batch === row.batch && batch.path === row.path &&
    batch.covers_from === row.covers_from && batch.covers_to === row.covers_to))) {
    throw new Error(`Manifest divergence for tenant ${tenant.id}; restore persisted entries and run db:setup for additions`);
  }
}

const identifier = (value: string) => `"${value.replaceAll('"', '""')}"`;
const literal = (value: string) => `'${value.replaceAll("'", "''")}'`;

/** Trusted provisioning boundary. Runtime ingestion never receives this pool. */
export async function setup(pool: pg.Pool, config: Config, batches: Batch[], urls: Record<string, string>): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query("SELECT pg_advisory_xact_lock(hashtext('e76-pipeline:setup'))");
    const current = await client.query<{ database: string; version: string }>('SELECT current_database() AS database, current_setting(\'server_version_num\') AS version');
    if (Number(current.rows[0]!.version) < 150000) throw new Error('PostgreSQL 15 or later is required');
    for (const file of readdirSync(resolve('migrations')).filter(f => /^\d+.*\.sql$/.test(f)).sort()) {
      const sql = readFileSync(resolve('migrations', file), 'utf8');
      const exists = await client.query("SELECT to_regclass('pipeline.schema_migrations') AS name");
      const applied = exists.rows[0].name ? await client.query('SELECT checksum FROM pipeline.schema_migrations WHERE version=$1', [file]) : null;
      if (applied?.rowCount) {
        if (applied.rows[0].checksum !== sha256(sql)) throw new Error(`Applied migration changed: ${file}`);
      } else {
        await client.query(sql);
        await client.query('INSERT INTO pipeline.schema_migrations(version, checksum) VALUES($1,$2)', [file, sha256(sql)]);
      }
    }
    for (const tenant of config.tenants) {
      const connectionString = urls[tenant.databaseUrlEnv];
      if (!connectionString) throw new Error(`Missing ${tenant.databaseUrlEnv}`);
      const url = new URL(connectionString);
      if (decodeURIComponent(url.username) !== tenant.role || decodeURIComponent(url.pathname.slice(1)) !== current.rows[0]!.database || !url.password) {
        throw new Error(`${tenant.databaseUrlEnv} must specify configured role, current database and a nonempty password`);
      }
      const existingRole = await client.query('SELECT rolsuper, rolcreaterole, rolcreatedb, rolbypassrls, rolreplication FROM pg_roles WHERE rolname=$1', [tenant.role]);
      if (existingRole.rowCount) {
        if (Object.values(existingRole.rows[0] as Record<string, boolean>).some(Boolean)) throw new Error(`Refusing privileged tenant role: ${tenant.role}`);
        const memberships = await client.query('SELECT 1 FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=$1)', [tenant.role]);
        if (memberships.rowCount) throw new Error(`Refusing tenant role with memberships: ${tenant.role}`);
      } else {
        await client.query(`CREATE ROLE ${identifier(tenant.role)} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS NOREPLICATION PASSWORD ${literal(decodeURIComponent(url.password))}`);
      }
      // Existing role passwords are not silently rotated by an idempotent setup.
      const existingTenant = await client.query('SELECT role_name, config_hash FROM pipeline.tenants WHERE tenant_id=$1', [tenant.id]);
      if (existingTenant.rowCount) {
        if (existingTenant.rows[0].role_name !== tenant.role || existingTenant.rows[0].config_hash !== configHash(tenant)) throw new Error(`Tenant config changed: ${tenant.id}; an explicit data migration is required`);
      } else {
        await client.query('INSERT INTO pipeline.tenants VALUES($1,$2,$3)', [tenant.id, tenant.role, configHash(tenant)]);
      }
      const role = identifier(tenant.role);
      await client.query(`GRANT USAGE ON SCHEMA pipeline, staging, reporting TO ${role}`);
      await client.query(`GRANT SELECT ON pipeline.tenants, pipeline.expected_batches TO ${role}`);
      await client.query(`GRANT SELECT, INSERT ON pipeline.files, pipeline.raw_records, pipeline.batch_receipts, pipeline.ingest_attempts, staging.orders, staging.email_events, staging.ad_spend TO ${role}`);
      await client.query(`GRANT UPDATE(status, finished_at, error_code, error_message) ON pipeline.ingest_attempts TO ${role}`);
      await client.query(`GRANT SELECT ON ALL TABLES IN SCHEMA reporting TO ${role}`);
    }
    const persisted = await client.query<{ tenant_id: string; source: string; batch: number }>('SELECT tenant_id,source,batch FROM pipeline.expected_batches');
    for (const row of persisted.rows) {
      if (!batches.some(batch => batch.tenant === row.tenant_id && batch.source === row.source && batch.batch === row.batch)) {
        throw new Error(`Manifest batch removed: ${row.tenant_id}/${row.source}/${row.batch}; persisted expectations cannot be deleted`);
      }
    }
    for (const batch of batches) {
      const params = [batch.tenant, batch.source, batch.batch, batch.path, batch.covers_from, batch.covers_to];
      const result = await client.query('INSERT INTO pipeline.expected_batches VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING batch', params);
      if (!result.rowCount) {
        const same = await client.query('SELECT 1 FROM pipeline.expected_batches WHERE tenant_id=$1 AND source=$2 AND batch=$3 AND path=$4 AND covers_from=$5 AND covers_to=$6', params);
        if (!same.rowCount) throw new Error(`Existing manifest batch changed: ${batch.tenant}/${batch.source}/${batch.batch}`);
      }
    }
    await client.query('COMMIT');
  } catch (error) { await client.query('ROLLBACK'); throw error; }
  finally { client.release(); }
}
