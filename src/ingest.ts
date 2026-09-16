import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import { type Batch, type Tenant } from './config.js';
import { parseBatch, type Order, type EmailEvent, type AdSpend, type Canonical } from './adapters.js';
import { configHash, sha256 } from './db.js';

export interface IngestResult { status: string; rows: number; inserted: number; duplicates: number; hash: string }

export async function ingestBytes(pool: pg.Pool, tenant: Tenant, batch: Batch, bytes: Buffer): Promise<IngestResult> {
  if (batch.tenant !== tenant.id) throw new Error('Batch tenant does not match worker configuration');
  const hash = sha256(bytes);
  const attempt = randomUUID();
  await pool.query('INSERT INTO pipeline.ingest_attempts(attempt_id,tenant_id,source,batch,file_hash,status) VALUES($1,$2,$3,$4,$5,\'running\')', [attempt, tenant.id, batch.source, batch.batch, hash]);
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`e76-pipeline:${tenant.id}`]);
    const identity = await client.query('SELECT tenant_id, config_hash FROM pipeline.tenants');
    if (identity.rowCount !== 1 || identity.rows[0].tenant_id !== tenant.id || identity.rows[0].config_hash !== configHash(tenant)) throw new Error('Worker identity or config does not match provisioned tenant');
    await client.query(`UPDATE pipeline.ingest_attempts SET status='abandoned', finished_at=clock_timestamp(), error_code='INTERRUPTED', error_message='Previous attempt ended without a committed receipt; retry is safe' WHERE tenant_id=$1 AND source=$2 AND batch=$3 AND status='running' AND attempt_id<>$4 AND started_at < (SELECT started_at FROM pipeline.ingest_attempts WHERE attempt_id=$4)`, [tenant.id, batch.source, batch.batch, attempt]);
    const receipt = await client.query('SELECT file_hash FROM pipeline.batch_receipts WHERE tenant_id=$1 AND source=$2 AND batch=$3', [tenant.id, batch.source, batch.batch]);
    if (receipt.rowCount && receipt.rows[0].file_hash !== hash) throw new Error('BATCH_CONFLICT: already processed batch has different bytes');
    const existing = await client.query('SELECT row_count FROM pipeline.files WHERE tenant_id=$1 AND source=$2 AND file_hash=$3', [tenant.id, batch.source, hash]);
    let result: IngestResult;
    if (existing.rowCount) {
      result = { status: 'replayed', rows: existing.rows[0].row_count, inserted: 0, duplicates: existing.rows[0].row_count, hash };
    } else {
      if (bytes.length > 10 * 1024 * 1024) throw new Error('File exceeds the supported 10 MiB limit');
      const parsed = parseBatch(batch.source, bytes, tenant);
      let inserted = 0;
      for (const [index, row] of parsed.rows.entries()) {
        await client.query('INSERT INTO pipeline.raw_records VALUES($1,$2,$3,$4,$5,$6,$7)', [tenant.id, batch.source, hash, index + 1, row.lineStart, row.lineEnd, row.rawText]);
        inserted += await insertCanonical(client, tenant.id, batch.source, hash, index + 1, row.canonical);
      }
      result = { status: 'processed', rows: parsed.rows.length, inserted, duplicates: parsed.rows.length - inserted, hash };
      await client.query('INSERT INTO pipeline.files(tenant_id,source,file_hash,body,schema_version,config_hash,row_count,inserted_count,duplicate_count) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)', [tenant.id, batch.source, hash, bytes, parsed.schemaVersion, configHash(tenant), result.rows, inserted, result.duplicates]);
    }
    await client.query('INSERT INTO pipeline.batch_receipts(tenant_id,source,batch,file_hash) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING', [tenant.id, batch.source, batch.batch, hash]);
    await client.query('UPDATE pipeline.ingest_attempts SET status=$2,finished_at=clock_timestamp() WHERE attempt_id=$1', [attempt, result.status]);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    const message = error instanceof Error ? error.message : 'Unknown ingestion failure';
    await pool.query("UPDATE pipeline.ingest_attempts SET status='failed',finished_at=clock_timestamp(),error_code='INGEST_FAILED',error_message=$2 WHERE attempt_id=$1", [attempt, message.slice(0, 1000)]);
    throw error;
  } finally { client.release(); }
}

async function insertCanonical(client: pg.PoolClient, tenant: string, source: string, hash: string, row: number, value: Canonical): Promise<number> {
  let inserted: pg.QueryResult;
  let equal: pg.QueryResult;
  if (source === 'orders') {
    const order = value as Order;
    inserted = await client.query('INSERT INTO staging.orders(tenant_id,order_id,occurred_at,channel,gross,currency,customer_email,canonical_payload,file_hash,row_number) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING RETURNING order_id', [tenant, order.orderId, order.occurredAt, order.channel, order.gross, order.currency, order.customerEmail, value, hash, row]);
    if (inserted.rowCount) return 1;
    equal = await client.query('SELECT 1 FROM staging.orders WHERE tenant_id=$1 AND order_id=$2 AND canonical_payload=$3::jsonb', [tenant, order.orderId, value]);
  } else if (source === 'email_events') {
    const event = value as EmailEvent;
    inserted = await client.query('INSERT INTO staging.email_events(tenant_id,event_id,occurred_at,event_type,email,campaign_id,canonical_payload,file_hash,row_number) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING RETURNING event_id', [tenant, event.eventId, event.occurredAt, event.type, event.email, event.campaignId, value, hash, row]);
    if (inserted.rowCount) return 1;
    equal = await client.query('SELECT 1 FROM staging.email_events WHERE tenant_id=$1 AND event_id=$2 AND canonical_payload=$3::jsonb', [tenant, event.eventId, value]);
  } else {
    const ad = value as AdSpend;
    inserted = await client.query('INSERT INTO staging.ad_spend(tenant_id,date,campaign_id,platform,amount,currency,canonical_payload,file_hash,row_number) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT DO NOTHING RETURNING campaign_id', [tenant, ad.date, ad.campaignId, ad.platform, ad.amount, ad.currency, value, hash, row]);
    if (inserted.rowCount) return 1;
    equal = await client.query('SELECT 1 FROM staging.ad_spend WHERE tenant_id=$1 AND date=$2 AND platform=$3 AND campaign_id=$4 AND canonical_payload=$5::jsonb', [tenant, ad.date, ad.platform, ad.campaignId, value]);
  }
  if (!equal.rowCount) throw new Error(`RECORD_CONFLICT: ${source} row ${row} changed an existing business key; no source version is available`);
  return 0;
}
