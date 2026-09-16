import assert from 'node:assert/strict';
import test from 'node:test';
import { parseBatch } from '../src/adapters.js';
import { loadConfig } from '../src/config.js';

const tenant = () => loadConfig().tenants[0]!;
test('orders adapter normalizes a configured channel while retaining money exactly', () => {
  const file = Buffer.from('order_id,created_at,channel,gross,currency,customer_email\nA,2026-01-06T00:00:00Z,facebook,10.20,USD,a@example.invalid\n');
  const result = parseBatch('orders', file, tenant());
  assert.equal(result.schemaVersion, 'orders.v1');
  assert.deepEqual(result.rows[0]?.canonical, {
    orderId: 'A', occurredAt: '2026-01-06T00:00:00.000Z', channel: 'paid_social',
    gross: '10.20', currency: 'USD', customerEmail: 'a@example.invalid',
  });
  assert.equal(result.rows[0]?.lineStart, 2);
  assert.equal(result.rows[0]?.rawText, 'A,2026-01-06T00:00:00Z,facebook,10.20,USD,a@example.invalid\n');
});

test('explicit ad schema rename adapts without inventing legacy currency', () => {
  const legacy = parseBatch('ad_spend', Buffer.from('date,campaign_id,platform,spend\n2026-01-06,c1,facebook,12.30\n'), tenant());
  const renamed = parseBatch('ad_spend', Buffer.from('date,campaign_id,platform,cost_usd\n2026-01-24,c1,facebook,12.30\n'), tenant());
  assert.equal(legacy.schemaVersion, 'ad_spend.spend.v1');
  assert.equal(renamed.schemaVersion, 'ad_spend.cost_usd.v2');
  assert.deepEqual(legacy.rows[0]?.canonical, { date: '2026-01-06', campaignId: 'c1', platform: 'meta', amount: '12.30', currency: 'UNKNOWN' });
  assert.equal((renamed.rows[0]?.canonical as { currency: string }).currency, 'USD');
});

test('email events normalize explicit tenant case conventions and accept old event dates', () => {
  const lumen = loadConfig().tenants[1]!;
  const value = { event_id: 'E1', type: 'OPEN', email: 'a@example.invalid', campaign_id: 'c1', occurred_at: '2026-01-12T09:00:00+02:00' };
  const row = parseBatch('email_events', Buffer.from(`${JSON.stringify(value)}\n`), lumen).rows[0]!;
  assert.deepEqual(row.canonical, { eventId: 'E1', type: 'open', email: 'a@example.invalid', campaignId: 'c1', occurredAt: '2026-01-12T07:00:00.000Z' });
  assert.equal(row.lineStart, 1);
});

for (const [name, csv] of [
  ['unknown header', 'date,campaign_id,platform,amount\n2026-01-06,c,facebook,1.00\n'],
  ['ambiguous simultaneous spend columns', 'date,campaign_id,platform,spend,cost_usd\n2026-01-06,c,facebook,1.00,1.00\n'],
  ['invalid calendar day', 'date,campaign_id,platform,spend\n2026-02-30,c,facebook,1.00\n'],
  ['unmapped platform', 'date,campaign_id,platform,spend\n2026-01-06,c,mystery,1.00\n'],
  ['excess monetary precision', 'date,campaign_id,platform,spend\n2026-01-06,c,facebook,1.001\n'],
  ['negative monetary amount', 'date,campaign_id,platform,spend\n2026-01-06,c,facebook,-1.00\n'],
] as const) {
  test(`ad adapter rejects ${name}`, () => assert.throws(() => parseBatch('ad_spend', Buffer.from(csv), tenant())));
}

test('email adapter rejects unknown fields, invalid timestamps, unknown types and empty lines', () => {
  const valid = { event_id: 'E1', type: 'open', email: 'a@example.invalid', campaign_id: 'c1', occurred_at: '2026-01-12T09:00:00Z' };
  for (const record of [{ ...valid, surprise: true }, { ...valid, occurred_at: '2026-02-30T09:00:00Z' }, { ...valid, occurred_at: '2026-01-12T09:00:00' }, { ...valid, type: 'mystery' }]) {
    assert.throws(() => parseBatch('email_events', Buffer.from(JSON.stringify(record)), tenant()));
  }
  assert.throws(() => parseBatch('email_events', Buffer.from('\n'), tenant()));
  assert.throws(() => parseBatch('email_events', Buffer.from('{broken}'), tenant()), /Line 1: Invalid JSON/);
});

test('CSV quoting keeps original record evidence and exact money without floating point', () => {
  const file = Buffer.from('order_id,created_at,channel,gross,currency,customer_email\r\n"A,quoted",2026-01-06T00:00:00Z,email,999999999999.99,USD,a@example.invalid\r\n');
  const row = parseBatch('orders', file, tenant()).rows[0]!;
  assert.equal((row.canonical as { gross: string }).gross, '999999999999.99');
  assert.equal(row.rawText, '"A,quoted",2026-01-06T00:00:00Z,email,999999999999.99,USD,a@example.invalid\r\n');
});
