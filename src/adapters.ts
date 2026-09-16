import { parse } from 'csv-parse/sync';
import { z } from 'zod';
import { dateSchema, type Source, type Tenant } from './config.js';

export interface Order {
  orderId: string; occurredAt: string; channel: string; gross: string; currency: string; customerEmail: string;
}
export interface EmailEvent {
  eventId: string; occurredAt: string; type: string; email: string; campaignId: string;
}
export interface AdSpend {
  date: string; campaignId: string; platform: string; amount: string; currency: string;
}
export type Canonical = Order | EmailEvent | AdSpend;
export interface ParsedRow { canonical: Canonical; rawText: string; lineStart: number; lineEnd: number }
export interface ParsedBatch { schemaVersion: string; rows: ParsedRow[] }
interface CsvRecord { record: string[]; raw: string; info: { lines: number } }

const text = z.string().min(1).max(500);
const email = z.email().max(320);
const amount = z.string().regex(/^\d{1,12}(\.\d{1,2})?$/, 'Expected non-negative decimal with at most two fractional digits')
  .transform(s => { const [whole, fraction = ''] = s.split('.'); return `${BigInt(whole!)}.${fraction.padEnd(2, '0')}`; });
const timestamp = z.iso.datetime({ offset: true }).transform(s => new Date(s).toISOString());
const orderSchema = z.strictObject({
  order_id: text, created_at: timestamp, channel: text, gross: amount,
  currency: z.string().regex(/^[A-Z]{3}$/), customer_email: email,
});

function mapped(values: Record<string, string>, value: string, label: string): string {
  const result = Object.hasOwn(values, value) ? values[value] : undefined;
  if (!result) throw new Error(`Unmapped ${label}: ${value}`);
  return result;
}

const adLegacySchema = z.strictObject({ date: dateSchema, campaign_id: text, platform: text, spend: amount });
const adUsdSchema = z.strictObject({ date: dateSchema, campaign_id: text, platform: text, cost_usd: amount });

export function parseBatch(source: Source, bytes: Buffer, tenant: Tenant): ParsedBatch {
  const input = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
  if (source === 'email_events') return parseEmail(input, tenant);
  const records = parse(input, { bom: true, info: true, raw: true, skip_empty_lines: false }) as unknown as CsvRecord[];
  const header = records.shift();
  const fields = header?.record.join(',');
  let schemaVersion: string;
  if (source === 'orders' && fields === 'order_id,created_at,channel,gross,currency,customer_email') schemaVersion = 'orders.v1';
  else if (source === 'ad_spend' && fields === 'date,campaign_id,platform,spend') schemaVersion = 'ad_spend.spend.v1';
  else if (source === 'ad_spend' && fields === 'date,campaign_id,platform,cost_usd') schemaVersion = 'ad_spend.cost_usd.v2';
  else throw new Error(`Unsupported ${source} schema: ${fields ?? '(empty file)'}`);
  if (!header) throw new Error('Empty file');
  const physicalLines = input.match(/[^\r\n]*(?:\r\n|\n|\r|$)/g) ?? [];
  let previousLine = header.info.lines;
  const rows = records.map(row => {
    const record = Object.fromEntries(header.record.map((key, i) => [key, row.record[i]]));
    let canonical: Canonical;
    try {
      if (source === 'orders') {
        const value = orderSchema.parse(record);
        if (!tenant.orderCurrencies.includes(value.currency)) throw new Error(`Unconfigured order currency: ${value.currency}`);
        canonical = {
          orderId: value.order_id, occurredAt: value.created_at, channel: mapped(tenant.channels, value.channel, 'channel'),
          gross: value.gross, currency: value.currency, customerEmail: value.customer_email,
        };
      } else {
        const value = schemaVersion === 'ad_spend.spend.v1' ? adLegacySchema.parse(record) : adUsdSchema.parse(record);
        canonical = {
          date: value.date, campaignId: value.campaign_id, platform: mapped(tenant.platforms, value.platform, 'platform'),
          amount: 'spend' in value ? value.spend : value.cost_usd, currency: 'spend' in value ? 'UNKNOWN' : 'USD',
        };
      }
    } catch (error) { throw new Error(`Line ${previousLine + 1}: ${safeValidationMessage(error)}`); }
    const result = { canonical, rawText: physicalLines.slice(previousLine, row.info.lines).join(''), lineStart: previousLine + 1, lineEnd: row.info.lines };
    previousLine = row.info.lines;
    return result;
  });
  return { schemaVersion, rows };
}

// Validation diagnostics identify fields, not customer values or complete payloads.
function safeValidationMessage(error: unknown): string {
  if (error instanceof z.ZodError) return error.issues.map(i => `${i.path.join('.')}: ${i.message}`).join('; ');
  return error instanceof Error ? error.message : 'Invalid record';
}

const eventSchema = z.strictObject({ event_id: text, type: text, email, campaign_id: text, occurred_at: timestamp });
function parseEmail(input: string, tenant: Tenant): ParsedBatch {
  const lines = input.match(/[^\n]*\n|[^\n]+$/g) ?? [];
  const rows = lines.map((rawText, index) => {
    try {
      const value = eventSchema.parse(JSON.parse(rawText));
      const canonical: EmailEvent = {
        eventId: value.event_id, type: mapped(tenant.eventTypes, value.type, 'event type'), email: value.email,
        campaignId: value.campaign_id, occurredAt: value.occurred_at,
      };
      return { canonical, rawText, lineStart: index + 1, lineEnd: index + 1 };
    } catch (error) {
      const diagnostic = error instanceof SyntaxError ? 'Invalid JSON record' : safeValidationMessage(error);
      throw new Error(`Line ${index + 1}: ${diagnostic}`);
    }
  });
  if (!rows.length) throw new Error('Empty email_events file');
  return { schemaVersion: 'email_events.v1', rows };
}
