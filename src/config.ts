import { readFileSync } from 'node:fs';
import { resolve, relative, isAbsolute } from 'node:path';
import { z } from 'zod';

export const SOURCES = { ORDERS: 'orders', EMAIL: 'email_events', ADS: 'ad_spend' } as const;
export type Source = (typeof SOURCES)[keyof typeof SOURCES];
const sourceSchema = z.enum(Object.values(SOURCES));
const identifier = z.string().regex(/^[a-z][a-z0-9_]{0,39}$/);
const mapping = z.record(z.string().min(1), z.string().regex(/^[a-z][a-z0-9_]*$/));
const tenantSchema = z.strictObject({
  id: identifier, role: identifier, databaseUrlEnv: z.string().regex(/^[A-Z][A-Z0-9_]*$/),
  channels: mapping, platforms: mapping, eventTypes: mapping,
  orderCurrencies: z.array(z.string().regex(/^[A-Z]{3}$/)).min(1),
});
export type Tenant = z.infer<typeof tenantSchema>;
export const dateSchema = z.iso.date();
const batchSchema = z.strictObject({
  tenant: identifier, source: sourceSchema, batch: z.number().int().positive(), path: z.string().min(1),
  covers_from: dateSchema, covers_to: dateSchema,
}).refine(b => b.covers_to >= b.covers_from, 'Coverage ends before it starts');
export type Batch = z.infer<typeof batchSchema>;
export interface Config { tenants: Tenant[] }

export function loadConfig(path = 'config/tenants.json'): Config {
  const config = z.strictObject({ tenants: z.array(tenantSchema).min(1) }).parse(JSON.parse(readFileSync(path, 'utf8')));
  for (const key of ['id', 'role', 'databaseUrlEnv'] as const) {
    if (new Set(config.tenants.map(t => t[key])).size !== config.tenants.length) throw new Error(`Duplicate tenant ${key}`);
  }
  return config;
}

export function safeFixturePath(root: string, path: string): string {
  const absolute = resolve(root, path);
  const local = relative(resolve(root), absolute);
  if (isAbsolute(path) || local === '..' || local.startsWith('../')) throw new Error(`Fixture path escapes root: ${path}`);
  return absolute;
}

export function loadManifest(root: string, config: Config): Batch[] {
  const manifest = z.strictObject({ generated_for: z.string(), batches: z.array(batchSchema) })
    .parse(JSON.parse(readFileSync(resolve(root, 'manifest.json'), 'utf8')));
  const keys = new Set<string>();
  for (const batch of manifest.batches) {
    if (!config.tenants.some(t => t.id === batch.tenant)) throw new Error(`Unknown manifest tenant: ${batch.tenant}`);
    safeFixturePath(root, batch.path);
    const key = `${batch.tenant}/${batch.source}/${batch.batch}`;
    if (keys.has(key)) throw new Error(`Duplicate manifest batch: ${key}`);
    keys.add(key);
  }
  return manifest.batches;
}
