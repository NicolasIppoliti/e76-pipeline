CREATE SCHEMA pipeline;
CREATE SCHEMA staging;
CREATE SCHEMA reporting;
REVOKE ALL ON SCHEMA pipeline, staging, reporting FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

CREATE TABLE pipeline.schema_migrations (
  version text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE pipeline.tenants (
  tenant_id text PRIMARY KEY,
  role_name name NOT NULL UNIQUE,
  config_hash text NOT NULL
);
CREATE TABLE pipeline.expected_batches (
  tenant_id text NOT NULL REFERENCES pipeline.tenants,
  source text NOT NULL CHECK (source IN ('orders', 'email_events', 'ad_spend')),
  batch integer NOT NULL CHECK (batch > 0),
  path text NOT NULL,
  covers_from date NOT NULL,
  covers_to date NOT NULL CHECK (covers_to >= covers_from),
  PRIMARY KEY (tenant_id, source, batch)
);
CREATE TABLE pipeline.files (
  tenant_id text NOT NULL REFERENCES pipeline.tenants,
  source text NOT NULL CHECK (source IN ('orders', 'email_events', 'ad_spend')),
  file_hash text NOT NULL CHECK (file_hash ~ '^[0-9a-f]{64}$'),
  body bytea NOT NULL,
  schema_version text NOT NULL,
  config_hash text NOT NULL,
  row_count integer NOT NULL CHECK (row_count >= 0),
  inserted_count integer NOT NULL CHECK (inserted_count >= 0),
  duplicate_count integer NOT NULL CHECK (duplicate_count >= 0),
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, source, file_hash),
  CHECK (inserted_count + duplicate_count = row_count)
);
CREATE TABLE pipeline.raw_records (
  tenant_id text NOT NULL,
  source text NOT NULL,
  file_hash text NOT NULL,
  row_number integer NOT NULL CHECK (row_number > 0),
  line_start integer NOT NULL CHECK (line_start > 0),
  line_end integer NOT NULL CHECK (line_end >= line_start),
  raw_text text NOT NULL,
  PRIMARY KEY (tenant_id, source, file_hash, row_number),
  FOREIGN KEY (tenant_id, source, file_hash) REFERENCES pipeline.files DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE pipeline.batch_receipts (
  tenant_id text NOT NULL,
  source text NOT NULL,
  batch integer NOT NULL,
  file_hash text NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, source, batch),
  FOREIGN KEY (tenant_id, source, batch) REFERENCES pipeline.expected_batches,
  FOREIGN KEY (tenant_id, source, file_hash) REFERENCES pipeline.files
);
CREATE TABLE pipeline.ingest_attempts (
  attempt_id uuid PRIMARY KEY,
  tenant_id text NOT NULL,
  source text NOT NULL,
  batch integer NOT NULL,
  file_hash text,
  status text NOT NULL CHECK (status IN ('running', 'processed', 'replayed', 'failed', 'abandoned')),
  started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  finished_at timestamptz,
  error_code text,
  error_message text,
  FOREIGN KEY (tenant_id, source, batch) REFERENCES pipeline.expected_batches
);
CREATE INDEX attempts_by_batch ON pipeline.ingest_attempts (tenant_id, source, batch, started_at DESC);

CREATE TABLE staging.orders (
  tenant_id text NOT NULL,
  order_id text NOT NULL,
  occurred_at timestamptz NOT NULL,
  channel text NOT NULL,
  gross numeric(14,2) NOT NULL CHECK (gross >= 0),
  currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  customer_email text NOT NULL,
  canonical_payload jsonb NOT NULL,
  source text NOT NULL DEFAULT 'orders' CHECK (source = 'orders'),
  file_hash text NOT NULL,
  row_number integer NOT NULL,
  PRIMARY KEY (tenant_id, order_id),
  FOREIGN KEY (tenant_id, source, file_hash, row_number) REFERENCES pipeline.raw_records DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE staging.email_events (
  tenant_id text NOT NULL,
  event_id text NOT NULL,
  occurred_at timestamptz NOT NULL,
  event_type text NOT NULL,
  email text NOT NULL,
  campaign_id text NOT NULL,
  canonical_payload jsonb NOT NULL,
  source text NOT NULL DEFAULT 'email_events' CHECK (source = 'email_events'),
  file_hash text NOT NULL,
  row_number integer NOT NULL,
  PRIMARY KEY (tenant_id, event_id),
  FOREIGN KEY (tenant_id, source, file_hash, row_number) REFERENCES pipeline.raw_records DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE staging.ad_spend (
  tenant_id text NOT NULL,
  date date NOT NULL,
  campaign_id text NOT NULL,
  platform text NOT NULL,
  amount numeric(14,2) NOT NULL CHECK (amount >= 0),
  currency text NOT NULL CHECK (currency = 'UNKNOWN' OR currency = 'USD'),
  canonical_payload jsonb NOT NULL,
  source text NOT NULL DEFAULT 'ad_spend' CHECK (source = 'ad_spend'),
  file_hash text NOT NULL,
  row_number integer NOT NULL,
  PRIMARY KEY (tenant_id, date, platform, campaign_id),
  FOREIGN KEY (tenant_id, source, file_hash, row_number) REFERENCES pipeline.raw_records DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX orders_daily ON staging.orders (tenant_id, occurred_at);
CREATE INDEX email_daily ON staging.email_events (tenant_id, occurred_at);

-- The registry is readable only for the authenticated database login. No mutable
-- application setting or caller-supplied tenant value can change this identity.
ALTER TABLE pipeline.tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE pipeline.tenants FORCE ROW LEVEL SECURITY;
CREATE POLICY login_identity ON pipeline.tenants USING (role_name = session_user);
DO $$
DECLARE target text;
BEGIN
  FOREACH target IN ARRAY ARRAY[
    'pipeline.expected_batches', 'pipeline.files', 'pipeline.raw_records',
    'pipeline.batch_receipts', 'pipeline.ingest_attempts',
    'staging.orders', 'staging.email_events', 'staging.ad_spend'
  ] LOOP
    EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', target);
    EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', target);
    EXECUTE format('CREATE POLICY tenant_isolation ON %s USING (tenant_id = (SELECT tenant_id FROM pipeline.tenants WHERE role_name = session_user)) WITH CHECK (tenant_id = (SELECT tenant_id FROM pipeline.tenants WHERE role_name = session_user))', target);
  END LOOP;
END $$;
