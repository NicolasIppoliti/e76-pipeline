ALTER TABLE pipeline.files ADD COLUMN rejected_count integer NOT NULL DEFAULT 0 CHECK (rejected_count >= 0);
ALTER TABLE pipeline.files DROP CONSTRAINT files_check;
ALTER TABLE pipeline.files ADD CONSTRAINT files_count_balance CHECK (inserted_count + duplicate_count + rejected_count = row_count);

CREATE TABLE pipeline.order_quarantine (
  tenant_id text NOT NULL,
  source text NOT NULL DEFAULT 'orders' CHECK (source = 'orders'),
  file_hash text NOT NULL,
  row_number integer NOT NULL CHECK (row_number > 0),
  source_file text NOT NULL,
  line_number integer NOT NULL CHECK (line_number > 0),
  reason text NOT NULL,
  PRIMARY KEY (tenant_id, source, file_hash, row_number),
  FOREIGN KEY (tenant_id, source, file_hash, row_number)
    REFERENCES pipeline.raw_records DEFERRABLE INITIALLY DEFERRED
);
CREATE OR REPLACE VIEW reporting.batch_status WITH (security_invoker = true) AS
SELECT b.tenant_id, b.source, b.batch, b.path, b.covers_from::text AS covers_from, b.covers_to::text AS covers_to,
       CASE WHEN r.file_hash IS NOT NULL THEN 'processed' ELSE 'pending' END AS status,
       r.file_hash, f.schema_version, f.row_count, f.inserted_count, f.duplicate_count, r.processed_at,
       a.status AS latest_attempt_status, a.started_at AS latest_attempt_started_at, a.error_code, a.error_message,
       f.rejected_count
FROM pipeline.expected_batches b
LEFT JOIN pipeline.batch_receipts r USING (tenant_id, source, batch)
LEFT JOIN pipeline.files f ON f.tenant_id=r.tenant_id AND f.source=r.source AND f.file_hash=r.file_hash
LEFT JOIN LATERAL (
  SELECT status, started_at, error_code, error_message FROM pipeline.ingest_attempts a
  WHERE a.tenant_id=b.tenant_id AND a.source=b.source AND a.batch=b.batch
  ORDER BY started_at DESC LIMIT 1
) a ON true;

ALTER TABLE pipeline.order_quarantine ENABLE ROW LEVEL SECURITY;
ALTER TABLE pipeline.order_quarantine FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON pipeline.order_quarantine
  USING (tenant_id = (SELECT tenant_id FROM pipeline.tenants WHERE role_name = session_user))
  WITH CHECK (tenant_id = (SELECT tenant_id FROM pipeline.tenants WHERE role_name = session_user));
