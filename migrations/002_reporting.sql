CREATE VIEW reporting.batch_status WITH (security_invoker = true) AS
SELECT b.tenant_id, b.source, b.batch, b.path, b.covers_from::text AS covers_from, b.covers_to::text AS covers_to,
       CASE WHEN r.file_hash IS NOT NULL THEN 'processed' ELSE 'pending' END AS status,
       r.file_hash, f.schema_version, f.row_count, f.inserted_count, f.duplicate_count, r.processed_at,
       a.status AS latest_attempt_status, a.started_at AS latest_attempt_started_at, a.error_code, a.error_message
FROM pipeline.expected_batches b
LEFT JOIN pipeline.batch_receipts r USING (tenant_id, source, batch)
LEFT JOIN pipeline.files f ON f.tenant_id=r.tenant_id AND f.source=r.source AND f.file_hash=r.file_hash
LEFT JOIN LATERAL (
  SELECT status, started_at, error_code, error_message FROM pipeline.ingest_attempts a
  WHERE a.tenant_id=b.tenant_id AND a.source=b.source AND a.batch=b.batch
  ORDER BY started_at DESC LIMIT 1
) a ON true;

CREATE VIEW reporting.daily_coverage WITH (security_invoker = true) AS
WITH expected AS (
  SELECT b.tenant_id, b.source, (b.covers_from + day.day_number)::text AS date,
         count(*)::int AS expected_batches, count(r.file_hash)::int AS processed_batches,
         bool_and(r.file_hash IS NOT NULL) AS is_complete
  FROM pipeline.expected_batches b
  CROSS JOIN LATERAL generate_series(0, b.covers_to - b.covers_from) AS day(day_number)
  LEFT JOIN pipeline.batch_receipts r USING (tenant_id, source, batch)
  GROUP BY b.tenant_id, b.source, b.covers_from + day.day_number
), observed AS (
  SELECT tenant_id, 'orders'::text AS source, (occurred_at AT TIME ZONE 'UTC')::date::text AS date, count(*) AS n
  FROM staging.orders GROUP BY tenant_id, (occurred_at AT TIME ZONE 'UTC')::date
  UNION ALL
  SELECT tenant_id, 'email_events', (occurred_at AT TIME ZONE 'UTC')::date::text, count(*)
  FROM staging.email_events GROUP BY tenant_id, (occurred_at AT TIME ZONE 'UTC')::date
  UNION ALL
  SELECT tenant_id, 'ad_spend', date::text, count(*) FROM staging.ad_spend GROUP BY tenant_id, date
)
SELECT e.*, CASE WHEN e.is_complete THEN coalesce(o.n, 0) ELSE o.n END AS observed_records
FROM expected e LEFT JOIN observed o USING (tenant_id, source, date);

CREATE VIEW reporting.daily_orders WITH (security_invoker = true) AS
SELECT o.tenant_id, (o.occurred_at AT TIME ZONE 'UTC')::date::text AS date, o.channel, o.currency,
       count(*) AS order_count, sum(o.gross) AS gross_order_value,
       c.is_complete AS manifest_complete
FROM staging.orders o
LEFT JOIN reporting.daily_coverage c ON c.tenant_id=o.tenant_id AND c.source='orders' AND c.date=(o.occurred_at AT TIME ZONE 'UTC')::date::text
GROUP BY o.tenant_id, (o.occurred_at AT TIME ZONE 'UTC')::date, o.channel, o.currency, c.is_complete;

CREATE VIEW reporting.daily_email WITH (security_invoker = true) AS
SELECT e.tenant_id, (e.occurred_at AT TIME ZONE 'UTC')::date::text AS date, e.campaign_id, e.event_type,
       count(*) AS event_count, c.is_complete AS manifest_complete
FROM staging.email_events e
LEFT JOIN reporting.daily_coverage c ON c.tenant_id=e.tenant_id AND c.source='email_events' AND c.date=(e.occurred_at AT TIME ZONE 'UTC')::date::text
GROUP BY e.tenant_id, (e.occurred_at AT TIME ZONE 'UTC')::date, e.campaign_id, e.event_type, c.is_complete;

CREATE VIEW reporting.daily_ad_spend WITH (security_invoker = true) AS
SELECT a.tenant_id, a.date::text AS date, a.platform, a.currency,
       sum(a.amount) AS observed_spend, count(*) AS campaign_count, c.is_complete AS manifest_complete
FROM staging.ad_spend a
LEFT JOIN reporting.daily_coverage c ON c.tenant_id=a.tenant_id AND c.source='ad_spend' AND c.date=a.date::text
GROUP BY a.tenant_id, a.date, a.platform, a.currency, c.is_complete;
