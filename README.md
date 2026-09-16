# One pipeline, two tenants

A local TypeScript batch service ingests storefront orders, email events, and ad spend into PostgreSQL. The focused scope is **safe replay and database-enforced tenant isolation**: the same adapters and models serve both tenants, with normalization defined in configuration.

```text
Tenant configuration + expected-batch manifest
                       |
              Three source adapters
                       |
       PostgreSQL raw records + provenance
                       |
         Typed, tenant-scoped staging
                       |
  Daily reporting views + completeness status
```

## Run from a clean checkout

Prerequisites: **Node.js 22+**, npm, and Docker with Compose. Run these commands from the repository root. The local database uses port `55476`; the example credentials are for this isolated development database only.

```sh
npm ci
cp .env.example .env
docker compose up -d --wait
npm run db:setup
npm run ingest -- --all  # Expected exit 2: the supplied Lumen ad batch 03 is absent.
npm run status -- --all  # Expected exit 2 for the same unresolved coverage gap.
npm run report -- --tenant northwind
npm run report -- --tenant lumen  # Reports observed data and exits 2 for incomplete coverage.
```

The repository includes the supplied fixtures, so no download or portal access is needed. This is a batch CLI, not an HTTP service or hosted dashboard.

Run the lines separately rather than chaining them with `&&`: the expected incomplete-coverage exit is intentional. Exit codes are `0` for complete selected coverage, `2` for incomplete coverage, and `1` for an ingestion/configuration failure.

## Expected result

The manifest contains 30 expected batches, but only 29 files were supplied. **Lumen ad batch 03, January 18–23, is missing.** Later arrivals must not erase that gap.

| Canonical records after ingestion | Northwind | Lumen |
|---|---:|---:|
| Orders | 680 | 662 |
| Email events | 1,507 | 1,473 |
| Ad rows | 90 | 72 |

Northwind order exports contain 14 overlapping records; staging counts them once. Northwind's final email batch adds 24 events to January 12–17, changing those dates' combined count from 297 to 321. Reports reflect the latest corrected data rather than preserving historical report snapshots.

Order values are **gross**, not net revenue: USD 79,303.38 for Northwind and EUR 79,610.98 for Lumen. Ad `cost_usd` amounts are USD; legacy `spend` amounts have unknown currency. No FX or attribution is invented. Check completeness before treating an absent reporting row as zero. More evidence: [fixture audit](docs/fixture-audit.md).

The report command emits JSON with `tenant`, `timezone`, `orders`, `email`, `adSpend`, and `coverage` fields. Money is represented as decimal strings, not floating-point numbers; unknown currency is labelled `UNKNOWN`.

## Replay and operating commands

```sh
# Repeat all inputs: canonical counts and totals must not change.
npm run ingest -- --all

# Replay one batch, or ingest the manifest in reverse order.
npm run ingest -- --tenant northwind --source orders --batch 03
npm run ingest -- --all --reverse

# Inspect tenant-scoped coverage and daily aggregates.
npm run status -- --tenant lumen
npm run report -- --tenant lumen
```

`--config <path>` selects another tenant configuration file; `--fixtures <directory>` selects another fixture directory containing `manifest.json`. Defaults are `config/tenants.json` and `fixtures/`.

A file's raw rows, staging changes, and success receipt commit together. A crash before commit requires replaying that file; a crash after commit is a harmless retry. File fingerprints prevent repeat delivery from duplicating raw storage, while tenant-scoped business keys prevent overlapping exports from duplicating reporting records. Changed canonical values for an existing key fail rather than silently overwriting history. An already processed batch arriving with different bytes also fails.

An attempt journal is separate from that publication transaction, so failed attempts remain visible. A killed process can leave a `running` entry; a subsequent successful retry marks the interrupted attempt `abandoned`. There is no automatic stuck-run watchdog. Missing files are printed as `missing`; status retains the expected batch as `pending` with `file_present: false`.

Correct invalid input in an unprocessed batch and rerun it. Existing tenant configuration is frozen after provisioning: changing a mapping requires an explicit data migration, not a blind replay. That migration workflow is not implemented. A missing expected file remains missing until it is supplied and successfully processed. Do not “repair” gaps by creating empty files or deleting manifest entries.

## Queryable models and isolation

| Schema | Purpose |
|---|---|
| `pipeline` | Raw records, delivery provenance, and batch-control state |
| `staging` | Typed canonical records keyed by tenant and business identity |
| `reporting` | `daily_orders`, `daily_email`, `daily_ad_spend`, `batch_status`, and `daily_coverage` |

Daily orders and email reports use UTC event dates; ads retain their source date. Reports keep currencies separate. The reporting views preserve caller permissions and row-level security. A tenant's identity is bound to its database login role, not a caller-controlled session variable.

`DATABASE_URL` is for trusted provisioning. `NORTHWIND_DATABASE_URL` and `LUMEN_DATABASE_URL` are separate tenant credentials. Tenant roles are not owners, superusers, or `BYPASSRLS` roles. Administrators remain trusted and can inspect all data; this is not protection from compromised administration. Never distribute the provisioning credential to clients.

The demo tenant roles can ingest their own data as well as query it; they are not read-only analytics credentials. Production writer/reader separation is deferred. When connected with a tenant role, a direct client query needs no tenant filter to preserve isolation:

```sql
SELECT * FROM reporting.daily_orders ORDER BY 1 LIMIT 10;
```

## Add a third tenant

No TypeScript or SQL model fork is needed for the supported contracts:

1. Add a tenant object to `config/tenants.json` with a unique `id`, dedicated `role`, and `databaseUrlEnv`; set its `channels`, `platforms`, `eventTypes`, and allowed `orderCurrencies` mappings/values by copying the existing object shape, not its business assumptions.
2. Add the corresponding tenant database URL to your local `.env` using a distinct login and password. Keep credentials out of version control.
3. Add that tenant's expected batches to `fixtures/manifest.json` and its files under the configured paths. Keep expected-but-absent batches in the manifest.
4. Run `npm run db:setup`, then `npm run ingest -- --tenant <id>`, `npm run status -- --tenant <id>`, and `npm run report -- --tenant <id>`.
5. Confirm sample totals, mappings, incomplete intervals, and cross-tenant access denial before delivering the tenant credentials.

Setup creates new roles with the password in the configured URL. Rerunning setup does not rotate an existing role's password. Configuration-only does not mean zero operational work. A different source contract needs an adapter change; reporting currency, revision rules, and delivery expectations need agreement with the client.

## Verification

```sh
npm run check
```

This builds TypeScript and runs the unit tests. Database integration tests require a **separate disposable database**:

```sh
# One-time creation; an "already exists" error means it was created previously.
docker compose exec db createdb -U pipeline_admin pipeline_test
npm run test:integration
```

The harness reads `TEST_DATABASE_URL` from `.env` and refuses database names that do not end in `_test`. It **drops and recreates the `pipeline`, `staging`, and `reporting` schemas** in that database. Never point it at valuable data, even if the name ends in `_test`. Existing environment variables take precedence over `.env`.

The tests cover source contracts, replay/overlap, concurrent delivery, changed-record conflicts, actual subprocess termination with `SIGKILL`, late events, schema variants, incomplete coverage, restricted-role isolation, and adding a configured third tenant. Supplied-fixture checks are distinguished from deliberately injected failures in the [audit](docs/fixture-audit.md).

## Scope and submission status

The implementation focuses on transactional replay, overlapping-export deduplication, tenant-scoped models/access, known schema drift, late-event reporting, and manifest-based coverage. It deliberately omits production scheduling, external alerts, chunk checkpoints, correction workflows, historical report snapshots, FX, attribution, and production secret/backup operations.

Read [TRADEOFFS.md](TRADEOFFS.md) for the rationale and next-week priorities. [The walkthrough script](docs/walkthrough.md) is a recording aid, **not a recorded walkthrough**. Recording, public repository publication, and portal submission are separate remaining steps; no video URL is fabricated here.
