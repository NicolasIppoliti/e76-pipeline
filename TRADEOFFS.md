# Tradeoffs

## The choice: replay and isolation before breadth

The assessment has an eight-hour ceiling, including fixture analysis and documentation. The selected scope is a small TypeScript batch service backed by PostgreSQL, not a scheduler, dashboard, attribution engine, or production data platform. The priorities are **safe replay** and **database-enforced tenant isolation**. The remaining failure cases get explicit, bounded behavior rather than speculative infrastructure.

The fixture audit changed the design in three important ways:

1. Repeating a file is not the only replay problem: Northwind's third order export overlaps the second by 14 orders. File checksums alone are insufficient.
2. The latest arrival does not prove completeness: Lumen has ad batches 04 and 05 but is still missing batch 03.
3. A renamed amount field is also a semantic question: `cost_usd` declares a currency; `spend` does not. Parsing both is not permission to treat both as USD.

See [fixture evidence](docs/fixture-audit.md).

## Decisions and their costs

| Decision | Why | Cost / boundary |
|---|---|---|
| One transaction per file, including raw rows, staging, and success receipt | An interrupted file is either published completely or not at all | Retry the entire file; no resumable chunk checkpoints. Files are buffered in memory, and inputs above 10 MiB are rejected before parsing. This is not a design for multi-gigabyte exports |
| Serialize ingestion within each tenant | Concurrent retries cannot race through the publication boundary | Simpler correctness at the cost of per-tenant throughput; different tenants use different locks |
| File identity plus tenant-scoped business keys | A byte-identical file replay and overlapping exports are different problems | Raw provenance can include repeated business records, while staging and reports count each business key once |
| Reject changed canonical values for an existing business key, or different bytes for a processed batch | A retry must not silently become an undocumented correction | Legitimate amendments need an explicit correction/version policy; that workflow is deferred |
| Freeze an existing tenant's configuration after provisioning | Replaying a previously accepted file must not silently reinterpret it under new mappings | Changing established mappings requires an explicit data migration; automatic remodelling is deferred |
| One shared schema with tenant-scoped keys and restricted database roles | One model serves every tenant, with isolation below application filters | Privileged administrators and the provisioning path remain trusted; RLS is not protection against a database administrator |
| Daily SQL views over staging | Late events naturally restate their event dates without backfill jobs | These are current corrected results, not historical snapshots of what a dashboard showed yesterday |
| Explicit known schema versions; reject unexpected shapes | The supplied rename is handled deliberately and remains observable | New source contracts require adapter work; configuration-only onboarding applies to supported contracts |
| Separate orders, email events, and ad reports | Each has a defensible grain and metric | No speculative cross-source attribution or joined ROAS |
| Manifest-based coverage status | Missing expected batches are visible even after newer arrivals | This proves a coverage gap, not a breach of an unprovided delivery deadline |

## What the numbers mean

- **Orders:** unique gross order value by order creation date in UTC, channel, and source currency. The fixtures contain no payment status, cancellation status, refunds, or exchange rates. This is not net revenue, paid revenue, or consolidated reporting-currency revenue.
- **Email:** event counts by event date in UTC and normalized event type. A late event belongs to its original date; it is not discarded because a later date was already processed. Counts are events, not unique people or delivered-message rates.
- **Ads:** staging retains tenant/date/platform/campaign records; daily reports aggregate by date, platform, and currency with a campaign count. Legacy `spend` is labelled `UNKNOWN`; `cost_usd` is USD. Establishing legacy currency would require an agreed source contract and an explicit migration, not an undocumented configuration guess. Unknown currency is not zero, and Lumen's EUR orders must not be divided by USD or unknown ad amounts to fabricate ROAS.
- **Coverage:** a missing interval is incomplete data, not a zero-valued observation. A successfully processed batch is evidence of ingestion, not proof that the source export itself contained every real-world event.

## The trust boundary

Tenant identity is established through the database role, not a tenant ID supplied in a query. Tenant-facing roles must be non-owner and have neither superuser nor `BYPASSRLS` privileges. Views must preserve the caller's restrictions. A migration/provisioning credential is intentionally privileged and must never be handed to a tenant.

The local fixture operator controls configuration, input paths, and provisioning. This is not an untrusted upload API. A party that can edit another tenant's configuration or obtain administrative credentials is already outside the tenant isolation boundary. The demo tenant role can ingest its own data as well as query it; a compromised tenant credential can affect that tenant's data. Production would separate ingestion, read-only analytics, and administration credentials; audit provisioning; and manage secrets outside source control.

## Client three: no fork, not no work

For a third client using the supported source contracts, onboarding is configuration plus database provisioning:

1. Obtain the client's source contract: identifiers, timezone, channel/event conventions, amount semantics, and expected batch coverage.
2. Add one tenant entry with a unique tenant ID, dedicated database role, and explicit normalization mappings. Do not infer currency from the client's domicile or order currency.
3. Add its expected batches to the manifest and place exports in the configured input location.
4. Run the same provisioning and ingestion commands, then check coverage, sample reports, and a cross-tenant denial before handing over credentials. Exact commands are in the [README](README.md).

No new model or named-client branch is required. Contract verification, credential delivery, monitoring, and incident ownership still have real operational cost. A genuinely different source format is adapter development, not a configuration-only promise.

## Deliberately not built

- **Production scheduling, object storage, queues, and external alerts.** A local batch command plus queryable status is sufficient to demonstrate the state machine. No delivery cadence or alert destination was supplied.
- **Chunk checkpoints and a large-file memory strategy.** Whole-file retry is simpler and auditable at fixture scale. It must be revisited for larger inputs.
- **A correction UI or automatic last-write-wins updates.** No source revision/version contract exists; fail visibly rather than hide a change.
- **Historical report snapshots and close periods.** Current-truth views answer corrected historical totals; locked financial reporting needs an additional policy and storage layer.
- **FX, attribution, net revenue, and customer identity resolution.** Required business rules are absent. Similar campaign IDs or email addresses across tenants are not evidence of a shared identity.
- **Production security operations.** No managed secret store, credential rotation, backup/restore drill, encryption policy, or external penetration test is claimed.
- **Hosted deployment or a recorded walkthrough.** The repository includes a recording script, not a video. Publishing and recording remain separate submission steps.

## Hardest part

The hardest design problem was distinguishing a safe replay from a source correction without letting the desire for a clean demo erase uncertainty. There are three separate identities: the delivered file, the tenant-scoped business record, and the reported event date. A file hash answers only the first question. A business key prevents the overlapping orders from inflating totals. Event-date views let genuinely new late events change old totals. Treating any one of those as a universal watermark would lose data or double-count it.

## With another week

First agree on revision semantics, legacy ad currency, tenant reporting timezones, and arrival SLAs with the source owners. Then add an explicit correction history, deadline-aware alert delivery, narrowly scoped writer/reader credentials with secret management, and recovery/backup exercises. Measure realistic file sizes before choosing streaming ingestion and chunk checkpoints. Add report snapshots only if users need reproducible “as reported at” numbers, rather than the latest corrected truth.
