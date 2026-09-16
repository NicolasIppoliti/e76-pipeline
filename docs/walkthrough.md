# Walkthrough recording script

**Target length: 7–9 minutes. This is a script, not a recorded video.** Record the actual terminal output and describe only the checks that pass in that run. Do not invent a work duration or portray planned work as completed.

## Before recording

- Follow the README on a disposable local database, and run both test suites.
- Have `README.md`, `TRADEOFFS.md`, `config/tenants.json`, the transaction code, and the database migration open.
- Avoid showing real credentials. The committed example credentials are local-only; no production secrets belong in the recording.
- If demonstrating first ingestion, start with a clean disposable database. Otherwise say that the initial load is already complete and show a replay honestly.
- After recording, make the video accessible to reviewers and put its real link in the submission notes. This file is not a substitute for that step.

## 0:00–0:50 — The choice

**Show:** README architecture and expected counts.

> This is one TypeScript batch service and one PostgreSQL database, serving two tenants with the same adapters and models. The assessment asks for more than fits the time limit, so the depth is in safe replay and tenant isolation. There is no dashboard, scheduler, or attribution system hidden behind the demo. The remaining failure cases have explicit behavior, and the limitations are written down.

> The audit found 29 of 30 expected files. It also found overlapping orders, late email events, a renamed ad-spend field, and business keys shared by both tenants. Those findings—not a preferred framework—determined the design.

## 0:50–2:00 — Load and replay

**Run:**

```sh
npm run ingest -- --all
npm run report -- --tenant northwind
npm run ingest -- --all
npm run report -- --tenant northwind
```

**Point out:** the repeated delivery outcome and unchanged daily results. Refer to the fixture audit for aggregate acceptance counts; do not imply the report command prints a grand total if it prints daily rows.

> Whole-file replay and overlapping exports are separate problems. A fingerprint recognizes the same file. A tenant-scoped order ID recognizes the 14 orders copied into a different export. Raw data retains delivery provenance, while canonical staging counts each order once. The correct Northwind total is 680 unique orders and USD 79,303.38 gross order value—not net revenue.

> The raw rows, staging changes, and successful receipt share a transaction. A file that dies before commit is retried in full. A file that committed before the operator lost the response is safe to replay. There are no chunk checkpoints; these files are small, so whole-file retry is the simpler boundary.

## 2:00–3:00 — Demonstrate failure recovery

**Show:** `tests/integration.test.ts`, the test named `SIGKILL one third through a file rolls back partial work; full retry equals a clean run`. Run `npm run test:integration` after following the README's disposable-test-database setup. Show the test output and final summary.

> Process termination is a deliberately injected test, not a corrupt file that came with the fixtures. A test-only database trigger pauses ingestion one third through a file; the test kills the actual child process with SIGKILL. It checks that no partial file, raw rows, or receipt survived, then checks that a full retry matches a clean load across all three sources. There is no production crash flag. Another test distinguishes a harmless duplicate from changed data under the same business key. Changed data fails visibly because the source gives us no revision ordering or correction policy.

## 3:00–4:00 — Missing and late are different

**Run:**

```sh
npm run status -- --tenant lumen
npm run report -- --tenant northwind
```

**Show:** Lumen's missing ad batch 03 and the January 18–23 incomplete interval. Show the late-arrival test's assertion for Northwind January 12–17.

> Lumen's latest batches have arrived, but January 18–23 is still missing. A recent timestamp is not a completeness signal, and the missing period is not zero spend. The manifest gives us expected coverage. It does not give a delivery deadline, so this is a coverage check, not a fabricated SLA alert.

> Northwind's final email export contains 24 genuinely new events for January 12–17. They belong to those event dates, so the combined count changes from 297 to 321. The daily views show current corrected truth. If a customer needs to reproduce what they saw last Tuesday, we need a separate reporting-snapshot policy; it is not implemented here.

## 4:00–5:15 — Isolation and client three

**Show:** tenant-key constraints, database role policy, caller-restricted reporting views, and the integration assertions that tenant A cannot read or write tenant B's rows. Then open `config/tenants.json`.

> Tenant identity is enforced in PostgreSQL through a dedicated login role, not just a WHERE clause in TypeScript or a tenant variable that the caller can change. The tests use restricted roles, not the database owner. The provisioning credential is explicitly outside that boundary and must never be handed to a customer.

> For another client using the supported contracts, onboarding means adding one config entry, a database credential, expected manifest batches, and input files, then running the same setup and ingestion commands. No client-name branch or copied SQL model is necessary. That avoids a code fork; it does not make onboarding or operational support literally free.

## 5:15–6:15 — Drift and honest metrics

**Show:** an ad batch 03 header and batch 04 header, the explicit adapter branches, and a currency-separated ad report.

> Both tenants switch from `spend` to `cost_usd`. The adapters recognize those two known schemas and reject unknown shapes. But successfully reading both columns is not enough. The old column does not declare a currency; the new one explicitly says USD. The 90 legacy rows remain unknown-currency, while 72 rows are USD-labelled.

> Lumen orders are EUR. There is no exchange-rate or attribution contract, so the system does not manufacture a blended total or ROAS. Orders are gross order value because payment status and refunds were not supplied. Reporting names should be as defensible as the ingestion code.

## 6:15–7:30 — What is not here, and next

**Show:** TRADEOFFS.md and the real incremental commit history.

> The deliberate omissions are production scheduling and external alerts, large-file checkpoints, automatic correction handling, report snapshots, FX, attribution, and production secret and backup operations. The hardest part was separating file identity, business-record identity, and event date. Using one watermark for all three would either double-count or lose data.

> With another week, the first work would be agreements with the source owners: correction semantics, legacy currency, reporting timezones, and delivery deadlines. Then I would add versioned corrections, deadline-aware alerts, narrower production credentials and recovery drills, and measure file sizes before changing the ingestion strategy. The goal is not a longer checklist. It is a system whose guarantees remain true outside the demo.

**Close:** state the checks actually demonstrated and any failure that remains. Add an actual elapsed-work statement only if supported by a real record. Do not infer hours worked from commits or script pacing.
