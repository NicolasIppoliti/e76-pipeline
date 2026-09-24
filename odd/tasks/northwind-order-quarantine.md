# Northwind Order Quarantine

## Objective

Ingest valid Northwind orders from a mixed-quality file while recording every rejected row with an auditable reason; exact replay must leave both published orders and quarantine unchanged.

## Problem and scope

Today an invalid order fails the whole file. Northwind's new rule requires row-level rejection for a blank `order_id`, negative `gross`, or currency different from Northwind's configured currency (USD). Other tenants retain their existing behavior. The source fixture is `fixtures/northwind/orders/batch_06.csv` (2026-02-05); the checked-in manifest does not list it. The first row is the header and data rows are numbered 2–5 in the physical CSV file.

## Constraints and decisions

- Preserve the original four fixture rows, including all three defects. Do not silently discard rejected rows or publish them to staging/reporting numbers.
- Record tenant, source-file identity, physical CSV row number, and a distinct reason for each rejected row. Quarantine may be a table or file; prefer a transactional database table for atomic publish/replay, subject to test-driven implementation.
- Valid rows continue in the same file; exact re-ingestion is idempotent for orders and quarantine.
- Keep tenant identity and RLS boundaries intact. No behavior change for Lumen or other tenants.
- Avoid touching the unrelated `.gitignore` change and existing `odd/tasks/fix-review-findings.md`.
- The test registers batch 6 only in its test manifest/setup, rather than silently changing the production manifest. Production delivery of batch 6 remains unregistered until explicitly requested.
- The user confirmed the existing `ingestBytes` integration seam with a real test database, including persisted order/quarantine observations.
- Strict TDD mode: explicitly requested by the user; runner `npm run test:integration` (`TEST_DATABASE_URL` must end in `_test`). Each behavior slice starts RED, implements the minimum GREEN, then checks the existing suite. Do not write all future tests in a horizontal batch.
- User authorized commit and push. Deliver on `feat/northwind-order-quarantine`; do not commit the unrelated `.gitignore` modification or `odd/tasks/fix-review-findings.md`. No PR or merge was requested.

## Acceptance

For test-only registration of the supplied file: one Northwind order (`NO-LIVE-1`, USD 45.00) ingests; three other rows are quarantined with three distinct reasons and source/tenant/physical line provenance. The same bytes run a second time without changing either count or duplicating any identity. Non-Northwind ingest behavior remains unchanged.

## Tasks

- [x] **NWQ-1 — Establish the mixed-file RED tracer** (delegated writer; integration test file, existing public seam). Register batch 6 in test-only setup, ingest the actual fixture through `ingestBytes`, assert the literal one-order/three-rejection outcome, provenance and distinct reasons, then exact replay yields identical counts. Observe and record the expected RED; do not modify production code in this task.
- [x] **NWQ-2 — Persist quarantine and continue valid rows** (delegated writer; `src/adapters.ts`, `src/ingest.ts`, new numbered migration, `src/db.ts` grants, and focused integration test as needed). From the RED tracer, add a tenant-scoped quarantine model and transactionally persist rejected Northwind rows while continuing valid rows; keep other tenants' existing rejection behavior. Add one test per newly discovered behavioral edge only in vertical RED→GREEN cycles. Include schema/RLS and test reset for isolation.
- [x] **NWQ-3 — Prove replay and tenant isolation** (delegated writer; focused integration tests and only required production corrections). Independently exercise exact replay counts/identities and Lumen's unchanged all-or-nothing rejection, adding RED first for any behavior not covered by NWQ-1. Verify invalid rows cannot reach order staging/reporting values.
- [x] **NWQ-4 — Verify and document operational behavior** (delegated verifier if needed; narrow docs). Run `npm run test:integration`, `npm run check`, and diff hygiene; report required `TEST_DATABASE_URL` or other unavailable checks explicitly. Document quarantine observability and the manifest-registration boundary without promising batch 6 is registered in production.

## Verification and progress

- Mapping: exported `ingestBytes` is the public integration seam; the adapter currently fails the whole batch on the first invalid row. No quarantine persistence exists. Northwind currency is USD. The fixture is not registered in the checked-in manifest.
- Forecast: about 200–350 authored changed lines across migration, ingestion, adapter, integration tests, and documentation; revisit at each work-unit boundary. Delivery strategy: ask-on-risk, no chain strategy selected.
- NWQ-1–NWQ-3: implemented and GREEN. The original test-only batch 6 tracer first observed RED on physical line 3 negative gross, then GREEN for 1 valid order, 3 quarantined physical lines with distinct reasons/source file, unchanged persisted counts and no duplicate on exact replay. `pipeline.files` and `reporting.batch_status` show 4 total / 1 inserted / 0 duplicate / 3 rejected; `reporting.daily_orders` counts only one. A separate RED→GREEN cycle corrected replay duplicate count and exposed `rejected_count` in the view. Whitespace-only IDs and lowercase wrong currency have a focused RED→GREEN edge test. Lumen's original atomic rejection and its inability to see Northwind quarantine rows through a restricted login pass as regression checks (first run passed, not RED→GREEN).
- NWQ-4: README documents the tenant-only contract, quarantine inspection query, and manifest boundary. New migration 003 creates the RLS table and count invariant without modifying applied 001/002; worker grants are in `src/db.ts`. The isolated SIGKILL test with its original 29-batch double ingest intermittently exceeded its hard 30-second limit. A controlled 120-second instrumented run passed in 8.497 seconds (first retry and snapshot 5.349s, second 2.711s), so the overrun was not a deterministic lower bound. The test now retries only the crashed source's three Northwind order batches, retaining partial rollback, receipt/raw-row, abandoned-attempt, and clean-snapshot assertions, with its 30-second limit unchanged. Writer `npm run test:integration` passed 20/20; independent verifier repeated it 20/20 in 9.548s (SIGKILL 3.702s). After adding cross-tenant RLS coverage, worker repeated full integration 20/20. Build and 12/12 unit tests pass; `git diff --check` passes. Earlier transient `staging.orders` disappearance was not explained and no baseline flake-rate guarantee is claimed.
- Delivery evidence: work-unit commit `45dc855b902d3f4e7907a983ea4d5f0352ee989d` (`feat: quarantine invalid Northwind orders without dropping valid rows`) on `feat/northwind-order-quarantine`; staged scope was eight intended paths, excluding unrelated `.gitignore` and `odd/tasks/fix-review-findings.md`. Independent pre-commit verification passed 20/20 integration tests, build, 12/12 unit tests, and staged diff check. Native committed-range review START refused before creating a lineage (`candidate-target-projection-drift`); this is not review approval. A separate independent verifier passed, and review unavailability is disclosed rather than bypassed or retried blindly.
- Next: push the feature branch and verify remote identity. Batch 6 remains absent from the checked-in production manifest. No deployment, PR, or merge was requested.
