# Fixture audit

The supplied manifest expects **30 batches**: two tenants × three sources × five windows, January 6 through February 4, 2026. **29 files are present**, containing **4,498 source records**. The inputs are preserved as supplied; missing data is not synthesized.

## Counts and grains

| Source | Northwind | Lumen | Business grain |
|---|---:|---:|---|
| Orders, received | 694 | 662 | One source row |
| Orders, unique | 680 | 662 | Tenant + order ID |
| Email events | 1,507 | 1,473 | Tenant + event ID |
| Ad rows | 90 | 72 | Tenant + date + normalized platform + campaign ID |

Unique gross order value is **USD 79,303.38** for Northwind and **EUR 79,610.98** for Lumen. These must not be added into a single money total. Unique orders total 1,342; source order rows total 1,356.

## Supplied failure cases

| Evidence | Consequence | Chosen behavior |
|---|---|---|
| `northwind/orders/batch_03.csv`, lines 2–15, repeats 14 identical January 17 orders from batch 02 | Naive summation adds USD 1,922.93 twice | Keep delivery provenance; count each tenant-scoped order once |
| `northwind/email_events/batch_05.ndjson`, lines 305–328, contains 24 new events dated January 12–17 | A latest-event watermark would lose legitimate arrivals | Accept new event IDs and restate event-date reports; January 12–17 count changes from 297 to 321 |
| Both tenants' ad batches 04–05 use `cost_usd` instead of `spend` | A rigid old parser fails; a permissive parser can silently lose amounts | Explicit adapters identify the schema version; unknown shapes fail |
| `lumen/ad_spend/batch_03.csv` is listed in the manifest but absent | January 18–23 ad coverage is incomplete despite later batches | Preserve a missing-batch status, never convert the gap to zero spend |
| 72 date/campaign ad keys occur in both tenants, with different amounts | Globally keyed upserts would overwrite one tenant with another | Tenant identity is part of every business key and database access policy |
| Northwind order channels are `facebook/email/direct/google`; Lumen uses `Meta/Newsletter/Direct/Google` | Equivalent concepts arrive with different labels | Explicit tenant configuration normalizes supported values |
| Email event types are lowercase for Northwind and uppercase for Lumen | Case-sensitive reporting splits equivalent event types | Explicit tenant configuration normalizes supported values |

## Important unknowns

- There are **90 legacy ad rows** with a `spend` field and no declared currency, and **72 USD-labelled rows** with `cost_usd`. A rename does not establish that legacy amounts were USD.
- Order records provide `gross`, currency, channel, timestamp, order ID, and customer email. They do not provide payment/refund/cancellation facts.
- Shared emails and campaign IDs across tenants do not authorize identity joins or attribution. In particular, source campaign names alone do not define how orders should be attributed to ads or email.
- The manifest states expected coverage, not arrival deadlines, timezone rules, or revision ordering. Out-of-window late events are valid here; the coverage window is not a row rejection rule.

## Supplied evidence versus synthetic tests

The provided data contains overlapping exports, late arrivals, a known column rename, a missing batch, and cross-tenant key collisions. It does **not** contain a killed process, an exact repeated whole file, or a changed-payload conflict. Those behaviors must be exercised with deliberately constructed test scenarios. A fault-injection test is evidence of recovery behavior; it is not a claim that a corrupt or crashed file was present in the supplied fixtures.
