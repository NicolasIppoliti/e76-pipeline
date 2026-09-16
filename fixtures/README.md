# Deep dive fixtures

Three sources for two tenants, delivered the way exports actually land: a
sequence of batch files per source, one format per source.

- `orders/`        CSV, one row per order
- `email_events/`  NDJSON, one JSON object per line
- `ad_spend/`      CSV, daily spend per campaign

`manifest.json` lists every batch the set is supposed to contain, with the
window each one covers.

These fixtures contain the failures described in the brief. They are there
on purpose and they are not all obvious. Reading them before you start
building is time well spent.
