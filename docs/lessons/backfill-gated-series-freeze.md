# backfill-gated-series-freeze

**Status:** remediated (2026-08, PR #17)

**Symptom:** a time series stops advancing while the daily job that owns it
keeps succeeding. No error anywhere.

**Cause:** the hourly online-nodes series (hopr-network) was fetched only in
`--mode=backfill`. The upstream API returns the WHOLE series on every call —
which was misread as "only needs fetching once". New hours accrue continuously,
so the daily job updated the roster while the series stayed frozen at the last
manual backfill.

**Fix:** both modes now fetch the full series every run; ReplacingMergeTree
collapses the repeats (~20k rows, negligible). This also makes the table
self-healing against upstream gaps — an "only insert rows newer than
max(observed_at)" optimisation would permanently forfeit that repair.

**Rule:** "returns everything each time" means it needs no INCREMENTAL logic,
not that it needs no RE-RUNNING. Never gate a growing series behind a
manually-triggered mode.

**Evidence:** click-runner PR #17, `hopr_network_ingestor.py` call-site comment
and `queries/hopr/network_online_hourly_create.sql` header.
