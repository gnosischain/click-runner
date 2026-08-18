# prune-pattern-silent-failure

**Status:** observed

**Symptom:** duplicate rows accumulate in a table whose ingestor "deduplicates"
after insert; or dedup silently stopped at some past date.

**Cause:** the older idempotency pattern (external_prices) inserts into a plain
MergeTree then prunes duplicates with a post-insert `ALTER TABLE ... DELETE`.
If the ClickHouse user lacks the ALTER grant, the prune fails while the insert
succeeded — the job reports success, the duplicates stay.

**Rule:** for snapshot/daily-grain tables use `ReplacingMergeTree(ingested_at)`
with ORDER BY = the grain (forum, hopr pattern). Idempotency belongs in the
engine, not in a follow-up mutation that can fail independently.

**Evidence:** `ingestors/external_prices_ingestor.py` prune path vs. the
ReplacingMergeTree DDLs in `queries/governance/` and `queries/hopr/`.
