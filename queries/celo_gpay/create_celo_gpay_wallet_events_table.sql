-- Unified append-only wallet-lifecycle event log for GP Celo card Safes:
-- issued_at (initial owner(s), unrolled from SafeSetup) + add_owner +
-- remove_owner. Replaces the old celo_gpay_wallets table (per-safe rows
-- whose is_activated/first_spend_at/owner_address mutated over time) and
-- celo_gpay_owner_events. A source with mutable rows breaks incremental
-- models downstream (a row processed once, before it's fully caught up,
-- never gets a correct second look) — an append-only event log has no such
-- row to ever change, so this is immune to that class of problem by
-- construction, not by carefully keeping two separate models in sync.
-- first_spend_at/is_activated are NOT events here — derived in dbt
-- directly from crawlers_data.celo_gpay_transfers instead (also per
-- feedback: that fact is already fully present in the transactions data,
-- no need to ingest it twice).
CREATE TABLE IF NOT EXISTS crawlers_data.celo_gpay_wallet_events
(
    `safe_address`    String,
    `action`          LowCardinality(String),
    `action_value`    Nullable(String),
    `event_time`      DateTime,
    `ingested_at`      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
PARTITION BY toStartOfMonth(event_time)
ORDER BY (safe_address, action, action_value, event_time)
SETTINGS allow_nullable_key = 1;
