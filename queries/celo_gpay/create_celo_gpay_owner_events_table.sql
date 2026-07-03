-- Raw AddedOwner/RemovedOwner events for GP Celo card Safes. Immutable
-- facts (an on-chain event never changes once it happens), same nature as
-- celo_gpay_transfers, so same ingestion shape: full backfill + daily
-- 7-day-window append, deduped via ReplacingMergeTree + FINAL reads.
CREATE TABLE IF NOT EXISTS crawlers_data.celo_gpay_owner_events
(
    `safe_address`    String,
    `action`          LowCardinality(String),
    `owner_address`   String,
    `event_time`      DateTime,
    `tx_hash`         String,
    `log_index`       Int32
)
ENGINE = ReplacingMergeTree
PARTITION BY toStartOfMonth(event_time)
ORDER BY (safe_address, owner_address, action, event_time, tx_hash, log_index);
