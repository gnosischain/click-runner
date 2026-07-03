-- One-time backfill: run manually once via
--   --queries=queries/celo_gpay/create_celo_gpay_owner_events_table.sql,queries/celo_gpay/insert_celo_gpay_owner_events_full.sql
INSERT INTO crawlers_data.celo_gpay_owner_events
    (safe_address, action, owner_address, event_time, tx_hash, log_index)
SELECT
    lower(safe_address)                              AS safe_address,
    action,
    lower(owner_address)                             AS owner_address,
    parseDateTimeBestEffortOrNull(event_time)         AS event_time,
    tx_hash,
    toInt32(log_index)                                AS log_index
FROM url(
  'https://api.dune.com/api/v1/query/{{CELO_GPAY_OWNER_EVENTS_QUERY_ID_FULL}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'safe_address String, action String, owner_address String, event_time String, tx_hash String, log_index Int32'
);
