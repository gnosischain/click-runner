-- Daily incremental: 3-day lookback window, same resilience/overlap
-- reasoning as insert_celo_gpay_transfers_daily.sql (tolerates the job
-- not running for up to 2 days straight).
-- action_value NULL sentinel handling: see insert_celo_gpay_wallet_events_full.sql
-- header comment ('' and Dune's '<nil>' literal both mean unknown owner).
INSERT INTO crawlers_data.celo_gpay_wallet_events
    (safe_address, action, action_value, event_time)
SELECT
    lower(safe_address)                                                AS safe_address,
    action,
    if(action_value IN ('', '<nil>'), NULL, lower(action_value))       AS action_value,
    parseDateTimeBestEffortOrNull(event_time)                          AS event_time
FROM url(
  'https://api.dune.com/api/v1/query/{{CELO_GPAY_WALLET_EVENTS_QUERY_ID_DAY}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'safe_address String, action String, action_value String, event_time String'
);
