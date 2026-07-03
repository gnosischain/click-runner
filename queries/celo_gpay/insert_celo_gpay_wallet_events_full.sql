-- One-time backfill: run manually once via
--   --queries=queries/celo_gpay/create_celo_gpay_wallet_events_table.sql,queries/celo_gpay/insert_celo_gpay_wallet_events_full.sql
-- action_value can come back non-NULL-but-empty from Dune when an issued_at
-- event's owner isn't known yet (SafeSetup not decoded when the wallet was
-- recognized via the activation signal instead): observed as both a plain
-- empty string AND, for a LEFT JOIN UNNEST-produced null specifically, the
-- literal 5-char text '<nil>' (confirmed via playground_max backfill
-- verification, 5/230 wallets) — Dune's CSV serialization of that null
-- apparently differs from a plain empty field. Convert both to a true NULL
-- explicitly, same reasoning as the old owner_address handling: neither
-- sentinel is an unambiguous "no data" the way NULL is.
INSERT INTO crawlers_data.celo_gpay_wallet_events
    (safe_address, action, action_value, event_time)
SELECT
    lower(safe_address)                                                AS safe_address,
    action,
    if(action_value IN ('', '<nil>'), NULL, lower(action_value))       AS action_value,
    parseDateTimeBestEffortOrNull(event_time)                          AS event_time
FROM url(
  'https://api.dune.com/api/v1/query/{{CELO_GPAY_WALLET_EVENTS_QUERY_ID_FULL}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'safe_address String, action String, action_value String, event_time String'
);
