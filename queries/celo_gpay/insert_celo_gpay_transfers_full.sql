-- One-time backfill: run manually once via
--   --queries=queries/celo_gpay/create_celo_gpay_transfers_table.sql,queries/celo_gpay/insert_celo_gpay_transfers_full.sql,queries/celo_gpay/optimize_celo_gpay_transfers.sql
-- Cheap because GP Celo launched 2026-06-23 — "full history" is only ~1 week of data.
-- Points at a Dune query with no date floor (or floored at launch date).
INSERT INTO crawlers_data.celo_gpay_transfers
    (block_date, block_time, tx_hash, safe_address, token_symbol, token_address, action, amount, amount_usd, counterparty)
SELECT
    toDate(parseDateTimeBestEffort(block_date))  AS block_date,
    parseDateTimeBestEffort(block_time)          AS block_time,
    tx_hash,
    lower(safe_address)                          AS safe_address,
    token_symbol,
    lower(token_address)                         AS token_address,
    action,
    toFloat64(amount)                            AS amount,
    toFloat64(amount_usd)                        AS amount_usd,
    lower(counterparty)                          AS counterparty
FROM url(
  'https://api.dune.com/api/v1/query/{{CELO_GPAY_TRANSFERS_QUERY_ID_FULL}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'block_date String, block_time String, tx_hash String, safe_address String, token_symbol String, token_address String, action String, amount Float64, amount_usd Float64, counterparty String'
);
