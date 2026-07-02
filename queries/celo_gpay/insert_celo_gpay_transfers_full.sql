-- One-time backfill: run manually once via
--   --queries=queries/celo_gpay/create_celo_gpay_transfers_table.sql,queries/celo_gpay/insert_celo_gpay_transfers_full.sql
-- Cheap because GP Celo launched 2026-06-23 — "full history" is only ~1 week of data.
-- Raw rows only — classification happens downstream in dbt (int_celo_gpay_activity.sql).
INSERT INTO {{CELO_GPAY_DB}}.celo_gpay_transfers
    (block_date, block_time, tx_hash, sender, receiver, token_symbol, token_address, amount, amount_usd)
SELECT
    toDate(parseDateTimeBestEffortOrNull(block_date))  AS block_date,
    parseDateTimeBestEffortOrNull(block_time)          AS block_time,
    tx_hash,
    lower(sender)                                      AS sender,
    lower(receiver)                                    AS receiver,
    token_symbol,
    lower(token_address)                               AS token_address,
    amount,
    amount_usd
FROM url(
  'https://api.dune.com/api/v1/query/{{CELO_GPAY_TRANSFERS_QUERY_ID_FULL}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'block_date String, block_time String, tx_hash String, sender String, receiver String, token_symbol String, token_address String, amount Float64, amount_usd Nullable(Float64)'
);
