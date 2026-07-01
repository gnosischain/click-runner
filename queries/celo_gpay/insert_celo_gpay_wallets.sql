INSERT INTO crawlers_data.celo_gpay_wallets (safe_address, owner_address, issued_at, first_spend_at, is_activated)
SELECT
    lower(safe_address)                          AS safe_address,
    lower(owner_address)                         AS owner_address,
    toDate(parseDateTimeBestEffort(issued_at))   AS issued_at,
    if(first_spend_at = '', NULL, toDate(parseDateTimeBestEffort(first_spend_at))) AS first_spend_at,
    toUInt8(is_activated)                        AS is_activated
FROM url(
  'https://api.dune.com/api/v1/query/{{CELO_GPAY_WALLETS_QUERY_ID}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'safe_address String, owner_address String, issued_at String, first_spend_at String, is_activated UInt8'
);
