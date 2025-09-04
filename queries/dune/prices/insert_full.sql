INSERT INTO playground_max.gnosis_daily_bluechip_prices (block_date, symbol, price)
SELECT toDate(parseDateTimeBestEffort(block_date)) AS block_date, symbol, toFloat64(price) AS price
FROM url(
  'https://api.dune.com/api/v1/query/{{DUNE_QUERY_ID_FULL}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'block_date String, symbol String, price Float64'
);