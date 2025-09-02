INSERT INTO playground_max.gnosis_daily_bluechip_prices (block_date, symbol, price)
SELECT toDate(parseDateTimeBestEffort(block_date)) AS block_date, symbol, toFloat64(price) AS price
FROM url(
  'https://api.dune.com/api/v1/query/5701957/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'block_date String, symbol String, price Float64'
)
WHERE toDate(parseDateTimeBestEffort(block_date)) BETWEEN toDate('{{START_DATE}}') AND toDate('{{END_DATE}}');