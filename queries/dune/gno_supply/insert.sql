INSERT INTO crawlers_data.dune_gno_supply (label, block_date, supply)
SELECT label, toDate(parseDateTimeBestEffort(block_date)) AS block_date, toFloat64(supply) AS supply
FROM url(
  'https://api.dune.com/api/v1/query/{{DUNE_GNO_SUPPLY_QUERY_ID}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'label String, block_date String, supply Float64'
);