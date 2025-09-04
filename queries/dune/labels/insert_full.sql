INSERT INTO playground_max.dune_labels (address, label, introduced_at)
SELECT address, label, parseDateTimeBestEffort(introduced_at) AS introduced_at
FROM url(
  'https://api.dune.com/api/v1/query/{{DUNE_QUERY_ID_FULL}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'address String, label String, introduced_at String, source String'
);