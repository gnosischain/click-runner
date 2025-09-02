INSERT INTO playground_max.dune_labels (address, label, introduced_at, source)
SELECT address, label, parseDateTimeBestEffort(introduced_at) AS introduced_at, source
FROM url(
  'https://api.dune.com/api/v1/query/5718249/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'address String, label String, introduced_at String, source String'
);