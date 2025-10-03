INSERT INTO crawlers_data.dune_labels (address, label, introduced_at, source)
SELECT address, label, parseDateTimeBestEffort(introduced_at) AS introduced_at, source
FROM url(
  'https://api.dune.com/api/v1/execution/{{DUNE_LABELS_EXECUTION_ID}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'address String, label String, introduced_at String, source String'
);