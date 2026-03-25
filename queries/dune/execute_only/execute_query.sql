INSERT INTO FUNCTION url(
  'https://api.dune.com/api/v1/query/{{DUNE_EXECUTE_ONLY_QUERY_ID}}/execute',
  'RawBLOB',
  'payload String',
  headers(
    'Content-Type'='application/json',
    'X-Dune-Api-Key'='{{DUNE_API_KEY}}'
  )
)
SELECT '{}';
