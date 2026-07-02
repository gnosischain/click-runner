INSERT INTO crawlers_data.dune_bridge_flows (
  timestamp, bridge, source_chain, dest_chain, token, amount_token, amount_usd, net_usd
)
SELECT
  parseDateTimeBestEffort(timestamp) AS timestamp,
  bridge,
  source_chain,
  dest_chain,
  token,
  toFloat64(amount_token) AS amount_token,
  toFloat64(amount_usd)   AS amount_usd,
  toFloat64(net_usd)      AS net_usd
FROM url(
  'https://api.dune.com/api/v1/query/{{DUNE_BRIDGE_FLOWS_QUERY_ID_FULL}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'timestamp String, bridge String, source_chain String, dest_chain String, token String, amount_token Float64, amount_usd Float64, net_usd Float64'
);