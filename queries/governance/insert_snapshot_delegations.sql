INSERT INTO {{GOVERNANCE_DATABASE}}.snapshot_delegations (
  tx_hash, block_number, log_index, block_time, action, delegator, delegate
)
SELECT
  lower(tx_hash)         AS tx_hash,
  toUInt64(block_number)  AS block_number,
  toUInt32(log_index)     AS log_index,
  parseDateTimeBestEffort(block_time) AS block_time,
  action,
  lower(delegator)       AS delegator,
  lower(delegate)        AS delegate
FROM url(
  'https://api.dune.com/api/v1/query/{{DUNE_DELEGATIONS_QUERY_ID}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'action String, block_time String, block_number Float64, log_index Float64, tx_hash String, delegator String, delegate String'
);
