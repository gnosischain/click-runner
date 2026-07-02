-- owner_address / owners: Dune returns empty text (not the literal 'NULL')
-- when a Safe's SafeSetup event hasn't been decoded yet (possible now that
-- the spine can surface a Safe via the bridge-module or activation signal
-- alone). Convert that explicitly to a true NULL / empty array here rather
-- than storing '' — an empty string is an ambiguous sentinel (looks like a
-- real-but-empty address, and would silently collide with other unknown
-- rows on any join), whereas NULL and [] are unambiguous "no data" markers.
INSERT INTO crawlers_data.celo_gpay_wallets (safe_address, owner_address, owners, issued_at, first_spend_at, is_activated)
-- Use the *OrNull parse variants, not if(x = '', NULL, parse(x)): ClickHouse's
-- if() evaluates both branches columnar-wise before picking per row, so
-- parseDateTimeBestEffort() still runs (and throws) on the '' rows even
-- though that branch is never selected for them. OrNull returns NULL on
-- unparseable input instead of throwing, so there's nothing left to fail.
SELECT
    lower(safe_address)                                                AS safe_address,
    if(owner_address = '', NULL, lower(owner_address))                 AS owner_address,
    if(owners_csv = '', [], splitByChar(',', lower(owners_csv)))       AS owners,
    toDate(parseDateTimeBestEffortOrNull(issued_at))                   AS issued_at,
    toDate(parseDateTimeBestEffortOrNull(first_spend_at))              AS first_spend_at,
    toUInt8(is_activated)                                              AS is_activated
FROM url(
  'https://api.dune.com/api/v1/query/{{CELO_GPAY_WALLETS_QUERY_ID}}/results/csv?api_key={{DUNE_API_KEY}}',
  'CSVWithNames',
  'safe_address String, owner_address String, owners_csv String, issued_at String, first_spend_at String, is_activated Bool'
);
