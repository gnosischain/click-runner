-- Daily full-replace of the Circles v2 bot/sybil blacklist.
--
-- Upstream is a JSON HTTP endpoint with no timestamp:
--   { "addresses": [ { "address": "0x..", "reason": "..." }, ... ] }
--
-- We rely on a preceding TRUNCATE (see truncate.sql in the same folder)
-- because click-runner executes each query file as a single statement and
-- the endpoint returns the full current state on every call 
INSERT INTO crawlers_data.circles_blacklisted (address, reason)
SELECT
    lower(JSONExtractString(item, 'address')) AS address,
    nullIf(JSONExtractString(item, 'reason'), '') AS reason
FROM url(
    '{{CIRCLES_BLACKLIST_URL}}',
    'JSONAsString',
    'json String'
)
ARRAY JOIN JSONExtractArrayRaw(JSONExtractRaw(json, 'addresses')) AS item
WHERE JSONExtractString(item, 'address') != ''
