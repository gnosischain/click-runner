# edge-403-is-a-throttle

**Status:** enforced (cow ingestor); observed elsewhere

**Symptom:** the cow-fees job exits 0 with `run_success` and inserts a sliver
of the day's trades; the log is a wall of `cow_owner_api_error` lines with no
status code. On 2026-09-10 a GKE run logged 1,626 of them and nothing alerted.

**Cause:** CoW's edge is CloudFront with a per-source-IP rate rule. It answers
the rule with a **403** (body: CloudFront's "The request could not be
satisfied" page), not a 429, and the block lasts about an hour. The ingestor
only backed off on 429; `raise_for_status()` turned the 403 into one error
line, the owner was counted as "skipped", `api_failed` stayed False, so the
10-consecutive-failure abort never tripped and the run reported success. The
trip point measured that day was ~2,000-3,500 requests inside a 5-13 minute
burst from one IP — and on GKE the IP is a single Cloud NAT address shared
with cow-indexer, whose own bursts kept it in cooldown most of every hour.

**Rule:** from a CDN-fronted API, a 403 with an HTML body is a throttle, not
an authorisation error. Retry it with a long backoff (honour `Retry-After`),
log the status, body head and edge request id (`x-amz-cf-id` / `x-cache`) so
the edge is identifiable, count a give-up as a failed owner, and make a run
with any failed owner exit non-zero. Cap the process with a sliding-window
request budget; a fixed per-request sleep does not bound the rate when the
egress IP is shared. `ingestors/cow_ingestor.py`: `THROTTLE_STATUSES`,
`MAX_REQUESTS_PER_WINDOW`, `_api_get`, `cow_run_summary`.

**Evidence:** infrastructure-gnosis-analytics-deployments
`google/deployments/gnosis-analytics/click-runner/README.md` ("Known
follow-ups"), Grafana rule `cow-fees-api-errors`, and
`tests/test_cow_ingestor_throttle.py`.
