# Lessons index

Mistake classes this repo has already paid for. Status: `observed` (seen, no
safeguard) → `remediated` (instance fixed, recurrence possible) → `enforced`
(a gate/code fix prevents recurrence). Evidence required. Check here before
diagnosing a data-quality symptom or changing an ingestor's write path.

- [backfill-gated-series-freeze](backfill-gated-series-freeze.md) `remediated` — gating a full-series re-fetch behind a backfill mode froze the series silently while daily runs stayed green
- [prune-pattern-silent-failure](prune-pattern-silent-failure.md) `observed` — MergeTree + post-insert ALTER DELETE prune fails silently without the ALTER grant
- [ephemeral-pod-metrics-unreliable](ephemeral-pod-metrics-unreliable.md) `enforced` — Prometheus counters read No Data between CronJob runs; ClickHouse freshness alerts are the real backstop
- [ddl-attributed-to-query-ingestor](ddl-attributed-to-query-ingestor.md) `observed` — all DDL metrics carry ingestor="query" regardless of caller
- [cdn-requires-browser-ua](cdn-requires-browser-ua.md) `observed` — network.hoprnet.org's CDN serves error pages to non-browser user agents
