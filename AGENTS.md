# click-runner — agent guide

Modular ClickHouse ingestion toolkit: one Python ingestor per external source,
packaged as Docker jobs, deployed as Kubernetes CronJobs via Terraform. This
file records how the repo actually works so sessions don't re-derive it.

## Architecture boundaries

- click-runner pulls OFF-CHAIN sources (APIs, CSVs, S3, GraphQL) into ClickHouse
  at snapshot/daily grain. On-chain, block-ordered data belongs to the indexers
  (cryo-indexer, rpc-log-indexer, ...); protocol DECODING belongs to dbt-cerebro
  (`decode_logs` over `execution.logs`). Do not build chain-decoding here.
- Default target database: `crawlers_data`. Exceptions exist (governance_db,
  mixpanel_ga) and are set per-job in Terraform, not hardcoded.

## The BaseIngestor contract (`ingestors/base.py`)

- `__init__(client, variables)` stores a live clickhouse_connect Client and a
  dict of SQL template vars. No sessions/retries — subclasses own transport.
- `load_sql_file(path)` reads + substitutes `{{VAR}}` (dumb string replace; an
  unmatched `{{VAR}}` passes through and fails at ClickHouse — check your vars).
- `ingest(self, skip_table_creation: bool = False, **kwargs) -> bool` is the
  de facto signature. Tables self-create from `queries/<area>/*_create.sql`.
- `execute_queries` labels ALL DDL as `ingestor="query"` in metrics
  (see lesson ddl-attributed-to-query-ingestor).

## House style for new ingestors (follow external_prices / hopr, not forum)

- `requests.Session` + hand-rolled retry loop (nobody uses urllib3.Retry):
  retry 5xx and transport errors with linear/exponential backoff; fail fast on
  other 4xx; treat API-level error payloads as HARD failures — never coerce to
  an empty result (an empty snapshot is indistinguishable from "no data").
- `INSERT_SETTINGS = {"optimize_on_insert": 0, "max_insert_threads": 1}`.
- Idempotency via `ReplacingMergeTree(ingested_at)` with ORDER BY = the stated
  grain. Do NOT use MergeTree + post-insert ALTER DELETE prune
  (see lesson prune-pattern-silent-failure).
- Database via a `{{<AREA>_DATABASE>}}` template var + `--<area>-database` arg,
  merged into query_variables in the runner (pattern: run_hopr_blokli_ingestor).

## New-ingestor checklist (every step, in order)

1. `ingestors/<name>_ingestor.py` (BaseIngestor subclass)
2. `queries/<area>/*_create.sql` DDL with `{{<AREA>_DATABASE}}`
3. `run_queries.py`: import, `--ingestor` choices entry, area args,
   `run_<name>_ingestor()` wrapper (obs.update_health + obs.time_operation),
   dispatch branch in `main()`
4. `docker-compose.yml`: a service per job mode (env-mapped from `.env`)
5. Terraform (`infrastructure-gnosis-analytics-deployments/.../scrapers/
   click-runner/preview/`): locals in `2_data.tf` (service name + cron slot —
   03:00 UTC batch is saturated, stagger in 15-min steps; 04:45+ free as of
   2026-08), a numbered `<NN>_<name>_cron.tf` (ConfigMap + CronJob), and the
   image pin bump (re-rolls ALL jobs — additive changes only)
6. `alerting/alerts/click-runner.yaml`: a freshness row per new table —
   REQUIRED (see lesson ephemeral-pod-metrics-unreliable)

## Deploy model

Merge to main → CI builds ghcr.io/gnosischain/gc-click-runner:<short-sha> →
NOTHING deploys until the digest pin in `2_data.tf` is bumped. Applies are
`-target`-only: blanket `terraform apply` re-runs immutable Jobs against
production data and is forbidden (see that stack's AGENTS.md).

## Observability truth

Prometheus counters and `/health` are unreliable for ephemeral CronJob pods
(scrape races; "No Data" between runs). The real alarm is the ClickHouse
freshness query set in `alerting/alerts/click-runner.yaml` plus Loki log
events. A job can be green while its table goes stale — only freshness
alerting catches that.

## Lessons

`docs/lessons/INDEX.md` — status-tracked (`observed` → `remediated` →
`enforced`), evidence required. Add a record whenever a new mistake class is
diagnosed; update status when a safeguard lands.
