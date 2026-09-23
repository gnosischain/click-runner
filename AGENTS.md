# AGENTS.md — click-runner

Modular ClickHouse ingestion toolkit: one Python ingestor per OFF-CHAIN source, one image, one
CLI (`run_queries.py --ingestor=<mode>`), deployed as Kubernetes CronJobs on GKE Autopilot via
Terraform. Production data flow: external API / CSV URL / S3 Parquet / GraphQL → `run_queries.py`
→ ClickHouse Cloud databases `crawlers_data`, `governance_db`, `mixpanel_ga`, `hopr_db`.
Boundary: off-chain sources only, at snapshot/daily grain; on-chain block-ordered data belongs to the indexers
(cryo-indexer, rpc-log-indexer, …) and protocol decoding to dbt-cerebro (`decode_logs` over `execution.logs`) — never build chain-decoding here.
The single most important rule: **a green job is not fresh data** — the only health signal is the
per-table freshness query below, and every number in this file is re-derivable from the code it cites.

## Where this runs (production)

GKE Autopilot; 14 CronJobs from ONE Terraform stack in the private deployments repository
(`gnosisdevops/infrastructure-gnosis-analytics`, the click-runner stack under its GKE tree:
`locals.tf` holds an `ingestors` map and a `configmaps` map; `cronjobs.tf` renders the first and `configmaps.tf` the
second, each ConfigMap merged with the shared observability and ClickHouse keys, `configmaps.tf:19`). Every cron
runs the image's `ENTRYPOINT ["python", "run_queries.py"]` (Dockerfile:24): 12 append flags as `args`,
2 (Dune daily, ProbeLab) replace `command` with `/bin/bash -c` plus a script from the stack's `scripts/` (stack `cronjobs.tf:111-115`),
so a one-leg rerun of those two must override `command` too or pass the whole invocation as ONE shell-string arg (a bare flag list under `bash -c` fails); every other cron needs an `args` override only.
CI builds and pushes both `:latest` and `:<short-sha>` of `ghcr.io/gnosischain/gc-click-runner` on every push to `main`
(`.github/workflows/build-and-release.yml:43-53`); production pins `<short-sha>@sha256` digests only, so `:latest` is never
what runs. Nothing deploys until a pin in the stack's `locals.tf` moves, and there are THREE pins (default / dune+hopr / cow,
`locals.tf:20-38`), so a bump rolls only its crons, each at its next slot.

| Role | What it runs (args to `run_queries.py`) | Schedule (UTC) | Writes |
| --- | --- | --- | --- |
| Dune daily, 3 legs in one pod (bash wrapper) | `--ingestor=csv --create-table-sql=queries/dune/<leg>/create_table.sql --insert-sql=queries/dune/<leg>/insert_daily.sql` for `labels`, `prices`, `bridge_flows` | `0 3 * * *` | `crawlers_data.dune_labels`, `dune_prices`, `dune_bridge_flows` |
| CoW fees daily | `--ingestor=cow --cow-mode=daily --cow-lookback-days=7 --create-table-sql=queries/cow/create_table.sql --table-name=crawlers_data.cow_api_trade_fees --cow-source-table=dbt.int_execution_cow_trades --cow-request-delay=0.25` | `0 3 * * *` | `crawlers_data.cow_api_trade_fees` |
| Mixpanel events daily | `--ingestor=mixpanel --mixpanel-mode=daily --create-table-sql=queries/mixpanel/create_events_table.sql` | `0 3 * * *` | `mixpanel_ga.mixpanel_raw_events`, `mixpanel_ga.mixpanel_ingestion_state` |
| Circles blacklist daily | `--ingestor=query --queries=queries/circles/blacklist/create_table.sql,…/truncate.sql,…/insert.sql,…/optimize.sql` | `0 3 * * *` | `crawlers_data.circles_blacklisted` (TRUNCATE, then INSERT from `url()`) |
| Mixpanel profiles daily | `--ingestor=mixpanel-profiles --create-table-sql=queries/mixpanel_profiles/create_profiles_table.sql` | `30 3 * * *` | `mixpanel_ga.mixpanel_raw_profiles` |
| Snapshot governance daily | `--ingestor=snapshot --snapshot-mode=daily` | `45 3 * * *` | `governance_db.snapshot_{space,proposals,votes,follows}` |
| Forum governance daily | `--ingestor=forum --forum-mode=daily` | `0 4 * * *` | `governance_db.forum_{categories,topics,posts,users,likes,polls}` |
| DefiLlama prices daily | `--ingestor=external-prices --external-prices-source=defillama --external-prices-mode=daily --external-prices-daily-lag-days=1 --external-prices-database=crawlers_data` | `15 4 * * *` | `crawlers_data.defillama_prices` |
| CoinGecko prices daily | as above with `--external-prices-source=coingecko` | `30 4 * * *` | `crawlers_data.coingecko_prices` |
| HOPR Blokli daily | `--ingestor=hopr-blokli --hopr-blokli-networks=jura --hopr-database=hopr_db` | `45 4 * * *` | `hopr_db.hopr_blokli_network_snapshot`, `hopr_db.hopr_blokli_nodes` |
| HOPR network daily | `--ingestor=hopr-network --hopr-network-ids=3 --hopr-network-mode=daily --hopr-database=hopr_db` | `0 5 * * *` | `hopr_db.hopr_network_nodes`, `hopr_db.hopr_network_online_hourly` |
| Ember, twice a month | `--ingestor=csv --create-table-sql=queries/ember/create_ember_table.sql --insert-sql=queries/ember/insert_ember_data.sql --optimize-sql=queries/ember/optimize_ember_data.sql` | `0 0 8,21 * *` | `crawlers_data.ember_electricity_data` |
| Dune execute-only (created SUSPENDED) | `--ingestor=dune-execute-only` | `0 3 * * *`, never fires | nothing |
| ProbeLab latest (created SUSPENDED) | bash wrapper: 13 × `--ingestor=parquet … --mode=latest` chained with `&&` | `0 3 * * *`, never fires | `crawlers_data.probelab_*` (13 tables) |

Sources: stack `locals.tf:216-481` and its two wrapper scripts under `scripts/`.
Env every cron gets: `CH_PORT=443`, `CH_SECURE=true`, `CH_VERIFY=False`, `OBSERVABILITY_ENABLED=true`,
`OBSERVABILITY_PORT=9090`, `LOG_FORMAT=json`, `PYTHONUNBUFFERED=1` (stack `locals.tf:61-75`); `CLICK_RUNNER_JOB_NAME` from the pod's `app`
label (`cronjobs.tf:148-155`, read at `observability.py:168-169`). Per role: `CH_QUERY_VAR_COW_DATABASE=crawlers_data`,
`COW_SOURCE_TABLE=dbt.int_execution_cow_trades`, `CH_QUERY_VAR_GOVERNANCE_DATABASE=governance_db`,
`CH_QUERY_VAR_SNAPSHOT_SPACE=gnosis.eth`, `HOPR_DATABASE=hopr_db`, `EXTERNAL_PRICES_DATABASE=crawlers_data`,
`MIXPANEL_REGION=EU`, `CH_QUERY_VAR_MIXPANEL_DATABASE=mixpanel_ga` (`locals.tf:78-142,197-200`). Also per role: `CH_DB` =
`crawlers_data` / `governance_db` (forum, snapshot) / `hopr_db` / `mixpanel_ga` (`locals.tf:80-139`);
`CH_QUERY_VAR_CIRCLES_BLACKLIST_URL`, `CH_QUERY_VAR_EMBER_DATA_URL`, `CH_QUERY_VAR_DISCOURSE_BASE_URL`,
`DUNE_EXECUTE_ONLY_QUERY_IDS=4565681`, `CH_QUERY_VAR_S3_REGION=us-east-1` (`locals.tf:81,99,104,116,140`). The Dune
ConfigMap keys `DUNE_{LABELS,PRICES,BRIDGE_FLOWS}_QUERY_ID` reach the pod renamed to `CH_QUERY_VAR_DUNE_<LEG>_QUERY_ID_DAY`
through `configMapKeyRef` (`locals.tf:230-234`, `cronjobs.tf:187-203`), so a Job that copies only `envFrom` gets no query
IDs; only a Job cloned from the CronJob's own template keeps them. Secret env is per cron (`locals.tf:163-171` and each entry's `secret_env`): the Dune key on the two Dune crons, the
CoinGecko key on coingecko only, the CoW key on cow only, the S3 keys on probelab only (`locals.tf:229,250,293,322,477`). The two Mixpanel crons
connect as a SEPARATE ClickHouse user (stack `secrets.tf:15-21`); the other 12 share one scraper user.

**Restart semantics:** `restart_policy = "OnFailure"`, `backoff_limit = 6`, no `active_deadline_seconds` (stack
`cronjobs.tf:28-32,46-48,75`). A non-zero exit re-runs the WHOLE invocation in the same pod up to 6 times, with kubelet
back-off from seconds up to 5 min: a partial CoW run (exit 1, ingestors/cow_ingestor.py:834) re-hits the edge inside its
~1 h 403 cooldown, although the comment at cow_ingestor.py:802-804 assumes the retry comes after the block clears; circles
re-TRUNCATEs on every attempt; a failed Dune leg is never retried, because the wrapper exits 0 unless all three legs fail.

**LOCAL-ONLY artifacts:** `docker-compose.yml` (34 services, `.env`-substituted by compose), `.env` / `.env.example`,
`credentials_dgrive.json`, `test.ipynb`, `tests/`. There is no Makefile. `scripts/` is copied into the image
(Dockerfile:16) but nothing schedules it. Production runs only the CLI invocations in the table above.

**Local → production blast radius:** with production credentials in `.env`, EVERY compose service writes production tables (targets are fully qualified; `CH_DB` does
not redirect them), using your local uncommitted code: compose builds `.` and bind-mounts `./queries` (docker-compose.yml:3-8), `./config` into the two
external-prices services (a locally edited token allowlist runs against production; docker-compose.yml:804,832) and the host's Google ADC credentials file into
the gpay-wallets compose service (docker-compose.yml:253); it never pulls the pinned image. Exceptions: mixpanel defaults to database `mixpanel` and hopr to `crawlers_data` (production
`hopr_db`), so they create stray tables (docker-compose.yml:613-620,869-873). A bare `python run_queries.py` does NOT read `.env` (no dotenv call in any `*.py`,
although python-dotenv is in requirements.txt:3); it uses only the shell env.

**Single-writer rule:** one cron per target table; `concurrency_policy = "Forbid"` guards each cron against itself only (stack `cronjobs.tf:32`). The stack's
`cutover_complete` (true since the GKE move, `locals.tf:209-212`) now only selects whether each ingestor's `suspend_after_cutover` applies (`cronjobs.tf:23`); there
is no other cluster. The app has no lock, lease or heartbeat: a hand-made Job, a compose run or a laptop run is a second writer that nothing detects. Convention
only.

**Deliberately NOT deployed:** every backfill/repair mode (`--cow-mode=backfill|repair`, `--mixpanel-mode=backfill`,
`--snapshot-mode=backfill`, `--forum-mode=backfill`, `--external-prices-mode=backfill`, the Dune `insert_full.sql`
files, parquet `--mode=all|date`), `--ingestor=gdrive`, `scripts/full_history_coingecko_prices.py`, the Celo GP
crons (decommissioned 2026-09-10; compose services remain), and the old cluster's 12 one-shot Job resources
(stack `README.md:64-68`, which is also why this stack needs no targeted-apply discipline). No Deployment exists:
`cronjobs.tf` is the stack's only workload file.

## Modes and commands — the complete list

`--ingestor` accepts exactly 13 values, default `query` (run_queries.py:129). With no args at all (the image ENTRYPOINT
alone) it runs query mode (run_queries.py:761-762): the metrics server starts if `OBSERVABILITY_ENABLED` (690-695), it
connects to ClickHouse FIRST (726-734; exit 1 if that fails), then runs the files in `CH_QUERIES` if set, else logs
"No queries specified" and exits 1 (370-378). Connection flags `--host --port --user --password --db --secure --verify`
default from `CH_HOST CH_PORT CH_USER CH_PASSWORD CH_DB CH_SECURE CH_VERIFY`
(`localhost`/`9000`/`default`/``/`default`/`False`/`True`, run_queries.py:120-126). `{{VAR}}` in any SQL file is
replaced from `CH_QUERY_VAR_VAR` (collected at run_queries.py:77-96, substituted at ingestors/base.py:74-93; dune-execute-only
uses run_queries.py:102-113). Runners override a few names: `HOPR_DATABASE` and `EXTERNAL_PRICES_DATABASE` always come from
their flags (run_queries.py:611,651,672); `MIXPANEL_DATABASE` / `GOVERNANCE_DATABASE` are re-injected as the `CH_QUERY_VAR_`
value or else the default `mixpanel` / `governance_db` (402,415,449,462,550,557,577,582); `DUNE_EXECUTE_ONLY_QUERY_ID`,
`GDRIVE_FILE_ID` and `TARGET_TABLE` are set per run (495; ingestors/gdrive_ingestor.py:58-59). Observability env:
`OBSERVABILITY_ENABLED` (observability.py:165), `OBSERVABILITY_PORT` (default 9090, run_queries.py:693), `LOG_FORMAT`,
`LOG_LEVEL` (observability.py:293-297), and `CLICK_RUNNER_JOB_NAME`, which falls back to `HOSTNAME` and then `local`
(observability.py:169).

Env twins that override a default with no flag (an inherited value wins silently): `CH_TABLE_NAME` (target for gdrive,
mixpanel, mixpanel-profiles and cow; run_queries.py:320,403,450,518); `COW_SOURCE_TABLE` / `COW_BACKFILL_FROM` /
`COW_REQUEST_DELAY` (174-182); `EXTERNAL_PRICES_{SOURCE,MODE,TOKENS_CONFIG,CHART_SPAN_DAYS,DAILY_LAG_DAYS,DEFILLAMA_FULL_HISTORY,DATABASE}`
(220-245; `SOURCE` defaults to `both`, and an inherited `EXTERNAL_PRICES_MODE=backfill` flips the mode); `MIXPANEL_REGION`
(default US, production EU; 163, stack `locals.tf:135`); `CH_MIXPANEL_STATE_SQL` (389); `HOPR_*` (203-218);
`DUNE_EXECUTE_ONLY_QUERY_IDS` (150-154); `CH_QUERIES` (257,290,327,375,392,439); `CH_GDRIVE_FILE_ID` (319). SQL vars the
production SQL needs: query mode (circles) needs `CH_QUERY_VAR_CIRCLES_BLACKLIST_URL` (queries/circles/blacklist/insert.sql:14);
csv mode (Ember) needs `CH_QUERY_VAR_EMBER_DATA_URL` (queries/ember/insert_ember_data.sql:44).

| Invocation | What it does | Writes? | Safe beside the live writer? | Required args / env | Source |
| --- | --- | --- | --- | --- | --- |
| `--ingestor=query --queries=a.sql,b.sql` | renders each file, runs each as ONE `client.command` in order, stops at the first failure | whatever the SQL does (prod: TRUNCATE+INSERT) | only if the SQL is | `--queries` or `CH_QUERIES`; `--skip-table-creation` ignored | run_queries.py:370-384; ingestors/base.py:96-118 |
| `--ingestor=csv --create-table-sql=… --insert-sql=… [--optimize-sql=…]` | CREATE (unless skipped) → `INSERT … FROM url()` → row-count delta → optional OPTIMIZE | append | NO for Dune tables (plain MergeTree: duplicates); yes for Ember (`ReplacingMergeTree(version)`) | both paths, or `CH_QUERIES` positions 0/1/2; Dune legs need `CH_QUERY_VAR_DUNE_API_KEY` + `CH_QUERY_VAR_DUNE_<LEG>_QUERY_ID_DAY` / `_FULL` | run_queries.py:249-278; ingestors/csv_ingestor.py:40-103 |
| `--ingestor=parquet --create-table-sql=… --s3-path=…/{{DATE}}.parquet --table-name=db.t --mode=latest` | newest `YYYY-MM-DD.parquet` under the prefix, by filename date | append (`ReplacingMergeTree()`, dedupes on merge) | yes; the cron is suspended | `--create-table-sql` (or `CH_QUERIES` position 0), `--s3-path`, `--table-name` (all required, else exit 1, run_queries.py:289-296); `CH_QUERY_VAR_S3_ACCESS_KEY`, `_S3_SECRET_KEY`, `_S3_BUCKET` (`_S3_REGION` defaults to `us-east-1`, parquet_ingestor.py:78) | run_queries.py:280-312; ingestors/parquet_ingestor.py:75-97; utils/s3.py:61-114 |
| `… --mode=date --date=YYYY-MM-DD` | one file with `{{DATE}}` substituted | append | yes | `--date` required | ingestors/parquet_ingestor.py:99-107 |
| `… --mode=all` | every object under the prefix, one INSERT each | append | yes, but re-inserts all history | | ingestors/parquet_ingestor.py:109-121 |
| `--ingestor=gdrive --create-table-sql=… --file-id=… [--table-name=…] [--optimize-sql=…] [--max-rows=1000000]` | downloads a Drive CSV with Application Default Credentials, maps columns via DESCRIBE, `client.insert` | append (plain MergeTree) | not deployed; duplicates on re-run | `--file-id` or `CH_GDRIVE_FILE_ID`; `GOOGLE_APPLICATION_CREDENTIALS` (also via `CH_QUERY_VAR_`) | run_queries.py:314-368; ingestors/gdrive_ingestor.py:62-63,73-77,348-420 |
| `--ingestor=dune-execute-only --dune-execute-only-query-ids=a,b` | POSTs Dune `/execute` per id via `INSERT INTO FUNCTION url()`; continues past failures; exit 1 if any failed | no table | yes (spends Dune credits); the cron is suspended | ids or `DUNE_EXECUTE_ONLY_QUERY_IDS`; `CH_QUERY_VAR_DUNE_API_KEY` | run_queries.py:476-513; queries/dune/execute_only/execute_query.sql:1-10 |
| `--ingestor=mixpanel --mixpanel-mode=daily [--mixpanel-region=US\|EU\|IN] [--mixpanel-event-filter=…] [--table-name=…]` | from `max(completed_date)+1` in `<database of the target table>.mixpanel_ingestion_state` (mixpanel_ingestor.py:99-100; diverges from the DDL's `{{MIXPANEL_DATABASE}}`, queries/mixpanel/create_state_table.sql:1, if `--table-name` or `CH_TABLE_NAME` points elsewhere) to yesterday; skips complete days; a day < 3 days old with 0 events is retried next run, but a day with ANY events is marked complete on first fetch (mixpanel_ingestor.py:259-262), so a partially-exported yesterday is never refetched by the cron (refill: delete that day's state row, then a backfill of that day); ≥ 100k events/day splits by hour; a failed state write is only a warning, not an exit 1 (231-243); that day is re-fetched next run ONLY if no later day was marked in the same run, because daily resumes at `max(completed_date)+1` (178-182); otherwise it keeps its events but never gets a state row | append (`ReplacingMergeTree()`) + state row per day | skips days the cron completed, but it is still a second writer | `CH_QUERY_VAR_MIXPANEL_PROJECT_ID`, `_SA_USERNAME`, `_SA_SECRET`; db from `CH_QUERY_VAR_MIXPANEL_DATABASE` (code default `mixpanel`, prod `mixpanel_ga`) | run_queries.py:386-431; ingestors/mixpanel_ingestor.py:99-100,102-197,231-243,245-275 |
| `--ingestor=mixpanel --mixpanel-mode=backfill --mixpanel-from-date=… --mixpanel-to-date=…` | walks the range; STILL skips days marked complete | append | long (60 req/h limiter) | both dates required | ingestors/mixpanel_ingestor.py:25-32,142-144,170-175 |
| `--ingestor=mixpanel-profiles [--table-name=…]` | full Engage snapshot every run; there is no daily/backfill flag | append (`ReplacingMergeTree(synced_at)`) | yes | same Mixpanel credentials | run_queries.py:434-473; ingestors/mixpanel_profiles_ingestor.py:26-31,61-126 |
| `--ingestor=cow --cow-mode=daily --table-name=db.t --cow-source-table=db.t [--cow-lookback-days=7] [--cow-request-delay=s] [--cow-max-pages=500]` | takers active in the lookback window; newest-first per owner, stops at a `(order_uid, tx_hash, log_index)` key known from the last 14 days of `ingested_at` (cow_ingestor.py:177-182); if that lookup fails the set is silently empty and every owner pages its full history up to `--cow-max-pages` (201-202); exit 1 on any failed owner | append (`ReplacingMergeTree(ingested_at)`) | data-safe; shares the CoW partner-gateway budget and egress address with the live cron and cow-indexer | `--table-name` (or `CH_TABLE_NAME`), `--cow-source-table` (or `COW_SOURCE_TABLE`); `CH_QUERY_VAR_COW_DATABASE` for the DDL; `CH_QUERY_VAR_COW_API_KEY` (the gateway rejects keyless); `--cow-request-delay` defaults to 0.1 s keyed / 0.6 s keyless, env twin `COW_REQUEST_DELAY` (cow_ingestor.py:23-24,104-108; run_queries.py:181-182), production 0.25 | run_queries.py:515-545; ingestors/cow_ingestor.py:15-44,104-108,137-143,165-202,661-834 |
| `--cow-mode=backfill [--cow-backfill-from=YYYY-MM-DD]` | all takers (from that date if given), full history, no early stop | append | data-safe; hours of API load | as above | ingestors/cow_ingestor.py:127-136,442-443 |
| `--cow-mode=repair [--cow-backfill-from=…]` | anti-joins source fills against target keys, re-fetches the full history of every owner with a missing fill; idempotent; exit 1 if any owner failed | append | data-safe; heavy | as above | ingestors/cow_ingestor.py:336-370,546-659 |
| `--ingestor=snapshot --snapshot-mode=daily [--snapshot-vote-refresh-days=5]` | space + ALL proposals every run; votes only for active/pending proposals or those ended within N days; follows best-effort | append (`ReplacingMergeTree(ingested_at)`) | yes | `CH_QUERY_VAR_GOVERNANCE_DATABASE` (default `governance_db`), `_SNAPSHOT_SPACE` (default `gnosis.eth`); optional `_SNAPSHOT_API_KEY`, `_SNAPSHOT_GRAPHQL_URL` | run_queries.py:548-572; ingestors/snapshot_ingestor.py:319-374,403-413 |
| `--snapshot-mode=backfill` | votes for every proposal | append | yes, ~1 req/s | | ingestors/snapshot_ingestor.py:404-405 |
| `--ingestor=forum --forum-mode=daily [--forum-max-pages=400]` | categories, user directory, like graph for users whose `likes_given` grew, then topics bumped after `max(bumped_at)` plus every topic with an open poll; a failed watermark read means a full crawl | append (`ReplacingMergeTree(ingested_at)`) | yes | none required: `CH_QUERY_VAR_GOVERNANCE_DATABASE` (default `governance_db`), `_DISCOURSE_BASE_URL` (default `https://forum.gnosis.io`) (run_queries.py:577-578) | run_queries.py:575-597; ingestors/forum_ingestor.py:217-256,366-456,655-733 |
| `--forum-mode=backfill` | all `/latest.json` pages up to `--forum-max-pages`; likes for every user with more likes than stored | append | yes, 2 req/s | | ingestors/forum_ingestor.py:236,681-683 |
| `--ingestor=external-prices --external-prices-source=defillama\|coingecko\|both --external-prices-mode=daily [--external-prices-daily-lag-days=1] [--external-prices-database=crawlers_data] [--external-prices-tokens-config=config/external_prices_tokens.yml]` | one settled 00:00 UTC day (today − lag); INSERT, then `ALTER TABLE … DELETE` older rows of that day; a failed per-token CoinGecko fetch is skipped, and a run that lands 0 rows still exits 0, keyed or not (DefiLlama too when the payload has no usable coins; external_prices_ingestor.py:533-535,647-649) | append into plain MergeTree, prune after | a re-run replaces the day ONLY if the prune succeeds | `COINGECKO_API_KEY` or `CH_QUERY_VAR_COINGECKO_API_KEY` (effectively required) | run_queries.py:600-642; ingestors/external_prices_ingestor.py:175-223,273-338,511-561,647-649 |
| `… --external-prices-mode=backfill [--external-prices-chart-span-days=365] [--external-prices-defillama-full-history]` | DefiLlama `/chart` windows (full history pages back 500 points at a time), then a prune scoped to tokens that returned data; CoinGecko `market_chart` ≤ 365 d with NO prune | append | DefiLlama re-runnable; CoinGecko DUPLICATES on re-run | same | ingestors/external_prices_ingestor.py:366-481,563-605 |
| `--ingestor=hopr-blokli [--hopr-blokli-networks=jura] [--hopr-database=crawlers_data]` | one snapshot per (network, today): chain info, channel stats, safes, every account by keyid; a GraphQL `errors` array is a hard failure; an unknown network (incl. `dufour`) raises | append (`ReplacingMergeTree(ingested_at)`) | yes | env twins `HOPR_BLOKLI_NETWORKS`, `HOPR_DATABASE`; prod passes `hopr_db` explicitly | run_queries.py:645-663; ingestors/hopr_blokli_ingestor.py:69-77,86-126,267-328 |
| `--ingestor=hopr-network [--hopr-network-ids=3] [--hopr-network-mode=daily\|backfill] [--hopr-database=crawlers_data]` | roster snapshot per env id + the WHOLE hourly online series, every run, in BOTH modes | append (`ReplacingMergeTree(ingested_at)`) | yes | env twins `HOPR_NETWORK_IDS`, `HOPR_NETWORK_MODE`, `HOPR_DATABASE` | run_queries.py:666-685; ingestors/hopr_network_ingestor.py:82-96,265-335 |
| LOCAL-ONLY `python scripts/full_history_coingecko_prices.py [--dry-run] [--only SYM,…] [--database crawlers_data] [--table db.t] [--tokens-config config/external_prices_tokens.yml] [--host --port --user --password --db --secure --verify] [--no-replace] [--include-today] [--skip-table-creation] [--delay] [--max-retries] [--impersonate]` | full CoinGecko daily history from the undocumented etl2 chart feed via curl_cffi; per token INSERT then prune older rows ≤ last date; exit 1 if any token failed; not wired into `run_queries.py` | `<db>.coingecko_prices` | replaces per token; skips today so the cron's row survives | connection flags default from `CH_*` (script:290-296); `--verify` here is on only for true/1/yes (script:347), the opposite parse of run_queries.py:721; `--dry-run` opens no connection | scripts/full_history_coingecko_prices.py:1-28,286-330,339-348; scripts/README.md:3-5,100-121 |
| LOCAL-ONLY `docker compose run --rm <service>` | the rows above with `.env` substituted (`CH_DB_HOST`→`CH_HOST`, `CH_NATIVE_PORT`→`CH_PORT`, `CH_VERIFY=False`), built from your working tree; services in the table below | production tables with production credentials (blast-radius note above) | NO — a second writer | `.env` | docker-compose.yml:1-897 |
| Stack wrapper scripts (not in this repo) | the Dune wrapper runs the 3 csv legs, prints `Dune <LEG> ingestion FAILED` / `completed successfully`, exits 0 unless ALL three failed; the ProbeLab wrapper chains 13 parquet runs with `&&` | as the legs | n/a | | stack `scripts/` Dune wrapper:10-81, ProbeLab wrapper:1-13 |
| `python -m unittest tests/test_cow_ingestor_throttle.py` (bare `python -m unittest` finds 0 tests: `tests/` has no `__init__.py`) | 7 offline tests of the CoW throttle path (stubbed curl_cffi) | no | yes | | tests/test_cow_ingestor_throttle.py:1-5,68-154 |

Local Compose services: these are `docker-compose.yml` service names, not cluster objects; production CronJob names live only in the private stack.

| Compose service(s) (local docker-compose.yml) | Invocation | Notes | Source |
| --- | --- | --- | --- |
| `click-runner` | no command → query mode | connects, then exits 1 ("No queries specified"): the service sets no `CH_QUERIES` | docker-compose.yml:2-30 |
| `ember-ingestor` | csv, Ember | | docker-compose.yml:33-60 |
| `cow-fees-{backfill,daily}-ingestor` | cow backfill / daily | daily lacks `--cow-request-delay` (production passes 0.25) | docker-compose.yml:63-117 |
| `probelab-agent-semvers-ingestor` | parquet `--mode=all`, one table | | docker-compose.yml:120-146 |
| `probelab-{all,date}-ingestor` | 13 × parquet `--mode=all` / `--mode=date`, chained with `&&` | `date` hardcodes `--date=2025-04-22` | docker-compose.yml:148-220 |
| `probelab-test` (no `-ingestor` suffix) | parquet `--mode=latest`, one table | | docker-compose.yml:222-245 |
| `gpay-wallets-ingestor` | gdrive → `crawlers_data.gpay_wallets` | | docker-compose.yml:246-274 |
| `dune-{labels,prices,bridge-flows}-{daily,full}-ingestor` | csv, one leg each; `full` uses `insert_full.sql` | | docker-compose.yml:278-325,353-445 |
| `circles-blacklist-daily-ingestor` | query, the 4-file chain | different default source URL from production | docker-compose.yml:327-350 |
| `dune-all-daily-ingestor` | 3 csv legs chained with `&&` | first failure aborts the rest (the production wrapper runs all three) | docker-compose.yml:448-473 |
| `celo-gpay-{transfers,wallet-events}-{full,daily}-ingestor`, `celo-gpay-all-daily-ingestor` | csv | no production cron since 2026-09-10 | docker-compose.yml:479-598 |
| `mixpanel-events-{daily,backfill}-ingestor`, `mixpanel-profiles-daily-ingestor` | mixpanel daily / backfill; mixpanel-profiles | db default `mixpanel`, region default US | docker-compose.yml:601-679 |
| `dune-execute-only-daily-ingestor` | dune-execute-only | | docker-compose.yml:681-700 |
| `governance-{snapshot,forum}-{backfill,daily}-ingestor` | snapshot / forum | db default `governance_db` (production) | docker-compose.yml:707-794 |
| `external-prices-{daily,backfill}-ingestor` | external-prices | `--external-prices-source=both` in one run | docker-compose.yml:797-851 |
| `hopr-{blokli,network}-ingestor` | hopr-blokli / hopr-network | db default `crawlers_data` (production `hopr_db`) | docker-compose.yml:853-897 |

## What does not exist, or does not do what its name says

- README.md:71 says "four primary running modes"; `--ingestor` has 13 choices (run_queries.py:129). The public docs page lists 11 (misses `hopr-blokli`, `hopr-network`; cerebro-docs docs/data-pipeline/ingestion/click-runner.md:22).
- README.md:222-227 `cron_setup.sh`: no such file. README.md:544 "logs are saved to the logs/ directory": logging is a stream handler only (observability.py:296-302).
- README.md:63 "S3_BUCKET (default …)": no code default; unset gives an empty bucket in the `s3://` path (ingestors/parquet_ingestor.py:75,96).
- README.md:426,438-448,470-471 (HOPR dev-only, writes `crawlers_data`, no compose service) and README.md:421-422 (external-prices writes "deferred") are wrong: both HOPR crons write `hopr_db` daily, two price crons write `crawlers_data` daily (stack `locals.tf:127-130,257-296,396-423`), and the HOPR compose services exist (docker-compose.yml:853-897).
- Schema migrations: none. Every run executes its DDL as `CREATE TABLE IF NOT EXISTS` (e.g. ingestors/csv_ingestor.py:54-62, cow_ingestor.py:671-684), and no `ALTER … ADD/MODIFY COLUMN` exists in `ingestors/`, `queries/` or `scripts/`, so editing a `*create*.sql` never alters an existing table; a column change needs a hand-run ALTER on the warehouse (proposed to a human) before the new image rolls.
- `--hopr-network-mode` changes nothing: daily and backfill are identical (run_queries.py:211-215; ingestors/hopr_network_ingestor.py:82-89,313-334).
- `--insert-sql` is read for `gdrive` and dropped (run_queries.py:317; ingestors/gdrive_ingestor.py:27-36).
- `--skip-table-creation` is ignored by `query` (run_queries.py:384; ingestors/base.py:110) and `dune-execute-only` (run_queries.py:476-513).
- `--cow-max-pages` defaults to 500 and is never None (run_queries.py:179), so the ingestor's "daily = 20 pages" fallback is unreachable (ingestors/cow_ingestor.py:121-123); an explicit `--cow-max-pages=N` still takes effect in every mode. `--cow-lookback-days` is daily-only (137-143); `--cow-backfill-from` is backfill and repair, never daily (127-130,341-343).
- `--mixpanel-to-date` is ignored in daily (end is always yesterday, ingestors/mixpanel_ingestor.py:197); `--mixpanel-from-date` in daily applies only when the state table has no completed date (183-184).
- `{{COW_DATABASE}}` has no code default: unset, the literal placeholder stays in the DDL and the CREATE fails (queries/cow/create_table.sql:1; ingestors/base.py:74-93 via ingestors/cow_ingestor.py:672; run_cow_ingestor passes
  query_vars unchanged, run_queries.py:529-531). DDL database and `--table-name` are set independently; production sets both to `crawlers_data`.
- `--table-name` is required by `parquet` (run_queries.py:294) and by `cow` unless `CH_TABLE_NAME` is set (518,521), an override for `gdrive`, `mixpanel`, `mixpanel-profiles` (320,403,450), and ignored by `csv`, `query`, `dune-execute-only`, `snapshot`, `forum`, `external-prices`, `hopr-*`. `--create-table-sql` is ignored by `query`, `dune-execute-only` (run_queries.py:370-384,476-513), `snapshot`, `forum`, `external-prices`, `hopr-*` (hardcoded DDL: run_queries.py:560-563,585-590; external_prices_ingestor.py:76-77; hopr_blokli_ingestor.py:65-66; hopr_network_ingestor.py:78-79). `--optimize-sql` is csv/gdrive only; `--date` parquet `--mode=date` only; `--max-rows` gdrive only (run_queries.py:135,142,146).
- `--db` / `CH_DB` is the connection default only; every write is fully qualified (run_queries.py:124,731; queries/dune/labels/insert_daily.sql:1). `crawlers_data` is hardcoded in the dune/circles/ember/probelab/gpay/celo_gpay SQL; only cow, external-prices, hopr, mixpanel and governance take their database from a flag or `CH_QUERY_VAR_*` (run_queries.py:402,550,577,607,648,669).
- `--secure` / `--verify` are strings: secure only for `true|1|yes`, verify off only for `false|0|no` (run_queries.py:720-721). The code default port is 9000 (run_queries.py:121, and `.env.example:3` `CH_NATIVE_PORT=9000`), but clickhouse-connect (requirements.txt:1) is an HTTP(S)-interface driver and run_queries.py:47-55 passes the port
  straight through, so a working local port is 8123/8443; production is 443 HTTPS with verification OFF (stack `locals.tf:71-75`).
- Compose env `CH_INGESTOR`, `CH_EMBER_CREATE_TABLE/INSERT/OPTIMIZE` (docker-compose.yml:19-24) and production env `REGION` (stack `cronjobs.tf:157-163`) are read by nothing in `*.py`; `queries/dune/{labels,prices}/insert_from_execution.sql` and `DUNE_*_EXECUTION_ID` (.env.example:16,21,26) have no consumer.
- `queries/celo_gpay/` and the five `celo-gpay-*` compose services (docker-compose.yml:479-598) have had no production cron since 2026-09-10 (stack `README.md:70-71`).
- The production Dune wrapper exits 0 unless ALL legs fail (stack `scripts/` Dune wrapper:69-81); the compose service `dune-all-daily-ingestor` chains with `&&`, so its FIRST failure aborts the rest (docker-compose.yml:468-473): opposite failure semantics.
- `/health` on `:9090` answers 503 until ClickHouse connects and backs no probe (observability.py:196-198; the stack's `cronjobs.tf` declares none). `dune-execute-only` triggers every remaining id after a failure, then exits 1 (run_queries.py:489-513).
- The public docs page says CoW fetches "open orders and trade fees" (cerebro-docs docs/data-pipeline/ingestion/click-runner.md:13,98); the code calls only `/trades?owner=` (ingestors/cow_ingestor.py:400).
- `--cow-mode=repair` is described in `--help` (run_queries.py:167-170) and the class docstring (ingestors/cow_ingestor.py:71-75) as a per-orderUid fetch; the code re-fetches the FULL per-owner history of every owner with a missing fill via `/trades?owner=` (ingestors/cow_ingestor.py:546-620,400), which costs far more API budget than the help implies.
- `utils/db.py` and `utils/date.py` are imported by nothing (only `utils/s3.py` is used, ingestors/parquet_ingestor.py:10); ClickHouse connections are made in run_queries.py:27-75. Do not extend the dead helpers.
- Lesson `ddl-attributed-to-query-ingestor` claims all DDL is labelled `ingestor="query"`; only `query`, `external-prices`, `hopr-blokli`, `hopr-network` route DDL through `execute_queries` (ingestors/base.py:58-60; external_prices_ingestor.py:123,126; hopr_blokli_ingestor.py:271; hopr_network_ingestor.py:269). The other eight label their own `create_table` (csv_ingestor.py:61; parquet_ingestor.py:71; gdrive_ingestor.py:378; mixpanel_ingestor.py:107,112; mixpanel_profiles_ingestor.py:66; cow_ingestor.py:683; snapshot_ingestor.py:385; forum_ingestor.py:269).

## Hazards

- Circles blacklist: `truncate.sql` runs before `insert.sql`; a failed INSERT (a missing `READ ON URL` grant emptied the table on 2026-09-15), a kill mid-run or a partial re-run of the four-file chain leaves the table EMPTY, and nothing alerts on zero rows. Run the whole chain or nothing; check the grant with a `SELECT … FROM url()` first (queries/circles/blacklist/truncate.sql:3, insert.sql:9-14; runbook "Rerun").
- Dune tables are plain MergeTree: re-running a csv leg for a day that already landed doubles rows and bridge-flow sums. Delete the day with `SETTINGS mutations_sync = 2` first, and never for a day that simply has not landed (T-2) (queries/dune/*/create_table.sql; ingestors/csv_ingestor.py:77-78; runbook "The Dune lag rule", "Corrupt rows").
- External prices: the daily prune is logged, not fatal, so a missing ALTER grant leaves duplicate rows under a green job; CoinGecko backfill has no prune at all. Delete the day/range first, or use the full-history script which prunes per token (ingestors/external_prices_ingestor.py:214-223,563-605; scripts/README.md:100-121).
- Mixpanel: a backfill over a range already marked complete skips every day and exits 0; a daily run after a long gap walks every missing day in one pod at 60 req/h. Delete the range from `<db>.mixpanel_ingestion_state` before re-ingesting (ingestors/mixpanel_ingestor.py:25-32,142-144; runbook "Backfill, per ingestor").
- Snapshot: votes are refreshed only for proposals open or ended within `--snapshot-vote-refresh-days` (5); a longer gap loses votes silently. Recover with a manual run with a wider window or `--snapshot-mode=backfill` (ingestors/snapshot_ingestor.py:403-413; runbook "Backfill, per ingestor").
- CoW: every run shares the partner-gateway budget and the egress address with the live cron and cow-indexer; the edge answers a 403 that lasts about an hour, and the app gives up after 3 throttle retries per request and 10 consecutive failed owners. Never start backfill/repair while 403s are being logged or near 03:00 UTC; the table dedupes, so re-running later costs nothing (ingestors/cow_ingestor.py:15-44,303-315,738-754; runbook "CoW 403").
- HOPR: all four HOPR tables are partitioned by YEAR (queries/hopr/network_online_hourly_create.sql:16, network_nodes_create.sql:26, blokli_nodes_create.sql:18, blokli_network_snapshot_create.sql:28); a `DROP PARTITION` removes a whole year, and the hourly series is the only multi-year HOPR history. Never re-gate the hourly fetch on `backfill` (docs/lessons/backfill-gated-series-freeze.md).
- Any laptop or compose run against production credentials is a second writer invisible to `concurrency_policy = "Forbid"`; an unsuspend inside the 1800 s starting deadline replays the missed slot at once, which for circles is a truncate (stack `cronjobs.tf:32,38`; runbook "Rerun").
- gdrive: `--max-rows` (default 1,000,000) silently truncates a larger CSV and reports success (ingestors/gdrive_ingestor.py:184-186).
- Forum daily: a failed watermark read falls back to a full 400-page crawl instead of erroring — slow and noisy, not destructive (ingestors/forum_ingestor.py:366-379).
- Mixpanel daily: if the state-table read fails (e.g. a grant or memory error), `_get_last_completed_date` returns None and the run fetches only yesterday (or `--mixpanel-from-date`), silently skipping any gap and exiting 0 (ingestors/mixpanel_ingestor.py:183-191,215-217). After an outage, verify with a range query on `mixpanel_ingestion_state`.

## Health and verification is a warehouse query

The ONE liveness query: data age per table divided by its SLA. Healthy is `rows > 0` AND `staleness_ratio < 1` on every
row. SLAs: 30 h for daily crons (one missed run), 60 h for Dune-backed tables (Dune lands T-1/T-2, so one day
behind is normal and three is a stall), 480 h for Ember (runs on the 8th and 21st). `rows = 0` is a failure, not a stale-read
artefact: circles_blacklisted empties when the INSERT fails after the TRUNCATE (queries/circles/blacklist/truncate.sql:3,
insert.sql:9-19); only a table whose cron has never run is legitimately empty. This is the `crawlers-data-stale` / `hopr-db-stale` rule SQL
(the deployments repository's alerting stack, `alerts/click-runner.yaml:52-61,81-86`) plus `count()`.

```sql
SELECT tbl, dateDiff('hour', latest_data, now()) / threshold_h AS staleness_ratio, rows
FROM (
  SELECT 'cow_api_trade_fees' AS tbl, toDateTime(max(ingested_at)) AS latest_data, 30 AS threshold_h, count() AS rows FROM crawlers_data.cow_api_trade_fees
  UNION ALL SELECT 'dune_labels', toDateTime(max(introduced_at)), 60, count() FROM crawlers_data.dune_labels
  UNION ALL SELECT 'dune_prices', toDateTime(max(block_date)), 60, count() FROM crawlers_data.dune_prices
  UNION ALL SELECT 'dune_bridge_flows', toDateTime(max(timestamp)), 60, count() FROM crawlers_data.dune_bridge_flows
  UNION ALL SELECT 'ember_electricity_data', max(version), 480, count() FROM crawlers_data.ember_electricity_data
  UNION ALL SELECT 'circles_blacklisted', toDateTime(max(ingested_at)), 30, count() FROM crawlers_data.circles_blacklisted
  UNION ALL SELECT 'defillama_prices', toDateTime(max(ingested_at)), 30, count() FROM crawlers_data.defillama_prices
  UNION ALL SELECT 'coingecko_prices', toDateTime(max(ingested_at)), 30, count() FROM crawlers_data.coingecko_prices
  UNION ALL SELECT 'hopr_blokli_network_snapshot', toDateTime(max(ingested_at)), 30, count() FROM hopr_db.hopr_blokli_network_snapshot
  UNION ALL SELECT 'hopr_blokli_nodes', toDateTime(max(ingested_at)), 30, count() FROM hopr_db.hopr_blokli_nodes
  UNION ALL SELECT 'hopr_network_nodes', toDateTime(max(ingested_at)), 30, count() FROM hopr_db.hopr_network_nodes
  UNION ALL SELECT 'hopr_network_online_hourly', toDateTime(max(ingested_at)), 30, count() FROM hopr_db.hopr_network_online_hourly
);
```

`governance_db` and `mixpanel_ga` have NO freshness rule today. For governance, `max(ingested_at)` (a server-side `DEFAULT now()`,
queries/governance/create_snapshot_proposals_table.sql:18, create_forum_topics_table.sql:19) is first-seen, not last-verified: an identical re-inserted block is
deduped (queries/governance/create_forum_polls_table.sql:30-36), so a quiet space or forum can freeze it under a healthy cron. Confirm the run's `run_success` log
event (run_queries.py:765) instead, and use `max(ingested_at)` on `governance_db.snapshot_proposals` / `forum_topics` only as a secondary check. Check
`max(event_time)` on `mixpanel_ga.mixpanel_raw_events` by hand (Mixpanel's export lags 24–48 h, ingestors/mixpanel_ingestor.py:264-266). Coverage for CoW is the
anti-join of on-chain fills against the raw fee table (`cow-fees-coverage-incomplete`, same alerts file `:139-152`): healthy is `missing_ratio = 0` for days three or
more days old; the two newest days are always short because the 03:00 run reads dbt output from 06:00 the previous day.

```sql
SELECT toDate(t.block_timestamp) AS trade_day, countIf(f.order_uid = '') / count() AS missing_ratio
FROM dbt.int_execution_cow_trades AS t
LEFT JOIN (SELECT DISTINCT order_uid, tx_hash, log_index FROM crawlers_data.cow_api_trade_fees) AS f
  ON f.order_uid = t.order_uid AND f.tx_hash = t.transaction_hash AND f.log_index = t.log_index
WHERE t.block_timestamp >= today() - 5 AND t.block_timestamp < today() - 2
GROUP BY trade_day;
```

Besides freshness and coverage, the working alarms are three Loki log rules in the same alerts file (`dune-query-failed`,
`click-runner-run-failure`, `cow-fees-api-errors`, `:6-41,95-115`). Suspending a cron silences its `cron-miss-*` alert
(runbook "Rerun"); only the freshness rows catch it, and the two suspended crons (dune-execute-only, probelab) have none.
Zero pods outside 03:00–05:00 UTC (and Ember's 00:00 on the 8th/21st) is the normal steady state (runbook, top).
The two governance crons (snapshot, forum) have NO alert of any kind: no freshness row, not in the `click-runner-run-failure` pod regex (alerts file `:34`), and no `cron-miss-*` rule; cron-miss rules exist only for the Dune daily, CoW, Mixpanel events and profiles, circles and Ember crons (the alerting stack's `alerts/platform.yaml:32-121`), so external-prices and HOPR have none either. A governance failure is visible only by hand.

NOT a health signal: a pod in Running/Completed or a Job marked succeeded; exit 0 (the Dune wrapper, keyless CoinGecko, any external-prices daily run that wrote 0
rows, a failed prune, the forum watermark fallback, forum topics whose `/t/{id}.json` fetch failed (skipped, and not retried until re-bumped once a later topic
advances the watermark; ingestors/forum_ingestor.py:366-369,464-466), forum categories/users/likes and Snapshot follows (best-effort;
forum_ingestor.py:222-234,280-284; snapshot_ingestor.py:362-371), a Mixpanel state skip, failed state read or failed state write, and a CoW run that found 0 owners
because the dbt owner source has no trade inside the lookback window, ingestors/cow_ingestor.py:138-143,692-695, all exit 0; a merely lagging owner source also exits
0 with days short, which only the coverage query catches, alerts file `:121-126`); the `Rows inserted:` deltas the csv and parquet paths log (duplicates count as
progress, ingestors/csv_ingestor.py:80-85); `click_runner_*` Prometheus counters and `/health` (ephemeral pods, scrape races, counters reset per pod —
observability.py:164-169,196-198; docs/lessons/ephemeral-pod-metrics-unreliable.md); the `fee_source` ratio in the dbt CoW mart (floors at ~97–99.8 % by design).

## Rules for agents

- Derive, do not recall: `--ingestor` choices from run_queries.py:129, per-mode args from its `run_*` function, target
  tables from `queries/**/*create*.sql`, production args from the stack's `locals.tf`. Never from README.md, never from this file's tables.
- Local tooling is not production: compose services, `.env`, `scripts/` and the unittest suite are laptop surfaces; the
  only production invocations are the `args` in the stack's `locals.tf`.
- Never run a writer against the production warehouse from a laptop: with production credentials in `.env`, every compose
  service writes production tables from your uncommitted code, and nothing detects the second writer.
- Never assume access to the cluster or the private deployments repository. Propose exact commands for a human to run. Never run a git write, a Terraform apply, a cluster-mutating command or an image push yourself; read-only plans and read-only cluster reads are fine where you already have access. Plan first, then propose the exact apply for a human (the deployments repository's own rule).
- Never a second writer: one cron per table; never add a cron with `concurrencyPolicy: Allow`; never trigger a manual Job
  while the cron has an active Job or near 03:00–05:00 UTC; never scale anything — there is nothing here to scale.
- App-specific never-dos: never run the circles chain partially; never re-ingest a Dune day that has not landed; never
  `DROP PARTITION` on the yearly HOPR tables; never re-gate the hourly HOPR series on `backfill`; never treat a
  CDN 403-with-HTML as an authorisation error (retry with backoff, count the owner as failed, exit non-zero); never use a
  "current price" endpoint for a daily price row; never add a MergeTree + post-insert-prune table; never coerce an API
  error into an empty result; never rename the pod `app` label semantics (`CLICK_RUNNER_JOB_NAME` is the metric and log identity).
- `ingest()` returning True is not completeness; after any change run the freshness query and, for CoW, the coverage query.
- Diagnosed a new mistake class? Add `docs/lessons/<slug>.md` (symptom, cause, rule, evidence) and a status row
  (`observed` → `remediated` → `enforced`) in `docs/lessons/INDEX.md`; update the status when a safeguard lands.

## The BaseIngestor contract (`ingestors/base.py`)

- `__init__(client, variables)` stores a live clickhouse_connect Client and a dict of SQL template vars; no sessions or retries, subclasses own transport.
- `load_sql_file(path)` substitutes `{{VAR}}` by plain string replace; an unmatched `{{VAR}}` passes through and fails at ClickHouse.
- `ingest(self, skip_table_creation: bool = False, **kwargs) -> bool` is the de facto signature (abstract `ingest(**kwargs)`, base.py:30; `QueryIngestor` ignores the flag). Tables self-create with `CREATE TABLE IF NOT EXISTS` and are never altered (see "Schema migrations" above).
- `execute_queries` labels everything routed through it `ingestor="query"`: query mode plus the external-prices and both HOPR DDLs; the other eight ingestors with DDL label their own (corrected scope above).

## House style for new ingestors

- Follow hopr_blokli / hopr_network; take only the HTTP retry loop from external_prices, never its MergeTree + prune storage.
- `requests.Session` + hand-rolled retry loop (no urllib3.Retry; cow alone uses `curl_cffi` browser TLS impersonation because CoW's edge screens by JA3 — keep that, `ingestors/cow_ingestor.py:26-30`): retry 5xx and transport errors with backoff; fail fast on other 4xx EXCEPT a 403 with an HTML body from a CDN-fronted API, which is an edge rate-limit treated like 429 (lesson edge-403-is-a-throttle); API-level error payloads are HARD failures, never an empty result.
- `INSERT_SETTINGS = {"optimize_on_insert": 0, "max_insert_threads": 1}`.
- Idempotency via `ReplacingMergeTree(ingested_at)` with ORDER BY = the stated grain; never MergeTree + post-insert ALTER DELETE prune (lesson prune-pattern-silent-failure).
- Database via a `{{<AREA>_DATABASE}}` template var + `--<area>-database` arg, merged into query_variables in the runner (pattern: run_hopr_blokli_ingestor).

## New-ingestor checklist (every step, in order)

1. `ingestors/<name>_ingestor.py` (BaseIngestor subclass).
2. `queries/<area>/*_create.sql` DDL with `{{<AREA>_DATABASE}}`.
3. `run_queries.py`: import, `--ingestor` choices entry, area args, `run_<name>_ingestor()` wrapper (obs.update_health + obs.time_operation), dispatch branch in `main()`.
4. `docker-compose.yml`: a service per job mode (env substituted from `.env`).
5. Terraform, in the click-runner stack under the GKE tree of the deployments repository: an `ingestors` entry and, if it needs its own env, a `configmaps` entry in `locals.tf` (`cronjobs.tf` renders `ingestors`, `configmaps.tf` renders `configmaps`; secrets in `secrets.tf`). Pick a free slot: 03:15 (freed by the 2026-09-10 celo-gpay removal) and 05:15+ are free as of 2026-09; 03:00 already carries six crons (stack `locals.tf:216-481`). Pins are per group (`image_default`, `image_hopr`, `image_cow`); bumping one re-rolls every cron on that pin — additive changes only.
6. The deployments repository's alerting stack, `alerts/click-runner.yaml`: a freshness row per new table (`crawlers-data-stale`, or `hopr-db-stale` for `hopr_db`) — REQUIRED (lesson ephemeral-pod-metrics-unreliable).

## Where the full procedures live

- `README.md` — CLI reference, external-prices and HOPR design notes; least trusted: its production claims are wrong (see above).
- `scripts/README.md` — the full-history CoinGecko script; `queries/mixpanel/README.md` — Mixpanel RBAC and env.
- `docs/lessons/INDEX.md` — mistake classes with status and evidence; check it before diagnosing a data-quality symptom.
- Private runbook: <https://github.com/gnosisdevops/infrastructure-gnosis-analytics/blob/main/runbooks/26-click-runner.md> — rerun, backfill per ingestor, delete-then-re-ingest, CoW 403, dbt follow-up (public: <https://docs.analytics.gnosis.io/operations/runbooks/dbt-reprocess/>); the cluster commands live only there. `runbooks/60-gke-one-shot-jobs.md` for a one-off Job (public: <https://docs.analytics.gnosis.io/operations/runbooks/one-shot-jobs/>), `runbooks/00-morning-check.md` for the daily freshness pass.
- Public docs page: <https://docs.analytics.gnosis.io/data-pipeline/ingestion/click-runner/> — the same procedures without cluster commands, plus table schemas.
- Deployments stack: the click-runner stack under the GKE deployments tree of `gnosisdevops/infrastructure-gnosis-analytics` — `README.md` (what changed on the port, follow-ups), `locals.tf` (ingestors, configmaps, pins, cutover switch), `cronjobs.tf`, `secrets.tf`, `scripts/`.
- Alerting-as-code: the alerting stack in the same repository, `alerts/click-runner.yaml` — freshness SLAs, Loki event rules, the CoW coverage anti-join.

Verified 2026-09-23 against: run_queries.py:1-784, ingestors/base.py:1-118, ingestors/csv_ingestor.py:1-103, ingestors/parquet_ingestor.py:1-190, ingestors/gdrive_ingestor.py:1-420, ingestors/mixpanel_ingestor.py:1-477, ingestors/mixpanel_profiles_ingestor.py:1-228, ingestors/cow_ingestor.py:1-847, ingestors/snapshot_ingestor.py:1-508, ingestors/forum_ingestor.py:1-787, ingestors/external_prices_ingestor.py:1-680, ingestors/hopr_blokli_ingestor.py:1-328, ingestors/hopr_network_ingestor.py:1-335, observability.py:1-318, utils/db.py:1-99, utils/s3.py:1-118, utils/date.py:1-51, queries/**/*.sql (CREATE/INSERT/TRUNCATE/OPTIMIZE headers, PARTITION BY; queries/governance/create_forum_polls_table.sql:30-36), docker-compose.yml:1-897, Dockerfile:1-24, requirements.txt:1-14, .github/workflows/build-and-release.yml:1-53, .env.example:1-77, scripts/full_history_coingecko_prices.py:1-415, scripts/README.md:1-162, tests/test_cow_ingestor_throttle.py:1-154, docs/lessons/INDEX.md:1-13, docs/lessons/*.md, README.md:1-548; deployments click-runner stack README.md:1-162, locals.tf:1-488, cronjobs.tf:1-225, configmaps.tf:1-20, secrets.tf:1-65, and its two wrapper scripts under scripts/ (84 and 13 lines); alerting/alerts/click-runner.yaml:1-159, alerting/alerts/platform.yaml:1-121; runbooks/26-click-runner.md:1-149; cerebro-docs docs/data-pipeline/ingestion/click-runner.md:1-403, docs/operations/runbooks/one-shot-jobs.md, docs/operations/runbooks/dbt-reprocess.md
