# scripts/

One-off maintenance scripts. Unlike everything under `ingestors/`, these are **not**
wired into `run_queries.py` and are **not** scheduled in `docker-compose.yml` — they
are run by hand when needed.

---

## `full_history_coingecko_prices.py`

Backfills the **complete** CoinGecko daily price history into
`{database}.coingecko_prices`.

### Why this exists

The scheduled `external-prices` ingestor
(`--ingestor=external-prices --external-prices-mode=backfill`) fetches from the
public API's `/coins/{id}/market_chart`, which is **capped at `days=365`** on the
free tier — `days=max` and the `/market_chart/range` endpoint need a paid plan.
See the comment at `ingestors/external_prices_ingestor.py:358`.

This script instead reads the feed that powers CoinGecko's own price charts:

```
https://www.coingecko.com/etl2/price_charts/{coingecko_id}/usd/max.json
```

which returns the entire daily series with no API key. GNO goes back to
2017-05-01 (3373 points) and USDT to 2015-02-25, versus 365 days from the API.

The endpoint is undocumented and sits behind a bot check that answers plain
`requests`/`curl` with a **403 challenge page**, so requests go through
`curl_cffi` with a Chrome TLS fingerprint — the same technique already used in
`ingestors/cow_ingestor.py`. `curl_cffi` is already in `requirements.txt`; no
browser or extra dependency is needed.

Response shape:

```json
{ "stats": [[1493596800000, 74.81], ...], "total_volumes": [[1493596800000, 13002600.0], ...] }
```

`stats` is `[unix_ms, price_usd]` at daily granularity. `total_volumes` is
available for free but is **not** stored — `coingecko_prices` has no volume
column.

### Token list

Read from `config/external_prices_tokens.yml` — the **same allowlist the daily
ingestor uses**, so the two can never drift apart. There is deliberately no
hardcoded list in the script. To add a token, add it to that YAML with
`coingecko: true` and a `coingecko_id`, and it is picked up by both.

Entries are deduplicated by `coingecko_id` (CoinGecko prices are asset-level,
not chain-level) and entries with `coingecko_id: null` are skipped. As of the
last run that is 35 tokens; 3 (`xDAI`, `svZCHF`, `bC3M`) have no CoinGecko
listing and are covered by DefiLlama only.

### Usage

```bash
# See what would be fetched; touches no database at all
python scripts/full_history_coingecko_prices.py --dry-run

# A subset, by symbol or by coingecko_id
python scripts/full_history_coingecko_prices.py --only GNO,COW,bNVDA

# Full run
python scripts/full_history_coingecko_prices.py --database crawlers_data
```

In Docker (the image now includes `scripts/`; note compose maps
`CH_DB_HOST`→`CH_HOST` and `CH_NATIVE_PORT`→`CH_PORT`):

```bash
set -a; . ./.env; set +a
docker build -t click-runner .
docker run --rm --env-file .env \
  -e CH_HOST="$CH_DB_HOST" -e CH_PORT="$CH_NATIVE_PORT" -e CH_VERIFY=False \
  --entrypoint python click-runner \
  scripts/full_history_coingecko_prices.py --database crawlers_data
```

Connection flags/env vars are the same as `run_queries.py`: `CH_HOST`, `CH_PORT`,
`CH_USER`, `CH_PASSWORD`, `CH_DB`, `CH_SECURE`, `CH_VERIFY`. Target database
defaults to `EXTERNAL_PRICES_DATABASE` (`crawlers_data`).

| Flag | Default | Notes |
| --- | --- | --- |
| `--dry-run` | off | Fetch and report only; never opens a DB connection |
| `--only` | all | Comma-separated symbols or coingecko_ids |
| `--database` / `--table` | `crawlers_data` | `--table` overrides the full name |
| `--tokens-config` | `config/external_prices_tokens.yml` | |
| `--no-replace` | off | Append without pruning; see below |
| `--include-today` | off | Keep today's partial-day price |
| `--skip-table-creation` | off | Assume the table already exists |
| `--delay` | `1.5` | Seconds between requests |
| `--max-retries` | `3` | Per token |

### Re-running is safe (insert-then-prune)

`coingecko_prices` is a plain `MergeTree`/`SharedMergeTree` — it does **not**
dedupe — so a naive re-run would double every row, including over the 365 days
the scheduled backfill already wrote.

Instead, per token, the script:

1. inserts the fresh series stamped with this run's `ingested_at`, then
2. `ALTER TABLE ... DELETE WHERE coingecko_id = X AND block_date <= <last inserted date> AND ingested_at < <this run>`.

Insert-first means the table is never missing the token, and a crash mid-run
leaves recoverable duplicates rather than a hole. The `block_date` bound means a
row for a **later** date written by the daily ingestor (i.e. today, which this
script skips by default) is left alone. The mutation runs with
`mutations_sync=2` so a failure surfaces here instead of half-applying.

Verified behaviour on a re-run: 0 duplicate dates, today's daily-ingestor row
preserved, stale prices corrected, other tokens untouched.

Use `--no-replace` to skip the prune when the table is known to be empty (e.g.
straight after a `TRUNCATE`), which avoids one pointless mutation per token.

### Gotchas

- **Partition limit.** The table is `PARTITION BY toStartOfMonth(block_date)`
  and ClickHouse rejects an insert block spanning more than
  `max_partitions_per_insert_block` (default 100). A decade of daily history is
  ~111 months, so inserts are chunked on month boundaries as well as row count.
  This only bites on full history — the 365-day backfill spans 13 partitions and
  never hit it.
- **Today is excluded by default — but not because it is partial.** Every point
  in `stats`, including today's, is stamped exactly `00:00:00 UTC` and is
  already final (verified: today's GNO point was byte-identical across two
  fetches 25 minutes apart, and differs from the live price). It is skipped
  because the daily ingestor already writes today and the table does not dedupe,
  so writing it too would leave two rows for today. Pass `--include-today` to
  override.
- **Slight divergence from the daily ingestor's source.** Both are `00:00 UTC`
  snapshots, but they come from different endpoints: this script uses etl2, the
  daily job uses `/coins/{id}/history`. Compared across 34 tokens for
  2026-07-28, **31 matched byte-for-byte** and 3 differed by 0.05–0.16% — GHO,
  EURe and the COP peso, all thin-liquidity. Verified this is genuine endpoint
  divergence, not late revision: etl2's value was unchanged 40 minutes later
  while `/history` still disagreed. Liquid tokens (GNO, WETH, WBTC) match
  exactly. Harmless for most uses; be aware if you diff a backfilled date
  against a daily-written one for a thin asset.
- **Source data has early outliers.** A handful of stablecoin points from the
  first days after listing are far off peg (USDC 0.00067 on its 2018-10-04 debut,
  USDT 0.57–1.32 through 2015 and mid-2018, one WxDAI spike to 1.97 on
  2024-01-23) — 17 rows in total. These come from CoinGecko's thin early
  liquidity, not from this script. Filter downstream if it matters.
- **Undocumented endpoint.** No stability guarantee; it can change shape or
  tighten the bot check without notice. The script fails loudly per token
  (non-JSON responses are treated as a challenge and retried) and exits non-zero
  if any token failed.
- **Not idempotent across a schema change.** If `coingecko_prices` ever becomes a
  `ReplacingMergeTree`, the prune step becomes redundant — drop it rather than
  leaving both.

### Last full run

2026-07-29 — 35 tokens, 45,328 rows, 2015-02-25 → 2026-07-28, 0 duplicates.
