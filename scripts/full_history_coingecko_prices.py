#!/usr/bin/env python3
"""One-shot backfill of *full* CoinGecko price history into coingecko_prices.

The regular `--ingestor=external-prices --external-prices-mode=backfill` path
uses the public API's `/coins/{id}/market_chart`, which is capped at
`days=365` on the free tier. This script instead reads CoinGecko's own chart
feed:

    https://www.coingecko.com/etl2/price_charts/{coingecko_id}/usd/max.json

which returns the whole daily series (GNO goes back to 2017-05-01) with no
API key. It is undocumented and sits behind a bot check that rejects plain
`requests`/curl with a 403 challenge page, so we go through curl_cffi with a
Chrome TLS fingerprint -- the same approach as ingestors/cow_ingestor.py.

Token set comes from config/external_prices_tokens.yml (the same allowlist the
daily ingestor uses) so the two can never drift apart.

Writes to the existing {database}.coingecko_prices table. That table is a plain
MergeTree, so re-running would otherwise duplicate rows; this script inserts the
fresh series first and then prunes the superseded rows for the same tokens,
which makes it safely re-runnable and never leaves a gap.

Usage:
    python scripts/full_history_coingecko_prices.py --dry-run
    python scripts/full_history_coingecko_prices.py --only GNO,COW
    python scripts/full_history_coingecko_prices.py --database crawlers_data
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml
from curl_cffi import requests as curl_requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import observability as obs  # noqa: E402
from run_queries import connect_clickhouse  # noqa: E402

logger = logging.getLogger("clickhouse_runner")

CHART_URL = "https://www.coingecko.com/etl2/price_charts/{cg_id}/usd/max.json"
HOME_URL = "https://www.coingecko.com/"
# curl_cffi TLS/JA3 profile; the endpoint 403s anything that looks non-browser.
IMPERSONATE_BROWSER = "chrome"

DEFAULT_TOKENS_CONFIG = "config/external_prices_tokens.yml"
DEFAULT_CREATE_SQL = "queries/external_prices/coingecko/create_table.sql"
DEFAULT_DATABASE = "crawlers_data"

COLUMNS = ["block_date", "coingecko_id", "symbol", "price", "ingested_at"]
INSERT_BATCH_SIZE = 5000
INSERT_SETTINGS = {"optimize_on_insert": 0, "max_insert_threads": 1}
# coingecko_prices is PARTITION BY toStartOfMonth(block_date) and ClickHouse
# rejects an insert block spanning more than max_partitions_per_insert_block
# (default 100). A decade of daily history is ~120 months, so batches are cut on
# month boundaries as well as row count. The 365-day backfill never hit this.
MAX_MONTHS_PER_BATCH = 90
# Wait for the prune mutation to finish before moving on, so a failure surfaces
# here rather than as a silently half-applied delete.
MUTATION_SETTINGS = {"mutations_sync": 2}

REQUEST_DELAY_S = 1.5
MAX_RETRIES = 3
REQUEST_TIMEOUT_S = 90

# coingecko_id goes into SQL literals; keep it to the slug charset it actually uses.
SAFE_ID = re.compile(r"^[a-z0-9][a-z0-9._-]*$")


# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------


def load_tokens(config_path: str, only: Optional[str]) -> List[Dict[str, str]]:
    """Read the shared allowlist, keeping CoinGecko-enabled tokens.

    Deduplicated by coingecko_id: CoinGecko prices are asset-level, so a token
    listed on several chains must only be fetched once.
    """
    path = Path(config_path)
    if not path.is_absolute():
        path = REPO_ROOT / path
    if not path.exists():
        raise FileNotFoundError(f"Token allowlist not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    wanted: Optional[set] = None
    if only:
        wanted = {s.strip().lower() for s in only.split(",") if s.strip()}

    seen: set = set()
    out: List[Dict[str, str]] = []
    skipped_no_id = 0
    for entry in data.get("tokens") or []:
        symbol = str(entry.get("symbol") or "").strip()
        cg_id = (entry.get("coingecko_id") or "").strip()
        if not symbol:
            continue
        if not entry.get("coingecko") or not cg_id:
            skipped_no_id += 1
            continue
        if not SAFE_ID.match(cg_id):
            logger.warning("Skipping token %s: unexpected coingecko_id %r", symbol, cg_id)
            continue
        if cg_id in seen:
            continue
        if wanted is not None and symbol.lower() not in wanted and cg_id.lower() not in wanted:
            continue
        seen.add(cg_id)
        out.append({"symbol": symbol, "coingecko_id": cg_id})

    if wanted is not None:
        matched = {t["symbol"].lower() for t in out} | {t["coingecko_id"].lower() for t in out}
        unknown = sorted(wanted - matched)
        if unknown:
            logger.warning("--only matched nothing for: %s", ", ".join(unknown))

    logger.info(
        "Loaded %s CoinGecko-enabled tokens from %s (%s without a coingecko_id)",
        len(out),
        path,
        skipped_no_id,
    )
    return out


# ----------------------------------------------------------------------
# Fetch
# ----------------------------------------------------------------------


def make_session(impersonate: str) -> Any:
    """Browser-fingerprinted session, warmed up on the homepage for cookies."""
    session = curl_requests.Session(impersonate=impersonate)
    session.headers.update({"Accept": "application/json", "Referer": HOME_URL})
    try:
        session.get(HOME_URL, timeout=REQUEST_TIMEOUT_S)
    except Exception as e:
        # Not fatal: the chart endpoint usually answers without a prior visit.
        logger.warning("Homepage warm-up failed (continuing): %s", e)
    return session


def fetch_series(session: Any, cg_id: str, *, delay_s: float, max_retries: int) -> Optional[List[List]]:
    """Return the raw [[unix_ms, price], ...] series, or None if unavailable."""
    url = CHART_URL.format(cg_id=cg_id)
    last_err: Optional[str] = None

    for attempt in range(1, max_retries + 1):
        time.sleep(delay_s if attempt == 1 else delay_s * attempt)
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT_S)
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            logger.warning("Request failed (%s/%s) for %s: %s", attempt, max_retries, cg_id, last_err)
            continue

        if resp.status_code == 404:
            logger.warning("No chart feed for %s (HTTP 404)", cg_id)
            return None
        if resp.status_code != 200:
            last_err = f"HTTP {resp.status_code}"
            logger.warning("%s for %s (%s/%s)", last_err, cg_id, attempt, max_retries)
            continue

        # A bot challenge comes back as 200 text/html, not JSON -- treat as retryable.
        content_type = resp.headers.get("content-type") or ""
        if "json" not in content_type.lower():
            last_err = f"non-JSON response ({content_type[:40]})"
            logger.warning("Bot challenge or bad payload for %s (%s/%s)", cg_id, attempt, max_retries)
            continue

        try:
            payload = resp.json()
        except Exception as e:
            last_err = f"JSON decode: {e}"
            continue

        series = payload.get("stats") if isinstance(payload, dict) else None
        if not isinstance(series, list):
            last_err = f"unexpected payload keys: {sorted(payload)[:6] if isinstance(payload, dict) else type(payload).__name__}"
            logger.error("Unexpected payload shape for %s: %s", cg_id, last_err)
            return None
        return series

    logger.error("Giving up on %s: %s", cg_id, last_err)
    return None


def to_rows(
    series: Sequence,
    cg_id: str,
    symbol: str,
    ingested_at: datetime,
    *,
    include_today: bool,
) -> List[Tuple]:
    """Collapse the series to one price per UTC date (last observation wins)."""
    today = datetime.now(timezone.utc).date()
    by_date: Dict[date, float] = {}
    for point in series:
        if not isinstance(point, (list, tuple)) or len(point) < 2:
            continue
        ts_ms, price = point[0], point[1]
        if ts_ms is None or price is None:
            continue
        block_date = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).date()
        if block_date == today and not include_today:
            continue
        by_date[block_date] = float(price)

    return [(d, cg_id, symbol, p, ingested_at) for d, p in sorted(by_date.items())]


# ----------------------------------------------------------------------
# ClickHouse
# ----------------------------------------------------------------------


def env_bool(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() in ("true", "1", "yes")


def batch_rows(rows: List[Tuple]) -> List[List[Tuple]]:
    """Split date-sorted rows into blocks bounded by row count and month span."""
    batches: List[List[Tuple]] = []
    current: List[Tuple] = []
    months: set = set()
    for row in rows:
        month = (row[0].year, row[0].month)
        if current and (len(current) >= INSERT_BATCH_SIZE or
                        (month not in months and len(months) >= MAX_MONTHS_PER_BATCH)):
            batches.append(current)
            current, months = [], set()
        current.append(row)
        months.add(month)
    if current:
        batches.append(current)
    return batches


def insert_rows(client: Any, table: str, rows: List[Tuple]) -> None:
    for batch in batch_rows(rows):
        with obs.time_operation(obs.get_job_name(), "external-prices", "insert_coingecko_history"):
            client.insert(table, batch, column_names=COLUMNS, settings=INSERT_SETTINGS)


def prune_superseded(client: Any, table: str, cg_id: str, ingested_at: datetime, max_date: date) -> None:
    """Drop this token's pre-existing rows now that the fresh series has landed.

    Insert-then-prune (rather than delete-then-insert) means the table is never
    missing the token, and a crash mid-run leaves duplicates -- recoverable --
    instead of a hole. Bounded by max_date so that a row for a later date
    written by the daily ingestor (e.g. today, when --include-today is off) is
    left alone.
    """
    sql = (
        f"ALTER TABLE {table} DELETE WHERE coingecko_id = '{cg_id}' "
        f"AND block_date <= toDate('{max_date.isoformat()}') "
        f"AND ingested_at < toDateTime('{ingested_at.strftime('%Y-%m-%d %H:%M:%S')}')"
    )
    client.command(sql, settings=MUTATION_SETTINGS)


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backfill full CoinGecko price history from the etl2 chart feed."
    )
    p.add_argument("--host", default=os.getenv("CH_HOST", "localhost"), help="ClickHouse host")
    p.add_argument("--port", type=int, default=int(os.getenv("CH_PORT", "9000")), help="ClickHouse port")
    p.add_argument("--user", default=os.getenv("CH_USER", "default"), help="ClickHouse user")
    p.add_argument("--password", default=os.getenv("CH_PASSWORD", ""), help="ClickHouse password")
    p.add_argument("--db", default=os.getenv("CH_DB", "default"), help="ClickHouse connection database")
    p.add_argument("--secure", default=os.getenv("CH_SECURE", "False"), help="Use TLS connection")
    p.add_argument("--verify", default=os.getenv("CH_VERIFY", "True"), help="Verify TLS certificate")

    p.add_argument(
        "--database",
        default=os.getenv("EXTERNAL_PRICES_DATABASE", DEFAULT_DATABASE),
        help="Database holding coingecko_prices",
    )
    p.add_argument("--table", help="Full target table name (overrides --database)")
    p.add_argument("--tokens-config", default=DEFAULT_TOKENS_CONFIG, help="Token allowlist YAML")
    p.add_argument("--only", help="Comma-separated symbols or coingecko_ids to limit the run")
    p.add_argument("--dry-run", action="store_true", help="Fetch and report, touch no database")
    p.add_argument(
        "--no-replace",
        action="store_true",
        help="Append without pruning superseded rows (will duplicate on re-run)",
    )
    p.add_argument(
        "--include-today",
        action="store_true",
        help=(
            "Also write today's point. It is a final 00:00 UTC value, not a partial one, "
            "but the daily ingestor already writes today and the table does not dedupe, "
            "so it is skipped by default to avoid a same-day duplicate."
        ),
    )
    p.add_argument("--skip-table-creation", action="store_true", help="Assume the table exists")
    p.add_argument("--delay", type=float, default=REQUEST_DELAY_S, help="Seconds between requests")
    p.add_argument("--max-retries", type=int, default=MAX_RETRIES, help="Retries per token")
    p.add_argument("--impersonate", default=IMPERSONATE_BROWSER, help="curl_cffi browser profile")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    obs.setup_logging()

    table = args.table or f"{args.database}.coingecko_prices"
    tokens = load_tokens(args.tokens_config, args.only)
    if not tokens:
        logger.error("No tokens to process")
        return 1

    client = None
    if not args.dry_run:
        client = connect_clickhouse(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.db,
            secure=str(args.secure).strip().lower() in ("true", "1", "yes"),
            verify=str(args.verify).strip().lower() in ("true", "1", "yes"),
        )
        if not args.skip_table_creation:
            create_sql = (REPO_ROOT / DEFAULT_CREATE_SQL).read_text(encoding="utf-8")
            client.command(create_sql.replace("{{EXTERNAL_PRICES_DATABASE}}", args.database))

    session = make_session(args.impersonate)
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)

    total_rows = 0
    failed: List[str] = []
    empty: List[str] = []

    for i, token in enumerate(tokens, 1):
        cg_id, symbol = token["coingecko_id"], token["symbol"]
        series = fetch_series(session, cg_id, delay_s=args.delay, max_retries=args.max_retries)
        if series is None:
            failed.append(cg_id)
            continue

        rows = to_rows(series, cg_id, symbol, ingested_at, include_today=args.include_today)
        if not rows:
            logger.warning("[%s/%s] %s (%s): empty series", i, len(tokens), symbol, cg_id)
            empty.append(cg_id)
            continue

        if not args.dry_run:
            try:
                insert_rows(client, table, rows)
                if not args.no_replace:
                    prune_superseded(client, table, cg_id, ingested_at, rows[-1][0])
            except Exception as e:
                logger.error(
                    "[%s/%s] %s (%s): write failed: %s",
                    i, len(tokens), symbol, cg_id, e,
                    extra={"event": "coingecko_history_write_failure", "symbol": symbol},
                )
                failed.append(cg_id)
                continue

        total_rows += len(rows)
        logger.info(
            "[%s/%s] %s (%s): %s days, %s -> %s",
            i, len(tokens), symbol, cg_id, len(rows), rows[0][0], rows[-1][0],
            extra={
                "event": "coingecko_history_backfill",
                "symbol": symbol,
                "coingecko_id": cg_id,
                "rows": len(rows),
            },
        )

    logger.info(
        "Done: %s rows across %s tokens%s",
        total_rows,
        len(tokens) - len(failed) - len(empty),
        " (dry run, nothing written)" if args.dry_run else f" -> {table}",
        extra={"event": "coingecko_history_done", "rows": total_rows},
    )
    if empty:
        logger.warning("No data returned for: %s", ", ".join(empty))
    if failed:
        logger.error("Failed: %s", ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
