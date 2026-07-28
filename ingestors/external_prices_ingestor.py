"""Ingest daily / historical USD prices from DefiLlama and CoinGecko.

Phase 1 standbein: writes {database}.defillama_prices /
{database}.coingecko_prices for an allowlisted token set (database from
EXTERNAL_PRICES_DATABASE / --external-prices-database). Does not touch
the dbt price hub.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
import yaml
from clickhouse_connect.driver.client import Client

import observability as obs
from .base import BaseIngestor

logger = logging.getLogger("clickhouse_runner")

DEFILLAMA_BASE = "https://coins.llama.fi"
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
CHAIN = "gnosis"

INSERT_BATCH_SIZE = 5000
INSERT_SETTINGS = {"optimize_on_insert": 0, "max_insert_threads": 1}

# Chart / market_chart calls are heavier; daily batch is a single request.
BACKFILL_DELAY_S = 1.2
DAILY_DELAY_S = 0.3
KEYLESS_CG_DELAY_S = 2.5
MAX_RETRIES = 5


class ExternalPricesIngestor(BaseIngestor):
    """Fetch allowlisted token prices from DefiLlama and/or CoinGecko."""

    def __init__(
        self,
        client: Client,
        variables: Dict[str, str],
        tokens_config: str,
        source: str = "both",
        mode: str = "daily",
        defillama_create_sql: str = "queries/external_prices/defillama/create_table.sql",
        coingecko_create_sql: str = "queries/external_prices/coingecko/create_table.sql",
        defillama_table: str = "crawlers_data.defillama_prices",
        coingecko_table: str = "crawlers_data.coingecko_prices",
        coingecko_api_key: Optional[str] = None,
        chart_span_days: int = 365,
    ):
        super().__init__(client, variables)
        if source not in ("defillama", "coingecko", "both"):
            raise ValueError(f"Invalid source: {source}")
        if mode not in ("daily", "backfill"):
            raise ValueError(f"Invalid mode: {mode}")

        self.source = source
        self.mode = mode
        self.tokens_config = tokens_config
        self.defillama_create_sql = defillama_create_sql
        self.coingecko_create_sql = coingecko_create_sql
        self.defillama_table = defillama_table
        self.coingecko_table = coingecko_table
        self.coingecko_api_key = coingecko_api_key or None
        self.chart_span_days = chart_span_days
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "gnosis-click-runner-external-prices/1.0",
            }
        )
        if self.coingecko_api_key:
            # Demo key header; Pro keys use x-cg-pro-api-key + pro host (not used here).
            self.session.headers["x-cg-demo-api-key"] = self.coingecko_api_key

    def ingest(self, skip_table_creation: bool = False, **kwargs) -> bool:
        tokens = self._load_tokens()
        if not tokens:
            logger.error("No tokens loaded from %s", self.tokens_config)
            return False

        if not skip_table_creation:
            if self.source in ("defillama", "both"):
                if not self.execute_queries([self.load_sql_file(self.defillama_create_sql)]):
                    return False
            if self.source in ("coingecko", "both"):
                if not self.execute_queries([self.load_sql_file(self.coingecko_create_sql)]):
                    return False

        ok = True
        if self.source in ("defillama", "both"):
            ok = self._ingest_defillama(tokens) and ok
        if self.source in ("coingecko", "both"):
            ok = self._ingest_coingecko(tokens) and ok
        return ok

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------

    def _load_tokens(self) -> List[Dict[str, Any]]:
        path = Path(self.tokens_config)
        if not path.exists():
            raise FileNotFoundError(f"Token allowlist not found: {path}")
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        tokens = data.get("tokens") or []
        out: List[Dict[str, Any]] = []
        for t in tokens:
            symbol = str(t.get("symbol") or "").strip()
            if not symbol:
                continue
            out.append(
                {
                    "symbol": symbol,
                    "address": (t.get("address") or "").strip().lower(),
                    "coingecko_id": (t.get("coingecko_id") or None),
                    "defillama": bool(t.get("defillama")),
                    "coingecko": bool(t.get("coingecko")),
                }
            )
        logger.info("Loaded %s allowlisted tokens from %s", len(out), path)
        return out

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _get_json(self, url: str, *, delay_s: float) -> Optional[Dict[str, Any]]:
        last_err: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                time.sleep(delay_s if attempt == 1 else delay_s * attempt)
                resp = self.session.get(url, timeout=60)
                if resp.status_code == 429:
                    wait = min(60.0, delay_s * (2**attempt))
                    logger.warning("HTTP 429 for %s — sleeping %.1fs (attempt %s)", url, wait, attempt)
                    time.sleep(wait)
                    continue
                if resp.status_code >= 400:
                    logger.error("HTTP %s for %s: %s", resp.status_code, url, resp.text[:300])
                    return None
                return resp.json()
            except Exception as e:
                last_err = e
                logger.warning("Request failed (%s/%s) %s: %s", attempt, MAX_RETRIES, url, e)
                time.sleep(delay_s * attempt)
        logger.error("Giving up on %s: %s", url, last_err)
        return None

    # ------------------------------------------------------------------
    # DefiLlama
    # ------------------------------------------------------------------

    def _ingest_defillama(self, tokens: Sequence[Dict[str, Any]]) -> bool:
        llama_tokens = [t for t in tokens if t["defillama"] and t["address"]]
        if not llama_tokens:
            logger.warning("No DefiLlama-enabled tokens in allowlist")
            return True

        if self.mode == "daily":
            return self._defillama_daily(llama_tokens)
        return self._defillama_backfill(llama_tokens)

    def _defillama_daily(self, tokens: Sequence[Dict[str, Any]]) -> bool:
        coin_keys = [f"{CHAIN}:{t['address']}" for t in tokens]
        url = f"{DEFILLAMA_BASE}/prices/current/{','.join(coin_keys)}"
        payload = self._get_json(url, delay_s=DAILY_DELAY_S)
        if not payload:
            return False

        coins = payload.get("coins") or {}
        # Map address -> allowlist symbol (API symbol casing varies).
        by_addr = {t["address"]: t["symbol"] for t in tokens}
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        # Prefer calendar date of the quote timestamp; fall back to UTC today.
        rows: List[Tuple] = []
        for key, quote in coins.items():
            # key like gnosis:0x...
            addr = key.split(":", 1)[-1].lower()
            symbol = by_addr.get(addr)
            if not symbol:
                continue
            price = quote.get("price")
            if price is None:
                continue
            ts = quote.get("timestamp")
            if ts:
                block_date = datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
            else:
                block_date = now.date()
            rows.append(
                (
                    block_date,
                    CHAIN,
                    addr,
                    symbol,
                    float(price),
                    float(quote["confidence"]) if quote.get("confidence") is not None else None,
                    now,
                )
            )

        missing = sorted(set(by_addr) - {r[2] for r in rows})
        if missing:
            logger.warning("DefiLlama daily missing %s addresses: %s", len(missing), missing[:10])

        return self._insert_rows(
            self.defillama_table,
            ["block_date", "chain", "token_address", "symbol", "price", "confidence", "ingested_at"],
            rows,
            source="defillama",
        )

    def _defillama_backfill(self, tokens: Sequence[Dict[str, Any]]) -> bool:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        all_rows: List[Tuple] = []
        for t in tokens:
            coin = f"{CHAIN}:{t['address']}"
            url = (
                f"{DEFILLAMA_BASE}/chart/{coin}"
                f"?period=1d&span={self.chart_span_days}"
            )
            payload = self._get_json(url, delay_s=BACKFILL_DELAY_S)
            if not payload:
                logger.error("DefiLlama chart failed for %s (%s)", t["symbol"], coin)
                continue
            series = (payload.get("coins") or {}).get(coin) or {}
            prices = series.get("prices") or []
            for pt in prices:
                ts = pt.get("timestamp")
                price = pt.get("price")
                if ts is None or price is None:
                    continue
                block_date = datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
                all_rows.append(
                    (
                        block_date,
                        CHAIN,
                        t["address"],
                        t["symbol"],
                        float(price),
                        float(series["confidence"]) if series.get("confidence") is not None else None,
                        now,
                    )
                )
            logger.info(
                "DefiLlama backfill %s: %s points",
                t["symbol"],
                len(prices),
                extra={"event": "external_prices_backfill", "source": "defillama", "symbol": t["symbol"]},
            )

        if not all_rows:
            logger.error("DefiLlama backfill produced zero rows")
            return False
        return self._insert_rows(
            self.defillama_table,
            ["block_date", "chain", "token_address", "symbol", "price", "confidence", "ingested_at"],
            all_rows,
            source="defillama",
        )

    # ------------------------------------------------------------------
    # CoinGecko
    # ------------------------------------------------------------------

    def _cg_delay(self) -> float:
        return DAILY_DELAY_S if self.coingecko_api_key else KEYLESS_CG_DELAY_S

    def _ingest_coingecko(self, tokens: Sequence[Dict[str, Any]]) -> bool:
        cg_tokens = [
            t for t in tokens if t["coingecko"] and t.get("coingecko_id")
        ]
        if not cg_tokens:
            logger.warning("No CoinGecko-enabled tokens with coingecko_id in allowlist")
            return True

        if self.mode == "daily":
            return self._coingecko_daily(cg_tokens)
        return self._coingecko_backfill(cg_tokens)

    def _coingecko_daily(self, tokens: Sequence[Dict[str, Any]]) -> bool:
        ids = [t["coingecko_id"] for t in tokens]
        by_id = {t["coingecko_id"]: t["symbol"] for t in tokens}
        url = (
            f"{COINGECKO_BASE}/simple/price"
            f"?ids={','.join(ids)}&vs_currencies=usd&include_last_updated_at=true"
        )
        payload = self._get_json(url, delay_s=self._cg_delay())
        if not payload:
            return False

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows: List[Tuple] = []
        for cg_id, quote in payload.items():
            symbol = by_id.get(cg_id)
            if not symbol or quote.get("usd") is None:
                continue
            ts = quote.get("last_updated_at")
            if ts:
                block_date = datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
            else:
                block_date = now.date()
            rows.append((block_date, cg_id, symbol, float(quote["usd"]), now))

        missing = sorted(set(by_id) - {r[1] for r in rows})
        if missing:
            logger.warning("CoinGecko daily missing %s ids: %s", len(missing), missing)

        return self._insert_rows(
            self.coingecko_table,
            ["block_date", "coingecko_id", "symbol", "price", "ingested_at"],
            rows,
            source="coingecko",
        )

    def _coingecko_backfill(self, tokens: Sequence[Dict[str, Any]]) -> bool:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        all_rows: List[Tuple] = []
        # Free tier: days=365 (days=max rejected without paid plan).
        days = min(self.chart_span_days, 365)
        for t in tokens:
            cg_id = t["coingecko_id"]
            url = (
                f"{COINGECKO_BASE}/coins/{cg_id}/market_chart"
                f"?vs_currency=usd&days={days}"
            )
            payload = self._get_json(url, delay_s=max(BACKFILL_DELAY_S, self._cg_delay()))
            if not payload:
                logger.error("CoinGecko market_chart failed for %s (%s)", t["symbol"], cg_id)
                continue
            prices = payload.get("prices") or []
            # Collapse intraday points to one price per UTC date (last observation).
            by_date: Dict[Any, float] = {}
            for pt in prices:
                if not pt or len(pt) < 2:
                    continue
                ts_ms, price = pt[0], pt[1]
                block_date = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).date()
                by_date[block_date] = float(price)
            for block_date, price in sorted(by_date.items()):
                all_rows.append((block_date, cg_id, t["symbol"], price, now))
            logger.info(
                "CoinGecko backfill %s: %s daily points (from %s raw)",
                t["symbol"],
                len(by_date),
                len(prices),
                extra={"event": "external_prices_backfill", "source": "coingecko", "symbol": t["symbol"]},
            )

        if not all_rows:
            logger.error("CoinGecko backfill produced zero rows")
            return False
        return self._insert_rows(
            self.coingecko_table,
            ["block_date", "coingecko_id", "symbol", "price", "ingested_at"],
            all_rows,
            source="coingecko",
        )

    # ------------------------------------------------------------------
    # ClickHouse insert
    # ------------------------------------------------------------------

    def _insert_rows(
        self,
        table: str,
        columns: List[str],
        rows: List[Tuple],
        *,
        source: str,
    ) -> bool:
        if not rows:
            logger.warning("No rows to insert into %s (%s)", table, source)
            return True
        try:
            before = self.get_row_count(table)
        except Exception:
            before = 0

        for i in range(0, len(rows), INSERT_BATCH_SIZE):
            batch = rows[i : i + INSERT_BATCH_SIZE]
            try:
                with obs.time_operation(obs.get_job_name(), "external-prices", f"insert_{source}"):
                    self.client.insert(
                        table,
                        batch,
                        column_names=columns,
                        settings=INSERT_SETTINGS,
                    )
            except Exception as e:
                logger.error("Insert into %s failed: %s", table, e)
                return False

        try:
            after = self.get_row_count(table)
        except Exception:
            after = before + len(rows)
        logger.info(
            "Inserted %s rows into %s (%s → %s)",
            len(rows),
            table,
            before,
            after,
            extra={"event": "external_prices_insert", "source": source, "rows": len(rows)},
        )
        return True
