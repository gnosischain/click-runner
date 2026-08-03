"""Ingest HOPR node quality/uptime from HOPR's network dashboard REST API.

Source: https://network.hoprnet.org/api/* -- public and unauthenticated, but it
requires a browser-like User-Agent and a Referer header, otherwise the CDN in
front of it can answer with an error page.

WHAT THIS IS AND IS NOT
-----------------------
This dashboard is a heartbeat/ping prober bound to the LEGACY v3 release train:
`/api/getStableHoprdVersion` returns 3.0.0 (Kaunas), its metrics are ping-derived,
and `/api/loadNetworks` lists only dufour (id 3, operational), monte_rosa (2) and
paleochora (1) -- both retired. There is deliberately no `jura`: v4 replaced this
indexing path with Blokli, and the prober was never ported. So this ingestor is a
DUFOUR-ONLY asset; jura node state comes from hopr_blokli_ingestor.

WHY IT IS WORTH INGESTING
-------------------------
It is the only source of node QUALITY -- per-node availability over 24h/7d/30d/6m/1y
and round-trip latency -- and it keys on the node's on-chain address, so it joins
directly to the channel events we decode ourselves. That join is what lets us ask
whether more stake buys better uptime or more earnings, which nothing public
answers today.

VALIDATED against chain before being trusted: of the 368 distinct dufour
ChannelOpened destinations in July 2026, 357 appear in this roster (97.0%), and
93.7% of roster nodes were seen on-chain that month. The gaps are real and
directional, not noise -- 11 addresses are PAID on-chain yet absent from the
prober (so this roster is not the population of record), and 24 roster nodes were
simply offline in July but retained by the 1-year history window.

Grain: one snapshot per (network, snapshot_date, node_address) for the roster,
plus append-only historical series that only need one backfill.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

import requests
from clickhouse_connect.driver.client import Client

import observability as obs
from .base import BaseIngestor

logger = logging.getLogger("clickhouse_runner")

BASE_URL = "https://network.hoprnet.org"
# The CDN in front of this host returns an error page without a browser-like UA.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": f"{BASE_URL}/",
}
REQUEST_TIMEOUT_S = 60
MAX_RETRIES = 4
RETRY_BACKOFF_S = 2.0
INSERT_SETTINGS = {"optimize_on_insert": 0, "max_insert_threads": 1}
INSERT_BATCH_SIZE = 5000


class HoprNetworkIngestor(BaseIngestor):
    """Snapshot HOPR node availability/latency from the network dashboard API."""

    def __init__(
        self,
        client: Client,
        variables: Dict[str, str],
        network_ids: Sequence[int] = (3,),
        mode: str = "daily",
        nodes_table: str = "crawlers_data.hopr_network_nodes",
        online_table: str = "crawlers_data.hopr_network_online_hourly",
        nodes_create_sql: str = "queries/hopr/network_nodes_create.sql",
        online_create_sql: str = "queries/hopr/network_online_hourly_create.sql",
    ):
        super().__init__(client, variables)
        if mode not in ("daily", "backfill"):
            raise ValueError(f"Invalid mode: {mode} (expected daily|backfill)")
        self.network_ids = list(network_ids)
        self.mode = mode
        self.nodes_table = nodes_table
        self.online_table = online_table
        self.nodes_create_sql = nodes_create_sql
        self.online_create_sql = online_create_sql
        self.session = requests.Session()

    # ---------------------------------------------------------------- transport

    def _get(self, path: str) -> Optional[Any]:
        url = f"{BASE_URL}{path}"
        last_err: Optional[str] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_S)
                if resp.status_code >= 500:
                    last_err = f"HTTP {resp.status_code}"
                elif resp.status_code != 200:
                    logger.error(
                        f"{path}: HTTP {resp.status_code} (not retrying)",
                        extra={"event": "hopr_net_http_error", "ingestor": "hopr-network"},
                    )
                    return None
                else:
                    return resp.json()
            except (requests.RequestException, ValueError) as e:
                last_err = str(e)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_S * attempt)
        logger.error(
            f"{path}: giving up after {MAX_RETRIES} attempts ({last_err})",
            extra={"event": "hopr_net_exhausted", "ingestor": "hopr-network"},
        )
        return None

    # ------------------------------------------------------------------ helpers

    def discover_networks(self) -> List[Dict[str, Any]]:
        """List the networks this dashboard knows about. jura will NOT be here."""
        payload = self._get("/api/loadNetworks")
        return payload if isinstance(payload, list) else []

    @staticmethod
    def _num(value: Any) -> Optional[float]:
        """Coerce a numeric API field to float, or None.

        The prober mixes types within the same field: `latency` comes back as int
        for most nodes, float for a few (averaged RTT, e.g. 33.5) and null for the
        ~76 it cannot reach; availability fields mix int 1 with floats. Passing
        that mixture straight to clickhouse_connect raises
        "required argument is not an integer" / DataError, so normalize here.
        """
        if value is None or isinstance(value, bool):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _ms_to_dt(value: Any) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).replace(tzinfo=None)
        except (TypeError, ValueError, OSError):
            return None

    def _insert(self, table: str, rows: List[Dict[str, Any]], columns: List[str]) -> bool:
        if not rows:
            logger.warning(
                f"No rows to insert into {table}",
                extra={"event": "hopr_net_no_rows", "ingestor": "hopr-network"},
            )
            return True
        try:
            with obs.time_operation(obs.get_job_name(), "hopr-network", f"insert:{table}"):
                for i in range(0, len(rows), INSERT_BATCH_SIZE):
                    chunk = rows[i:i + INSERT_BATCH_SIZE]
                    self.client.insert(
                        table, [[r.get(c) for c in columns] for r in chunk],
                        column_names=columns, settings=INSERT_SETTINGS,
                    )
        except Exception as e:
            logger.error(
                f"Insert into {table} failed: {e}",
                extra={"event": "hopr_net_insert_failure", "ingestor": "hopr-network"},
            )
            return False
        logger.info(
            f"Inserted {len(rows)} rows into {table}",
            extra={"event": "hopr_net_insert_ok", "ingestor": "hopr-network"},
        )
        return True

    # ------------------------------------------------------------------ fetches

    def _fetch_nodes(self, env: int, snapshot_date, ingested_at: datetime) -> Optional[List[Dict]]:
        payload = self._get(f"/api/getNodes?env={env}")
        if not isinstance(payload, dict):
            return None
        nodes = payload.get("nodes")
        if not isinstance(nodes, list):
            logger.error(
                f"getNodes?env={env}: no 'nodes' array -- refusing to write",
                extra={"event": "hopr_net_bad_nodes", "ingestor": "hopr-network"},
            )
            return None
        cfg = payload.get("config") or {}
        last_run = self._ms_to_dt(cfg.get("lastRun"))
        rows: List[Dict[str, Any]] = []
        for n in nodes:
            addr = (n.get("address") or "").lower()
            if not addr:
                continue
            rows.append({
                "snapshot_date": snapshot_date,
                "network_id": env,
                "node_address": addr,
                "latency_ms": self._num(n.get("latency")),
                "availability_24h": self._num(n.get("availability24h")),
                "availability_7d": self._num(n.get("availability7d")),
                "availability_30d": self._num(n.get("availability30d")),
                "availability_6m": self._num(n.get("availability6m")),
                "availability_1y": self._num(n.get("availability1y")),
                "first_seen": self._ms_to_dt(n.get("firstseen")),
                "last_seen": self._ms_to_dt(n.get("lastseen")),
                "prober_last_run": last_run,
                "ingested_at": ingested_at,
            })
        return rows

    def _fetch_online_hourly(self, env: int, ingested_at: datetime) -> Optional[List[Dict]]:
        """Hourly online-node count. History starts 2023-09-02; backfill once."""
        payload = self._get(f"/api/statistics/getHistoricalOnlineByHour?env={env}")
        if not isinstance(payload, list):
            logger.error(
                f"getHistoricalOnlineByHour?env={env}: unexpected payload",
                extra={"event": "hopr_net_bad_hourly", "ingestor": "hopr-network"},
            )
            return None
        rows: List[Dict[str, Any]] = []
        for p in payload:
            raw_x, raw_y = p.get("x"), p.get("y")
            if not raw_x:
                continue
            # Format is '2023-9-13 16:00' -- non-zero-padded month/day.
            try:
                dt = datetime.strptime(str(raw_x), "%Y-%m-%d %H:%M")
            except ValueError:
                logger.warning(
                    f"Unparseable timestamp {raw_x!r}; skipping",
                    extra={"event": "hopr_net_bad_ts", "ingestor": "hopr-network"},
                )
                continue
            try:
                online = int(raw_y)
            except (TypeError, ValueError):
                continue
            rows.append({
                "network_id": env,
                "observed_at": dt,
                "nodes_online": online,
                "ingested_at": ingested_at,
            })
        return rows

    # ------------------------------------------------------------------- ingest

    def ingest(self, skip_table_creation: bool = False, **kwargs) -> bool:
        if not skip_table_creation:
            for path in (self.nodes_create_sql, self.online_create_sql):
                try:
                    if not self.execute_queries([self.load_sql_file(path)]):
                        return False
                except FileNotFoundError:
                    logger.error(
                        f"Create-table SQL not found: {path}",
                        extra={"event": "hopr_net_missing_sql", "ingestor": "hopr-network"},
                    )
                    return False

        known = {n.get("id"): n for n in self.discover_networks()}
        if known:
            logger.info(
                "Dashboard networks: "
                + ", ".join(f"{n.get('name')}(id={n.get('id')},op={n.get('operational')})"
                            for n in known.values()),
                extra={"event": "hopr_net_networks", "ingestor": "hopr-network"},
            )

        snapshot_date = datetime.now(timezone.utc).date()
        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

        node_cols = [
            "snapshot_date", "network_id", "node_address", "latency_ms",
            "availability_24h", "availability_7d", "availability_30d",
            "availability_6m", "availability_1y", "first_seen", "last_seen",
            "prober_last_run", "ingested_at",
        ]
        online_cols = ["network_id", "observed_at", "nodes_online", "ingested_at"]

        ok = True
        for env in self.network_ids:
            if known and env not in known:
                logger.warning(
                    f"env={env} is not in /api/loadNetworks; fetching anyway "
                    "(hidden ids exist, e.g. 4 is a retired staging net)",
                    extra={"event": "hopr_net_unknown_env", "ingestor": "hopr-network"},
                )

            rows = self._fetch_nodes(env, snapshot_date, ingested_at)
            if rows is None:
                ok = False
            elif not self._insert(self.nodes_table, rows, node_cols):
                ok = False

            if self.mode == "backfill":
                hourly = self._fetch_online_hourly(env, ingested_at)
                if hourly is None:
                    ok = False
                elif not self._insert(self.online_table, hourly, online_cols):
                    ok = False
        return ok
