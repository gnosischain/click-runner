"""
Snapshot DelegateRegistry (Ethereum mainnet) → ClickHouse.

Replaces the Dune query https://dune.com/queries/8075078 with direct
eth_getLogs against the Snapshot/Gnosis DelegateRegistry contract:

    0x469788fE6E9E9681C6ebF3bF78e7Fd26Fc015446

Events (both fully indexed):
    SetDelegate(address delegator, bytes32 id, address delegate)
    ClearDelegate(address delegator, bytes32 id, address delegate)

We filter topic2 to the space id bytes32 (default: "gnosis.eth"), which keeps
the scan tiny (~100 lifetime events) instead of walking every registry log.

Modes (--delegations-mode):
  backfill  scan from registry deploy (or --delegations-from-block) → tip
  daily     scan from max(block_number) in CH minus a reorg overlap → tip;
            empty table falls back to a full backfill

Optional: --delegations-dry-run (no ClickHouse writes), --delegations-csv.

Env:
  MAINNET_RPC_URL          required JSON-RPC endpoint with historical eth_getLogs
  GOVERNANCE_DATABASE      target DB (default crawlers_data)
  SNAPSHOT_SPACE           space id string (default gnosis.eth)
  DELEGATIONS_FROM_BLOCK   optional override of the registry deploy block
"""

from __future__ import annotations

import csv
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
from clickhouse_connect.driver.client import Client

import observability as obs
from .base import BaseIngestor

logger = logging.getLogger("clickhouse_runner")

DELEGATE_REGISTRY = "0x469788fe6e9e9681c6ebf3bf78e7fd26fc015446"
# First Snapshot DelegateRegistry creation on Ethereum mainnet.
DEFAULT_FROM_BLOCK = 11_225_329
TOPIC_SET = "0xa9a7fd460f56bddb880a465a9c3e9730389c70bc53108148f16d55a87a6c468e"
TOPIC_CLEAR = "0x9c4f00c4291262731946e308dc2979a56bd22cce8f95906b975065e96cd5a064"

# Start large; providers that reject the range trigger adaptive halving.
INITIAL_CHUNK = 100_000
MIN_CHUNK = 1_000
INSERT_BATCH = 5_000
RPC_TIMEOUT = 60
# Ethereum finality cushion when resuming from a ClickHouse watermark.
DEFAULT_REORG_OVERLAP_BLOCKS = 128


def space_id_topic(space: str) -> str:
    """UTF-8 space id left-aligned in a bytes32 word (Snapshot convention)."""
    raw = space.encode("utf-8")
    if len(raw) > 32:
        raise ValueError(f"space id longer than 32 bytes: {space!r}")
    return "0x" + raw.hex().ljust(64, "0")


def _topic_address(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def _hex_int(value: str) -> int:
    return int(value, 16)


class RpcError(RuntimeError):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(f"RPC {code}: {message}")
        self.code = code
        self.message = message


def _is_range_error(code: int, message: str) -> bool:
    lowered = message.lower()
    markers = (
        "block range",
        "too many",
        "query returned more than",
        "response size",
        "limit exceeded",
        "please limit",
        "timeout",
    )
    return code in {-32005, -32016, -32602, -32603} or any(m in lowered for m in markers)


class SnapshotDelegationsIngestor(BaseIngestor):
    """Fetch gnosis.eth (or other space) DelegateRegistry events from mainnet RPC."""

    def __init__(
        self,
        client: Optional[Client],
        variables: Dict[str, str],
        *,
        rpc_url: str,
        database: str,
        space: str = "gnosis.eth",
        create_table_sql: str = "queries/governance/create_snapshot_delegations_table.sql",
        mode: str = "daily",
        from_block: Optional[int] = None,
        to_block: Optional[int] = None,
        reorg_overlap_blocks: int = DEFAULT_REORG_OVERLAP_BLOCKS,
        dry_run: bool = False,
        csv_path: Optional[str] = None,
    ) -> None:
        super().__init__(client, variables)  # type: ignore[arg-type]
        if mode not in ("backfill", "daily"):
            raise ValueError(f"unsupported delegations mode: {mode!r}")
        self.rpc_url = rpc_url
        self.database = database
        self.space = space
        self.space_topic = space_id_topic(space)
        self.create_table_sql = create_table_sql
        self.mode = mode
        self.from_block_override = from_block
        self.to_block = to_block
        self.reorg_overlap_blocks = max(0, int(reorg_overlap_blocks))
        self.dry_run = dry_run
        self.csv_path = csv_path
        self.table = f"{database}.snapshot_delegations"
        self._rpc_id = 0
        self._session = requests.Session()

    def ingest(self, **kwargs) -> bool:
        skip_table_creation = bool(kwargs.get("skip_table_creation", False))
        try:
            if not self.dry_run:
                if self.client is None:
                    logger.error("ClickHouse client required when dry_run=False")
                    return False
                if not skip_table_creation and not self._create_table():
                    return False

            latest = self._eth_block_number()
            end = self.to_block if self.to_block is not None else latest
            start, start_reason = self._resolve_start_block(end)
            if end < start:
                logger.info(
                    "Nothing to scan: start=%s end=%s (%s). Exiting OK.",
                    start,
                    end,
                    start_reason,
                )
                return True

            logger.info(
                "Scanning DelegateRegistry %s for space=%s topic2=%s "
                "mode=%s blocks=[%s, %s] (%s; latest=%s, dry_run=%s)",
                DELEGATE_REGISTRY,
                self.space,
                self.space_topic,
                self.mode,
                start,
                end,
                start_reason,
                latest,
                self.dry_run,
            )

            rows = self._scan_logs(start, end)
            rows.sort(key=lambda r: (r["block_number"], r["log_index"], r["tx_hash"]))
            fetched = len(rows)

            # Skip keys already in CH so ReplacingMergeTree never holds
            # unmerged duplicate versions that dbt/dashboards can briefly see
            # (project preference: avoid relying on FINAL downstream).
            if not self.dry_run and rows:
                rows = self._filter_existing_rows(rows)
                skipped = fetched - len(rows)
                if skipped:
                    logger.info(
                        "Skipped %s already-stored event(s); %s new to insert",
                        skipped,
                        len(rows),
                    )

            self._attach_block_times(rows)

            logger.info(
                "Fetched %s delegation events (%s new after dedupe; %s set, %s clear)",
                fetched,
                len(rows),
                sum(1 for r in rows if r["action"] == "set"),
                sum(1 for r in rows if r["action"] == "clear"),
            )
            if rows:
                first, last = rows[0], rows[-1]
                logger.info(
                    "Range: first block=%s %s → last block=%s %s",
                    first["block_number"],
                    first["block_time"].isoformat(),
                    last["block_number"],
                    last["block_time"].isoformat(),
                )
                for sample in rows[:3]:
                    logger.info(
                        "  sample %s delegator=%s delegate=%s tx=%s",
                        sample["action"],
                        sample["delegator"],
                        sample["delegate"],
                        sample["tx_hash"],
                    )

            if self.csv_path:
                self._write_csv(rows, self.csv_path)
                logger.info("Wrote CSV: %s (%s rows)", self.csv_path, len(rows))

            if self.dry_run:
                logger.info("Dry-run complete — no ClickHouse writes.")
                return True

            self._insert_rows(rows)
            count = self.get_row_count(self.table)
            logger.info(
                "ClickHouse %s row count (incl. duplicates pre-merge): %s",
                self.table,
                count,
            )
            return True
        except Exception as e:
            logger.exception("snapshot-delegations ingest failed: %s", e)
            return False
        finally:
            self._session.close()

    def _resolve_start_block(self, end: int) -> Tuple[int, str]:
        """Return (from_block, reason) for the scan window."""
        if self.from_block_override is not None:
            return self.from_block_override, "cli/env override"

        if self.mode == "backfill":
            return DEFAULT_FROM_BLOCK, "backfill from registry deploy"

        # daily: resume from watermark; empty table → full history once.
        watermark = self._max_stored_block()
        if watermark is None:
            return DEFAULT_FROM_BLOCK, "daily with empty table → full backfill"

        start = max(DEFAULT_FROM_BLOCK, watermark - self.reorg_overlap_blocks)
        return (
            start,
            f"daily from watermark {watermark} - overlap {self.reorg_overlap_blocks}",
        )

    def _max_stored_block(self) -> Optional[int]:
        if self.client is None:
            return None
        try:
            result = self.client.query(
                f"SELECT max(block_number) FROM {self.table}"
            )
            if not result.result_rows:
                return None
            value = result.result_rows[0][0]
            if value is None:
                return None
            value_int = int(value)
            # ClickHouse max() on empty table returns 0 for UInt64.
            if value_int <= 0:
                return None
            return value_int
        except Exception as e:
            logger.warning(
                "Could not read watermark from %s (%s); daily will full-backfill.",
                self.table,
                e,
            )
            return None

    def _filter_existing_rows(
        self, rows: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Drop rows whose (tx_hash, log_index) already exist in ClickHouse."""
        if self.client is None or not rows:
            return rows

        # Bound the lookup to the scanned block window (small for daily).
        min_block = min(r["block_number"] for r in rows)
        max_block = max(r["block_number"] for r in rows)
        try:
            result = self.client.query(
                f"""
                SELECT lower(tx_hash) AS tx_hash, log_index
                FROM {self.table}
                WHERE block_number >= {{min_block:UInt64}}
                  AND block_number <= {{max_block:UInt64}}
                """,
                parameters={"min_block": min_block, "max_block": max_block},
            )
        except Exception as e:
            # Fail closed on insert path would risk dupes; fail open with a
            # warning so a transient CH read error does not abort the job.
            logger.warning(
                "Could not load existing keys from %s (%s); inserting without "
                "pre-dedupe (RMT may hold temporary duplicates).",
                self.table,
                e,
            )
            return rows

        existing = {
            (str(tx_hash).lower(), int(log_index))
            for tx_hash, log_index in result.result_rows
        }
        if not existing:
            return rows
        return [
            row
            for row in rows
            if (row["tx_hash"], row["log_index"]) not in existing
        ]

    # ------------------------------------------------------------------ #
    # RPC
    # ------------------------------------------------------------------ #
    def _rpc(self, method: str, params: Sequence[Any]) -> Any:
        self._rpc_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": self._rpc_id,
            "method": method,
            "params": list(params),
        }
        with obs.time_operation(obs.get_job_name(), "snapshot_delegations", method):
            response = self._session.post(
                self.rpc_url, json=payload, timeout=RPC_TIMEOUT
            )
        response.raise_for_status()
        body = response.json()
        if "error" in body and body["error"] is not None:
            err = body["error"]
            raise RpcError(int(err.get("code", -1)), str(err.get("message", "unknown")))
        return body.get("result")

    def _eth_block_number(self) -> int:
        return _hex_int(self._rpc("eth_blockNumber", []))

    def _get_logs(self, from_block: int, to_block: int, topic0: str) -> List[Dict]:
        params = [{
            "address": DELEGATE_REGISTRY,
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "topics": [topic0, None, self.space_topic],
        }]
        result = self._rpc("eth_getLogs", params)
        if not isinstance(result, list):
            raise RuntimeError(f"eth_getLogs returned non-list: {type(result)}")
        return result

    def _scan_logs(self, start: int, end: int) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for topic0, action in ((TOPIC_SET, "set"), (TOPIC_CLEAR, "clear")):
            cursor = start
            chunk = INITIAL_CHUNK
            while cursor <= end:
                chunk_end = min(cursor + chunk - 1, end)
                try:
                    logs = self._get_logs(cursor, chunk_end, topic0)
                except RpcError as e:
                    if _is_range_error(e.code, e.message) and chunk > MIN_CHUNK:
                        chunk = max(MIN_CHUNK, chunk // 2)
                        logger.warning(
                            "RPC range limit (%s); reducing chunk to %s",
                            e.message,
                            chunk,
                        )
                        continue
                    raise
                for log in logs:
                    rows.append(self._decode_log(log, action))
                if chunk_end == end or (chunk_end - cursor + 1) % (INITIAL_CHUNK) == 0:
                    logger.info(
                        "  %s scan %s–%s (%s logs so far for action)",
                        action,
                        cursor,
                        chunk_end,
                        sum(1 for r in rows if r["action"] == action),
                    )
                cursor = chunk_end + 1
                # Grow chunk again after successes (up to initial).
                chunk = min(INITIAL_CHUNK, chunk * 2)
                time.sleep(0.05)
        return rows

    def _decode_log(self, log: Dict[str, Any], action: str) -> Dict[str, Any]:
        topics = log.get("topics") or []
        if len(topics) < 4:
            raise RuntimeError(f"log missing indexed topics: {log}")
        return {
            "tx_hash": (log.get("transactionHash") or "").lower(),
            "block_number": _hex_int(log["blockNumber"]),
            "log_index": _hex_int(log["logIndex"]),
            "block_time": datetime(1970, 1, 1, tzinfo=timezone.utc),  # filled later
            "action": action,
            "delegator": _topic_address(topics[1]),
            "delegate": _topic_address(topics[3]),
            "block_hash": (log.get("blockHash") or "").lower(),
        }

    def _attach_block_times(self, rows: List[Dict[str, Any]]) -> None:
        blocks = sorted({r["block_number"] for r in rows})
        cache: Dict[int, datetime] = {}
        for i, block in enumerate(blocks, 1):
            header = self._rpc("eth_getBlockByNumber", [hex(block), False])
            if not header or "timestamp" not in header:
                raise RuntimeError(f"missing block header for {block}")
            cache[block] = datetime.fromtimestamp(
                _hex_int(header["timestamp"]), tz=timezone.utc
            )
            if i % 50 == 0 or i == len(blocks):
                logger.info("  block timestamps %s/%s", i, len(blocks))
            time.sleep(0.02)
        for row in rows:
            row["block_time"] = cache[row["block_number"]]

    # ------------------------------------------------------------------ #
    # Persistence
    # ------------------------------------------------------------------ #
    def _create_table(self) -> bool:
        try:
            sql = self.load_sql_file(self.create_table_sql)
            with obs.time_operation(obs.get_job_name(), "snapshot_delegations", "create_table"):
                self.client.command(sql)
            logger.info("Table ready: %s", self.table)
            return True
        except Exception as e:
            logger.error("Failed to create table: %s", e)
            return False

    def _insert_rows(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            logger.info("No rows to insert.")
            return
        columns = [
            "tx_hash",
            "block_number",
            "log_index",
            "block_time",
            "action",
            "delegator",
            "delegate",
        ]
        batch: List[List[Any]] = []
        for row in rows:
            batch.append([
                row["tx_hash"],
                row["block_number"],
                row["log_index"],
                row["block_time"],
                row["action"],
                row["delegator"],
                row["delegate"],
            ])
            if len(batch) >= INSERT_BATCH:
                self.client.insert(self.table, batch, column_names=columns)
                obs.observe_rows("snapshot_delegations", self.table, len(batch))
                batch = []
        if batch:
            self.client.insert(self.table, batch, column_names=columns)
            obs.observe_rows("snapshot_delegations", self.table, len(batch))
        logger.info("Inserted %s rows into %s", len(rows), self.table)

    def _write_csv(self, rows: List[Dict[str, Any]], path: str) -> None:
        fieldnames = [
            "action",
            "block_time",
            "block_number",
            "log_index",
            "tx_hash",
            "delegator",
            "delegate",
        ]
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({
                    **row,
                    "block_time": row["block_time"].strftime("%Y-%m-%d %H:%M:%S.000 UTC"),
                })
