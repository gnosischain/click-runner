import json
import logging
import time
from typing import Dict, List, Optional, Set, Tuple

import requests
from clickhouse_connect.driver.client import Client

import observability as obs
from .base import BaseIngestor

logger = logging.getLogger("clickhouse_runner")

COW_API_BASE = "https://api.cow.fi/xdai/api/v2"
PAGE_LIMIT = 1000
RATE_LIMIT_DELAY = 0.6  # ~100 req/min
MAX_RETRIES = 2

# Larger batches => far fewer parts => far less background-merge pressure,
# which is what saturates memory on small ClickHouse nodes. 5k rows is a
# trivial sort (~a few MB) so this costs no insert memory.
INSERT_BATCH_SIZE = 5000
# Per-insert settings: skip the synchronous dedup merge on each insert
# (background merges + read-time FINAL still dedup) and use a single sort
# thread, both to keep the per-insert memory spike minimal.
INSERT_SETTINGS = {"optimize_on_insert": 0, "max_insert_threads": 1}
# Pause after each insert so background merges can keep pace instead of
# being outrun by a burst of inserts (the merge backlog is what OOMs).
INSERT_THROTTLE_DELAY = 0.5
# Heavy discovery anti-join: prefer grace_hash (spills to disk, lighter on a
# memory-tight node) but fall back to plain hash, which always supports
# LEFT ANTI JOIN. partial_merge does NOT support anti-join, so it's excluded.
QUERY_SETTINGS = {"join_algorithm": "grace_hash,hash"}


class CowIngestor(BaseIngestor):
    """
    Ingestor for CoW Protocol trade fee data from the CoW API.
    Extracts executedProtocolFees per trade (per fill). Modes:
      daily    - owners active in the last lookback_days, newest-first with
                 early-stop on already-ingested fills
      backfill - full history for all owners (optionally bounded by
                 backfill_from)
      repair   - anti-join on-chain fills (source_table) against the target
                 table's (order_uid, tx_hash, log_index) keys and fetch each
                 order with missing fills via /trades?orderUid=. One request
                 returns all fills of an order. Use after a copy/migration or
                 to verify completeness; idempotent.
    """

    def __init__(
        self,
        client: Client,
        variables: Dict[str, str],
        create_table_sql: str,
        table_name: str,
        source_table: str,
        mode: str = "daily",
        # 7 days tolerates multi-day job failures and upstream staleness of the
        # owner-source table (dbt.int_execution_cow_trades); a 2-day window
        # silently yields zero owners whenever that table lags > 2 days.
        lookback_days: int = 7,
        backfill_from: Optional[str] = None,
        max_pages: Optional[int] = None,
    ):
        super().__init__(client, variables)
        self.create_table_sql = create_table_sql
        self.table_name = table_name
        self.source_table = source_table
        self.mode = mode
        self.lookback_days = lookback_days
        self.backfill_from = backfill_from
        # Daily: cap at 20 pages (new owners only need recent trades; backfill handles history).
        # Backfill: 500 pages to fetch full history.
        self.max_pages = max_pages if max_pages is not None else (20 if mode == "daily" else 500)

    def _get_owners(self) -> List[str]:
        """Get list of owner addresses to query from ClickHouse."""
        if self.mode == "backfill":
            date_filter = ""
            if self.backfill_from:
                date_filter = f"AND block_timestamp >= toDate('{self.backfill_from}')"
            sql = f"""
                SELECT DISTINCT taker
                FROM {self.source_table}
                WHERE taker IS NOT NULL AND taker != ''
                  {date_filter}
            """
        else:
            sql = f"""
                SELECT DISTINCT taker
                FROM {self.source_table}
                WHERE taker IS NOT NULL AND taker != ''
                  AND block_timestamp >= now() - INTERVAL {self.lookback_days} DAY
            """
        with obs.time_operation(obs.get_job_name(), "cow", "owner_lookup"):
            result = self.client.query(sql)
        owners = [row[0] for row in result.result_rows]
        obs.cow_owners_total.labels(
            job=obs.get_job_name(),
            mode=self.mode,
            result="discovered",
        ).inc(len(owners))
        logger.info(
            f"Found {len(owners)} owners to query ({self.mode} mode)",
            extra={
                "event": "cow_owners_discovered",
                "ingestor": "cow",
                "mode": self.mode,
                "table": self.table_name,
                "source_table": self.source_table,
                "owners": len(owners),
            },
        )
        return owners

    def _get_existing_trade_keys(self, owners: List[str]) -> Set[Tuple[str, str, int]]:
        """Get (order_uid, tx_hash, log_index) keys already in the table for the
        given owners. Only used in daily mode to skip already-ingested trades.
        Keyed per fill, not per order: partially-fillable orders produce multiple
        trades sharing an order_uid, each with its own fees, so skipping by
        order_uid alone would drop new fills of known orders.
        Scoped to the last 14 days so the query stays small regardless of
        how much historical data accumulates per owner."""
        if not owners:
            return set()

        owner_list = ", ".join(f"'{o}'" for o in owners)
        sql = f"""
            SELECT DISTINCT order_uid, tx_hash, log_index
            FROM {self.table_name}
            WHERE owner IN ({owner_list})
              AND ingested_at >= now() - INTERVAL 14 DAY
        """
        try:
            with obs.time_operation(obs.get_job_name(), "cow", "existing_uid_lookup"):
                result = self.client.query(sql)
            keys = {
                (row[0].lower(), row[1].lower(), int(row[2]))
                for row in result.result_rows
            }
            logger.info(
                f"Found {len(keys)} existing trade keys to skip",
                extra={
                    "event": "cow_existing_uids_loaded",
                    "ingestor": "cow",
                    "mode": self.mode,
                    "table": self.table_name,
                    "existing_trade_keys": len(keys),
                },
            )
            return keys
        except Exception:
            return set()

    def _api_get(self, url: str) -> Optional[requests.Response]:
        """GET with retry (up to MAX_RETRIES) and rate limit handling."""
        for attempt in range(MAX_RETRIES + 1):
            try:
                with obs.time_operation(obs.get_job_name(), "cow", "api_get"):
                    resp = requests.get(url, timeout=30)
                result = "success" if resp.status_code < 400 else "failure"
                obs.cow_api_requests_total.labels(
                    job=obs.get_job_name(),
                    status_code=str(resp.status_code),
                    result=result,
                ).inc()
                if resp.status_code == 429:
                    logger.warning(
                        "Rate limited, sleeping 10s...",
                        extra={
                            "event": "cow_api_rate_limited",
                            "ingestor": "cow",
                            "mode": self.mode,
                            "status_code": resp.status_code,
                            "attempt": attempt + 1,
                        },
                    )
                    time.sleep(10)
                    continue
                return resp
            except requests.RequestException as e:
                obs.cow_api_requests_total.labels(
                    job=obs.get_job_name(),
                    status_code="exception",
                    result="failure",
                ).inc()
                if attempt < MAX_RETRIES:
                    logger.warning(
                        f"Retry {attempt + 1}/{MAX_RETRIES} for {url[:80]}... ({e})",
                        extra={
                            "event": "cow_api_retry",
                            "ingestor": "cow",
                            "mode": self.mode,
                            "attempt": attempt + 1,
                        },
                    )
                    time.sleep(3)
                else:
                    logger.error(
                        f"Failed after {MAX_RETRIES} retries: {e}",
                        extra={
                            "event": "cow_api_failure",
                            "ingestor": "cow",
                            "mode": self.mode,
                            "attempt": attempt + 1,
                        },
                    )
                    return None
        return None

    def _missing_fills_sql(self, select: str, group_by: str = "") -> str:
        """Anti-join of on-chain fills (source_table) against the target
        table's (order_uid, tx_hash, log_index) keys, so orders whose fills
        were only partially ingested (e.g. collapsed by an order-keyed schema)
        are picked up alongside fully missing orders."""
        date_filter = ""
        if self.backfill_from:
            date_filter = f"AND block_timestamp >= toDate('{self.backfill_from}')"
        return f"""
            SELECT {select}
            FROM (
                SELECT taker, order_uid, lower(transaction_hash) AS tx_hash, log_index
                FROM {self.source_table}
                WHERE order_uid IS NOT NULL AND order_uid != ''
                  {date_filter}
            ) t
            LEFT ANTI JOIN (
                SELECT order_uid, lower(tx_hash) AS tx_hash, log_index
                FROM {self.table_name}
            ) f
            ON  f.order_uid = t.order_uid
            AND f.tx_hash   = t.tx_hash
            AND f.log_index = t.log_index
            {group_by}
        """

    def _get_missing_by_owner(self) -> Dict[str, int]:
        """Owners of missing fills, with their missing-order counts."""
        sql = self._missing_fills_sql(
            "t.taker, uniqExact(t.order_uid) AS missing_orders",
            "GROUP BY t.taker",
        )
        with obs.time_operation(obs.get_job_name(), "cow", "missing_owner_lookup"):
            result = self.client.query(sql, settings=QUERY_SETTINGS)
        return {row[0]: int(row[1]) for row in result.result_rows}

    @staticmethod
    def _trade_key(trade: dict) -> Tuple[str, str, int]:
        """Per-fill identity of a trade: (order_uid, tx_hash, log_index)."""
        return (
            trade.get("orderUid", "").lower(),
            trade.get("txHash", "").lower(),
            int(trade.get("logIndex") or 0),
        )

    def _fetch_trades_for_owner(
        self,
        owner: str,
        existing_keys: Optional[Set[Tuple[str, str, int]]] = None,
    ) -> Tuple[List[dict], bool]:
        """Fetch trades for an owner from the CoW API.

        API returns newest trades first. In daily mode, stops paginating
        once it hits trades already in the DB. In backfill mode, fetches everything.
        Returns (trades, api_failed) where api_failed is True if any page
        exhausted all retries (e.g. sustained 429s).
        """
        all_trades = []
        offset = 0
        page = 0
        hit_existing = False
        api_failed = False

        while page < self.max_pages:
            url = f"{COW_API_BASE}/trades?owner={owner}&limit={PAGE_LIMIT}&offset={offset}"
            resp = self._api_get(url)

            if resp is None:
                api_failed = True
                break
            if resp.status_code == 404:
                break

            try:
                resp.raise_for_status()
                trades = resp.json()
            except (requests.RequestException, ValueError) as e:
                logger.error(
                    f"API error for owner {owner}: {e}",
                    extra={
                        "event": "cow_owner_api_error",
                        "ingestor": "cow",
                        "mode": self.mode,
                        "owner": owner,
                    },
                )
                break

            if not trades:
                break

            if existing_keys is not None:
                new_trades = []
                for t in trades:
                    if self._trade_key(t) in existing_keys:
                        hit_existing = True
                    else:
                        new_trades.append(t)
                all_trades.extend(new_trades)
                if hit_existing:
                    break
            else:
                all_trades.extend(trades)

            if len(trades) < PAGE_LIMIT:
                break

            offset += PAGE_LIMIT
            page += 1
            time.sleep(RATE_LIMIT_DELAY)

        if page >= self.max_pages:
            logger.warning(
                f"Owner {owner} hit max pages ({self.max_pages}), fetched {len(all_trades)} trades",
                extra={
                    "event": "cow_owner_max_pages",
                    "ingestor": "cow",
                    "mode": self.mode,
                    "owner": owner,
                    "max_pages": self.max_pages,
                    "trades": len(all_trades),
                },
            )

        obs.cow_trades_fetched_total.labels(
            job=obs.get_job_name(),
            mode=self.mode,
        ).inc(len(all_trades))
        return all_trades, api_failed

    COLUMNS = [
        "order_uid", "tx_hash", "block_number", "log_index",
        "owner", "sell_token", "buy_token", "sell_amount",
        "buy_amount", "fee_token", "fee_amount", "fee_policies",
    ]

    def _trade_to_row(self, trade: dict) -> list:
        """Map an API trade response entry to a ClickHouse row (see COLUMNS)."""
        fee_token, fee_amount, fee_policies = self._parse_fees(trade)
        return [
            trade.get("orderUid", ""),
            trade.get("txHash", ""),
            trade.get("blockNumber", 0),
            trade.get("logIndex", 0),
            trade.get("owner", ""),
            trade.get("sellToken", ""),
            trade.get("buyToken", ""),
            trade.get("sellAmount", "0"),
            trade.get("buyAmount", "0"),
            fee_token,
            fee_amount,
            fee_policies,
        ]

    def _parse_fees(self, trade: dict) -> tuple:
        """Extract fee info from a single trade response.

        Returns (fee_token, fee_amount_total, fee_policies_json).
        """
        fees = trade.get("executedProtocolFees", [])
        if not fees:
            return ("", "0", "[]")

        token_sums: Dict[str, int] = {}
        for fee in fees:
            token = fee.get("token", "")
            amount = int(fee.get("amount", "0"))
            token_sums[token] = token_sums.get(token, 0) + amount

        fee_token = max(token_sums, key=token_sums.get)
        fee_total = str(token_sums[fee_token])

        policies = json.dumps(
            [{"policy": f.get("policy", {}), "amount": f.get("amount", "0"), "token": f.get("token", "")} for f in fees]
        )

        return (fee_token, fee_total, policies)

    def _insert_batch(self, rows: List[list], columns: List[str]) -> None:
        """Insert a batch of rows into ClickHouse."""
        if not rows:
            return
        with obs.time_operation(obs.get_job_name(), "cow", "insert_batch"):
            self.client.insert(
                self.table_name, rows, column_names=columns, settings=INSERT_SETTINGS
            )
        if INSERT_THROTTLE_DELAY:
            time.sleep(INSERT_THROTTLE_DELAY)
        obs.cow_trades_inserted_total.labels(
            job=obs.get_job_name(),
            mode=self.mode,
            table=self.table_name,
        ).inc(len(rows))
        obs.observe_rows("cow", self.table_name, len(rows))
        logger.info(
            f"Inserted {len(rows)} rows",
            extra={
                "event": "cow_insert_batch",
                "ingestor": "cow",
                "mode": self.mode,
                "table": self.table_name,
                "rows": len(rows),
            },
        )

    def _ingest_missing_orders(self, count_before: int) -> bool:
        """Repair mode: every owner that has any missing fill is re-fetched
        per-owner (full history, 1000 fills per request). Per-owner returns
        exactly the orders the CoW API has for that owner, so a single pass
        recovers collapsed multi-fill rows, the post-copy date gap, and any
        never-ingested owner — without chasing on-chain order_uids that have
        no orderbook/fee record (volume-only trades, AMM-side settlement legs),
        which are the overwhelming majority of "missing" on-chain fills and
        would otherwise cost ~20x the API calls for zero fee data.
        Already-present fills come back too and dedupe via the
        ReplacingMergeTree key, so this is idempotent and safe to rerun."""
        by_owner = self._get_missing_by_owner()
        if not by_owner:
            logger.info("No orders with missing fills found")
            return True

        # Heaviest owners first: if the API starts rate-limiting, we reach the
        # consecutive-failure abort early rather than deep into the run.
        owners = sorted(by_owner, key=lambda o: -by_owner[o])
        logger.info(
            f"Repair: {len(owners)} owners have missing fills; re-fetching each per-owner",
            extra={
                "event": "cow_repair_plan",
                "ingestor": "cow",
                "mode": self.mode,
                "table": self.table_name,
                "owners_total": len(owners),
            },
        )

        if not self._repair_fetch_owners(owners):
            return False

        count_after = self.get_row_count(self.table_name)
        logger.info(f"Repair done. Row count after: {count_after} (delta: {count_after - count_before})")
        return True

    def _repair_fetch_owners(self, owners: List[str]) -> bool:
        """Phase 1 of repair: full per-owner re-fetch for heavy owners.
        No early-stop — their missing fills sit deep in history."""
        batch = []
        batch_size = INSERT_BATCH_SIZE
        total_trades = 0
        failed_owners = 0
        consecutive_api_failures = 0

        for i, owner in enumerate(owners):
            if i > 0 and i % 100 == 0:
                logger.info(
                    f"Repair progress: {i}/{len(owners)} owners, {total_trades} fills inserted, "
                    f"{failed_owners} failed",
                    extra={
                        "event": "cow_repair_progress",
                        "ingestor": "cow",
                        "mode": self.mode,
                        "table": self.table_name,
                        "owners_processed": i,
                        "owners_total": len(owners),
                        "trades": total_trades,
                        "owners_failed": failed_owners,
                    },
                )
            logger.info(
                f"  [owner {i+1}/{len(owners)}] re-fetching full history of {owner[:10]}...",
                extra={
                    "event": "cow_repair_owner_fetch",
                    "ingestor": "cow",
                    "mode": self.mode,
                    "table": self.table_name,
                    "owner": owner,
                    "owner_index": i + 1,
                    "owners_total": len(owners),
                },
            )
            trades, api_failed = self._fetch_trades_for_owner(owner, None)

            if api_failed:
                consecutive_api_failures += 1
                failed_owners += 1
                if consecutive_api_failures >= 10:
                    if batch:
                        self._insert_batch(batch, self.COLUMNS)
                        batch = []
                    logger.error(
                        f"Aborting: API rate-limiting all requests after {consecutive_api_failures} consecutive failures",
                        extra={
                            "event": "cow_abort_rate_limited",
                            "ingestor": "cow",
                            "mode": self.mode,
                            "table": self.table_name,
                            "consecutive_failures": consecutive_api_failures,
                        },
                    )
                    return False
            else:
                consecutive_api_failures = 0

            for trade in trades:
                batch.append(self._trade_to_row(trade))

            while len(batch) >= batch_size:
                self._insert_batch(batch[:batch_size], self.COLUMNS)
                total_trades += min(batch_size, len(batch))
                batch = batch[batch_size:]

            time.sleep(RATE_LIMIT_DELAY)

        if batch:
            self._insert_batch(batch, self.COLUMNS)
            total_trades += len(batch)

        logger.info(f"Per-owner repair phase done. Fills inserted: {total_trades}, failed owners: {failed_owners}")
        return True

    def ingest(self, skip_table_creation: bool = False, **kwargs) -> bool:
        """
        Execute the CoW fee ingestion:
        1. Create table (if needed)
        2. Get owner list from ClickHouse
        3. Daily mode: load existing (order_uid, tx_hash, log_index) keys to skip duplicates
        4. For each owner, fetch trades from CoW API (stop early if known)
        5. Extract fees and insert into ClickHouse
        """
        try:
            if not skip_table_creation:
                create_query = self.load_sql_file(self.create_table_sql)
                logger.info(
                    f"Creating table using {self.create_table_sql}",
                    extra={
                        "event": "cow_create_table_start",
                        "ingestor": "cow",
                        "mode": self.mode,
                        "table": self.table_name,
                        "source_table": self.source_table,
                    },
                )
                with obs.time_operation(obs.get_job_name(), "cow", "create_table"):
                    self.client.command(create_query)

            count_before = self.get_row_count(self.table_name)
            logger.info(f"Row count before: {count_before}")

            if self.mode == "repair":
                return self._ingest_missing_orders(count_before)

            owners = self._get_owners()
            if not owners:
                logger.warning("No owners found to query")
                return True

            # Daily mode: load known per-fill keys so we stop early per owner
            existing_keys = None
            if self.mode == "daily":
                existing_keys = self._get_existing_trade_keys(owners)

            columns = self.COLUMNS

            batch = []
            batch_size = INSERT_BATCH_SIZE
            total_trades = 0
            skipped_owners = 0
            failed_owners = 0
            consecutive_api_failures = 0

            for i, owner in enumerate(owners):
                if i > 0 and i % 50 == 0:
                    logger.info(
                        f"Progress: {i}/{len(owners)} owners, {total_trades} new trades, {skipped_owners} skipped",
                        extra={
                            "event": "cow_progress",
                            "ingestor": "cow",
                            "mode": self.mode,
                            "table": self.table_name,
                            "source_table": self.source_table,
                            "owners_processed": i,
                            "owners_total": len(owners),
                            "trades": total_trades,
                            "owners_skipped": skipped_owners,
                        },
                    )

                owner_start = time.monotonic()
                trades, api_failed = self._fetch_trades_for_owner(owner, existing_keys)
                obs.cow_owner_duration_seconds.labels(
                    job=obs.get_job_name(),
                    mode=self.mode,
                ).observe(time.monotonic() - owner_start)

                if api_failed:
                    consecutive_api_failures += 1
                    failed_owners += 1
                    if consecutive_api_failures >= 10:
                        # Flush already-fetched trades so they aren't lost on abort
                        if batch:
                            self._insert_batch(batch, columns)
                            total_trades += len(batch)
                            batch = []
                        logger.error(
                            f"Aborting: API rate-limiting all requests after {consecutive_api_failures} consecutive failures",
                            extra={
                                "event": "cow_abort_rate_limited",
                                "ingestor": "cow",
                                "mode": self.mode,
                                "table": self.table_name,
                                "consecutive_failures": consecutive_api_failures,
                            },
                        )
                        return False
                else:
                    consecutive_api_failures = 0

                if len(trades) == 0:
                    skipped_owners += 1
                    continue

                logger.info(
                    f"  [{i+1}/{len(owners)}] {owner[:10]}... -> {len(trades)} new trades",
                    extra={
                        "event": "cow_owner_trades",
                        "ingestor": "cow",
                        "mode": self.mode,
                        "table": self.table_name,
                        "source_table": self.source_table,
                        "owner": owner,
                        "owner_index": i + 1,
                        "owners_total": len(owners),
                        "trades": len(trades),
                    },
                )

                for trade in trades:
                    batch.append(self._trade_to_row(trade))

                if len(batch) >= batch_size:
                    self._insert_batch(batch, columns)
                    total_trades += len(batch)
                    batch = []

                time.sleep(RATE_LIMIT_DELAY)

            # Insert remaining
            if batch:
                self._insert_batch(batch, columns)
                total_trades += len(batch)

            count_after = self.get_row_count(self.table_name)
            logger.info(f"Done. New trades inserted: {total_trades}")
            logger.info(f"Owners skipped (all trades known): {skipped_owners}")
            logger.info(f"Failed owners: {failed_owners}")
            logger.info(f"Row count after: {count_after} (delta: {count_after - count_before})")
            obs.cow_owners_total.labels(
                job=obs.get_job_name(),
                mode=self.mode,
                result="skipped",
            ).inc(skipped_owners)
            obs.cow_owners_total.labels(
                job=obs.get_job_name(),
                mode=self.mode,
                result="failed",
            ).inc(failed_owners)

            return True

        except Exception as e:
            logger.error(
                f"Error in CoW ingestion: {e}",
                extra={
                    "event": "cow_ingest_failure",
                    "ingestor": "cow",
                    "mode": self.mode,
                    "table": self.table_name,
                    "source_table": self.source_table,
                },
            )
            return False
