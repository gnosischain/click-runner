import json
import logging
import time
from typing import Dict, List, Optional, Set

import requests
from clickhouse_connect.driver.client import Client

import observability as obs
from .base import BaseIngestor

logger = logging.getLogger("clickhouse_runner")

COW_API_BASE = "https://api.cow.fi/xdai/api/v2"
PAGE_LIMIT = 1000
RATE_LIMIT_DELAY = 0.6  # ~100 req/min
MAX_RETRIES = 2


class CowIngestor(BaseIngestor):
    """
    Ingestor for CoW Protocol trade fee data from the CoW API.
    Queries per-owner and extracts executedProtocolFees for each trade.
    """

    def __init__(
        self,
        client: Client,
        variables: Dict[str, str],
        create_table_sql: str,
        table_name: str,
        source_table: str,
        mode: str = "daily",
        lookback_days: int = 2,
        backfill_from: Optional[str] = None,
        max_pages: int = 500,
    ):
        super().__init__(client, variables)
        self.create_table_sql = create_table_sql
        self.table_name = table_name
        self.source_table = source_table
        self.mode = mode
        self.lookback_days = lookback_days
        self.backfill_from = backfill_from
        self.max_pages = max_pages

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

    def _get_existing_order_uids(self, owners: List[str]) -> Set[str]:
        """Get order_uids already in the table for the given owners.
        Only used in daily mode to skip already-ingested trades."""
        if not owners:
            return set()

        owner_list = ", ".join(f"'{o}'" for o in owners)
        sql = f"""
            SELECT DISTINCT order_uid
            FROM {self.table_name}
            WHERE owner IN ({owner_list})
        """
        try:
            with obs.time_operation(obs.get_job_name(), "cow", "existing_uid_lookup"):
                result = self.client.query(sql)
            uids = {row[0] for row in result.result_rows}
            logger.info(
                f"Found {len(uids)} existing order_uids to skip",
                extra={
                    "event": "cow_existing_uids_loaded",
                    "ingestor": "cow",
                    "mode": self.mode,
                    "table": self.table_name,
                    "existing_order_uids": len(uids),
                },
            )
            return uids
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

    def _fetch_trades_for_owner(
        self,
        owner: str,
        existing_uids: Optional[Set[str]] = None,
    ) -> List[dict]:
        """Fetch trades for an owner from the CoW API.

        API returns newest trades first. In daily mode, stops paginating
        once it hits trades already in the DB. In backfill mode, fetches everything.
        """
        all_trades = []
        offset = 0
        page = 0
        hit_existing = False

        while page < self.max_pages:
            url = f"{COW_API_BASE}/trades?owner={owner}&limit={PAGE_LIMIT}&offset={offset}"
            resp = self._api_get(url)

            if resp is None:
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

            if existing_uids is not None:
                new_trades = []
                for t in trades:
                    if t.get("orderUid", "") in existing_uids:
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
        return all_trades

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
            self.client.insert(self.table_name, rows, column_names=columns)
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

    def ingest(self, skip_table_creation: bool = False, **kwargs) -> bool:
        """
        Execute the CoW fee ingestion:
        1. Create table (if needed)
        2. Get owner list from ClickHouse
        3. Daily mode: load existing order_uids to skip duplicates
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

            owners = self._get_owners()
            if not owners:
                logger.warning("No owners found to query")
                return True

            # Daily mode: load known uids so we stop early per owner
            existing_uids = None
            if self.mode == "daily":
                existing_uids = self._get_existing_order_uids(owners)

            columns = [
                "order_uid", "tx_hash", "block_number", "log_index",
                "owner", "sell_token", "buy_token", "sell_amount",
                "buy_amount", "fee_token", "fee_amount", "fee_policies",
            ]

            batch = []
            batch_size = 500
            total_trades = 0
            skipped_owners = 0
            failed_owners = 0

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
                trades = self._fetch_trades_for_owner(owner, existing_uids)
                obs.cow_owner_duration_seconds.labels(
                    job=obs.get_job_name(),
                    mode=self.mode,
                ).observe(time.monotonic() - owner_start)
                if trades is None:
                    failed_owners += 1
                    continue

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
                    fee_token, fee_amount, fee_policies = self._parse_fees(trade)

                    row = [
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
                    batch.append(row)

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
