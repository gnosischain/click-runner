import json
import logging
import time
import collections
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
from clickhouse_connect.driver.client import Client

import observability as obs
from .base import BaseIngestor

logger = logging.getLogger("clickhouse_runner")

MIXPANEL_EXPORT_URLS = {
    "US": "https://data.mixpanel.com/api/2.0/export",
    "EU": "https://data-eu.mixpanel.com/api/2.0/export",
    "IN": "https://data-in.mixpanel.com/api/2.0/export",
}
MAX_EVENTS_PER_REQUEST = 100_000
INSERT_BATCH_SIZE = 50_000


class MixpanelRateLimiter:
    """Sliding-window rate limiter for Mixpanel API (3 req/sec, 60 req/hour)."""

    def __init__(self, per_second: int = 3, per_hour: int = 60):
        self.per_second = per_second
        self.per_hour = per_hour
        self.second_timestamps: collections.deque = collections.deque()
        self.hour_timestamps: collections.deque = collections.deque()

    def wait(self):
        now = time.monotonic()

        # Enforce per-second limit
        while len(self.second_timestamps) >= self.per_second:
            oldest = self.second_timestamps[0]
            wait_time = 1.0 - (now - oldest)
            if wait_time > 0:
                logger.debug(f"Rate limiter: waiting {wait_time:.2f}s (per-second limit)")
                time.sleep(wait_time)
                now = time.monotonic()
            self.second_timestamps.popleft()

        # Enforce per-hour limit
        while len(self.hour_timestamps) >= self.per_hour:
            oldest = self.hour_timestamps[0]
            wait_time = 3600.0 - (now - oldest)
            if wait_time > 0:
                logger.info(f"Rate limiter: waiting {wait_time:.0f}s (hourly limit reached)")
                time.sleep(wait_time)
                now = time.monotonic()
            self.hour_timestamps.popleft()

        self.second_timestamps.append(now)
        self.hour_timestamps.append(now)


class MixpanelIngestor(BaseIngestor):
    """
    Ingestor for Mixpanel raw event data.
    Fetches events via the Raw Event Export API and inserts into ClickHouse.
    """

    def __init__(
        self,
        client: Client,
        variables: Dict[str, str],
        create_table_sql: str,
        create_state_sql: str,
        table_name: str,
        project_id: str,
        sa_username: str,
        sa_secret: str,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        event_filter: Optional[str] = None,
        mode: str = "daily",
        region: str = "US",
    ):
        super().__init__(client, variables)
        self.create_table_sql = create_table_sql
        self.create_state_sql = create_state_sql
        self.table_name = table_name
        self.project_id = project_id
        self.sa_username = sa_username
        self.sa_secret = sa_secret
        self.from_date = from_date
        self.to_date = to_date
        self.event_filter = event_filter
        self.mode = mode
        self.export_url = MIXPANEL_EXPORT_URLS.get(region.upper(), MIXPANEL_EXPORT_URLS["US"])
        self.rate_limiter = MixpanelRateLimiter()
        logger.info(f"Mixpanel API endpoint: {self.export_url} (region={region.upper()})")

        # Derive state table name from the same database
        db = self.table_name.rsplit(".", 1)[0] if "." in self.table_name else "mixpanel"
        self.state_table = f"{db}.mixpanel_ingestion_state"

    def ingest(self, skip_table_creation: bool = False, **kwargs) -> bool:
        # Step 1: Create tables
        if not skip_table_creation:
            try:
                create_events_sql = self.load_sql_file(self.create_table_sql)
                with obs.time_operation(obs.get_job_name(), "mixpanel", "create_events_table"):
                    self.client.command(create_events_sql)
                logger.info("Events table created/verified.")

                create_state_sql = self.load_sql_file(self.create_state_sql)
                with obs.time_operation(obs.get_job_name(), "mixpanel", "create_state_table"):
                    self.client.command(create_state_sql)
                logger.info("State table created/verified.")
            except Exception as e:
                logger.error(f"Failed to create tables: {e}")
                return False

        # Step 2: Determine date range
        try:
            start_date, end_date = self._determine_date_range()
        except ValueError as e:
            logger.error(str(e))
            return False

        if end_date is None:
            return True

        logger.info(f"Processing date range: {start_date} to {end_date}")

        # Step 3: Log initial row count
        try:
            rows_before = self.get_row_count(self.table_name)
            logger.info(f"Row count before insert in {self.table_name}: {rows_before}")
        except Exception:
            rows_before = 0
            logger.warning("Could not get initial row count (table may be new)")

        # Step 4: Process each date
        dates = self._get_date_range(start_date, end_date)
        for date_str in dates:
            if self._is_date_complete(date_str):
                logger.info(f"Skipping {date_str} (already complete)")
                continue

            logger.info(f"Processing date: {date_str}")
            with obs.time_operation(obs.get_job_name(), "mixpanel", "fetch_day"):
                success, should_mark, rows = self._fetch_day(date_str)
            if not success:
                logger.error(f"Failed to process date {date_str}. Stopping.")
                return False

            if should_mark:
                self._mark_date_complete(date_str, rows_indexed=rows)
                logger.info(f"Completed date: {date_str} ({rows} rows)")

        # Step 5: Log final row count
        try:
            rows_after = self.get_row_count(self.table_name)
            logger.info(f"Row count after insert in {self.table_name}: {rows_after}")
            logger.info(f"Rows inserted: {rows_after - rows_before}")
        except Exception:
            logger.warning("Could not get final row count")

        return True

    def _determine_date_range(self) -> Tuple[str, str]:
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        if self.mode == "backfill":
            if not self.from_date or not self.to_date:
                raise ValueError(
                    "Backfill mode requires --mixpanel-from-date and --mixpanel-to-date"
                )
            return self.from_date, self.to_date

        # Daily mode: resume from last completed date
        last_date = self._get_last_completed_date()
        if last_date:
            start = (
                datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
        elif self.from_date:
            start = self.from_date
        else:
            # First run with no history: default to yesterday
            start = yesterday
            logger.info(
                "No completed dates in state table and no --mixpanel-from-date provided. "
                "Defaulting to yesterday."
            )

        if start > yesterday:
            logger.info("Already up to date (last completed date is yesterday or later)")
            return start, None  # Signal: nothing to do

        return start, yesterday

    def _get_last_completed_date(self) -> Optional[str]:
        try:
            result = self.client.query(
                f"SELECT max(completed_date) FROM {self.state_table} FINAL "
                f"WHERE project_id = %(project_id)s AND status = 'complete'",
                parameters={"project_id": self.project_id},
            )
            if result.result_rows and result.result_rows[0][0]:
                date_val = result.result_rows[0][0]
                if hasattr(date_val, "strftime"):
                    date_str = date_val.strftime("%Y-%m-%d")
                else:
                    date_str = str(date_val)
                # ClickHouse returns 1970-01-01 for max() on an empty table
                if date_str and date_str != "1970-01-01":
                    return date_str
        except Exception as e:
            logger.warning(f"Could not query state table: {e}")
        return None

    def _is_date_complete(self, date_str: str) -> bool:
        try:
            result = self.client.query(
                f"SELECT count() FROM {self.state_table} FINAL "
                f"WHERE project_id = %(project_id)s "
                f"AND completed_date = %(date)s AND status = 'complete'",
                parameters={"project_id": self.project_id, "date": date_str},
            )
            return result.result_rows[0][0] > 0
        except Exception:
            return False

    def _mark_date_complete(self, date_str: str, rows_indexed: int = 0):
        try:
            self.client.command(
                f"INSERT INTO {self.state_table} (project_id, completed_date, rows_indexed, status) "
                f"VALUES (%(project_id)s, %(date)s, %(rows)s, 'complete')",
                parameters={
                    "project_id": self.project_id,
                    "date": date_str,
                    "rows": rows_indexed,
                },
            )
        except Exception as e:
            logger.warning(f"Failed to mark date {date_str} as complete: {e}")

    def _fetch_day(self, date_str: str) -> Tuple[bool, bool, int]:
        """Fetch events for a single day.
        Returns (success, should_mark_complete, rows_indexed)."""
        events = self._export_events_with_retry(date_str, date_str)
        if events is None:
            return False, False, 0

        if len(events) >= MAX_EVENTS_PER_REQUEST:
            logger.warning(
                f"Date {date_str} returned {len(events)} events (possible truncation). "
                "Splitting into hourly windows."
            )
            return self._fetch_day_by_hours(date_str)

        if events:
            self._insert_events(events)
            logger.info(f"Inserted {len(events)} events for {date_str}")
            return True, True, len(events)

        # No events returned — don't mark recent dates as complete because
        # Mixpanel's export API has a ~24-48h delay, so data may arrive later.
        days_ago = (datetime.utcnow() - datetime.strptime(date_str, "%Y-%m-%d")).days
        if days_ago < 3:
            logger.info(
                f"No events for {date_str} (only {days_ago} day(s) ago). "
                "Skipping state update — will retry on next run."
            )
            return True, False, 0

        logger.info(f"No events for {date_str}")
        return True, True, 0

    def _fetch_day_by_hours(self, date_str: str) -> Tuple[bool, bool, int]:
        """Fetch events for a single day split into hourly windows.
        Returns (success, should_mark_complete, rows_indexed)."""
        all_events: List[dict] = []

        for hour in range(24):
            where_clause = f'properties["$hour"]=={hour}'
            events = self._export_events_with_retry(
                date_str, date_str, where=where_clause
            )
            if events is None:
                return False, False, 0

            if len(events) >= MAX_EVENTS_PER_REQUEST:
                logger.error(
                    f"Hour {hour} of {date_str} returned {len(events)} events "
                    "(still truncated). Manual intervention required."
                )
                return False, False, 0

            all_events.extend(events)
            if events:
                logger.info(f"  Hour {hour:02d}: {len(events)} events")

        if all_events:
            self._insert_events(all_events)
            logger.info(
                f"Inserted {len(all_events)} events for {date_str} (hourly split)"
            )
            return True, True, len(all_events)

        logger.info(f"No events for {date_str}")
        return True, True, 0

    def _export_events_with_retry(
        self,
        from_date: str,
        to_date: str,
        where: Optional[str] = None,
        max_retries: int = 3,
    ) -> Optional[List[dict]]:
        for attempt in range(max_retries):
            try:
                return self._export_events(from_date, to_date, where=where)
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0

                if status == 429:
                    retry_after = int(
                        e.response.headers.get("Retry-After", 60)
                    )
                    logger.warning(
                        f"Rate limited (429). Sleeping {retry_after}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(retry_after)
                    continue

                if 400 <= status < 500:
                    logger.error(f"Client error {status}: {e}")
                    return None

                if status >= 500:
                    wait = 5 * (3 ** attempt)
                    logger.warning(
                        f"Server error {status}. Retrying in {wait}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait)
                    continue

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                wait = 5 * (3 ** attempt)
                logger.warning(
                    f"Connection error: {e}. Retrying in {wait}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait)

        logger.error(f"Failed after {max_retries} attempts for {from_date} to {to_date}")
        return None

    def _export_events(
        self,
        from_date: str,
        to_date: str,
        where: Optional[str] = None,
    ) -> List[dict]:
        self.rate_limiter.wait()

        params = {
            "from_date": from_date,
            "to_date": to_date,
            "project_id": self.project_id,
        }
        if self.event_filter:
            params["event"] = self.event_filter
        if where:
            params["where"] = where

        with obs.time_operation(obs.get_job_name(), "mixpanel", "api_export"):
            response = requests.get(
                self.export_url,
                params=params,
                auth=(self.sa_username, self.sa_secret),
                headers={"Accept-Encoding": "gzip"},
                stream=True,
                timeout=300,
            )
        logger.info(f"API response status: {response.status_code}")
        if response.status_code != 200:
            logger.error(f"API error response: {response.text[:500]}")
        response.raise_for_status()

        # Check for auth errors returned as 200 with error body
        content_type = response.headers.get("Content-Type", "")
        logger.debug(f"Response Content-Type: {content_type}")

        events = []
        line_count = 0
        for line in response.iter_lines():
            if not line:
                continue
            line_str = line.decode("utf-8") if isinstance(line, bytes) else line
            line_str = line_str.strip()
            if not line_str:
                continue
            line_count += 1
            try:
                event = json.loads(line_str)
                if isinstance(event, dict) and "event" in event:
                    events.append(event)
                else:
                    # Mixpanel may return error objects or unexpected shapes
                    logger.warning(f"Unexpected response object: {line_str[:200]}")
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping malformed JSON line: {e} | Raw: [{line_str[:500]}]")

        if line_count > 0 and not events:
            logger.warning(f"API returned {line_count} line(s) but 0 valid events")

        return events

    def _insert_events(self, events: List[dict]):
        column_names = [
            "event_name",
            "distinct_id",
            "event_time",
            "insert_id",
            "project_id",
            "properties",
        ]

        rows = []
        for event in events:
            props = event.get("properties", {})
            timestamp = props.get("time", 0)
            try:
                event_time = datetime.utcfromtimestamp(timestamp)
            except (ValueError, OSError):
                logger.warning(f"Invalid timestamp {timestamp}, skipping event")
                continue

            rows.append([
                event.get("event", ""),
                str(props.get("distinct_id", "")),
                event_time,
                str(props.get("$insert_id", "")),
                self.project_id,
                json.dumps(props),
            ])

        # Insert in batches
        for i in range(0, len(rows), INSERT_BATCH_SIZE):
            batch = rows[i : i + INSERT_BATCH_SIZE]
            with obs.time_operation(obs.get_job_name(), "mixpanel", "insert_batch"):
                self.client.insert(
                    self.table_name,
                    batch,
                    column_names=column_names,
                )
            obs.mixpanel_events_inserted_total.labels(
                job=obs.get_job_name(),
                table=self.table_name,
            ).inc(len(batch))
            obs.observe_rows("mixpanel", self.table_name, len(batch))
            logger.info(
                f"  Batch inserted {len(batch)} rows "
                f"({i + len(batch)}/{len(rows)})"
            )

    @staticmethod
    def _get_date_range(start_date: str, end_date: str) -> List[str]:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        return dates
