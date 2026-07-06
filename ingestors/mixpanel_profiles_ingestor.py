import json
import logging
import time
from typing import Dict, List, Optional

import requests
from clickhouse_connect.driver.client import Client

import observability as obs
from .base import BaseIngestor
from .mixpanel_ingestor import MixpanelRateLimiter, INSERT_BATCH_SIZE

logger = logging.getLogger("clickhouse_runner")

MIXPANEL_ENGAGE_URLS = {
    "US": "https://mixpanel.com/api/2.0/engage",
    "EU": "https://eu.mixpanel.com/api/2.0/engage",
    "IN": "https://in.mixpanel.com/api/2.0/engage",
}


class MixpanelProfilesIngestor(BaseIngestor):
    """
    Ingestor for Mixpanel People/profile data.

    Fetches user profiles via the Engage query API and snapshots them into
    ClickHouse. Unlike the raw event export, profiles are current-state (no
    per-date history), so every run is a full snapshot. The target table is
    ReplacingMergeTree(synced_at) keyed by (project_id, distinct_id) so the
    newest snapshot of each profile wins on merge — no watermark/state table
    is needed.

    The full profile property bag ($properties) is stored as a JSON string,
    exactly like the events ingestor, so downstream dbt models extract
    individual properties (e.g. `pay`) with JSONExtract.
    """

    def __init__(
        self,
        client: Client,
        variables: Dict[str, str],
        create_table_sql: str,
        table_name: str,
        project_id: str,
        sa_username: str,
        sa_secret: str,
        region: str = "US",
        where: Optional[str] = None,
    ):
        super().__init__(client, variables)
        self.create_table_sql = create_table_sql
        self.table_name = table_name
        self.project_id = project_id
        self.sa_username = sa_username
        self.sa_secret = sa_secret
        self.where = where
        self.engage_url = MIXPANEL_ENGAGE_URLS.get(region.upper(), MIXPANEL_ENGAGE_URLS["US"])
        self.rate_limiter = MixpanelRateLimiter()
        logger.info(f"Mixpanel Engage endpoint: {self.engage_url} (region={region.upper()})")

    def ingest(self, skip_table_creation: bool = False, **kwargs) -> bool:
        # Step 1: Create table
        if not skip_table_creation:
            try:
                create_sql = self.load_sql_file(self.create_table_sql)
                with obs.time_operation(obs.get_job_name(), "mixpanel_profiles", "create_profiles_table"):
                    self.client.command(create_sql)
                logger.info("Profiles table created/verified.")
            except Exception as e:
                logger.error(f"Failed to create profiles table: {e}")
                return False

        # Step 2: Log initial row count
        try:
            rows_before = self.get_row_count(self.table_name)
            logger.info(f"Row count before snapshot in {self.table_name}: {rows_before}")
        except Exception:
            rows_before = 0
            logger.warning("Could not get initial row count (table may be new)")

        # Step 3: Page through the full Engage result set.
        # First request has no session_id; the response returns a session_id +
        # total. Subsequent requests replay the session_id and increment page
        # until a page comes back empty (canonical stop) or total is reached.
        session_id: Optional[str] = None
        page = 0
        total: Optional[int] = None
        total_fetched = 0
        total_inserted = 0

        while True:
            data = self._fetch_page_with_retry(session_id, page)
            if data is None:
                logger.error(f"Failed to fetch profiles page {page}. Stopping.")
                return False

            if session_id is None:
                session_id = data.get("session_id")
                total = data.get("total")
                logger.info(f"Engage session {session_id}: total={total} profiles")

            results = data.get("results", []) or []
            if not results:
                break

            total_fetched += len(results)
            total_inserted += self._insert_profiles(results)
            logger.info(
                f"  Page {page}: fetched {len(results)} "
                f"({total_fetched}/{total if total is not None else '?'} profiles)"
            )

            if total is not None and total_fetched >= total:
                break

            page += 1

        # Step 4: Log final row count
        try:
            rows_after = self.get_row_count(self.table_name)
            logger.info(f"Row count after snapshot in {self.table_name}: {rows_after}")
        except Exception:
            logger.warning("Could not get final row count")

        logger.info(f"Profiles snapshot complete: {total_inserted} profiles inserted")
        return True

    def _fetch_page_with_retry(
        self,
        session_id: Optional[str],
        page: int,
        max_retries: int = 3,
    ) -> Optional[dict]:
        for attempt in range(max_retries):
            try:
                return self._fetch_page(session_id, page)
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0

                if status == 429:
                    retry_after = int(e.response.headers.get("Retry-After", 60))
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

        logger.error(f"Failed after {max_retries} attempts for page {page}")
        return None

    def _fetch_page(self, session_id: Optional[str], page: int) -> dict:
        self.rate_limiter.wait()

        params: Dict[str, str] = {"project_id": self.project_id}
        if self.where:
            params["where"] = self.where
        if session_id is not None:
            params["session_id"] = session_id
            params["page"] = str(page)

        with obs.time_operation(obs.get_job_name(), "mixpanel_profiles", "api_engage"):
            response = requests.get(
                self.engage_url,
                params=params,
                auth=(self.sa_username, self.sa_secret),
                headers={"Accept": "application/json"},
                timeout=300,
            )
        logger.info(f"Engage API response status: {response.status_code} (page {page})")
        if response.status_code != 200:
            logger.error(f"Engage API error response: {response.text[:500]}")
        response.raise_for_status()

        return response.json()

    def _insert_profiles(self, results: List[dict]) -> int:
        column_names = ["distinct_id", "project_id", "properties"]

        rows = []
        for profile in results:
            distinct_id = profile.get("$distinct_id", "")
            if not distinct_id:
                continue
            props = profile.get("$properties", {}) or {}
            rows.append([
                str(distinct_id),
                self.project_id,
                json.dumps(props),
            ])

        if not rows:
            return 0

        # synced_at is filled by the table DEFAULT now() for each inserted row.
        for i in range(0, len(rows), INSERT_BATCH_SIZE):
            batch = rows[i : i + INSERT_BATCH_SIZE]
            with obs.time_operation(obs.get_job_name(), "mixpanel_profiles", "insert_batch"):
                self.client.insert(
                    self.table_name,
                    batch,
                    column_names=column_names,
                )
            obs.observe_rows("mixpanel_profiles", self.table_name, len(batch))
            logger.info(f"  Batch inserted {len(batch)} profiles")

        return len(rows)
