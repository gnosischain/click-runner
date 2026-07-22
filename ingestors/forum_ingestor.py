"""
Gnosis Forum (Discourse) ingestor.

Discourse serves structured JSON at the `.json` variant of every public route,
no auth required, so we never scrape HTML. Four tables:

    <db>.forum_categories  category tree
    <db>.forum_topics      one row per topic (discussion thread)
    <db>.forum_posts       one row per post — i.e. every comment/reply
    <db>.forum_users       public user directory (activity + like counts)

Each row keeps the full API object in `raw_json` plus typed key columns, exactly
like the Snapshot ingestor. Tables are ReplacingMergeTree(ingested_at) keyed by
entity id so re-fetched topics/posts replace prior versions on merge.

Modes:
  backfill  crawl /latest.json across all pages, then fetch each topic's full
            post_stream.
  daily     only (re)fetch topics whose bumped_at advanced past the max
            bumped_at already stored — this picks up new topics, new replies,
            and edits that bumped the thread. (Silent edits that do not bump a
            topic are not re-captured; acceptable for v1.)
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests
from clickhouse_connect.driver.client import Client

import observability as obs
from .base import BaseIngestor
from .mixpanel_ingestor import MixpanelRateLimiter

logger = logging.getLogger("clickhouse_runner")

INSERT_BATCH_SIZE = 5000
POST_IDS_CHUNK = 50
# Timezone-aware UTC epoch sentinel — see snapshot_ingestor for the rationale
# (naive 1970-01-01 crashes clickhouse-connect's DateTime write on Windows).
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
LIKE_ACTION_ID = 2  # Discourse post_action_type id for "like"


def _int(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _dt(value) -> datetime:
    """Parse a Discourse ISO-8601 timestamp to a timezone-aware UTC datetime."""
    if not value:
        return EPOCH
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return EPOCH


def _chunks(items: List, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


class ForumIngestor(BaseIngestor):
    """Ingestor for a Discourse forum."""

    def __init__(
        self,
        client: Client,
        variables: Dict[str, str],
        database: str,
        base_url: str,
        create_categories_sql: str,
        create_topics_sql: str,
        create_posts_sql: str,
        create_users_sql: str,
        mode: str = "daily",
        max_pages: int = 400,
        user_agent: str = "gnosis-analytics-click-runner/1.0",
    ):
        super().__init__(client, variables)
        self.database = database
        self.base_url = base_url.rstrip("/")
        self.create_categories_sql = create_categories_sql
        self.create_topics_sql = create_topics_sql
        self.create_posts_sql = create_posts_sql
        self.create_users_sql = create_users_sql
        self.mode = mode
        self.max_pages = max_pages

        self.categories_table = f"{database}.forum_categories"
        self.topics_table = f"{database}.forum_topics"
        self.posts_table = f"{database}.forum_posts"
        self.users_table = f"{database}.forum_users"

        self.headers = {"User-Agent": user_agent, "Accept": "application/json"}
        # Discourse anonymous limiting is ~60 req/min; 2 req/s stays under it.
        self.rate_limiter = MixpanelRateLimiter(per_second=2, per_hour=1_000_000)
        logger.info(
            f"Forum ingestor: base_url={self.base_url} mode={self.mode} "
            f"max_pages={self.max_pages}"
        )

    # ------------------------------------------------------------------ #
    # HTTP transport
    # ------------------------------------------------------------------ #
    def _get(self, path: str, params: Optional[Dict] = None, max_retries: int = 5) -> Optional[Dict]:
        url = f"{self.base_url}{path}"
        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait()
                with obs.time_operation(obs.get_job_name(), "forum", "http_get"):
                    response = requests.get(
                        url, params=params, headers=self.headers, timeout=60
                    )

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 10))
                    logger.warning(
                        f"Rate limited (429) on {path}. Sleeping {retry_after}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(retry_after)
                    continue

                if response.status_code in (403, 404):
                    logger.warning(f"{response.status_code} on {path}; skipping.")
                    return None

                if response.status_code >= 500:
                    wait = 5 * (2 ** attempt)
                    logger.warning(
                        f"Server error {response.status_code} on {path}. "
                        f"Retrying in {wait}s"
                    )
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                if 400 <= status < 500:
                    logger.error(f"Client error {status} on {path}: {e}")
                    return None
                wait = 5 * (2 ** attempt)
                logger.warning(f"HTTP error {status} on {path}. Retrying in {wait}s")
                time.sleep(wait)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                wait = 5 * (2 ** attempt)
                logger.warning(f"Connection error on {path}: {e}. Retrying in {wait}s")
                time.sleep(wait)
            except json.JSONDecodeError as e:
                logger.error(f"Non-JSON response on {path}: {e}")
                return None

        logger.error(f"GET {path} failed after {max_retries} attempts")
        return None

    # ------------------------------------------------------------------ #
    # Ingest
    # ------------------------------------------------------------------ #
    def ingest(self, skip_table_creation: bool = False, **kwargs) -> bool:
        if not skip_table_creation and not self._create_tables():
            return False

        # 1) Categories (best-effort) ----------------------------------------
        self._ingest_categories()

        # 2) Users (best-effort, bulk via directory_items) ------------------
        self._ingest_users()

        # 3) Topics + posts ---------------------------------------------------
        watermark = self._topic_watermark() if self.mode == "daily" else None
        if watermark:
            logger.info(f"Daily mode: refreshing topics bumped after {watermark}")
        topics = self._enumerate_topics(watermark)
        if topics is None:
            logger.error("Failed to enumerate topics; aborting.")
            return False
        logger.info(f"Ingesting {len(topics)} topics")

        total_posts = 0
        for idx, topic_stub in enumerate(topics, start=1):
            posts = self._ingest_topic(topic_stub)
            total_posts += posts
            if idx % 50 == 0 or idx == len(topics):
                logger.info(f"  topics: {idx}/{len(topics)}, {total_posts} posts")

        logger.info(f"Inserted {total_posts} posts across {len(topics)} topics")
        self._log_counts()
        return True

    def _create_tables(self) -> bool:
        try:
            for path in (
                self.create_categories_sql,
                self.create_topics_sql,
                self.create_posts_sql,
                self.create_users_sql,
            ):
                sql = self.load_sql_file(path)
                with obs.time_operation(obs.get_job_name(), "forum", "create_table"):
                    self.client.command(sql)
            logger.info("Forum tables created/verified.")
            return True
        except Exception as e:
            logger.error(f"Failed to create Forum tables: {e}")
            return False

    # ------------------------------------------------------------------ #
    # Categories
    # ------------------------------------------------------------------ #
    def _ingest_categories(self) -> None:
        data = self._get("/categories.json", {"include_subcategories": "true"})
        if data is None:
            logger.warning("Categories fetch failed; skipping (non-fatal).")
            return
        categories = (data.get("category_list") or {}).get("categories") or []
        rows = []
        for cat in categories:
            rows.append(self._category_row(cat))
            for sub in cat.get("subcategory_list") or []:
                rows.append(self._category_row(sub))
        if rows:
            self._batch_insert(
                self.categories_table,
                rows,
                ["id", "parent_id", "name", "slug", "topic_count",
                 "post_count", "description", "raw_json"],
            )
            logger.info(f"Inserted {len(rows)} categories")

    def _category_row(self, cat: Dict) -> List:
        return [
            _int(cat.get("id")),
            _int(cat.get("parent_category_id")) if cat.get("parent_category_id") else -1,
            cat.get("name") or "",
            cat.get("slug") or "",
            _int(cat.get("topic_count")),
            _int(cat.get("post_count")),
            cat.get("description_text") or cat.get("description") or "",
            json.dumps(cat),
        ]

    # ------------------------------------------------------------------ #
    # Users (bulk directory)
    # ------------------------------------------------------------------ #
    def _ingest_users(self) -> None:
        columns = ["id", "username", "name", "trust_level", "likes_received",
                   "likes_given", "post_count", "topic_count", "days_visited", "raw_json"]
        rows = []
        page = 0
        while page <= self.max_pages:
            data = self._get(
                "/directory_items.json", {"period": "all", "order": "post_count", "page": page}
            )
            if data is None:
                break
            items = data.get("directory_items") or []
            if not items:
                break
            for item in items:
                rows.append(self._user_row(item))
            page += 1
        if rows:
            self._batch_insert(self.users_table, rows, columns)
            logger.info(f"Inserted {len(rows)} users")
        else:
            logger.warning("No users fetched from directory_items (non-fatal).")

    def _user_row(self, item: Dict) -> List:
        user = item.get("user") or {}
        return [
            _int(user.get("id")),
            user.get("username") or "",
            user.get("name") or "",
            _int(user.get("trust_level")),
            _int(item.get("likes_received")),
            _int(item.get("likes_given")),
            _int(item.get("post_count")),
            _int(item.get("topic_count")),
            _int(item.get("days_visited")),
            json.dumps(item),
        ]

    # ------------------------------------------------------------------ #
    # Topics + posts
    # ------------------------------------------------------------------ #
    def _topic_watermark(self) -> Optional[datetime]:
        try:
            result = self.client.query(f"SELECT max(bumped_at) FROM {self.topics_table}")
            if result.result_rows and result.result_rows[0][0]:
                value = result.result_rows[0][0]
                if isinstance(value, datetime):
                    # CH returns naive UTC; make it aware to compare with _dt().
                    if value.tzinfo is None:
                        value = value.replace(tzinfo=timezone.utc)
                    if value > EPOCH:
                        return value
        except Exception as e:
            logger.warning(f"Could not read topic watermark: {e}")
        return None

    def _enumerate_topics(self, watermark: Optional[datetime]) -> Optional[List[Dict]]:
        """Page /latest.json and collect topic stubs to (re)fetch. Returns None
        only if the very first page fails (hard error)."""
        selected: Dict[int, Dict] = {}
        page = 0
        while page <= self.max_pages:
            data = self._get("/latest.json", {"no_definitions": "true", "page": page})
            if data is None:
                if page == 0:
                    return None
                break
            topic_list = data.get("topic_list") or {}
            topics = topic_list.get("topics") or []
            if not topics:
                break

            stop = False
            for topic in topics:
                bumped = _dt(topic.get("bumped_at") or topic.get("created_at"))
                if watermark and bumped <= watermark:
                    stop = True
                    continue
                tid = topic.get("id")
                if tid is not None:
                    selected[tid] = topic

            # Daily mode: once a page is fully at/under the watermark, stop.
            if watermark and stop:
                break
            if not topic_list.get("more_topics_url"):
                break
            page += 1

        return list(selected.values())

    def _ingest_topic(self, topic_stub: Dict) -> int:
        tid = topic_stub.get("id")
        if tid is None:
            return 0
        data = self._get(f"/t/{tid}.json")
        if data is None:
            return 0

        self._batch_insert(
            self.topics_table,
            [self._topic_row(data, topic_stub)],
            ["id", "title", "slug", "category_id", "posts_count", "reply_count",
             "views", "like_count", "participant_count", "tags", "created_at",
             "last_posted_at", "bumped_at", "closed", "archived", "pinned", "raw_json"],
        )

        stream_obj = data.get("post_stream") or {}
        posts = list(stream_obj.get("posts") or [])
        stream_ids = stream_obj.get("stream") or []
        loaded_ids = {p.get("id") for p in posts}
        remaining = [pid for pid in stream_ids if pid not in loaded_ids]

        for chunk in _chunks(remaining, POST_IDS_CHUNK):
            more = self._get(f"/t/{tid}/posts.json", {"post_ids[]": chunk})
            if more is None:
                continue
            posts.extend((more.get("post_stream") or {}).get("posts") or [])

        if posts:
            rows = [self._post_row(tid, p) for p in posts]
            self._batch_insert(
                self.posts_table,
                rows,
                ["id", "topic_id", "post_number", "user_id", "username",
                 "created_at", "updated_at", "reply_to_post_number", "reply_count",
                 "reads", "like_count", "cooked", "raw_json"],
            )
        return len(posts)

    def _topic_row(self, data: Dict, topic_stub: Optional[Dict] = None) -> List:
        # Store topic metadata without the (large) embedded post_stream.
        # bumped_at is present on /latest.json list stubs but omitted from
        # /t/{id}.json detail — fall back to the stub, then last_posted_at.
        stub = topic_stub or {}
        meta = {k: v for k, v in data.items() if k != "post_stream"}
        return [
            _int(data.get("id")),
            data.get("title") or "",
            data.get("slug") or "",
            _int(data.get("category_id")) if data.get("category_id") is not None else -1,
            _int(data.get("posts_count")),
            _int(data.get("reply_count")),
            _int(data.get("views")),
            _int(data.get("like_count")),
            _int(data.get("participant_count")),
            ",".join(data.get("tags") or []),
            _dt(data.get("created_at")),
            _dt(data.get("last_posted_at")),
            _dt(
                data.get("bumped_at")
                or stub.get("bumped_at")
                or data.get("last_posted_at")
            ),
            1 if data.get("closed") else 0,
            1 if data.get("archived") else 0,
            1 if data.get("pinned") else 0,
            json.dumps(meta),
        ]

    def _post_row(self, topic_id, post: Dict) -> List:
        like_count = 0
        for action in post.get("actions_summary") or []:
            if action.get("id") == LIKE_ACTION_ID:
                like_count = _int(action.get("count"))
                break
        return [
            _int(post.get("id")),
            _int(post.get("topic_id")) if post.get("topic_id") else _int(topic_id),
            _int(post.get("post_number")),
            _int(post.get("user_id")),
            post.get("username") or "",
            _dt(post.get("created_at")),
            _dt(post.get("updated_at")),
            _int(post.get("reply_to_post_number")),
            _int(post.get("reply_count")),
            _int(post.get("reads")),
            like_count,
            post.get("cooked") or "",
            json.dumps(post),
        ]

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _batch_insert(self, table: str, rows: List[List], columns: List[str]) -> None:
        for i in range(0, len(rows), INSERT_BATCH_SIZE):
            batch = rows[i : i + INSERT_BATCH_SIZE]
            with obs.time_operation(obs.get_job_name(), "forum", "insert_batch"):
                self.client.insert(table, batch, column_names=columns)
            obs.observe_rows("forum", table, len(batch))

    def _log_counts(self) -> None:
        for table in (self.categories_table, self.topics_table, self.posts_table, self.users_table):
            try:
                logger.info(f"{table}: {self.get_row_count(table)} rows (pre-merge)")
            except Exception:
                pass
