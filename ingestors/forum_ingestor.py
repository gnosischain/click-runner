"""
Gnosis Forum (Discourse) ingestor.

Discourse serves structured JSON at the `.json` variant of every public route,
no auth required, so we never scrape HTML. Six tables:

    <db>.forum_categories  category tree
    <db>.forum_topics      one row per topic (discussion thread)
    <db>.forum_posts       one row per post — i.e. every comment/reply; carries
                           both the raw markdown (via include_raw=1) and the
                           cooked HTML rendering
    <db>.forum_users       public user directory (activity + like counts)
    <db>.forum_likes       one row per like edge (liker → post), with the real
                           timestamp of the like, from /user_actions.json
    <db>.forum_polls       one row per (post, poll, option) with its vote count —
                           the GIP temperature checks, normalised out of the post
                           payload we already fetch

Each row keeps the full API object in `raw_json` plus typed key columns, exactly
like the Snapshot ingestor. Tables are ReplacingMergeTree(ingested_at) keyed by
entity id so re-fetched topics/posts replace prior versions on merge.

Modes:
  backfill  crawl /latest.json across all pages, then fetch each topic's full
            post_stream.
  daily     only (re)fetch topics whose bumped_at advanced past the max
            bumped_at already stored — this picks up new topics, new replies,
            and edits that bumped the thread. (Silent edits that do not bump a
            topic are not re-captured; acceptable for v1.) Topics with an OPEN
            poll are the documented exception and are always refreshed, because
            poll votes do not bump a topic and the counts would otherwise freeze.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

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

# Discourse's UserAction type id for a like GIVEN, used by /user_actions.json's
# `filter` param. This is a DIFFERENT enum from post_action_type above -- do not
# reuse LIKE_ACTION_ID here. (filter=2, "was liked", returns 404 on this forum.)
USER_ACTION_LIKE_GIVEN = 1
USER_ACTIONS_PAGE_SIZE = 30  # server-side page size for /user_actions.json
LIKES_FLUSH_EVERY_USERS = 50  # write partial like-graph progress this often


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


def _dt_or_null(value) -> Optional[datetime]:
    """Like _dt, but keeps a missing timestamp missing.

    _dt folds both absent and unparseable into EPOCH, which is what the
    non-nullable DateTime columns on every other governance table want.
    forum_polls.close_at is Nullable instead: most polls never set a close date,
    and an epoch there is a real value that compares as long past -- it would
    drag min(close_at) back to 1970 and read as "closed" to anyone filtering on
    the timestamp. NULL keeps absent absent. See the header comment on
    create_forum_polls_table.sql for why status, not close_at, decides openness.
    """
    if not value:
        return None
    parsed = _dt(value)
    return None if parsed == EPOCH else parsed


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
        create_likes_sql: str = "",
        create_polls_sql: str = "",
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
        # Optional so an older caller that does not pass it keeps working; the
        # like-graph step is skipped (with a warning) when it is empty.
        self.create_likes_sql = create_likes_sql
        # Same contract for polls: unset means the polls table is neither created
        # nor written, and the open-poll refresh below is skipped.
        self.create_polls_sql = create_polls_sql
        self.mode = mode
        self.max_pages = max_pages

        self.categories_table = f"{database}.forum_categories"
        self.topics_table = f"{database}.forum_topics"
        self.posts_table = f"{database}.forum_posts"
        self.users_table = f"{database}.forum_users"
        self.likes_table = f"{database}.forum_likes"
        self.polls_table = f"{database}.forum_polls"

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
        # Capture the PREVIOUS likes_given per user before the new snapshot
        # overwrites it -- daily mode targets users whose count actually grew.
        prior_likes = self._prior_like_totals()
        # Returns (username, likes_given) so the like-graph step below knows who
        # to crawl and how many pages to expect, without re-walking the directory.
        users = self._ingest_users()

        # 2b) Like graph (best-effort) ---------------------------------------
        self._ingest_likes(users, prior_likes)

        # 3) Topics + posts ---------------------------------------------------
        watermark = self._topic_watermark() if self.mode == "daily" else None
        if watermark:
            logger.info(f"Daily mode: refreshing topics bumped after {watermark}")
        topics = self._enumerate_topics(watermark)
        if topics is None:
            logger.error("Failed to enumerate topics; aborting.")
            return False
        if watermark:
            topics = self._with_open_poll_topics(topics)
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
                *( (self.create_likes_sql,) if self.create_likes_sql else () ),
                *( (self.create_polls_sql,) if self.create_polls_sql else () ),
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
    def _ingest_users(self) -> List[Tuple[str, int]]:
        """Ingest the public user directory.

        Returns (username, likes_given) for every user seen, so the like-graph
        step can drive its crawl off the same pages instead of re-walking them.
        """
        columns = ["id", "username", "name", "trust_level", "likes_received",
                   "likes_given", "post_count", "topic_count", "days_visited", "raw_json"]
        rows = []
        seen: List[Tuple[str, int]] = []
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
                username = ((item.get("user") or {}).get("username")) or ""
                if username:
                    seen.append((username, _int(item.get("likes_given"))))
            page += 1
        if rows:
            self._batch_insert(self.users_table, rows, columns)
            logger.info(f"Inserted {len(rows)} users")
        else:
            logger.warning("No users fetched from directory_items (non-fatal).")
        return seen

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
        only if the very first page fails (hard error).

        Daily mode stops only when every topic on a page is at/under the
        watermark. /latest.json is not strictly bumped_at-desc — globally
        pinned threads sit at the top with stale bumped_at — so stopping on
        the first stale topic would skip fresher threads on later pages.
        """
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

            page_has_fresh = False
            for topic in topics:
                bumped = _dt(topic.get("bumped_at") or topic.get("created_at"))
                if watermark and bumped <= watermark:
                    continue
                page_has_fresh = True
                tid = topic.get("id")
                if tid is not None:
                    selected[tid] = topic

            # Daily mode: entire page is at/under the watermark — nothing
            # newer remains further down the list.
            if watermark and not page_has_fresh:
                break
            if not topic_list.get("more_topics_url"):
                break
            page += 1

        return list(selected.values())

    def _with_open_poll_topics(self, topics: List[Dict]) -> List[Dict]:
        """Add topics whose polls are still open, watermark or not.

        Casting a poll vote does not bump the topic, so an open poll on a quiet
        thread never re-enters the daily crawl and its counts freeze at the last
        reply. Measured on this forum: 99 topics hold 106 open polls, and 97 of
        those topics had gone more than seven days without a bump (83 more than a
        year), so the watermark alone would essentially never revisit them.
        Closed polls are final and deliberately excluded, so this set shrinks as
        polls close rather than growing with the forum — ~99 extra topic fetches,
        under a minute at 2 req/s.

        The stub carries only `id`: /t/{id}.json omits bumped_at, so _topic_row
        falls back to last_posted_at for these, which is what an unbumped topic's
        bumped_at already is. It therefore cannot advance the watermark past real
        activity.
        """
        if not self.create_polls_sql:
            return topics
        try:
            result = self.client.query(
                f"SELECT DISTINCT topic_id FROM {self.polls_table} FINAL "
                "WHERE status = 'open' AND topic_id > 0"
            )
            open_poll_ids = {_int(row[0]) for row in result.result_rows}
        except Exception as e:
            logger.warning(f"Could not read open-poll topics: {e}. Skipping refresh.")
            return topics

        extra = sorted(open_poll_ids - {t.get("id") for t in topics})
        if extra:
            logger.info(f"Refreshing {len(extra)} topic(s) with open polls")
            topics = topics + [{"id": tid} for tid in extra]
        return topics

    def _ingest_topic(self, topic_stub: Dict) -> int:
        tid = topic_stub.get("id")
        if tid is None:
            return 0
        # include_raw=1 adds each post's raw markdown alongside the cooked HTML
        # (verified supported on both /t/{id}.json and /t/{id}/posts.json).
        data = self._get(f"/t/{tid}.json", {"include_raw": 1})
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
            more = self._get(
                f"/t/{tid}/posts.json", {"post_ids[]": chunk, "include_raw": 1}
            )
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
                 "reads", "like_count", "raw", "cooked", "raw_json"],
            )
            self._ingest_polls(tid, posts)
        return len(posts)

    def _ingest_polls(self, topic_id, posts: List[Dict]) -> None:
        """Normalise any polls carried by these posts into forum_polls.

        Polls are embedded in the post payload we already have, so this adds no
        HTTP request. Only ~180 of 886 topics carry one, hence the cheap exit.
        """
        if not self.create_polls_sql:
            return
        rows = []
        for post in posts:
            rows.extend(self._poll_rows(topic_id, post))
        if not rows:
            return
        self._batch_insert(
            self.polls_table,
            rows,
            ["post_id", "topic_id", "poll_id", "poll_name", "poll_type", "status",
             "results_visibility", "is_public", "close_at", "voters", "option_id",
             "option_html", "option_votes", "raw_json"],
        )

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
            post.get("raw") or "",
            post.get("cooked") or "",
            json.dumps(post),
        ]

    def _poll_rows(self, topic_id, post: Dict) -> List[List]:
        """Fan a post's polls out to one row per (poll, option).

        A post may host several named polls ("poll", "poll2"), so poll_name is
        carried on every row. `raw_json` keeps the poll header only: `options` is
        already normalised into these rows, and `preloaded_voters` (public polls
        only) is a 25-per-option truncated sample that stays available in
        forum_posts.raw_json rather than being duplicated once per option here.
        """
        rows = []
        for poll in post.get("polls") or []:
            options = poll.get("options") or []
            if not options:
                continue
            header = {k: v for k, v in poll.items()
                      if k not in ("options", "preloaded_voters")}
            for option in options:
                option_id = option.get("id")
                if not option_id:
                    continue
                votes = option.get("votes")
                rows.append([
                    _int(post.get("id")),
                    _int(post.get("topic_id")) if post.get("topic_id") else _int(topic_id),
                    _int(poll.get("id")),
                    poll.get("name") or "poll",
                    poll.get("type") or "",
                    poll.get("status") or "",
                    poll.get("results") or "",
                    1 if poll.get("public") else 0,
                    _dt_or_null(poll.get("close")),
                    _int(poll.get("voters")),
                    str(option_id),
                    option.get("html") or "",
                    # Absent while the results policy hides counts. -1, not 0, so
                    # withheld stays distinguishable from a genuine zero.
                    _int(votes) if votes is not None else -1,
                    json.dumps(header),
                ])
        return rows

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    # ------------------------------------------------------------------ #
    # Like graph
    # ------------------------------------------------------------------ #
    def _stored_like_counts(self) -> Dict[str, int]:
        """Like edges already held, per acting username.

        Used only to decide who to re-crawl in daily mode. A user who has UNliked
        something will read as stored >= directory and be skipped -- acceptable,
        since unlikes are not representable in the table anyway.
        """
        try:
            result = self.client.query(
                f"SELECT acting_username, count() FROM {self.likes_table} FINAL "
                f"GROUP BY acting_username"
            )
            return {str(row[0]): int(row[1]) for row in result.result_rows}
        except Exception as e:
            logger.warning(f"Could not read stored like counts ({e}); treating as empty.")
            return {}

    def _prior_like_totals(self) -> Dict[str, int]:
        """likes_given per username as of the PREVIOUS run's user snapshot.

        Read before _ingest_users() overwrites it. Empty on a fresh database, which
        correctly makes every user a target (i.e. a full backfill).
        """
        try:
            result = self.client.query(
                f"SELECT username, likes_given FROM {self.users_table} FINAL"
            )
            return {str(row[0]): int(row[1]) for row in result.result_rows}
        except Exception as e:
            logger.warning(f"Could not read prior like totals ({e}); treating as empty.")
            return {}

    def _ingest_likes(self, users: List[Tuple[str, int]],
                      prior_likes: Optional[Dict[str, int]] = None) -> None:
        """Crawl the per-like edges for the users worth crawling.

        backfill: every user whose likes_given exceeds the edges we already hold, so an
        interrupted backfill resumes rather than restarting.
        daily: only users whose likes_given GREW since the previous run.

        Daily deliberately keys off growth rather than off "do we hold all their edges".
        /user_actions.json 404s for a large subset of users whose /u/{username}.json is a
        healthy 200 -- 673 of 1169 on this forum. Those users can never be completed, so a
        "crawl anyone incomplete" rule re-fetches all 673 every single day forever, roughly
        5.6 minutes of requests returning nothing. Growth-based targeting reduces that to
        the handful who actually gained a like.
        """
        if not self.create_likes_sql:
            logger.warning("No forum_likes DDL configured; skipping like graph.")
            return
        if not users:
            logger.warning("No users known; skipping like graph.")
            return

        columns = ["post_id", "topic_id", "post_number", "acting_user_id",
                   "acting_username", "created_at", "hidden", "deleted", "raw_json"]

        with_likes = [(u, g) for u, g in users if g > 0]
        if self.mode == "backfill":
            baseline = self._stored_like_counts()
            reason = "not yet stored"
        else:
            baseline = prior_likes or {}
            reason = "gained likes since last run"
        targets = [(u, g) for u, g in with_likes if g > baseline.get(u, 0)]
        logger.info(
            f"Like graph ({self.mode}): {len(targets)} users to crawl ({reason}), "
            f"{len(with_likes) - len(targets)} skipped"
        )

        # Flush per chunk of users rather than accumulating the whole crawl. A full
        # backfill is several hundred HTTP requests over several minutes; buffering
        # it all would mean a mid-run failure discards every edge fetched so far,
        # and a re-run starts from zero. Flushing keeps progress durable (the table
        # is ReplacingMergeTree, so a partial run is safely re-runnable) and caps
        # memory regardless of forum size.
        rows: List[List] = []
        total = 0
        expected_total = 0
        users_empty = 0
        for idx, (username, given) in enumerate(targets, start=1):
            fetched = self._fetch_user_likes(username, given)
            expected_total += given
            if not fetched:
                # The directory says this user gave likes but their action feed
                # returned nothing -- /user_actions.json 404s for a subset of users
                # whose /u/{username}.json is a healthy 200, so this is a real and
                # not-yet-explained coverage gap, not an error on our side.
                users_empty += 1
            rows.extend(fetched)
            if idx % LIKES_FLUSH_EVERY_USERS == 0 or idx == len(targets):
                if rows:
                    self._batch_insert(self.likes_table, rows, columns)
                    total += len(rows)
                    rows = []
                logger.info(f"  like graph: {idx}/{len(targets)} users, {total} edges inserted")

        # Report the capture rate every run. A crawl that silently returns 70% of the
        # like graph looks identical to one that returns 100%, and every phase-resolved
        # like metric downstream inherits the shortfall -- so it gets measured, not
        # assumed. Do not remove this in favour of a bare success log.
        if expected_total:
            logger.info(
                f"Like graph capture: {total}/{expected_total} edges "
                f"({100.0 * total / expected_total:.1f}% of directory likes_given); "
                f"{users_empty}/{len(targets)} users returned nothing"
            )
        if total:
            logger.info(f"Inserted {total} like edges")
        else:
            logger.info("Like graph: nothing new to insert.")

    def _fetch_user_likes(self, username: str, expected: int) -> List[List]:
        """Page one user's GIVEN likes out of /user_actions.json."""
        out: List[List] = []
        offset = 0
        # Slack of 3 pages over the directory's own count, which can lag reality.
        for _ in range((expected // USER_ACTIONS_PAGE_SIZE) + 3):
            data = self._get(
                "/user_actions.json",
                {"username": username, "filter": USER_ACTION_LIKE_GIVEN, "offset": offset},
            )
            if data is None:
                break
            actions = data.get("user_actions") or []
            if not actions:
                break
            for action in actions:
                post_id = _int(action.get("post_id"))
                if not post_id:
                    # Non-post actions cannot be keyed; skip rather than store a 0 id.
                    continue
                out.append([
                    post_id,
                    _int(action.get("topic_id")),
                    _int(action.get("post_number")),
                    _int(action.get("acting_user_id")),
                    # The QUERIED username is authoritative for "who liked this".
                    # The payload's target_* fields echo the acting user rather than
                    # the post author, so they are deliberately not stored.
                    username,
                    _dt(action.get("created_at")),
                    1 if action.get("hidden") else 0,
                    1 if action.get("deleted") else 0,
                    json.dumps(action),
                ])
            if len(actions) < USER_ACTIONS_PAGE_SIZE:
                break
            offset += len(actions)
        return out

    def _batch_insert(self, table: str, rows: List[List], columns: List[str]) -> None:
        for i in range(0, len(rows), INSERT_BATCH_SIZE):
            batch = rows[i : i + INSERT_BATCH_SIZE]
            with obs.time_operation(obs.get_job_name(), "forum", "insert_batch"):
                self.client.insert(table, batch, column_names=columns)
            obs.observe_rows("forum", table, len(batch))

    def _log_counts(self) -> None:
        for table in (self.categories_table, self.topics_table, self.posts_table,
                      self.users_table, self.likes_table, self.polls_table):
            try:
                logger.info(f"{table}: {self.get_row_count(table)} rows (pre-merge)")
            except Exception:
                pass
