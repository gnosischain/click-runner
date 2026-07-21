"""
Snapshot (off-chain governance) ingestor.

Pulls a Snapshot space's governance data from the public Snapshot Hub GraphQL
API (https://hub.snapshot.org/graphql) and lands it in ClickHouse. Four tables
per space:

    <db>.snapshot_space      one row  (space metadata + strategies, in raw_json)
    <db>.snapshot_proposals  one row per proposal
    <db>.snapshot_votes      one row per vote
    <db>.snapshot_follows    one row per follower (best-effort)

Like the Mixpanel ingestors, each row keeps the full API object verbatim in a
`raw_json` String column plus a few typed key columns, so downstream dbt models
re-derive any field with JSONExtract without re-fetching. Tables are
ReplacingMergeTree(ingested_at) keyed by the entity id, so a re-ingested
proposal/vote simply replaces the previous version on merge.

Pagination uses a `created`-timestamp cursor (created_gte) rather than skip,
because Snapshot caps `skip` at 5000 — a plain first/skip pager silently stops
at 5k rows (the space has ~48k votes). Votes are fetched per-proposal, which
keeps each proposal's page count well under that cap and makes incremental
(daily) refresh trivial: only proposals that are still open, or closed within
`vote_refresh_days`, are re-fetched; long-closed proposals are immutable.

Operational contract (same as Mixpanel): run `--snapshot-mode=backfill` once to
load all history, then `--snapshot-mode=daily` on a schedule.
"""

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import requests
from clickhouse_connect.driver.client import Client

import observability as obs
from .base import BaseIngestor
from .mixpanel_ingestor import MixpanelRateLimiter

logger = logging.getLogger("clickhouse_runner")

DEFAULT_GRAPHQL_URL = "https://hub.snapshot.org/graphql"
PAGE_SIZE = 1000
INSERT_BATCH_SIZE = 5000
# Timezone-aware UTC epoch sentinel. Must be aware: clickhouse-connect writes
# DateTime via int(x.timestamp()); a naive 1970-01-01 underflows local time on
# Windows (OSError 22), and naive values are misread as local-time elsewhere.
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

SPACE_QUERY = """
query Space($id: String!) {
  space(id: $id) {
    id
    name
    about
    network
    symbol
    terms
    proposalsCount
    followersCount
    votesCount
    admins
    moderators
    members
    strategies { name network params }
  }
}
"""

PROPOSALS_QUERY = """
query Proposals($space: String!, $first: Int!, $created_gte: Int!) {
  proposals(
    first: $first
    skip: 0
    where: { space: $space, created_gte: $created_gte }
    orderBy: "created"
    orderDirection: asc
  ) {
    id
    title
    body
    choices
    type
    state
    scores
    scores_total
    scores_state
    scores_updated
    quorum
    privacy
    start
    end
    snapshot
    created
    updated
    author
    network
    votes
    link
    app
    plugins
    strategies { name network }
    space { id }
  }
}
"""

VOTES_QUERY = """
query Votes($proposal: String!, $first: Int!, $created_gte: Int!) {
  votes(
    first: $first
    skip: 0
    where: { proposal: $proposal, created_gte: $created_gte }
    orderBy: "created"
    orderDirection: asc
  ) {
    id
    voter
    created
    choice
    vp
    vp_by_strategy
    vp_state
    reason
    app
    ipfs
    proposal { id }
    space { id }
  }
}
"""

FOLLOWS_QUERY = """
query Follows($space: String!, $first: Int!, $created_gte: Int!) {
  follows(
    first: $first
    skip: 0
    where: { space: $space, created_gte: $created_gte }
    orderBy: "created"
    orderDirection: asc
  ) {
    id
    follower
    created
    space { id }
  }
}
"""


def _ts(value) -> datetime:
    """Convert a Snapshot unix-seconds value to a naive UTC datetime."""
    try:
        ivalue = int(value)
    except (TypeError, ValueError):
        return EPOCH
    if ivalue <= 0:
        return EPOCH
    try:
        return datetime.fromtimestamp(ivalue, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return EPOCH


def _int(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


class SnapshotIngestor(BaseIngestor):
    """Ingestor for a Snapshot governance space."""

    def __init__(
        self,
        client: Client,
        variables: Dict[str, str],
        database: str,
        space: str,
        create_space_sql: str,
        create_proposals_sql: str,
        create_votes_sql: str,
        create_follows_sql: str,
        api_key: Optional[str] = None,
        graphql_url: Optional[str] = None,
        mode: str = "daily",
        vote_refresh_days: int = 5,
        include_follows: bool = True,
    ):
        super().__init__(client, variables)
        self.database = database
        self.space = space
        self.create_space_sql = create_space_sql
        self.create_proposals_sql = create_proposals_sql
        self.create_votes_sql = create_votes_sql
        self.create_follows_sql = create_follows_sql
        self.endpoint = graphql_url or DEFAULT_GRAPHQL_URL
        self.mode = mode
        self.vote_refresh_days = vote_refresh_days
        self.include_follows = include_follows

        self.space_table = f"{database}.snapshot_space"
        self.proposals_table = f"{database}.snapshot_proposals"
        self.votes_table = f"{database}.snapshot_votes"
        self.follows_table = f"{database}.snapshot_follows"

        self.headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if api_key:
            self.headers["x-api-key"] = api_key

        # Snapshot allows ~100 req/min anonymous; ~1 req/s stays safely under it.
        self.rate_limiter = MixpanelRateLimiter(per_second=1, per_hour=1_000_000)
        logger.info(
            f"Snapshot ingestor: space={self.space} endpoint={self.endpoint} "
            f"mode={self.mode} api_key={'yes' if api_key else 'no'}"
        )

    # ------------------------------------------------------------------ #
    # GraphQL transport
    # ------------------------------------------------------------------ #
    def _gql(self, query: str, variables: Dict, max_retries: int = 5) -> Optional[Dict]:
        """POST a GraphQL query, returning the `data` object or None on failure."""
        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait()
                with obs.time_operation(obs.get_job_name(), "snapshot", "graphql"):
                    response = requests.post(
                        self.endpoint,
                        json={"query": query, "variables": variables},
                        headers=self.headers,
                        timeout=60,
                    )

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 20))
                    logger.warning(
                        f"Rate limited (429). Sleeping {retry_after}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(retry_after)
                    continue

                if response.status_code >= 500:
                    wait = 5 * (2 ** attempt)
                    logger.warning(
                        f"Server error {response.status_code}. Retrying in {wait}s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait)
                    continue

                response.raise_for_status()
                payload = response.json()

                if payload.get("errors"):
                    logger.error(f"GraphQL errors: {json.dumps(payload['errors'])[:1000]}")
                    return None

                return payload.get("data") or {}

            except requests.exceptions.HTTPError as e:
                status = e.response.status_code if e.response is not None else 0
                if 400 <= status < 500:
                    logger.error(f"Client error {status}: {e}")
                    return None
                wait = 5 * (2 ** attempt)
                logger.warning(f"HTTP error {status}. Retrying in {wait}s")
                time.sleep(wait)
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                wait = 5 * (2 ** attempt)
                logger.warning(f"Connection error: {e}. Retrying in {wait}s")
                time.sleep(wait)

        logger.error(f"GraphQL request failed after {max_retries} attempts")
        return None

    def _paginate_created(
        self, query: str, base_vars: Dict, entity: str
    ) -> Optional[List[Dict]]:
        """Page through an entity ordered by `created` asc using a created_gte
        cursor (Snapshot caps skip at 5000). Returns None on transport failure."""
        results: List[Dict] = []
        seen = set()
        cursor = 0
        while True:
            data = self._gql(query, {**base_vars, "first": PAGE_SIZE, "created_gte": cursor})
            if data is None:
                return None
            rows = data.get(entity) or []
            for row in rows:
                rid = row.get("id")
                if rid in seen:
                    continue
                seen.add(rid)
                results.append(row)
            if len(rows) < PAGE_SIZE:
                break
            last_created = _int(rows[-1].get("created"))
            # Advance the cursor; bump by 1 if a full page shares one timestamp
            # so we never stall (pathological, but keeps the loop finite).
            cursor = last_created if last_created != cursor else cursor + 1
        return results

    # ------------------------------------------------------------------ #
    # Ingest
    # ------------------------------------------------------------------ #
    def ingest(self, skip_table_creation: bool = False, **kwargs) -> bool:
        if not skip_table_creation and not self._create_tables():
            return False

        # 1) Space (metadata + strategies) -----------------------------------
        space = self._fetch_space()
        if space is None:
            logger.error("Failed to fetch space; aborting.")
            return False
        self._insert_space(space)

        # 2) Proposals --------------------------------------------------------
        proposals = self._paginate_created(
            PROPOSALS_QUERY, {"space": self.space}, "proposals"
        )
        if proposals is None:
            logger.error("Failed to fetch proposals; aborting.")
            return False
        logger.info(f"Fetched {len(proposals)} proposals for {self.space}")
        self._insert_proposals(proposals)

        # 3) Votes (per proposal) --------------------------------------------
        targets = self._proposals_needing_votes(proposals)
        logger.info(
            f"Fetching votes for {len(targets)}/{len(proposals)} proposals "
            f"(mode={self.mode})"
        )
        total_votes = 0
        for idx, proposal in enumerate(targets, start=1):
            pid = proposal.get("id")
            if not pid:
                continue
            votes = self._paginate_created(VOTES_QUERY, {"proposal": pid}, "votes")
            if votes is None:
                logger.error(f"Failed to fetch votes for proposal {pid}; aborting.")
                return False
            if votes:
                self._insert_votes(votes)
                total_votes += len(votes)
            if idx % 25 == 0 or idx == len(targets):
                logger.info(f"  votes: {idx}/{len(targets)} proposals, {total_votes} rows")
        logger.info(f"Inserted {total_votes} votes")

        # 4) Follows (best-effort — space.followersCount already has the total) --
        if self.include_follows:
            follows = self._paginate_created(
                FOLLOWS_QUERY, {"space": self.space}, "follows"
            )
            if follows is None:
                logger.warning("Follows fetch failed; skipping (non-fatal).")
            else:
                self._insert_follows(follows)
                logger.info(f"Inserted {len(follows)} follows")

        self._log_counts()
        return True

    def _create_tables(self) -> bool:
        try:
            for path in (
                self.create_space_sql,
                self.create_proposals_sql,
                self.create_votes_sql,
                self.create_follows_sql,
            ):
                sql = self.load_sql_file(path)
                with obs.time_operation(obs.get_job_name(), "snapshot", "create_table"):
                    self.client.command(sql)
            logger.info("Snapshot tables created/verified.")
            return True
        except Exception as e:
            logger.error(f"Failed to create Snapshot tables: {e}")
            return False

    def _fetch_space(self) -> Optional[Dict]:
        data = self._gql(SPACE_QUERY, {"id": self.space})
        if data is None:
            return None
        space = data.get("space")
        if not space:
            logger.error(f"Space '{self.space}' not found.")
            return None
        return space

    def _proposals_needing_votes(self, proposals: List[Dict]) -> List[Dict]:
        if self.mode == "backfill":
            return proposals
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.vote_refresh_days)
        selected = []
        for proposal in proposals:
            state = proposal.get("state")
            end_dt = _ts(proposal.get("end"))
            if state in ("active", "pending") or end_dt >= cutoff:
                selected.append(proposal)
        return selected

    # ------------------------------------------------------------------ #
    # Inserts
    # ------------------------------------------------------------------ #
    def _insert_space(self, space: Dict) -> None:
        columns = [
            "space_id", "name", "network", "symbol",
            "proposals_count", "followers_count", "votes_count", "raw_json",
        ]
        row = [
            space.get("id") or self.space,
            space.get("name") or "",
            str(space.get("network") or ""),
            space.get("symbol") or "",
            _int(space.get("proposalsCount")),
            _int(space.get("followersCount")),
            _int(space.get("votesCount")),
            json.dumps(space),
        ]
        self.client.insert(self.space_table, [row], column_names=columns)
        obs.observe_rows("snapshot", self.space_table, 1)

    def _insert_proposals(self, proposals: List[Dict]) -> None:
        columns = [
            "id", "space_id", "title", "state", "type", "author",
            "created_at", "start_at", "end_at", "snapshot_block",
            "scores_total", "quorum", "votes_count", "scores_state", "raw_json",
        ]
        rows = []
        for p in proposals:
            rows.append([
                p.get("id") or "",
                (p.get("space") or {}).get("id") or self.space,
                p.get("title") or "",
                p.get("state") or "",
                p.get("type") or "",
                p.get("author") or "",
                _ts(p.get("created")),
                _ts(p.get("start")),
                _ts(p.get("end")),
                _int(p.get("snapshot")),
                _float(p.get("scores_total")),
                _float(p.get("quorum")),
                _int(p.get("votes")),
                p.get("scores_state") or "",
                json.dumps(p),
            ])
        self._batch_insert(self.proposals_table, rows, columns)

    def _insert_votes(self, votes: List[Dict]) -> None:
        columns = [
            "id", "proposal_id", "space_id", "voter",
            "created_at", "vp", "vp_state", "raw_json",
        ]
        rows = []
        for v in votes:
            rows.append([
                v.get("id") or "",
                (v.get("proposal") or {}).get("id") or "",
                (v.get("space") or {}).get("id") or self.space,
                v.get("voter") or "",
                _ts(v.get("created")),
                _float(v.get("vp")),
                v.get("vp_state") or "",
                json.dumps(v),
            ])
        self._batch_insert(self.votes_table, rows, columns)

    def _insert_follows(self, follows: List[Dict]) -> None:
        columns = ["id", "follower", "space_id", "created_at", "raw_json"]
        rows = []
        for f in follows:
            rows.append([
                f.get("id") or "",
                f.get("follower") or "",
                (f.get("space") or {}).get("id") or self.space,
                _ts(f.get("created")),
                json.dumps(f),
            ])
        self._batch_insert(self.follows_table, rows, columns)

    def _batch_insert(self, table: str, rows: List[List], columns: List[str]) -> None:
        for i in range(0, len(rows), INSERT_BATCH_SIZE):
            batch = rows[i : i + INSERT_BATCH_SIZE]
            with obs.time_operation(obs.get_job_name(), "snapshot", "insert_batch"):
                self.client.insert(table, batch, column_names=columns)
            obs.observe_rows("snapshot", table, len(batch))

    def _log_counts(self) -> None:
        for table in (self.space_table, self.proposals_table, self.votes_table, self.follows_table):
            try:
                logger.info(f"{table}: {self.get_row_count(table)} rows (pre-merge)")
            except Exception:
                pass
