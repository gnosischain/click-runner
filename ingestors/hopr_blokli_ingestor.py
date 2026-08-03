"""Ingest HOPR mixnet state from Blokli, HOPR's own on-chain indexer.

Blokli (github.com/hoprnet/blokli) serves a public, unauthenticated GraphQL API
at https://blokli.<network>.hoprnet.link/graphql. The GnosisVPN client itself
depends on it (GNOSISVPN_HOPR_BLOKLI_URL), which is the reason to trust it as a
source: it is the network's own view of itself.

Networks: `jura` (HOPR v4 -- the network the GnosisVPN client defaults to, live
on Gnosis Chain since block 47415377 / 2026-07-27) and `rotsee` (development).
There is deliberately NO endpoint for `dufour`, the legacy production network --
Blokli only exists for v4. dufour has to be measured from execution.logs.

Why this ingestor exists when we already decode the chain ourselves: Blokli
carries state that is not in the event log at all --
  - the announced multiaddress per node (raw IP), for geo/ASN enrichment,
  - the ticket price and minimum winning probability as currently resolved by the
    oracles, which is what converts a ticket count into an expected value,
  - aggregated channel balances and Safe balances without us having to
    reconstruct running balances from diffs.

Grain: one snapshot per (network, snapshot_date). Re-running the same day
replaces that day's rows via ReplacingMergeTree(ingested_at), so backfills and
retries are safe.
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

BLOKLI_URL_TEMPLATE = "https://blokli.{network}.hoprnet.link/graphql"
# Only these have a Blokli deployment. `dufour` is intentionally absent.
KNOWN_NETWORKS = ("jura", "rotsee")
CHANNEL_STATUSES = ("OPEN", "PENDINGTOCLOSE", "CLOSED")

REQUEST_TIMEOUT_S = 30
MAX_RETRIES = 4
RETRY_BACKOFF_S = 2.0
# Accounts are fetched with GraphQL aliases batched into one request. 25 keeps
# the query well under any sane request-size limit while keeping round-trips low.
ACCOUNTS_PER_REQUEST = 25
INSERT_SETTINGS = {"optimize_on_insert": 0, "max_insert_threads": 1}


class HoprBlokliIngestor(BaseIngestor):
    """Snapshot HOPR network + node state from Blokli into ClickHouse."""

    def __init__(
        self,
        client: Client,
        variables: Dict[str, str],
        networks: Sequence[str] = ("jura",),
        network_table: str = "crawlers_data.hopr_blokli_network_snapshot",
        nodes_table: str = "crawlers_data.hopr_blokli_nodes",
        network_create_sql: str = "queries/hopr/blokli_network_snapshot_create.sql",
        nodes_create_sql: str = "queries/hopr/blokli_nodes_create.sql",
    ):
        super().__init__(client, variables)
        unknown = [n for n in networks if n not in KNOWN_NETWORKS]
        if unknown:
            raise ValueError(
                f"No Blokli endpoint for {unknown}. Known: {list(KNOWN_NETWORKS)}. "
                "Note dufour (legacy production) has no Blokli -- use execution.logs."
            )
        if not networks:
            raise ValueError("At least one network is required")
        self.networks = list(networks)
        self.network_table = network_table
        self.nodes_table = nodes_table
        self.network_create_sql = network_create_sql
        self.nodes_create_sql = nodes_create_sql
        self.session = requests.Session()

    # ---------------------------------------------------------------- transport

    def _post(self, network: str, query: str) -> Optional[Dict[str, Any]]:
        """POST a GraphQL query, retrying transient failures.

        Returns the `data` object, or None. A GraphQL `errors` array is treated as
        a hard failure and NOT coerced into an empty result -- a silently empty
        snapshot would look identical to "the network has no nodes".
        """
        url = BLOKLI_URL_TEMPLATE.format(network=network)
        last_err: Optional[str] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.post(
                    url, json={"query": query}, timeout=REQUEST_TIMEOUT_S,
                    headers={"content-type": "application/json"},
                )
                if resp.status_code >= 500:
                    last_err = f"HTTP {resp.status_code}"
                elif resp.status_code != 200:
                    logger.error(
                        f"Blokli {network}: HTTP {resp.status_code} (not retrying)",
                        extra={"event": "blokli_http_error", "ingestor": "hopr-blokli"},
                    )
                    return None
                else:
                    body = resp.json()
                    if body.get("errors"):
                        logger.error(
                            f"Blokli {network}: GraphQL errors: {body['errors']}",
                            extra={"event": "blokli_graphql_error", "ingestor": "hopr-blokli"},
                        )
                        return None
                    return body.get("data")
            except (requests.RequestException, ValueError) as e:
                last_err = str(e)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_S * attempt)
        logger.error(
            f"Blokli {network}: giving up after {MAX_RETRIES} attempts ({last_err})",
            extra={"event": "blokli_exhausted", "ingestor": "hopr-blokli"},
        )
        return None

    # ------------------------------------------------------------------ parsing

    @staticmethod
    def _token_amount(raw: Optional[str]) -> Optional[float]:
        """Blokli returns token values as '3835.0000600992 wxHOPR'.

        Kept as Float64 for convenience; the authoritative wei-precision figures
        come from our own event decoding, not from here.
        """
        if not raw:
            return None
        head = str(raw).strip().split(" ")[0]
        try:
            return float(head)
        except ValueError:
            return None

    # ------------------------------------------------------------------ fetches

    def _fetch_network(self, network: str) -> Optional[Dict[str, Any]]:
        status_fields = "\n".join(
            f'{s.lower()}: channelStats(status: {s}) '
            "{ ... on ChannelStats { count balance } }"
            for s in CHANNEL_STATUSES
        )
        query = f"""
        {{
          health
          compatibility {{ apiVersion }}
          chainInfo {{ ... on ChainInfo {{
            network chainId blockNumber ticketPrice minTicketWinningProbability
            channelClosureGracePeriod keyBindingFee contractAddresses
          }} }}
          accountCount {{ ... on Count {{ count }} }}
          allChannels: channelStats {{ ... on ChannelStats {{ count balance }} }}
          {status_fields}
          safes {{ ... on SafesList {{ safes {{ address }} }} }}
          safesBalance {{ ... on SafesBalance {{ balance }} }}
        }}
        """
        data = self._post(network, query)
        if not data:
            return None
        chain = data.get("chainInfo") or {}
        if not chain.get("blockNumber"):
            logger.error(
                f"Blokli {network}: chainInfo missing blockNumber -- refusing to write a snapshot",
                extra={"event": "blokli_bad_chaininfo", "ingestor": "hopr-blokli"},
            )
            return None
        safes = ((data.get("safes") or {}).get("safes")) or []
        row: Dict[str, Any] = {
            "network": chain.get("network") or network,
            "chain_id": int(chain.get("chainId") or 0),
            "block_number": int(chain["blockNumber"]),
            "api_version": ((data.get("compatibility") or {}).get("apiVersion")) or "",
            "ticket_price_wxhopr": self._token_amount(chain.get("ticketPrice")),
            "min_ticket_winning_probability": float(chain.get("minTicketWinningProbability") or 0.0),
            "key_binding_fee_wxhopr": self._token_amount(chain.get("keyBindingFee")),
            "channel_closure_grace_period_s": int(chain.get("channelClosureGracePeriod") or 0),
            "account_count": int(((data.get("accountCount") or {}).get("count")) or 0),
            "safes_count": len(safes),
            "safes_balance_wxhopr": self._token_amount(
                ((data.get("safesBalance") or {}).get("balance"))
            ),
        }
        allc = data.get("allChannels") or {}
        row["channels_total"] = int(allc.get("count") or 0)
        row["channels_balance_wxhopr"] = self._token_amount(allc.get("balance"))
        for s in CHANNEL_STATUSES:
            node = data.get(s.lower()) or {}
            row[f"channels_{s.lower()}"] = int(node.get("count") or 0)
            row[f"channels_{s.lower()}_balance_wxhopr"] = self._token_amount(node.get("balance"))
        return row

    def _fetch_nodes(self, network: str, account_count: int) -> List[Dict[str, Any]]:
        """Fetch every account by keyid.

        `accounts` requires at least one filter, so there is no list-all query --
        keyids are dense from 0, so we walk 0..account_count-1 in alias batches.
        A keyid that returns nothing is skipped rather than treated as an error,
        which keeps the walk robust if the sequence ever becomes sparse.
        """
        rows: List[Dict[str, Any]] = []
        fragment = (
            "fragment F on AccountsResult { ... on AccountsList { accounts "
            "{ keyid chainKey packetKey safeAddress multiAddresses } } }"
        )
        for start in range(0, account_count, ACCOUNTS_PER_REQUEST):
            ids = range(start, min(start + ACCOUNTS_PER_REQUEST, account_count))
            aliases = " ".join(f"a{i}: accounts(keyid: {i}) {{ ...F }}" for i in ids)
            data = self._post(network, f"{{ {aliases} }} {fragment}")
            if data is None:
                logger.error(
                    f"Blokli {network}: account batch {start} failed -- aborting node fetch",
                    extra={"event": "blokli_accounts_failed", "ingestor": "hopr-blokli"},
                )
                return []
            for payload in data.values():
                for acct in ((payload or {}).get("accounts")) or []:
                    multi = acct.get("multiAddresses") or []
                    rows.append({
                        "network": network,
                        "keyid": int(acct.get("keyid") or 0),
                        "chain_key": (acct.get("chainKey") or "").lower(),
                        "packet_key": acct.get("packetKey") or "",
                        "safe_address": (acct.get("safeAddress") or "").lower(),
                        "multiaddress": multi[0] if multi else "",
                        "multiaddress_count": len(multi),
                    })
        return rows

    # -------------------------------------------------------------------- write

    def _insert(self, table: str, rows: List[Dict[str, Any]], columns: List[str]) -> bool:
        if not rows:
            logger.warning(
                f"No rows to insert into {table}",
                extra={"event": "blokli_no_rows", "ingestor": "hopr-blokli"},
            )
            return True
        data = [[r.get(c) for c in columns] for r in rows]
        try:
            with obs.time_operation(obs.get_job_name(), "hopr-blokli", f"insert:{table}"):
                self.client.insert(table, data, column_names=columns, settings=INSERT_SETTINGS)
        except Exception as e:
            logger.error(
                f"Insert into {table} failed: {e}",
                extra={"event": "blokli_insert_failure", "ingestor": "hopr-blokli"},
            )
            return False
        logger.info(
            f"Inserted {len(rows)} rows into {table}",
            extra={"event": "blokli_insert_ok", "ingestor": "hopr-blokli"},
        )
        return True

    # ------------------------------------------------------------------- ingest

    def ingest(self, skip_table_creation: bool = False, **kwargs) -> bool:
        if not skip_table_creation:
            for path in (self.network_create_sql, self.nodes_create_sql):
                try:
                    if not self.execute_queries([self.load_sql_file(path)]):
                        return False
                except FileNotFoundError:
                    logger.error(
                        f"Create-table SQL not found: {path}",
                        extra={"event": "blokli_missing_sql", "ingestor": "hopr-blokli"},
                    )
                    return False

        snapshot_date = datetime.now(timezone.utc).date()
        ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

        net_cols = [
            "snapshot_date", "network", "chain_id", "block_number", "api_version",
            "ticket_price_wxhopr", "min_ticket_winning_probability",
            "key_binding_fee_wxhopr", "channel_closure_grace_period_s",
            "account_count", "safes_count", "safes_balance_wxhopr",
            "channels_total", "channels_balance_wxhopr",
            "channels_open", "channels_open_balance_wxhopr",
            "channels_pendingtoclose", "channels_pendingtoclose_balance_wxhopr",
            "channels_closed", "channels_closed_balance_wxhopr",
            "ingested_at",
        ]
        node_cols = [
            "snapshot_date", "network", "keyid", "chain_key", "packet_key",
            "safe_address", "multiaddress", "multiaddress_count", "ingested_at",
        ]

        ok = True
        for network in self.networks:
            net_row = self._fetch_network(network)
            if not net_row:
                logger.error(
                    f"Blokli {network}: network snapshot failed",
                    extra={"event": "blokli_network_failed", "ingestor": "hopr-blokli"},
                )
                ok = False
                continue
            net_row["snapshot_date"] = snapshot_date
            net_row["ingested_at"] = ingested_at
            if not self._insert(self.network_table, [net_row], net_cols):
                ok = False
                continue

            nodes = self._fetch_nodes(network, net_row["account_count"])
            if not nodes and net_row["account_count"] > 0:
                logger.error(
                    f"Blokli {network}: accountCount={net_row['account_count']} but no nodes fetched",
                    extra={"event": "blokli_nodes_empty", "ingestor": "hopr-blokli"},
                )
                ok = False
                continue
            for r in nodes:
                r["snapshot_date"] = snapshot_date
                r["ingested_at"] = ingested_at
            if not self._insert(self.nodes_table, nodes, node_cols):
                ok = False
        return ok
