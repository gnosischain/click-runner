#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import requests
import yaml

DUNE_BASE = "https://api.dune.com/api/v1"


def dune_key() -> str:
    key = os.environ.get("CH_QUERY_VAR_DUNE_API_KEY") or os.environ.get("DUNE_API_KEY")
    if not key:
        print("Missing Dune API key. Set CH_QUERY_VAR_DUNE_API_KEY (preferred) or DUNE_API_KEY.", file=sys.stderr)
        sys.exit(2)
    return key


def load_config(dataset_dir: Path) -> dict:
    cfg_path = dataset_dir / "config.yml"
    if not cfg_path.exists():
        print(f"Missing config.yml in {dataset_dir}", file=sys.stderr)
        sys.exit(2)
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def dune_execute(query_id: str, params_dict: dict) -> str:
    """
    Dune v1 execute expects:
      POST /query/{query_id}/execute
      body: {"query_parameters": { "<name>": "<value>", ... }}
    """
    payload = {"query_parameters": params_dict}
    headers = {
        "X-Dune-Api-Key": dune_key(),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    resp = requests.post(
        f"{DUNE_BASE}/query/{query_id}/execute",
        headers=headers,
        data=json.dumps(payload),
        timeout=60,
    )
    if resp.status_code >= 400:
        print(f"[dune] execute error {resp.status_code}: {resp.text}", file=sys.stderr)
        resp.raise_for_status()
    data = resp.json()
    exec_id = data.get("execution_id") or data.get("executionId")
    if not exec_id:
        print(f"[dune] unexpected execute response: {data}", file=sys.stderr)
        sys.exit(2)
    print(f"[dune] execution_id: {exec_id} (query_id: {query_id})")
    return exec_id


def dune_wait(exec_id: str, timeout_s: int = 900, poll_s: float = 2.0):
    """
    Poll execution status until completed/failed/timeout.
    """
    headers = {"X-Dune-Api-Key": dune_key(), "Accept": "application/json"}
    deadline = time.time() + timeout_s
    last_log = 0.0
    while time.time() < deadline:
        resp = requests.get(f"{DUNE_BASE}/execution/{exec_id}/status", headers=headers, timeout=30)
        if resp.status_code >= 400:
            print(f"[dune] status error {resp.status_code}: {resp.text}", file=sys.stderr)
            resp.raise_for_status()
        js = resp.json()
        state_raw = js.get("state") or ""
        st = state_raw.lower()
        if time.time() - last_log >= 10:
            print(f"[dune] state={st} progress={js.get('progress')} message={js.get('message')}")
            last_log = time.time()
        if st in ("completed", "success", "succeeded", "finished", "done", "query_state_completed"):
            print("[dune] execution completed")
            return
        if st in ("failed", "error", "cancelled", "query_state_failed"):
            print(f"[dune] execution failed: {js}", file=sys.stderr)
            # Try to fetch error details
            try:
                rr = requests.get(f"{DUNE_BASE}/execution/{exec_id}/results", headers=headers, timeout=30)
                print(f"[dune] results body on failure: {rr.status_code} {rr.text}", file=sys.stderr)
            except Exception as e:
                print(f"[dune] error fetching failure details: {e}", file=sys.stderr)
            sys.exit(1)
        time.sleep(poll_s)
    print(f"[dune] timed out waiting for execution_id={exec_id}", file=sys.stderr)
    print(f"[dune] check: GET {DUNE_BASE}/execution/{exec_id}/status", file=sys.stderr)
    sys.exit(3)


def main():
    ap = argparse.ArgumentParser(
        description="Run Dune param query from dataset config and ingest by execution_id"
    )
    ap.add_argument("--dataset-dir", required=True, help="e.g., queries/dune/prices or queries/dune/labels")
    ap.add_argument("--start", required=True, help="start value exactly as your Dune SQL expects")
    ap.add_argument("--end", required=True, help="end value exactly as your Dune SQL expects")
    ap.add_argument("--create-table-sql", help="defaults to <dataset-dir>/create_table.sql")
    ap.add_argument("--insert-sql", help="defaults to <dataset-dir>/insert_from_execution.sql")
    ap.add_argument("--timeout-seconds", type=int, default=900)
    ap.add_argument("--poll-seconds", type=float, default=2.0)
    # Optional overrides:
    ap.add_argument("--query-id", help="override dune_query_id_param from config.yml")
    ap.add_argument("--param-start-key", help="override param_start_key from config.yml")
    ap.add_argument("--param-end-key", help="override param_end_key from config.yml")
    args = ap.parse_args()

    dataset_dir = Path(args.dataset_dir).resolve()
    cfg = load_config(dataset_dir)

    dune_query_id_param = args.query_id or cfg.get("dune_query_id_param")
    if not dune_query_id_param:
        print("Missing dune_query_id_param (set in config.yml or pass --query-id).", file=sys.stderr)
        sys.exit(2)

    start_key = args.param_start_key or cfg.get("param_start_key") or "start_date"
    end_key = args.param_end_key or cfg.get("param_end_key") or "end_date"

    # Send values AS-IS so they match your Dune SQL CASTs
    params_dict = {start_key: args.start, end_key: args.end}
    print(f"[dune] executing query_id={dune_query_id_param} with params={params_dict}")

    exec_id = dune_execute(dune_query_id_param, params_dict)
    dune_wait(exec_id, timeout_s=args.timeout_seconds, poll_s=args.poll_seconds)

    # Prepare env for click-runner substitution
    env = dict(os.environ)
    env["CH_QUERY_VAR_DUNE_EXECUTION_ID"] = exec_id

    create_sql = args.create_table_sql or str(dataset_dir / "create_table.sql")
    insert_sql = args.insert_sql or str(dataset_dir / "insert_from_execution.sql")

    cmd = [
        sys.executable,
        "run_queries.py",
        "--ingestor=csv",
        "--create-table-sql",
        create_sql,
        "--insert-sql",
        insert_sql,
        "--skip-table-creation",
    ]
    print(f"[runner] ingesting with execution_id={exec_id}")
    subprocess.check_call(cmd, env=env)


if __name__ == "__main__":
    main()
