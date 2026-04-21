"""
Prometheus metrics, health endpoint, and structured logging helpers for
click-runner batch ingestors.
"""
import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Iterator

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)


DURATION_BUCKETS = (
    0.1,
    0.25,
    0.5,
    1,
    2.5,
    5,
    10,
    30,
    60,
    120,
    300,
    900,
    1800,
    3600,
)


runs_started_total = Counter(
    "click_runner_runs_started_total",
    "Total click-runner runs started.",
    ["job", "ingestor"],
)

runs_completed_total = Counter(
    "click_runner_runs_completed_total",
    "Total click-runner runs completed.",
    ["job", "ingestor", "result"],
)

run_duration_seconds = Histogram(
    "click_runner_run_duration_seconds",
    "Total duration of a click-runner run.",
    ["job", "ingestor"],
    buckets=DURATION_BUCKETS,
)

active_runs = Gauge(
    "click_runner_active_runs",
    "Currently active click-runner runs.",
    ["job", "ingestor"],
)

clickhouse_connected = Gauge(
    "click_runner_clickhouse_connected",
    "ClickHouse connection state for the current run.",
    ["job", "database"],
)

clickhouse_connect_duration_seconds = Histogram(
    "click_runner_clickhouse_connect_duration_seconds",
    "ClickHouse connection duration.",
    ["job", "database"],
    buckets=DURATION_BUCKETS,
)

operations_total = Counter(
    "click_runner_operations_total",
    "Total operations by ingestor, operation, and result.",
    ["job", "ingestor", "operation", "result"],
)

operation_duration_seconds = Histogram(
    "click_runner_operation_duration_seconds",
    "Operation duration by ingestor and operation.",
    ["job", "ingestor", "operation"],
    buckets=DURATION_BUCKETS,
)

rows_inserted_total = Counter(
    "click_runner_rows_inserted_total",
    "Rows inserted into ClickHouse.",
    ["job", "ingestor", "table"],
)

s3_files_selected_total = Counter(
    "click_runner_s3_files_selected_total",
    "S3 files selected for ingestion.",
    ["job", "mode", "table"],
)

mixpanel_events_inserted_total = Counter(
    "click_runner_mixpanel_events_inserted_total",
    "Mixpanel events inserted into ClickHouse.",
    ["job", "table"],
)

dune_queries_triggered_total = Counter(
    "click_runner_dune_queries_triggered_total",
    "Dune execute-only queries triggered.",
    ["job", "result"],
)

cow_owners_total = Counter(
    "click_runner_cow_owners_total",
    "CoW owners observed by result.",
    ["job", "mode", "result"],
)

cow_api_requests_total = Counter(
    "click_runner_cow_api_requests_total",
    "CoW API requests by status code and result.",
    ["job", "status_code", "result"],
)

cow_trades_fetched_total = Counter(
    "click_runner_cow_trades_fetched_total",
    "CoW trades fetched from the API.",
    ["job", "mode"],
)

cow_trades_inserted_total = Counter(
    "click_runner_cow_trades_inserted_total",
    "CoW trades inserted into ClickHouse.",
    ["job", "mode", "table"],
)

cow_owner_duration_seconds = Histogram(
    "click_runner_cow_owner_duration_seconds",
    "Time spent processing a single CoW owner.",
    ["job", "mode"],
    buckets=DURATION_BUCKETS,
)


_health_state: Dict[str, Any] = {
    "status": "starting",
    "clickhouse_connected": False,
    "ingestor": "",
    "job_name": "",
    "database": "",
    "table_name": "",
    "started_at": "",
    "finished_at": "",
    "last_error": "",
}
_health_lock = threading.Lock()
_server_started = False


def is_enabled() -> bool:
    return os.getenv("OBSERVABILITY_ENABLED", "").lower() in {"1", "true", "yes"}


def get_job_name() -> str:
    return os.getenv("CLICK_RUNNER_JOB_NAME") or os.getenv("HOSTNAME") or "local"


def utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def update_health(**kwargs: Any) -> None:
    with _health_lock:
        _health_state.update(kwargs)


def get_health() -> Dict[str, Any]:
    with _health_lock:
        return dict(_health_state)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/metrics":
            output = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(output)
            return

        if self.path == "/health":
            health = get_health()
            status_code = 200 if health.get("clickhouse_connected") else 503
            body = json.dumps(health).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, *_: Any) -> None:
        pass


def start_metrics_server(port: int = 9090) -> None:
    global _server_started

    if _server_started:
        return

    server = HTTPServer(("0.0.0.0", port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _server_started = True
    logging.getLogger("clickhouse_runner").info("Metrics server started on port %s", port)


@contextmanager
def time_operation(job: str, ingestor: str, operation: str) -> Iterator[None]:
    start = time.monotonic()
    result = "success"
    try:
        yield
    except Exception:
        result = "failure"
        raise
    finally:
        duration = time.monotonic() - start
        operation_duration_seconds.labels(job, ingestor, operation).observe(duration)
        operations_total.labels(job, ingestor, operation, result).inc()


def observe_rows(ingestor: str, table: str, rows: int) -> None:
    if rows > 0:
        rows_inserted_total.labels(get_job_name(), ingestor, table or "unknown").inc(rows)


class JsonFormatter(logging.Formatter):
    RESERVED_ATTRS = {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        data: Dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "job": get_job_name(),
        }

        for key, value in record.__dict__.items():
            if key not in self.RESERVED_ATTRS and not key.startswith("_"):
                data[key] = value

        if record.exc_info:
            data["exception"] = self.formatException(record.exc_info)

        return json.dumps(data, default=str)


def setup_logging(logger_names: tuple[str, ...] = ("clickhouse_runner", "click_runner")) -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler()
    if os.getenv("LOG_FORMAT", "").lower() == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    for logger_name in logger_names:
        named_logger = logging.getLogger(logger_name)
        named_logger.handlers.clear()
        named_logger.setLevel(level)
        named_logger.propagate = True


def log_event(logger_instance: logging.Logger, event: str, **fields: Any) -> None:
    message = fields.pop("message", event)
    logger_instance.info(message, extra={"event": event, **fields})
