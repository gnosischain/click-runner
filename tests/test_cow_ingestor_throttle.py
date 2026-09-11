"""Unit tests for CowIngestor._api_get throttle handling.

Runs without network and without curl_cffi installed: a stub module is injected
before ingestors.cow_ingestor is imported. Run with `python -m unittest`.
"""
import sys
import types
import unittest
from unittest import mock

# --- stub curl_cffi.requests ------------------------------------------------
_calls = []
_responses = []


class _RequestsError(Exception):
    pass


class _Response:
    def __init__(self, status_code, headers=None, text=""):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = text

    def raise_for_status(self):
        if self.status_code >= 400:
            raise _RequestsError(f"HTTP Error {self.status_code}")

    def json(self):
        return []


def _fake_get(url, headers=None, impersonate=None, timeout=None):
    _calls.append(url)
    nxt = _responses.pop(0)
    if isinstance(nxt, Exception):
        raise nxt
    return nxt


_curl = types.ModuleType("curl_cffi")
_curl_requests = types.ModuleType("curl_cffi.requests")
_curl_requests.get = _fake_get
_curl_requests.RequestsError = _RequestsError
_curl_requests.Response = _Response
_curl.requests = _curl_requests
sys.modules.setdefault("curl_cffi", _curl)
sys.modules.setdefault("curl_cffi.requests", _curl_requests)

from ingestors import cow_ingestor  # noqa: E402

CF_BODY = '<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"><TITLE>ERROR: The request could not be satisfied</TITLE>'


def _ingestor(**kw):
    return cow_ingestor.CowIngestor(
        client=mock.MagicMock(),
        variables={},
        create_table_sql="queries/cow/create_table.sql",
        table_name="crawlers_data.cow_api_trade_fees",
        source_table="dbt.int_execution_cow_trades",
        api_key="k",
        **kw,
    )


class ApiGetThrottleTests(unittest.TestCase):
    def setUp(self):
        _calls.clear()
        _responses.clear()
        self.sleeps = []
        patcher = mock.patch.object(cow_ingestor.time, "sleep", lambda s: self.sleeps.append(s))
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_403_is_retried_with_backoff_then_served(self):
        _responses.extend([
            _Response(403, {"x-amz-cf-id": "abc", "x-cache": "Error from cloudfront"}, CF_BODY),
            _Response(200),
        ])
        ing = _ingestor()
        with self.assertLogs("clickhouse_runner", level="WARNING") as logs:
            resp = ing._api_get("https://api.cow.fi/xdai/api/v2/trades?owner=0x1")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(_calls), 2)
        self.assertEqual(self.sleeps, [10])
        self.assertEqual(ing.throttle_exhausted, 0)
        rec = logs.records[0]
        self.assertEqual(rec.event, "cow_api_throttled")
        self.assertEqual(rec.status_code, 403)
        self.assertEqual(rec.x_amz_cf_id, "abc")
        self.assertIn("could not be satisfied", rec.body)

    def test_retry_after_header_is_honoured(self):
        _responses.extend([_Response(429, {"retry-after": "7"}), _Response(200)])
        ing = _ingestor()
        with self.assertLogs("clickhouse_runner", level="WARNING"):
            ing._api_get("u")
        self.assertEqual(self.sleeps, [7])

    def test_persistent_403_gives_up_and_counts(self):
        _responses.extend([_Response(403, {}, CF_BODY)] * 4)
        ing = _ingestor()
        with self.assertLogs("clickhouse_runner", level="WARNING") as logs:
            resp = ing._api_get("u")
        self.assertIsNone(resp)
        self.assertEqual(len(_calls), 4)  # 1 + MAX_THROTTLE_RETRIES
        self.assertEqual(self.sleeps, [10, 30, 60])
        self.assertEqual(ing.throttle_exhausted, 1)
        self.assertEqual(logs.records[-1].event, "cow_api_throttle_exhausted")

    def test_none_from_api_get_marks_owner_failed(self):
        _responses.extend([_Response(403, {}, CF_BODY)] * 4)
        ing = _ingestor()
        with self.assertLogs("clickhouse_runner", level="WARNING"):
            trades, api_failed = ing._fetch_trades_for_owner("0x1", None)
        self.assertEqual(trades, [])
        self.assertTrue(api_failed)

    def test_other_4xx_marks_owner_failed_with_diagnostics(self):
        _responses.append(_Response(500, {"x-amz-cf-id": "zzz"}, "boom"))
        ing = _ingestor()
        with self.assertLogs("clickhouse_runner", level="ERROR") as logs:
            trades, api_failed = ing._fetch_trades_for_owner("0x1", None)
        self.assertTrue(api_failed)
        self.assertEqual(logs.records[0].event, "cow_owner_api_error")
        self.assertEqual(logs.records[0].status_code, 500)
        self.assertEqual(logs.records[0].body, "boom")

    def test_request_delay_override_and_default(self):
        self.assertEqual(_ingestor().rate_limit_delay, cow_ingestor.AUTH_RATE_LIMIT_DELAY)
        self.assertEqual(_ingestor(request_delay=0.25).rate_limit_delay, 0.25)

    def test_window_budget_blocks_when_exhausted(self):
        ing = _ingestor()
        base = 1000.0
        clock = {"t": base}
        with mock.patch.object(cow_ingestor.time, "monotonic", lambda: clock["t"]):
            for _ in range(cow_ingestor.MAX_REQUESTS_PER_WINDOW):
                _responses.append(_Response(200))
                ing._api_get("u")
            self.assertEqual(self.sleeps, [])
            # One more inside the window must wait for the oldest to expire.
            clock["t"] = base + 100
            _responses.append(_Response(200))
            ing._api_get("u")
        self.assertEqual(len(self.sleeps), 1)
        self.assertAlmostEqual(self.sleeps[0], cow_ingestor.REQUEST_WINDOW_SECONDS - 100, places=3)
        self.assertEqual(ing.requests_made, cow_ingestor.MAX_REQUESTS_PER_WINDOW + 1)


if __name__ == "__main__":
    unittest.main()
