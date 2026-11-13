"""Unit tests for ForumIngestor._ingest_users: two directory orders, page cap, dedup.

No network and no warehouse: _get is stubbed and the client records inserts.
Run with `python -m unittest`.
"""
import unittest

from ingestors.forum_ingestor import DIRECTORY_LAST_PAGE, ForumIngestor


def item(uid, likes_given):
    return {"user": {"id": uid, "username": f"u{uid}"}, "likes_given": likes_given}


class FakeClient:
    def __init__(self):
        self.rows = []

    def insert(self, table, rows, column_names):
        self.rows.extend(rows)


class IngestUsers(unittest.TestCase):
    def run_users(self, pages_by_order, max_pages=400):
        client = FakeClient()
        ing = ForumIngestor(client, {}, "db", "https://forum.example", "x", "x", "x", "x",
                            max_pages=max_pages)
        calls = []

        def fake_get(path, params=None):
            calls.append((params["order"], params["page"]))
            pages = pages_by_order[params["order"]]
            items = pages[params["page"]] if params["page"] < len(pages) else []
            return {"directory_items": items, "meta": {"total_rows_directory_items": 2773}}

        ing._get = fake_get
        return ing._ingest_users(), client.rows, calls

    def test_walks_both_orders_and_dedups_users(self):
        seen, rows, calls = self.run_users({
            "post_count": [[item(1, 0), item(2, 5)]],
            "likes_given": [[item(2, 5), item(3, 9)]],
        })
        self.assertEqual(sorted(seen), [("u1", 0), ("u2", 5), ("u3", 9)])
        self.assertEqual(sorted(r[0] for r in rows), [1, 2, 3])
        self.assertEqual({order for order, _ in calls}, {"post_count", "likes_given"})

    def test_never_requests_past_the_anonymous_page_cap(self):
        full = [[item(p * 50 + i, 1) for i in range(50)] for p in range(15)]
        _, rows, calls = self.run_users({"post_count": full, "likes_given": full})
        self.assertEqual(max(page for _, page in calls), DIRECTORY_LAST_PAGE)
        self.assertEqual(len(rows), (DIRECTORY_LAST_PAGE + 1) * 50)

    def test_max_pages_still_bounds_the_walk(self):
        full = [[item(p * 50 + i, 1) for i in range(50)] for p in range(15)]
        _, _, calls = self.run_users({"post_count": full, "likes_given": full}, max_pages=0)
        self.assertEqual(calls, [("post_count", 0), ("likes_given", 0)])


if __name__ == "__main__":
    unittest.main()
