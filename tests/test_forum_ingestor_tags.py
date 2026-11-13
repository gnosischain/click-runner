"""Unit test for ForumIngestor._topic_row tag handling.

Discourse switched topic tags from strings to {"id", "name", "slug"} objects on
2026-10-27, and the join crashed every daily run. Run with `python -m unittest`.
"""
import unittest

from ingestors.forum_ingestor import ForumIngestor

TAGS_COLUMN = 9  # position of "tags" in _ingest_topic's column list


class TopicRowTags(unittest.TestCase):
    def tags(self, tags):
        # _topic_row reads no instance state, so no client is needed.
        return ForumIngestor._topic_row(None, {"id": 1, "tags": tags})[TAGS_COLUMN]

    def test_object_tags_use_name(self):
        self.assertEqual(
            self.tags([{"id": 8, "name": "phase-1", "slug": "phase-1"},
                       {"id": 9, "name": "gip", "slug": "gip"}]),
            "phase-1,gip",
        )

    def test_string_tags_still_work(self):
        self.assertEqual(self.tags(["phase-1", "gip"]), "phase-1,gip")

    def test_no_tags(self):
        self.assertEqual(self.tags(None), "")


if __name__ == "__main__":
    unittest.main()
