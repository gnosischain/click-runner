# forum-watermark-skips-crashed-topics

**Status:** observed (the 2026-10-27 crash is fixed in tree, the re-ingest is pending, and daily mode still ingests topics newest-first)

**Symptom:** after forum runs that crashed part-way, a thread's new posts never arrive,
even once the daily runs are green again. The topic stays in `forum_topics` with an old
`bumped_at` and post count.

**Cause:** daily mode re-fetches only topics with `bumped_at` above `max(bumped_at)` of
`forum_topics` (ingestors/forum_ingestor.py:236,366-379). Topics are ingested in
`/latest.json` order, roughly newest-bumped first, and each topic row is inserted before
the next topic is fetched. When a run crashes on topic N, the topics ingested before it
have already raised the watermark. A topic bumped earlier that the crash skipped is now
below the watermark, and daily mode never selects it again unless it is bumped again.

**Rule:** after any forum run that failed part-way, do not trust daily mode to catch up.
Re-ingest with `--forum-mode=backfill --forum-max-pages=<n>`, where page n is the last
`/latest.json` page (30 topics each) holding a topic bumped since the first failed run.
The tables are ReplacingMergeTree, so a re-fetch is idempotent. The structural fix, not
made: ingest the selected topics oldest-bumped first, so a crash leaves the watermark
below everything it skipped.

**Evidence:** Discourse started returning topic tags as objects on 2026-10-27, and
`_topic_row`'s `",".join(tags)` raised a TypeError on the first tagged topic of every
run, 7 attempts a day, through 2026-11-13, with no alert. Untagged topics ingested
before the crash moved the watermark to 2026-11-01 20:43:50 (topic 12493). Tagged topic
12435, bumped 2026-11-01 20:27:19 with 19 posts, stayed stored at its 2026-10-19
version. Tagged topics 12444 and 12503, bumped later, are above the watermark and need
no re-ingest. Found in the analytics morning checks of 2026-10-30 and 2026-11-13.
