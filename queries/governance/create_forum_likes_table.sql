-- Per-like edges from the Discourse like graph: one row per (post, liker).
--
-- Why this table exists. forum_posts.like_count is a SCALAR captured at fetch time
-- (derived from actions_summary[id==2].count), and daily mode only re-fetches a topic
-- whose bumped_at beat the watermark -- so a dormant thread's like_count is frozen at
-- its last activity and carries no timestamp at all. That makes it impossible to say
-- when a like arrived, or who gave it. This table fixes both: /user_actions.json
-- returns one row per like with its own created_at, so the whole history is
-- recoverable rather than needing a going-forward daily snapshot.
--
-- Grain and dedup. ORDER BY (post_id, acting_user_id) -- a person can like a given post
-- at most once, so that pair is the natural key and ReplacingMergeTree collapses
-- re-ingests of the same edge. An unlike-then-relike replaces the row with the newer
-- created_at, which is the desired behaviour. Note this means an UNLIKE is invisible:
-- the row simply stops being re-observed but is never deleted, same limitation the
-- other governance tables have.
--
-- acting_username is the username the crawl QUERIED (authoritative), not the payload's
-- own copy. The payload also carries target_user_id/target_username, but those were
-- verified to echo the acting user rather than the post's author, so they are NOT
-- stored -- resolve the post author by joining forum_posts on post_id.
CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DATABASE}}.forum_likes (
    post_id          UInt64,
    topic_id         UInt32,
    post_number      UInt32,
    acting_user_id   Int32,
    acting_username  String,
    created_at       DateTime,
    hidden           UInt8,
    deleted          UInt8,
    raw_json         String,
    ingested_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (post_id, acting_user_id)
SETTINGS index_granularity = 8192
