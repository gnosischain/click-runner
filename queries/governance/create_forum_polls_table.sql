-- Discourse poll options, one row per (post, poll, option) with its vote count.
--
-- Why this table exists. 189 polls sit across 180 topics going back to 2020-11-02, 152 of
-- them in GIP topics and 75 carrying an "In Favour" / "Against" temperature check (73 spell
-- it "In Favour", 2 "In Favor" -- match on both).
-- That is the only place in the whole governance corpus where one-person-one-vote
-- community sentiment is measured on the same question that later gets a token-weighted
-- Snapshot vote, so it is the missing middle of the discussion -> signal -> vote funnel.
-- The counts ride along inside the post payload of /t/{id}.json, which the ingestor
-- already fetches, so this costs no extra HTTP request per topic.
--
-- Grain and dedup. ORDER BY (post_id, poll_name, option_id). A single post can host
-- SEVERAL named polls ("poll", "poll2"), so poll_name is part of the key. option_id is
-- Discourse's md5 of the option TEXT, which means it is NOT globally unique -- the same
-- "In Favour" option id recurs across unrelated topics -- so it only identifies an option
-- WITHIN a poll and must never be joined on alone. Editing an option's wording changes
-- its md5, which lands as a new row and leaves the old wording behind; that is the same
-- limitation the other governance tables have (a row stops being re-observed rather than
-- being deleted).
--
-- Current state, not history. Like every other raw governance table this is
-- ReplacingMergeTree collapsing to the latest observation. An open poll's count is a
-- moving scalar with no per-event endpoint, so a TIMELINE of it can only come from
-- snapshotting -- that belongs in dbt alongside int_governance_engagement_counters_daily,
-- which exists for exactly this class of metric. What this table guarantees is that the
-- latest observation is actually current: the ingestor refreshes open-poll topics
-- unconditionally, because voting in a poll does not bump a topic and the daily
-- bumped_at watermark would otherwise freeze the counts at the topic's last reply.
--
-- ingested_at is FIRST-SEEN, not last-verified. ClickHouse block-level insert dedup
-- (insert_deduplicate = 1) drops a re-inserted identical block, so re-reading a poll whose
-- counts have not moved writes nothing at all and leaves ingested_at where it was. Measured:
-- a daily run refreshing all 99 open-poll topics added 0 rows. That is the behaviour we want
-- -- unchanged polls cost no writes, and a block with a changed count inserts normally and
-- wins the version comparison -- but it means a freshness alarm built on max(ingested_at)
-- will fire falsely on a healthy quiet poll. Use the ingestor's own run status for liveness.
--
-- option_votes = -1 means WITHHELD, never zero. Discourse omits per-option counts while a
-- poll's results policy hides them (results = on_vote / on_close / staff_only); 0 is a
-- real zero. Staging must map -1 to NULL rather than treating it as no votes. The withheld
-- case is rare and it moves as polls open and close (2 polls / 6 option rows when this was
-- written), but a metric that silently reads it as zero understates a live temperature check.
CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DATABASE}}.forum_polls (
    post_id            UInt64,
    topic_id           UInt32,
    poll_id            UInt32,
    poll_name          String,
    poll_type          LowCardinality(String),
    status             LowCardinality(String),
    results_visibility LowCardinality(String),
    is_public          UInt8,
    close_at           DateTime,
    voters             UInt32,
    option_id          String,
    option_html        String,
    option_votes       Int32,
    raw_json           String,
    ingested_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (post_id, poll_name, option_id)
SETTINGS index_granularity = 8192
