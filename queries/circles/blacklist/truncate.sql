-- Wipe the previous day's snapshot before re-inserting the current state.
-- See insert.sql for context on why we full-replace.
TRUNCATE TABLE crawlers_data.circles_blacklisted
