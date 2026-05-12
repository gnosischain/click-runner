CREATE TABLE IF NOT EXISTS crawlers_data.circles_blacklisted (
  address String,
  reason Nullable(String),
  ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY address;
