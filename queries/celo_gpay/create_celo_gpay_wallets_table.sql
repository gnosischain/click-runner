CREATE TABLE IF NOT EXISTS crawlers_data.celo_gpay_wallets
(
    `safe_address`    String,
    `owner_address`   Nullable(String),
    `owners`          Array(String),
    `issued_at`       Date,
    `first_spend_at`  Nullable(Date),
    `ingested_at`     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY safe_address;
