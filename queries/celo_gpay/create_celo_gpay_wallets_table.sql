CREATE TABLE IF NOT EXISTS {{CELO_GPAY_DB}}.celo_gpay_wallets
(
    `safe_address`    String,
    `owner_address`   Nullable(String),
    `owners`          Array(String),
    `issued_at`       Date,
    `first_spend_at`  Nullable(Date),
    `is_activated`    UInt8,
    `ingested_at`     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY safe_address;
