CREATE TABLE IF NOT EXISTS {{COW_DATABASE}}.cow_api_trade_fees
(
    `order_uid`        String,
    `tx_hash`          String,
    `block_number`     UInt64,
    `log_index`        UInt64,
    `owner`            String,
    `sell_token`       String,
    `buy_token`        String,
    `sell_amount`      String,
    `buy_amount`       String,
    `fee_token`        String,
    `fee_amount`       String,
    `fee_policies`     String,
    `ingested_at`      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (order_uid)
SETTINGS allow_nullable_key = 1;
