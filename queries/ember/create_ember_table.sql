-- Ember monthly electricity data, "generation" release: one row per
-- Area x month x `Electricity source`, metrics as columns (wide format).
-- Every run reloads the whole file; ReplacingMergeTree(ingested_at) keeps the newest
-- copy of each (Area, Electricity source, Date), so Ember's revisions of past months
-- replace the old values. Consumer: dbt-cerebro stg_crawlers_data__ember_generation_monthly.
--
-- Replaced crawlers_data.ember_electricity_data in 2026-11: Ember retired the
-- long-format file on 2026-11-02 (the old URL now serves a two-column notice CSV),
-- and that table stays frozen at its last load (data to 2026-06).
-- No PARTITION BY on purpose: one full-file load spans ~335 months, and an INSERT
-- touching more than 100 partitions fails (Code 252).
CREATE TABLE IF NOT EXISTS crawlers_data.ember_generation_monthly
(
    `Area`                                      String,
    `ISO 3 code`                                String,
    `Date`                                      Date,
    `Area type`                                 LowCardinality(String),
    `Electricity source`                        LowCardinality(String),
    `Is aggregated source`                      Bool,
    `Generation (TWh)`                          Nullable(Float64),
    `Generation YoY change (TWh)`               Nullable(Float64),
    `Generation YoY change (%)`                 Nullable(Float64),
    `Share of generation (%)`                   Nullable(Float64),
    `Share of generation YoY change (% points)` Nullable(Float64),
    `Emissions (MtCO2e)`                        Nullable(Float64),
    `Emissions YoY change (MtCO2e)`             Nullable(Float64),
    `Emissions YoY change (%)`                  Nullable(Float64),
    `Share of emissions (%)`                    Nullable(Float64),
    `Emissions intensity (gCO2e/kWh)`           Nullable(Float64),
    `Continent`                                 LowCardinality(String),
    `Ember region`                              LowCardinality(String),
    `EU member`                                 Bool,
    `OECD member`                               Bool,
    `G20 member`                                Bool,
    `G7 member`                                 Bool,
    `ASEAN member`                              Bool,
    `ingested_at`                               DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (`Area`, `Electricity source`, `Date`);
