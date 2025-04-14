CREATE TABLE IF NOT EXISTS crawlers_data.ember_electricity_data
(
    `Area`                String,
    `Country code`        String,
    `Date`                Date,
    `Area type`           String,
    `Continent`           String,
    `Ember region`        String,
    `EU`                  Float64,
    `OECD`                Float64,
    `G20`                 Float64,
    `G7`                  Float64,
    `ASEAN`               Float64,
    `Category`            String,
    `Subcategory`         String,
    `Variable`            String,
    `Unit`                String,
    `Value`               Float64,
    `YoY absolute change` Float64,
    `YoY % change`        Float64
)
ENGINE = MergeTree
ORDER BY (Date, Area, Category, Subcategory, Variable);