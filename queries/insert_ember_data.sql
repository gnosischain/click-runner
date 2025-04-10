INSERT INTO crawlers_data.ember_electricity_data 
(
    `Area`,
    `Country code`,
    `Date`,
    `Area type`,
    `Continent`,
    `Ember region`,
    `EU`,
    `OECD`,
    `G20`,
    `G7`,
    `ASEAN`,
    `Category`,
    `Subcategory`,
    `Variable`,
    `Unit`,
    `Value`,
    `YoY absolute change`,
    `YoY % change`
)
SELECT
    Area,
    `Country code`,
    parseDateTimeBestEffortOrNull(CAST(Date AS String)) AS Date,
    `Area type`,
    Continent,
    `Ember region`,
    EU,
    OECD,
    G20,
    G7,
    ASEAN,
    Category,
    Subcategory,
    Variable,
    Unit,
    Value,
    `YoY absolute change`,
    `YoY % change`
FROM url(
    'https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/monthly_full_release_long_format.csv',
    'CSV'
);
