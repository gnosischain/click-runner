INSERT INTO crawlers_data.ember_electricity_data
(
    `Area`,
    `ISO 3 code`, 
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
    `YoY % change`,
    `version`
)
SELECT
    Area,
    `ISO 3 code`, 
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
    `YoY % change`,
    now() AS version 
FROM url(
    '{{EMBER_DATA_URL}}',
    'CSV'
);