-- Full reload of Ember's monthly generation file. Columns are selected BY NAME from
-- the CSV header: if Ember renames or drops a column, or retires the file again,
-- the statement fails with Code 47 and writes nothing.
-- Types are inferred from the file, so every column is converted explicitly:
-- metrics may be empty (-> NULL), membership flags are written True/False.
-- now() is evaluated once per statement, so one load shares a single ingested_at.
INSERT INTO crawlers_data.ember_generation_monthly
(
    `Area`,
    `ISO 3 code`,
    `Date`,
    `Area type`,
    `Electricity source`,
    `Is aggregated source`,
    `Generation (TWh)`,
    `Generation YoY change (TWh)`,
    `Generation YoY change (%)`,
    `Share of generation (%)`,
    `Share of generation YoY change (% points)`,
    `Emissions (MtCO2e)`,
    `Emissions YoY change (MtCO2e)`,
    `Emissions YoY change (%)`,
    `Share of emissions (%)`,
    `Emissions intensity (gCO2e/kWh)`,
    `Continent`,
    `Ember region`,
    `EU member`,
    `OECD member`,
    `G20 member`,
    `G7 member`,
    `ASEAN member`,
    `ingested_at`
)
SELECT
    `Area`,
    ifNull(`ISO 3 code`, ''),
    toDate(`Date`),
    `Area type`,
    `Electricity source`,
    lower(toString(`Is aggregated source`)) = 'true',
    accurateCastOrNull(`Generation (TWh)`, 'Float64'),
    accurateCastOrNull(`Generation YoY change (TWh)`, 'Float64'),
    accurateCastOrNull(`Generation YoY change (%)`, 'Float64'),
    accurateCastOrNull(`Share of generation (%)`, 'Float64'),
    accurateCastOrNull(`Share of generation YoY change (% points)`, 'Float64'),
    accurateCastOrNull(`Emissions (MtCO2e)`, 'Float64'),
    accurateCastOrNull(`Emissions YoY change (MtCO2e)`, 'Float64'),
    accurateCastOrNull(`Emissions YoY change (%)`, 'Float64'),
    accurateCastOrNull(`Share of emissions (%)`, 'Float64'),
    accurateCastOrNull(`Emissions intensity (gCO2e/kWh)`, 'Float64'),
    ifNull(`Continent`, ''),
    ifNull(`Ember region`, ''),
    lower(toString(`EU member`)) = 'true',
    lower(toString(`OECD member`)) = 'true',
    lower(toString(`G20 member`)) = 'true',
    lower(toString(`G7 member`)) = 'true',
    lower(toString(`ASEAN member`)) = 'true',
    now()
FROM url(
    '{{EMBER_DATA_URL}}',
    'CSVWithNames'
);
