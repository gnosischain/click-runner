# Mixpanel Data Ingestion - Access Control Setup

This data contains sensitive user behavioral analytics. Access should be restricted.

## ClickHouse RBAC Setup

Run these commands as a ClickHouse admin to set up access control:

```sql
-- 1. Create the dedicated database
CREATE DATABASE IF NOT EXISTS mixpanel;

-- 2. Create a writer user (used by the ingestor service)
CREATE USER IF NOT EXISTS mixpanel_writer
  IDENTIFIED BY '<strong-password>'
  DEFAULT DATABASE mixpanel;

GRANT CREATE TABLE, INSERT, SELECT ON mixpanel.* TO mixpanel_writer;

-- 3. Create a reader role for analysts
CREATE ROLE IF NOT EXISTS mixpanel_reader;
GRANT SELECT ON mixpanel.* TO mixpanel_reader;

-- 4. Grant the reader role to specific users
-- GRANT mixpanel_reader TO analyst_username;
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `MIXPANEL_PROJECT_ID` | Yes | Mixpanel project ID |
| `MIXPANEL_SA_USERNAME` | Yes | Service account username |
| `MIXPANEL_SA_SECRET` | Yes | Service account secret (shown once at creation) |
| `MIXPANEL_DATABASE` | No | ClickHouse database (default: `mixpanel`) |
| `MIXPANEL_CH_USER` | No | Dedicated CH user (falls back to `CH_USER`) |
| `MIXPANEL_CH_PASSWORD` | No | Dedicated CH password (falls back to `CH_PASSWORD`) |
| `MIXPANEL_BACKFILL_FROM` | Backfill only | Start date (YYYY-MM-DD) |
| `MIXPANEL_BACKFILL_TO` | Backfill only | End date (YYYY-MM-DD) |

## Usage

```bash
# Daily ingestion (auto-resumes from last completed date)
docker compose run --rm mixpanel-events-daily-ingestor

# Historical backfill (set MIXPANEL_BACKFILL_FROM and MIXPANEL_BACKFILL_TO in .env)
docker compose run --rm mixpanel-events-backfill-ingestor
```

## Rate Limits

The Mixpanel Export API allows 60 requests/hour and 3 requests/second.
Backfilling processes ~60 days/hour for normal-volume days.
The state table tracks completed dates, so backfills can be safely interrupted and resumed.

## Querying the Data

Events are stored with dynamic properties as a JSON string:

```sql
SELECT
    event_name,
    JSONExtractString(properties, '$city') AS city,
    JSONExtractString(properties, '$browser') AS browser,
    count() AS cnt
FROM mixpanel.mixpanel_raw_events FINAL
WHERE event_time >= '2025-01-01'
GROUP BY event_name, city, browser
ORDER BY cnt DESC
```
