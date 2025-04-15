# ClickHouse Runner

A modular toolkit for ClickHouse data ingestion from various sources, particularly useful for running ETL jobs via Docker.

## Overview

`click-runner` is designed for flexible query execution and data ingestion with support for different data formats and sources:

- **CSV Ingestion**: Load data from CSV files using ClickHouse's URL engine
- **Parquet Ingestion**: Load data from Parquet files in S3 buckets
- **SQL Execution**: Run arbitrary SQL files against a ClickHouse database

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/) (for using the provided docker-compose.yml)
- ClickHouse server

## Project Structure

```
.
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── run_queries.py                 # Main CLI entry point 
├── ingestors/                     # Ingestor modules
│   ├── __init__.py
│   ├── base.py                    # Abstract base ingestor
│   ├── csv_ingestor.py            # CSV ingestion (e.g., Ember data)
│   └── parquet_ingestor.py        # Parquet ingestion (e.g., ProbeLab data)
├── utils/                         # Utility modules
│   ├── __init__.py
│   ├── s3.py                      # S3 utilities
│   ├── db.py                      # Database utilities
│   └── date.py                    # Date utilities
└── queries/                       # SQL query files
    ├── ember/                     # Ember electricity data
    │   ├── create_ember_table.sql
    │   ├── insert_ember_data.sql
    │   └── optimize_ember_data.sql
    └── probelab/                  # ProbeLab data
        ├── probelab_agent_semvers_avg_1d.up.sql
        ├── probelab_agent_types_avg_1d.up.sql
        └── ... (other create table queries)
```

## Environment Variables

Set the following environment variables to configure the ClickHouse connection:

- `CH_HOST`: ClickHouse host
- `CH_PORT`: Native ClickHouse port (default: 9000)
- `CH_USER`: Username for authentication
- `CH_PASSWORD`: Password for authentication
- `CH_DB`: Database to use
- `CH_SECURE`: Use TLS connection (`True` or `False`)
- `CH_VERIFY`: Verify TLS certificate (`True` or `False`)

For S3 integration:
- `S3_ACCESS_KEY`: AWS access key ID
- `S3_SECRET_KEY`: AWS secret access key
- `S3_BUCKET`: S3 bucket name (default: prod-use1-gnosis)
- `S3_REGION`: AWS region (default: us-east-1)

For Ember data:
- `EMBER_DATA_URL`: URL to the Ember CSV data

## Running Modes

The system supports three primary running modes, controlled by the `--ingestor` parameter:

### 1. Query Mode (`--ingestor=query`)

Execute arbitrary SQL queries directly against ClickHouse.

**Usage:**
```bash
# CLI
python run_queries.py --ingestor=query --queries=queries/file1.sql,queries/file2.sql

# Docker
docker-compose run click-runner --ingestor=query --queries=queries/file1.sql,queries/file2.sql
```

**Environment variable alternative:**
```
CH_QUERIES=queries/file1.sql,queries/file2.sql
```

**Use cases:**
- Running administrative queries
- Database maintenance
- Schema updates
- Custom data transformations

### 2. CSV Mode (`--ingestor=csv`)

Import data from CSV files using ClickHouse's URL engine. Typically used for Ember electricity data.

**Usage:**
```bash
# CLI
python run_queries.py --ingestor=csv \
  --create-table-sql=queries/ember/create_ember_table.sql \
  --insert-sql=queries/ember/insert_ember_data.sql \
  --optimize-sql=queries/ember/optimize_ember_data.sql

# Docker
docker-compose run ember-ingestor
```

**Use cases:**
- Importing public datasets available as CSV files
- Scheduled updates from static CSV URLs
- When data source is a REST API that returns CSV

### 3. Parquet Mode (`--ingestor=parquet`)

Import data from Parquet files in S3 buckets, with three ingestion strategies:

- **Latest** (`--mode=latest`): Import only the most recent file
- **Date** (`--mode=date`): Import a file for a specific date
- **All** (`--mode=all`): Import all available files

**Usage:**

Latest File:
```bash
python run_queries.py --ingestor=parquet \
  --create-table-sql=queries/probelab/probelab_agent_semvers_avg_1d.up.sql \
  --s3-path=assets/agent_semvers_avg_1d_data/{{DATE}}.parquet \
  --table-name=crawlers_data.probelab_agent_semvers_avg_1d \
  --mode=latest
```

Specific Date:
```bash
python run_queries.py --ingestor=parquet \
  --create-table-sql=queries/probelab/probelab_agent_semvers_avg_1d.up.sql \
  --s3-path=assets/agent_semvers_avg_1d_data/{{DATE}}.parquet \
  --table-name=crawlers_data.probelab_agent_semvers_avg_1d \
  --mode=date \
  --date=2025-04-13
```

All Files:
```bash
python run_queries.py --ingestor=parquet \
  --create-table-sql=queries/probelab/probelab_agent_semvers_avg_1d.up.sql \
  --s3-path=assets/agent_semvers_avg_1d_data/{{DATE}}.parquet \
  --table-name=crawlers_data.probelab_agent_semvers_avg_1d \
  --mode=all
```

Using Docker:
```bash
docker-compose run probelab-agent-semvers-ingestor
```

**Use cases:**
- Daily ingestion of time-series data stored as Parquet
- Backfilling historical Parquet data
- Importing structured data from data lakes

## Common Parameters

All modes share these common parameters:

- `--host`: ClickHouse host (default: from `CH_HOST` env var)
- `--port`: ClickHouse port (default: from `CH_PORT` env var)
- `--user`: ClickHouse user (default: from `CH_USER` env var)
- `--password`: ClickHouse password (default: from `CH_PASSWORD` env var)
- `--db`: ClickHouse database (default: from `CH_DB` env var)
- `--secure`: Use TLS connection (default: from `CH_SECURE` env var)
- `--verify`: Verify TLS certificate (default: from `CH_VERIFY` env var)
- `--skip-table-creation`: Skip table creation steps (optional flag)

## Docker Compose Services

The `docker-compose.yml` file includes several predefined services:

1. **click-runner**: Generic service that can run in any mode
2. **ember-ingestor**: Specialized for Ember CSV data
3. **probelab-agent-semvers-ingestor**: Example for one ProbeLab Parquet dataset

## Setting Up Cron Jobs

To run data ingestion as a daily cron job, you can use the provided Docker containers:

```bash
# Example crontab entry for daily Ember data update at 2 AM
0 2 * * * cd /path/to/click-runner && docker-compose run --rm ember-ingestor

# Example crontab entry for daily ProbeLab data update at 3 AM
0 3 * * * cd /path/to/click-runner && docker-compose run --rm probelab-agent-semvers-ingestor
```

For convenience, you can use the included `cron_setup.sh` script to automatically create these cron jobs:

```bash
chmod +x cron_setup.sh
sudo ./cron_setup.sh
```

## Adding New Data Sources

### 1. Adding a New CSV Data Source

1. Create table definition SQL file: `queries/new_source/create_table.sql`
2. Create insert SQL file: `queries/new_source/insert_data.sql`
3. (Optional) Create optimization SQL file: `queries/new_source/optimize.sql`
4. Add environment variable for data URL: `NEW_SOURCE_URL`
5. Run:
   ```bash
   python run_queries.py --ingestor=csv \
     --create-table-sql=queries/new_source/create_table.sql \
     --insert-sql=queries/new_source/insert_data.sql
   ```

### 2. Adding a New Parquet Data Source

1. Create table definition SQL file: `queries/new_source/new_source_table.up.sql`
2. Run:
   ```bash
   python run_queries.py --ingestor=parquet \
     --create-table-sql=queries/new_source/new_source_table.up.sql \
     --s3-path=assets/new_source_data/{{DATE}}.parquet \
     --table-name=database.new_source_table \
     --mode=latest
   ```

### 3. Adding to Docker Compose

For ease of use, add a new service to `docker-compose.yml`:

```yaml
new-source-ingestor:
  build:
    context: .
    dockerfile: Dockerfile
  container_name: new-source-ingestor
  volumes:
    - ./queries:/app/queries
  environment:
    CH_HOST: ${CH_DB_HOST}
    CH_PORT: ${CH_NATIVE_PORT}
    CH_USER: ${CH_USER}
    CH_PASSWORD: ${CH_PASSWORD}
    CH_DB: ${CH_DB}
    CH_SECURE: ${CH_SECURE}
    CH_VERIFY: "False"
    CH_QUERY_VAR_NEW_SOURCE_URL: ${NEW_SOURCE_URL:-}
    CH_QUERY_VAR_S3_ACCESS_KEY: ${S3_ACCESS_KEY:-}
    CH_QUERY_VAR_S3_SECRET_KEY: ${S3_SECRET_KEY:-}
    CH_QUERY_VAR_S3_BUCKET: ${S3_BUCKET:-}
    CH_QUERY_VAR_S3_REGION: ${S3_REGION:-}
  command: >
    --ingestor=parquet
    --create-table-sql=queries/new_source/new_source_table.up.sql
    --s3-path=assets/new_source_data/{{DATE}}.parquet
    --table-name=database.new_source_table
    --mode=latest
```

## Supporting a New File Format

If you need to support a new file format beyond CSV and Parquet:

1. Create a new ingestor class that extends `BaseIngestor` in `ingestors/new_format_ingestor.py`
2. Implement the `ingest()` method and any format-specific methods
3. Update `run_queries.py` to recognize the new ingestor type

### Example: Adding Support for Avro Files

If you wanted to add support for Avro files, you would:

1. Update `requirements.txt` to include Avro-related packages
2. Create `ingestors/avro_ingestor.py` extending `BaseIngestor`
3. Implement the specialized logic for Avro ingestion
4. Update `run_queries.py` to support `--ingestor=avro`
5. Create sample Avro ingestion Docker Compose services

## Advanced Usage

### Variable Substitution in SQL

SQL files can use variable placeholders with the `{{VARIABLE_NAME}}` syntax. These are replaced with values from environment variables prefixed with `CH_QUERY_VAR_`.

For example:
- Environment variable: `CH_QUERY_VAR_EMBER_DATA_URL=https://example.com/data.csv`
- In SQL: `FROM url('{{EMBER_DATA_URL}}', 'CSV')`

### Skip Table Creation

If tables already exist, you can skip the table creation step:

```bash
python run_queries.py --ingestor=csv \
  --create-table-sql=queries/ember/create_ember_table.sql \
  --insert-sql=queries/ember/insert_ember_data.sql \
  --skip-table-creation
```

### Running Multiple Ingestors

For complex workflows, you can chain multiple ingestors:

```bash
# First run ember ingestor
docker-compose run --rm ember-ingestor

# Then run probelab ingestor
docker-compose run --rm probelab-agent-semvers-ingestor
```

## Troubleshooting

### Common Issues

1. **S3 Access Denied**:
   - Verify S3 credentials
   - Check bucket permissions

2. **ClickHouse Connection Failure**:
   - Verify ClickHouse connection details
   - Check network connectivity

3. **Invalid SQL Syntax**:
   - Inspect SQL files for errors
   - Use ClickHouse client directly to test queries

### Logs

By default, logs are output to stdout/stderr. Docker Compose captures these logs.

To view logs:
```bash
docker-compose logs click-runner
```

When using cron jobs, logs are saved to the `logs/` directory with dated filenames.

## License

This project is licensed under the [MIT License](LICENSE).
