# Clickhouse Runner

This repository provides a lightweight Dockerized tool to run arbitrary SQL queries against a ClickHouse database.

## Overview

`click-runner` is designed for flexible query execution, particularly useful in data ingestion, preprocessing, and one-off ClickHouse tasks (e.g. loading CSVs or optimizing tables). You can run individual or multiple SQL files in sequence.

## Prerequisites

Ensure the following prerequisites are installed and properly configured:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Project Structure

```
.
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── run_queries.py
└── queries/
    ├── create_ember_table.sql
    ├── insert_ember_data.sql
    └── optimize_ember.sql
```

- `Dockerfile`: Defines the environment to run queries using Python.
- `docker-compose.yml`: Service definition to run query execution with configured parameters.
- `requirements.txt`: Lists Python dependencies (e.g., `clickhouse-connect`).
- `run_queries.py`: Script that loads and executes `.sql` files against a ClickHouse server.
- `queries/`: Folder containing your `.sql` files.

## Environment Variables

Set the following environment variables to configure the ClickHouse connection:

- `CH_HOST`: ClickHouse host.
- `CH_PORT`: Native ClickHouse port (default: 9000).
- `CH_USER`: Username for authentication.
- `CH_PASSWORD`: Password for authentication.
- `CH_DB`: Database to use.
- `CH_SECURE`: Use TLS connection (`True` or `False`).
- `CH_VERIFY`: Verify TLS certificate (`True` or `False`).
- `CH_QUERIES`: Comma-separated list of `.sql` file paths to execute. If omitted, all `.sql` files in the `queries/` directory will be executed in alphabetical order.

## Usage

### Build and Run

To run all queries in the `queries/` folder:

```bash
docker-compose up --build
```

To run a specific subset of queries:

```yaml
# In docker-compose.yml under environment:
CH_QUERIES: "queries/create_ember_table.sql,queries/insert_ember_data.sql"
```

Or pass them at runtime:

```bash
docker-compose run run-queries \
  queries=queries/create_ember_table.sql,queries/insert_ember_data.sql
```

## Query Files

Each `.sql` file can include one or multiple SQL statements separated by semicolons (`;`). They will be executed sequentially in the order specified.

Example:

```sql
-- queries/optimize_ember.sql
OPTIMIZE TABLE crawlers_data.ember_electricity_data FINAL;
```

## Dependencies

This utility relies on the following Python library:

- `clickhouse-connect`: Official client for connecting to ClickHouse over native TCP or HTTP protocols.

Dependencies are installed during Docker build via `requirements.txt`.

## Contributing

Contributions and suggestions are welcome! Feel free to open a pull request to propose improvements or fixes.

## License

This project is licensed under the [MIT License](LICENSE).
