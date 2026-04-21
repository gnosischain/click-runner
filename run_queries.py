#!/usr/bin/env python3
import os
import sys
import logging
import argparse
from typing import Dict, List, Optional
from datetime import datetime, timedelta

import clickhouse_connect
from clickhouse_connect.driver.client import Client

import observability as obs
from ingestors.csv_ingestor import CSVIngestor
from ingestors.parquet_ingestor import ParquetIngestor
from ingestors.gdrive_ingestor import GDriveIngestor
from ingestors.mixpanel_ingestor import MixpanelIngestor
from ingestors.cow_ingestor import CowIngestor

logger = logging.getLogger("clickhouse_runner")

def connect_clickhouse(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    secure: bool,
    verify: bool
) -> Client:
    """
    Connect to ClickHouse using clickhouse_connect and return a Client.
    Raises an exception if connection fails.
    """
    job = obs.get_job_name()
    start = datetime.utcnow()
    logger.info(
        f"Connecting to ClickHouse at {host}:{port}, secure={secure}, verify={verify}",
        extra={"event": "clickhouse_connect_start", "database": database},
    )
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            secure=secure,
            verify=verify
        )
        # Quick test
        client.command("SELECT 1")
        obs.clickhouse_connected.labels(job=job, database=database).set(1)
        obs.update_health(clickhouse_connected=True)
        logger.info(
            "ClickHouse connection established successfully.",
            extra={"event": "clickhouse_connect_success", "database": database},
        )
        return client
    except Exception as e:
        obs.clickhouse_connected.labels(job=job, database=database).set(0)
        obs.update_health(clickhouse_connected=False, last_error=str(e))
        logger.error(
            f"Error connecting to ClickHouse: {e}",
            extra={"event": "clickhouse_connect_failure", "database": database},
        )
        raise
    finally:
        duration = (datetime.utcnow() - start).total_seconds()
        obs.clickhouse_connect_duration_seconds.labels(job=job, database=database).observe(duration)

def get_query_variables() -> Dict[str, str]:
    """
    Extract all environment variables starting with CH_QUERY_VAR_ prefix.
    These will be accessible in SQL queries as {{VAR_NAME}}.
    """
    query_vars = {}
    prefix = "CH_QUERY_VAR_"
    
    for key, value in os.environ.items():
        if key.startswith(prefix):
            # Remove the prefix to get the variable name
            var_name = key[len(prefix):]
            query_vars[var_name] = value
            # Don't log sensitive values that might be in environment variables
            if "SECRET" in key or "PASSWORD" in key or "KEY" in key or "TOKEN" in key:
                logger.info(f"Loaded query variable: {var_name}=***REDACTED***")
            else:
                logger.info(f"Loaded query variable: {var_name}={value}")
    
    return query_vars

def parse_csv_list(raw: str) -> List[str]:
    """Parse a comma-separated string into a list of non-empty items."""
    return [item.strip() for item in (raw or "").split(",") if item.strip()]

def load_sql_template(file_path: str, variables: Dict[str, str]) -> str:
    """Load SQL from a file and replace {{VARIABLE}} placeholders."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        sql = f.read()

    for key, value in variables.items():
        sql = sql.replace("{{" + key + "}}", value)

    return sql

def create_argparser() -> argparse.ArgumentParser:
    """Create command line argument parser"""
    parser = argparse.ArgumentParser(description="ClickHouse query runner with support for different ingestors")
    
    # Connection parameters
    parser.add_argument("--host", default=os.getenv("CH_HOST", "localhost"), help="ClickHouse host")
    parser.add_argument("--port", type=int, default=int(os.getenv("CH_PORT", "9000")), help="ClickHouse port")
    parser.add_argument("--user", default=os.getenv("CH_USER", "default"), help="ClickHouse user")
    parser.add_argument("--password", default=os.getenv("CH_PASSWORD", ""), help="ClickHouse password")
    parser.add_argument("--db", default=os.getenv("CH_DB", "default"), help="ClickHouse database")
    parser.add_argument("--secure", default=os.getenv("CH_SECURE", "False"), help="Use TLS connection")
    parser.add_argument("--verify", default=os.getenv("CH_VERIFY", "True"), help="Verify TLS certificate")
    
    # Ingestor parameters
    parser.add_argument("--ingestor", choices=["csv", "parquet", "gdrive", "query", "dune-execute-only", "mixpanel", "cow"], default="query",
                       help="Type of ingestor to use")
    
    # CSV ingestor parameters
    parser.add_argument("--create-table-sql", help="Path to SQL file for table creation")
    parser.add_argument("--insert-sql", help="Path to SQL file for data insertion")
    parser.add_argument("--optimize-sql", help="Path to SQL file for table optimization")
    
    # Parquet ingestor parameters
    parser.add_argument("--table-name", help="Target table name for ingestion")
    parser.add_argument("--s3-path", help="S3 path pattern for Parquet files")
    parser.add_argument("--mode", choices=["latest", "date", "all"], default="latest",
                       help="Ingestion mode for Parquet files")
    parser.add_argument("--date", help="Specific date for 'date' mode (YYYY-MM-DD)")
    
    # Google Drive ingestor parameters
    parser.add_argument("--file-id", help="Google Drive file ID for CSV file")
    parser.add_argument("--max-rows", type=int, default=1000000, help="Maximum number of rows to process (default: 1,000,000)")
    
    # Generic query parameters
    parser.add_argument("--queries", help="Comma-separated list of query files to execute")
    parser.add_argument(
        "--dune-execute-only-query-ids",
        default=os.getenv("DUNE_EXECUTE_ONLY_QUERY_IDS", ""),
        help="Comma-separated list of dedicated Dune query IDs to execute without ingestion",
    )
    parser.add_argument("--skip-table-creation", action="store_true", help="Skip table creation steps")

    # Mixpanel ingestor parameters
    parser.add_argument("--mixpanel-mode", choices=["daily", "backfill"], default="daily",
                       help="Mixpanel ingestion mode")
    parser.add_argument("--mixpanel-from-date", help="Start date for Mixpanel export (YYYY-MM-DD)")
    parser.add_argument("--mixpanel-to-date", help="End date for Mixpanel export (YYYY-MM-DD)")
    parser.add_argument("--mixpanel-event-filter", help="JSON array of event names to filter")
    parser.add_argument("--mixpanel-region", choices=["US", "EU", "IN"], default=os.getenv("MIXPANEL_REGION", "US"),
                       help="Mixpanel data residency region (default: US)")

    # CoW ingestor parameters
    parser.add_argument("--cow-mode", choices=["daily", "backfill"], default="daily",
                       help="CoW ingestion mode: daily (recent owners) or backfill (all owners)")
    parser.add_argument("--cow-lookback-days", type=int, default=2,
                       help="Number of days to look back for daily mode (default: 2)")
    parser.add_argument("--cow-backfill-from",
                       default=os.getenv("COW_BACKFILL_FROM", ""),
                       help="Start date for backfill mode (YYYY-MM-DD), e.g. 2024-01-01")
    parser.add_argument("--cow-source-table",
                       default=os.getenv("COW_SOURCE_TABLE", ""),
                       help="Fully qualified table to read owner addresses from (e.g. dbt.int_execution_cow_trades)")
    parser.add_argument("--cow-max-pages", type=int, default=500,
                       help="Max API pages per owner (each page = 1000 trades, default: 500 = 500k trades)")

    return parser

def run_csv_ingestor(args, client, query_vars):
    """Run the CSV ingestor"""
    create_table_sql = args.create_table_sql
    insert_sql = args.insert_sql
    optimize_sql = args.optimize_sql
    
    # If paths aren't provided, try to use CH_QUERIES env var
    if not create_table_sql or not insert_sql:
        ch_queries = os.getenv("CH_QUERIES", "").split(",")
        if len(ch_queries) >= 2:
            create_table_sql = create_table_sql or ch_queries[0].strip()
            insert_sql = insert_sql or ch_queries[1].strip()
            if len(ch_queries) >= 3:
                optimize_sql = optimize_sql or ch_queries[2].strip()
    
    if not create_table_sql or not insert_sql:
        logger.error("Missing required SQL files for CSV ingestor")
        return False
    
    ingestor = CSVIngestor(
        client=client,
        variables=query_vars,
        create_table_sql=create_table_sql,
        insert_sql=insert_sql,
        optimize_sql=optimize_sql
    )
    
    obs.update_health(table_name=ingestor.extract_table_name(ingestor.load_sql_file(insert_sql)) if insert_sql else "")
    with obs.time_operation(obs.get_job_name(), "csv", "ingest"):
        return ingestor.ingest(skip_table_creation=args.skip_table_creation)

def run_parquet_ingestor(args, client, query_vars):
    """Run the Parquet ingestor"""
    create_table_sql = args.create_table_sql
    s3_path = args.s3_path
    table_name = args.table_name
    mode = args.mode
    date = args.date
    
    # If no create table SQL is provided, try to use CH_QUERIES env var
    if not create_table_sql:
        ch_queries = os.getenv("CH_QUERIES", "").split(",")
        if ch_queries[0].strip():
            create_table_sql = ch_queries[0].strip()
    
    if not create_table_sql or not s3_path or not table_name:
        logger.error("Missing required parameters for Parquet ingestor")
        return False
    
    ingestor = ParquetIngestor(
        client=client,
        variables=query_vars,
        create_table_sql=create_table_sql,
        s3_path_pattern=s3_path,
        table_name=table_name
    )
    
    obs.update_health(table_name=table_name)
    with obs.time_operation(obs.get_job_name(), "parquet", "ingest"):
        return ingestor.ingest(
            skip_table_creation=args.skip_table_creation,
            date=date,
            mode=mode
        )

def run_gdrive_ingestor(args, client, query_vars):
    """Run the Google Drive CSV ingestor"""
    create_table_sql = args.create_table_sql
    insert_sql = args.insert_sql  # This will be ignored in the simplified approach
    optimize_sql = args.optimize_sql
    file_id = args.file_id or os.getenv("CH_GDRIVE_FILE_ID", "")
    table_name = args.table_name or os.getenv("CH_TABLE_NAME", "")
    
    # Handle max_rows with a default value if the attribute doesn't exist
    max_rows = getattr(args, 'max_rows', 1000000)  # Default to 1,000,000 rows
    
    # If paths aren't provided, try to use CH_QUERIES env var
    if not create_table_sql:
        ch_queries = os.getenv("CH_QUERIES", "").split(",")
        if len(ch_queries) >= 1:
            create_table_sql = create_table_sql or ch_queries[0].strip()
        if len(ch_queries) >= 3 and not optimize_sql:
            optimize_sql = ch_queries[2].strip()
    
    if not create_table_sql or not file_id:
        logger.error("Missing required parameters for Google Drive ingestor")
        logger.error("Required: --create-table-sql, --file-id")
        return False
    
    # If table_name is not provided, try to extract it from create_table_sql
    if not table_name:
        try:
            create_sql = open(create_table_sql, 'r').read()
            # Extract table name from CREATE TABLE statement
            import re
            match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)', create_sql, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                logger.info(f"Extracted table name from SQL: {table_name}")
        except Exception as e:
            logger.error(f"Error extracting table name from SQL: {e}")
            return False
    
    if not table_name:
        logger.error("Table name is required. Specify with --table-name or ensure it's in the CREATE TABLE SQL.")
        return False
    
    ingestor = GDriveIngestor(
        client=client,
        variables=query_vars,
        create_table_sql=create_table_sql,
        file_id=file_id,
        table_name=table_name,
        optimize_sql=optimize_sql,
        max_rows=max_rows
    )
    
    obs.update_health(table_name=table_name)
    with obs.time_operation(obs.get_job_name(), "gdrive", "ingest"):
        return ingestor.ingest(skip_table_creation=args.skip_table_creation)

def run_query_ingestor(args, client, query_vars):
    """Run plain SQL queries from files"""
    from ingestors.base import BaseIngestor
    
    # Get queries from args or env var
    queries_str = args.queries or os.getenv("CH_QUERIES", "")
    if not queries_str:
        logger.error("No queries specified")
        return False
    
    query_files = [q.strip() for q in queries_str.split(",") if q.strip()]
    
    # Create a basic ingestor for executing the queries
    ingestor = BaseIngestor(client, query_vars)
    queries = []
    
    for file in query_files:
        try:
            sql = ingestor.load_sql_file(file)
            queries.append(sql)
        except FileNotFoundError:
            logger.error(f"Query file not found: {file}")
            return False
    
    with obs.time_operation(obs.get_job_name(), "query", "ingest"):
        return ingestor.execute_queries(queries)

def run_mixpanel_ingestor(args, client, query_vars):
    """Run the Mixpanel raw event export ingestor"""
    create_table_sql = args.create_table_sql
    create_state_sql = os.getenv("CH_MIXPANEL_STATE_SQL", "queries/mixpanel/create_state_table.sql")

    if not create_table_sql:
        ch_queries = os.getenv("CH_QUERIES", "").split(",")
        if ch_queries[0].strip():
            create_table_sql = ch_queries[0].strip()

    if not create_table_sql:
        create_table_sql = "queries/mixpanel/create_events_table.sql"

    project_id = query_vars.get("MIXPANEL_PROJECT_ID", "")
    sa_username = query_vars.get("MIXPANEL_SA_USERNAME", "")
    sa_secret = query_vars.get("MIXPANEL_SA_SECRET", "")
    database = query_vars.get("MIXPANEL_DATABASE", "mixpanel")
    table_name = args.table_name or os.getenv("CH_TABLE_NAME", f"{database}.mixpanel_raw_events")

    if not project_id or not sa_username or not sa_secret:
        logger.error(
            "Missing Mixpanel credentials. Required env vars: "
            "CH_QUERY_VAR_MIXPANEL_PROJECT_ID, CH_QUERY_VAR_MIXPANEL_SA_USERNAME, "
            "CH_QUERY_VAR_MIXPANEL_SA_SECRET"
        )
        return False

    ingestor = MixpanelIngestor(
        client=client,
        variables={**query_vars, "MIXPANEL_DATABASE": database},
        create_table_sql=create_table_sql,
        create_state_sql=create_state_sql,
        table_name=table_name,
        project_id=project_id,
        sa_username=sa_username,
        sa_secret=sa_secret,
        from_date=args.mixpanel_from_date,
        to_date=args.mixpanel_to_date,
        event_filter=args.mixpanel_event_filter,
        mode=args.mixpanel_mode,
        region=args.mixpanel_region,
    )

    obs.update_health(table_name=table_name)
    with obs.time_operation(obs.get_job_name(), "mixpanel", "ingest"):
        return ingestor.ingest(skip_table_creation=args.skip_table_creation)


def run_dune_execute_only(args, client, query_vars):
    """Trigger dedicated Dune queries without saving results to ClickHouse."""
    query_ids = parse_csv_list(args.dune_execute_only_query_ids)
    if not query_ids:
        logger.error("No execute-only Dune query IDs specified")
        return False
    if "DUNE_API_KEY" not in query_vars:
        logger.error("Missing CH_QUERY_VAR_DUNE_API_KEY for execute-only Dune queries")
        return False

    sql_file = "queries/dune/execute_only/execute_query.sql"
    success = True

    for query_id in query_ids:
        try:
            sql = load_sql_template(
                sql_file,
                {
                    **query_vars,
                    "DUNE_EXECUTE_ONLY_QUERY_ID": query_id,
                },
            )
            logger.info(
                f"Triggering execute-only Dune query {query_id}",
                extra={"event": "dune_execute_only_start", "query_id": query_id},
            )
            with obs.time_operation(obs.get_job_name(), "dune-execute-only", "trigger_query"):
                client.command(sql)
            obs.dune_queries_triggered_total.labels(job=obs.get_job_name(), result="success").inc()
        except Exception as e:
            success = False
            obs.dune_queries_triggered_total.labels(job=obs.get_job_name(), result="failure").inc()
            logger.error(
                f"Failed execute-only Dune query {query_id}: {e}",
                extra={"event": "dune_execute_only_failure", "query_id": query_id},
            )

    return success

def run_cow_ingestor(args, client, query_vars):
    """Run the CoW Protocol fee ingestor"""
    create_table_sql = args.create_table_sql or "queries/cow/create_table.sql"
    table_name = args.table_name or os.getenv("CH_TABLE_NAME", "")
    source_table = args.cow_source_table

    if not table_name or not source_table:
        logger.error("CoW ingestor requires --table-name and --cow-source-table")
        return False

    ingestor = CowIngestor(
        client=client,
        variables=query_vars,
        create_table_sql=create_table_sql,
        table_name=table_name,
        source_table=source_table,
        mode=args.cow_mode,
        lookback_days=args.cow_lookback_days,
        backfill_from=args.cow_backfill_from or None,
        max_pages=args.cow_max_pages,
    )

    obs.update_health(table_name=table_name, source_table=source_table)
    with obs.time_operation(obs.get_job_name(), "cow", "ingest"):
        return ingestor.ingest(skip_table_creation=args.skip_table_creation)


def main():
    """Main entry point"""
    obs.setup_logging()
    if obs.is_enabled():
        try:
            obs.start_metrics_server(int(os.getenv("OBSERVABILITY_PORT", "9090")))
        except Exception as e:
            logger.warning(f"Failed to start metrics server: {e}")

    parser = create_argparser()
    args = parser.parse_args()
    job = obs.get_job_name()
    start_time = datetime.utcnow()
    success = False
    exit_code = 1

    obs.runs_started_total.labels(job=job, ingestor=args.ingestor).inc()
    obs.active_runs.labels(job=job, ingestor=args.ingestor).inc()
    obs.update_health(
        status="running",
        clickhouse_connected=False,
        ingestor=args.ingestor,
        job_name=job,
        database=args.db,
        table_name=args.table_name or os.getenv("CH_TABLE_NAME", ""),
        started_at=obs.utc_now(),
        finished_at="",
        last_error="",
    )
    
    try:
        # Convert string booleans to actual booleans
        secure = args.secure.lower() in ("true", "1", "yes")
        verify = args.verify.lower() not in ("false", "0", "no")  # default True
        
        # Get variables for SQL queries from environment
        query_variables = get_query_variables()
        
        # Connect to ClickHouse
        client = connect_clickhouse(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.db,
            secure=secure,
            verify=verify
        )
        
        # Run the appropriate ingestor
        if args.ingestor == "csv":
            success = run_csv_ingestor(args, client, query_variables)
        elif args.ingestor == "parquet":
            success = run_parquet_ingestor(args, client, query_variables)
        elif args.ingestor == "gdrive":
            success = run_gdrive_ingestor(args, client, query_variables)
        elif args.ingestor == "mixpanel":
            success = run_mixpanel_ingestor(args, client, query_variables)
        elif args.ingestor == "cow":
            success = run_cow_ingestor(args, client, query_variables)
        elif args.ingestor == "dune-execute-only":
            success = run_dune_execute_only(args, client, query_variables)
        else:  # "query"
            success = run_query_ingestor(args, client, query_variables)

        if success:
            logger.info("All operations completed successfully!", extra={"event": "run_success"})
            exit_code = 0
        else:
            logger.error("Operation failed", extra={"event": "run_failure"})
    except Exception as e:
        success = False
        obs.update_health(last_error=str(e))
        logger.exception("Unhandled operation error", extra={"event": "run_exception"})
    finally:
        result = "success" if success else "failure"
        duration = (datetime.utcnow() - start_time).total_seconds()
        obs.runs_completed_total.labels(job=job, ingestor=args.ingestor, result=result).inc()
        obs.run_duration_seconds.labels(job=job, ingestor=args.ingestor).observe(duration)
        obs.active_runs.labels(job=job, ingestor=args.ingestor).dec()
        obs.update_health(status=result, finished_at=obs.utc_now())

    sys.exit(exit_code)

if __name__ == "__main__":
    main()
