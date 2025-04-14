#!/usr/bin/env python3
import os
import sys
import logging
import argparse
from typing import Dict, List, Optional
from datetime import datetime, timedelta

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from ingestors.csv_ingestor import CSVIngestor
from ingestors.parquet_ingestor import ParquetIngestor

# Setup logging
logger = logging.getLogger("clickhouse_runner")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

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
    logger.info(f"Connecting to ClickHouse at {host}:{port}, secure={secure}, verify={verify}")
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
        logger.info("ClickHouse connection established successfully.")
        return client
    except Exception as e:
        logger.error(f"Error connecting to ClickHouse: {e}")
        raise

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
    parser.add_argument("--ingestor", choices=["csv", "parquet", "query"], default="query", 
                        help="Type of ingestor to use")
    
    # CSV ingestor parameters
    parser.add_argument("--create-table-sql", help="Path to SQL file for table creation")
    parser.add_argument("--insert-sql", help="Path to SQL file for data insertion")
    parser.add_argument("--optimize-sql", help="Path to SQL file for table optimization")
    
    # Parquet ingestor parameters
    parser.add_argument("--table-name", help="Target table name for Parquet ingestion")
    parser.add_argument("--s3-path", help="S3 path pattern for Parquet files")
    parser.add_argument("--mode", choices=["latest", "date", "all"], default="latest",
                        help="Ingestion mode for Parquet files")
    parser.add_argument("--date", help="Specific date for 'date' mode (YYYY-MM-DD)")
    
    # Generic query parameters
    parser.add_argument("--queries", help="Comma-separated list of query files to execute")
    parser.add_argument("--skip-table-creation", action="store_true", help="Skip table creation steps")
    
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
    
    return ingestor.ingest(
        skip_table_creation=args.skip_table_creation,
        date=date,
        mode=mode
    )

def run_query_ingestor(args, client, query_vars):
    """Run plain SQL queries from files"""
    from click_runner.ingestors.base import BaseIngestor
    
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
    
    return ingestor.execute_queries(queries)

def main():
    """Main entry point"""
    parser = create_argparser()
    args = parser.parse_args()
    
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
    success = False
    if args.ingestor == "csv":
        success = run_csv_ingestor(args, client, query_variables)
    elif args.ingestor == "parquet":
        success = run_parquet_ingestor(args, client, query_variables)
    else:  # "query"
        success = run_query_ingestor(args, client, query_variables)
    
    if success:
        logger.info("All operations completed successfully!")
        sys.exit(0)
    else:
        logger.error("Operation failed")
        sys.exit(1)

if __name__ == "__main__":
    main()