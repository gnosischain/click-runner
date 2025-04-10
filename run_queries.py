import os
import sys
import logging
from typing import List

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError

logger = logging.getLogger("clickhouse_queries")
logger.setLevel(logging.INFO)

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


def run_queries(client: Client, sql_files: List[str]):
    """
    Read and execute the given SQL files in order.
    Splits each file on semicolons (;) to handle multiple statements.
    """
    for sql_file in sql_files:
        if not os.path.exists(sql_file):
            logger.warning(f"SQL file not found: {sql_file}")
            continue

        logger.info(f"Running queries from {sql_file} ...")

        with open(sql_file, "r", encoding="utf-8") as f:
            content = f.read()

        # Split on semicolons to handle multiple statements
        statements = [stmt.strip() for stmt in content.split(";") if stmt.strip()]
        try:
            for stmt in statements:
                client.command(stmt)
            logger.info(f"Successfully ran all statements in {sql_file}")
        except ClickHouseError as che:
            logger.error(f"Error running queries in {sql_file}: {che}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error for {sql_file}: {e}")
            raise


if __name__ == "__main__":
    """
    CLI usage:
      python run_queries.py host=... port=... user=... password=... db=... secure=... verify=... queries=...
    
    If not provided as CLI args, fallback to env variables:
      CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DB, CH_SECURE, CH_VERIFY, CH_QUERIES
    - CH_QUERIES can be a comma-separated list of file paths (relative to the working directory).
    Example: CH_QUERIES="queries/create_ember_table.sql,queries/insert_ember_data.sql"
    """
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

    args_dict = {}
    for arg in sys.argv[1:]:
        if "=" in arg:
            k, v = arg.split("=", 1)
            args_dict[k] = v

    host = args_dict.get("host", os.getenv("CH_HOST", "localhost"))
    port_str = args_dict.get("port", os.getenv("CH_PORT", "9000"))
    user = args_dict.get("user", os.getenv("CH_USER", "default"))
    password = args_dict.get("password", os.getenv("CH_PASSWORD", ""))
    db = args_dict.get("db", os.getenv("CH_DB", "default"))
    secure_str = args_dict.get("secure", os.getenv("CH_SECURE", "False"))
    verify_str = args_dict.get("verify", os.getenv("CH_VERIFY", "True"))
    queries_str = args_dict.get("queries", os.getenv("CH_QUERIES", ""))

    # Convert port
    try:
        port = int(port_str)
    except ValueError:
        logger.warning(f"Invalid port: '{port_str}'. Defaulting to 9000.")
        port = 9000

    # Convert booleans
    secure = secure_str.lower() in ("true", "1", "yes")
    verify = verify_str.lower() not in ("false", "0", "no")  # default True

    # Convert queries list (comma-separated)
    # If user doesn't specify anything, run all .sql in the "queries" folder as a fallback example
    if queries_str.strip():
        sql_files = [p.strip() for p in queries_str.split(",") if p.strip()]
    else:
        # By default, gather all .sql in the `queries` folder
        queries_folder = os.path.join(os.getcwd(), "queries")
        sql_files = [
            os.path.join(queries_folder, f)
            for f in sorted(os.listdir(queries_folder))
            if f.endswith(".sql")
        ]

    client = connect_clickhouse(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        secure=secure,
        verify=verify
    )

    run_queries(client, sql_files)
