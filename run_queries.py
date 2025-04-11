import os
import sys
import logging
import re
from typing import List, Dict

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

def replace_variables(sql_content: str, variables: Dict[str, str]) -> str:
    """
    Replace {{VARIABLE_NAME}} patterns in SQL with their values from environment variables.
    """
    # Use regex to find all {{VARIABLE_NAME}} patterns
    pattern = r"{{([A-Za-z0-9_]+)}}"
    
    def replace_match(match):
        var_name = match.group(1)
        if var_name in variables:
            return variables[var_name]
        else:
            logger.warning(f"Variable {{{{{{var_name}}}}}} not found in environment variables")
            return match.group(0)  # Keep original if not found
    
    # Replace all occurrences
    processed_sql = re.sub(pattern, replace_match, sql_content)
    return processed_sql

def run_queries(client: Client, sql_files: List[str], variables: Dict[str, str]):
    """
    Read and execute the given SQL files in order.
    Splits each file on semicolons (;) to handle multiple statements.
    Replaces {{VARIABLE_NAME}} with values from environment variables.
    """
    for sql_file in sql_files:
        if not os.path.exists(sql_file):
            logger.warning(f"SQL file not found: {sql_file}")
            continue

        logger.info(f"Running queries from {sql_file} ...")

        with open(sql_file, "r", encoding="utf-8") as f:
            content = f.read()
        
        # Replace variables in the SQL content
        content = replace_variables(content, variables)

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
    
    Query variables:
      - Define with environment variables starting with CH_QUERY_VAR_
      - Reference in SQL as {{VARIABLE_NAME}} (without the CH_QUERY_VAR_ prefix)
    
    Example: 
      CH_QUERY_VAR_DATA_URL="https://example.com/data.csv" -> {{DATA_URL}} in SQL
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

    # Get variables for SQL queries from environment
    query_variables = get_query_variables()

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

    run_queries(client, sql_files, query_variables)