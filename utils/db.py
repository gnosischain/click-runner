import logging
from typing import List, Dict, Any, Optional

from clickhouse_connect.driver.client import Client

logger = logging.getLogger("click_runner")

def table_exists(client: Client, table_name: str) -> bool:
    """
    Check if a table exists in the connected ClickHouse database.
    
    Args:
        client: ClickHouse client
        table_name: Name of the table to check
        
    Returns:
        True if the table exists, False otherwise
    """
    try:
        # This will throw an exception if the table doesn't exist
        client.command(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False

def get_table_columns(client: Client, table_name: str) -> List[Dict[str, Any]]:
    """
    Get the column definitions for a table.
    
    Args:
        client: ClickHouse client
        table_name: Name of the table
        
    Returns:
        List of column definitions
    """
    try:
        result = client.query(f"DESCRIBE TABLE {table_name}")
        return result.result_rows
    except Exception as e:
        logger.error(f"Error getting table columns: {e}")
        return []

def get_latest_data_date(client: Client, table_name: str, date_column: str) -> Optional[str]:
    """
    Get the latest date in a table.
    
    Args:
        client: ClickHouse client
        table_name: Name of the table
        date_column: Name of the date column
        
    Returns:
        Latest date as string (YYYY-MM-DD) or None if table is empty
    """
    try:
        result = client.query(f"SELECT MAX({date_column}) FROM {table_name}")
        if result.result_rows and result.result_rows[0][0]:
            return str(result.result_rows[0][0]).split()[0]  # Get just the date part
        return None
    except Exception as e:
        logger.error(f"Error getting latest date: {e}")
        return None

def truncate_table(client: Client, table_name: str) -> bool:
    """
    Truncate a table (remove all data).
    
    Args:
        client: ClickHouse client
        table_name: Name of the table
        
    Returns:
        True if successful, False otherwise
    """
    try:
        client.command(f"TRUNCATE TABLE {table_name}")
        return True
    except Exception as e:
        logger.error(f"Error truncating table: {e}")
        return False
        
def optimize_table(client: Client, table_name: str) -> bool:
    """
    Optimize a table to improve performance.
    
    Args:
        client: ClickHouse client
        table_name: Name of the table
        
    Returns:
        True if successful, False otherwise
    """
    try:
        client.command(f"OPTIMIZE TABLE {table_name} FINAL")
        return True
    except Exception as e:
        logger.error(f"Error optimizing table: {e}")
        return False