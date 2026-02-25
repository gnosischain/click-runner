from abc import ABC, abstractmethod
import logging
import os
import re
from typing import Dict, List, Optional, Any, Tuple

from clickhouse_connect.driver.client import Client

logger = logging.getLogger("clickhouse_runner")

class BaseIngestor(ABC):
    """
    Abstract base class for all data ingestors.
    Defines common interface and utility methods.
    """
    def __init__(self, client: Client, variables: Dict[str, str]):
        """
        Initialize the ingestor with a ClickHouse client and template variables.

        Args:
            client: Connected ClickHouse client
            variables: Dictionary of variable replacements for SQL templates
        """
        self.client = client
        self.variables = variables

    @abstractmethod
    def ingest(self, **kwargs) -> bool:
        """
        Execute the ingestion process.
        Returns True if successful, False otherwise.

        Implementation depends on the specific ingestor.
        """
        pass

    def extract_table_name(self, sql: str) -> Optional[str]:
        """Extract table name from an INSERT INTO statement."""
        match = re.search(r'INSERT\s+INTO\s+([^\s(]+)', sql, re.IGNORECASE)
        return match.group(1) if match else None

    def get_row_count(self, table_name: str) -> int:
        """Get current row count for a table."""
        result = self.client.query(f"SELECT count() FROM {table_name}")
        return result.result_rows[0][0]

    def execute_queries(self, queries: List[str]) -> bool:
        """
        Execute a list of SQL queries in sequence.
        Returns True if all queries succeed, False otherwise.
        """
        for query in queries:
            try:
                logger.info(f"Executing query: {query[:100]}...")
                self.client.command(query)
            except Exception as e:
                logger.error(f"Error executing query: {e}")
                logger.error(f"Failed query: {query}")
                return False
        return True
    
    def replace_variables(self, sql: str) -> str:
        """
        Replace all {{VARIABLE}} patterns in SQL with values from self.variables.
        """
        for key, value in self.variables.items():
            placeholder = "{{" + key + "}}"
            sql = sql.replace(placeholder, value)
        return sql
    
    def load_sql_file(self, file_path: str) -> str:
        """
        Load SQL from a file and replace variables.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"SQL file not found: {file_path}")
        
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        return self.replace_variables(content)