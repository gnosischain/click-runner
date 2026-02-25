import logging
import os
from typing import Dict, List, Optional

from clickhouse_connect.driver.client import Client

from .base import BaseIngestor

logger = logging.getLogger("clickhouse_runner")

class CSVIngestor(BaseIngestor):
    """
    Ingestor for CSV data using ClickHouse's URL engine.
    Typically used for sources like the Ember data.
    """
    def __init__(
        self, 
        client: Client, 
        variables: Dict[str, str],
        create_table_sql: str,
        insert_sql: str,
        optimize_sql: Optional[str] = None
    ):
        """
        Initialize the CSV ingestor.
        
        Args:
            client: ClickHouse client
            variables: Template variables
            create_table_sql: Path to SQL file for table creation
            insert_sql: Path to SQL file for data insertion
            optimize_sql: Optional path to SQL file for table optimization
        """
        super().__init__(client, variables)
        self.create_table_sql = create_table_sql
        self.insert_sql = insert_sql
        self.optimize_sql = optimize_sql
    
    def ingest(self, skip_table_creation: bool = False, **kwargs) -> bool:
        """
        Execute the CSV ingestion process:
        1. Create table (if needed)
        2. Insert data from CSV URL
        3. Optimize table (if needed)

        Args:
            skip_table_creation: If True, skip the table creation step

        Returns:
            True if successful, False otherwise
        """
        try:
            if not skip_table_creation:
                # Create table
                create_query = self.load_sql_file(self.create_table_sql)
                logger.info(f"Creating table using {self.create_table_sql}")
                self.client.command(create_query)

            # Insert data
            insert_query = self.load_sql_file(self.insert_sql)
            logger.info(f"Inserting data using {self.insert_sql}")

            table_name = self.extract_table_name(insert_query)
            count_before = 0
            if table_name:
                count_before = self.get_row_count(table_name)
                logger.info(f"Row count before insert in {table_name}: {count_before}")

            self.client.command(insert_query)

            if table_name:
                count_after = self.get_row_count(table_name)
                rows_inserted = count_after - count_before
                logger.info(f"Row count after insert in {table_name}: {count_after}")
                logger.info(f"Rows inserted: {rows_inserted}")

            # Optimize if specified
            if self.optimize_sql:
                optimize_query = self.load_sql_file(self.optimize_sql)
                logger.info(f"Optimizing table using {self.optimize_sql}")
                self.client.command(optimize_query)

            return True
        except Exception as e:
            logger.error(f"Error in CSV ingestion: {e}")
            return False