import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

from clickhouse_connect.driver.client import Client

from .base import BaseIngestor
from utils.s3 import list_s3_files, get_latest_file

logger = logging.getLogger("clickhouse_runner")

class ParquetIngestor(BaseIngestor):
    """
    Ingestor for Parquet data from S3, designed for ProbeLabā data.
    """
    def __init__(
        self, 
        client: Client, 
        variables: Dict[str, str],
        create_table_sql: str,
        s3_path_pattern: str,
        table_name: str
    ):
        """
        Initialize the Parquet ingestor.
        
        Args:
            client: ClickHouse client
            variables: Template variables
            create_table_sql: Path to SQL file for table creation
            s3_path_pattern: Pattern for S3 path (can contain {{DATE}} placeholder)
            table_name: Name of the target ClickHouse table
        """
        super().__init__(client, variables)
        self.create_table_sql = create_table_sql
        self.s3_path_pattern = s3_path_pattern
        self.table_name = table_name
    

    def ingest(
        self, 
        skip_table_creation: bool = False,
        date: Optional[str] = None,
        mode: str = "latest",  # "latest", "date", "all"
        **kwargs
    ) -> bool:
        """
        Execute the Parquet ingestion process:
        1. Create table (if needed)
        2. Determine S3 file(s) to ingest based on mode
        3. Generate and execute INSERT query
        
        Args:
            skip_table_creation: If True, skip the table creation step
            date: Specific date to ingest (format: YYYY-MM-DD)
            mode: Ingestion mode:
                - "latest": Only ingest the most recent file
                - "date": Ingest file for the specific date
                - "all": Ingest all available files
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # 1. Create table if needed
            if not skip_table_creation:
                create_query = self.load_sql_file(self.create_table_sql)
                logger.info(f"Creating table using {self.create_table_sql}")
                self.client.command(create_query)
            
            # 2. Determine S3 path(s) to ingest
            bucket = self.variables.get("S3_BUCKET", "")
            access_key = self.variables.get("S3_ACCESS_KEY", "")
            secret_key = self.variables.get("S3_SECRET_KEY", "")
            region = self.variables.get("S3_REGION", "us-east-1")
            
            base_s3_path = self.s3_path_pattern
            
            if mode == "latest":
                # Get the latest file from S3
                # Extract the directory path without the filename pattern
                prefix = base_s3_path.rsplit("/", 1)[0]
                logger.info(f"Looking for latest file with prefix: {prefix}")
                
                latest_file = get_latest_file(
                    bucket, prefix, access_key, secret_key, region
                )
                if not latest_file:
                    logger.error(f"No files found in S3 path: {bucket}/{prefix}")
                    return False
                
                s3_paths = [f"s3://{bucket}/{latest_file}"]
                logger.info(f"Ingesting latest file: {s3_paths[0]}")
                
            elif mode == "date":
                if not date:
                    logger.error("Date must be provided when mode is 'date'")
                    return False
                
                # Format S3 path with the provided date
                s3_path = base_s3_path.replace("{{DATE}}", date)
                s3_paths = [f"s3://{bucket}/{s3_path}"]
                logger.info(f"Ingesting file for date {date}: {s3_paths[0]}")
                
            elif mode == "all":
                # List all files matching the pattern
                prefix = base_s3_path.rsplit("/", 1)[0]
                logger.info(f"Looking for all files with prefix: {prefix}")
                
                files = list_s3_files(bucket, prefix, access_key, secret_key, region)
                if not files:
                    logger.error(f"No files found in S3 path: {bucket}/{prefix}")
                    return False
                
                s3_paths = [f"s3://{bucket}/{f}" for f in files]
                logger.info(f"Ingesting {len(s3_paths)} files")
                
            else:
                logger.error(f"Unknown ingestion mode: {mode}")
                return False
            
            # 3. Generate and execute the INSERT query
            count_before = self.get_row_count(self.table_name)
            logger.info(f"Row count before insert in {self.table_name}: {count_before}")

            for s3_path in s3_paths:
                insert_query = self._generate_insert_query(s3_path, access_key, secret_key, region)
                logger.info(f"Inserting data from {s3_path}")
                self.client.command(insert_query)

            count_after = self.get_row_count(self.table_name)
            rows_inserted = count_after - count_before
            logger.info(f"Row count after insert in {self.table_name}: {count_after}")
            logger.info(f"Rows inserted: {rows_inserted}")

            return True
            
        except Exception as e:
            logger.error(f"Error in Parquet ingestion: {e}")
            return False
    
    def _generate_insert_query(
        self, s3_path: str, access_key: str, secret_key: str, region: str
    ) -> str:
        """
        Generate an INSERT query for a Parquet file in S3.
        """
        # Check if this is a regular or s3:// path
        if s3_path.startswith("s3://"):
            bucket_path = s3_path[5:]  # Remove "s3://"
            parts = bucket_path.split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""
            s3_format = f"s3('{s3_path}', '{access_key}', '{secret_key}')"
        else:
            # Handle regular URLs if needed
            s3_format = f"url('{s3_path}')"
        
        # Build the INSERT query
        query = f"""
        INSERT INTO {self.table_name}
        SELECT *
        FROM {s3_format}
        FORMAT Parquet
        """
        return query