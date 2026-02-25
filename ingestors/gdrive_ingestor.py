import logging
import os
import io
import csv
import re
import datetime
from typing import Dict, List, Optional, Any, Tuple

import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from clickhouse_connect.driver.client import Client

from .base import BaseIngestor

logger = logging.getLogger("clickhouse_runner")

class GDriveIngestor(BaseIngestor):
    """
    Ingestor for CSV data from Google Drive.
    Downloads a CSV file from Google Drive and inserts it directly into ClickHouse.
    This works with ClickHouse Cloud by avoiding file() and url() functions.
    """
    def __init__(
        self, 
        client: Client, 
        variables: Dict[str, str],
        create_table_sql: str,
        file_id: str,
        table_name: str,
        optimize_sql: Optional[str] = None,
        max_rows: int = 1000000
    ):
        """
        Initialize the Google Drive ingestor.
        
        Args:
            client: ClickHouse client
            variables: Template variables
            create_table_sql: Path to SQL file for table creation
            file_id: Google Drive file ID
            table_name: Target table name (e.g., "database.table")
            optimize_sql: Optional path to SQL file for table optimization
            max_rows: Maximum number of rows to process
        """
        super().__init__(client, variables)
        self.create_table_sql = create_table_sql
        self.file_id = file_id
        self.table_name = table_name
        self.optimize_sql = optimize_sql
        self.max_rows = max_rows
        self.column_types = {}  # Map of column name to type
        
        # Add variables for SQL templates
        self.variables["GDRIVE_FILE_ID"] = file_id
        self.variables["TARGET_TABLE"] = table_name
        
        # Set environment variables from variables dictionary
        if "GOOGLE_APPLICATION_CREDENTIALS" in self.variables:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.variables["GOOGLE_APPLICATION_CREDENTIALS"]
    
    def download_file(self) -> Optional[io.BytesIO]:
        """
        Download a file from Google Drive.
        
        Returns:
            File content as BytesIO or None if download failed
        """
        try:
            # Use default credentials
            creds, _ = google.auth.default()
            
            # Create Drive API client
            service = build("drive", "v3", credentials=creds)
            
            # Get file metadata to determine file name
            file_metadata = service.files().get(fileId=self.file_id).execute()
            file_name = file_metadata.get('name', 'unknown_file')
            logger.info(f"Downloading file: {file_name} (ID: {self.file_id})")
            
            # Download file content
            request = service.files().get_media(fileId=self.file_id)
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                logger.info(f"Download progress: {int(status.progress() * 100)}%")
            
            logger.info(f"Download complete: {file_name}")
            file.seek(0)
            return file
            
        except HttpError as error:
            logger.error(f"Error downloading file from Google Drive: {error}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during Google Drive download: {e}")
            return None
    
    def clean_column_name(self, name: str) -> str:
        """
        Clean column names to make them compatible with ClickHouse.
        Removes BOM characters, whitespace, and non-alphanumeric characters.
        
        Args:
            name: Raw column name
            
        Returns:
            Cleaned column name
        """
        # Remove BOM character if present
        name = name.replace('\ufeff', '')
        
        # Remove leading/trailing whitespace
        name = name.strip()
        
        # Replace spaces with underscores for better compatibility
        name = re.sub(r'\s+', '_', name)
        
        # Remove non-alphanumeric characters except underscores
        name = re.sub(r'[^\w]', '', name)
        
        return name
    
    def get_table_columns_with_types(self) -> Dict[str, str]:
        """
        Get column names and types from the table.
        
        Returns:
            Dictionary mapping column names to their types
        """
        try:
            # Execute DESCRIBE query
            result = self.client.query(f"DESCRIBE TABLE {self.table_name}")
            
            # Extract column names and types
            columns = {}
            for row in result.result_rows:
                columns[row[0]] = row[1]
            
            logger.info(f"Table columns with types: {columns}")
            return columns
        except Exception as e:
            logger.error(f"Error getting table columns: {e}")
            return {}
    
    def parse_csv(self, file_content: io.BytesIO) -> Tuple[List[str], List[List[Any]]]:
        """
        Parse CSV content into column names and data rows.
        
        Args:
            file_content: CSV file content as BytesIO
            
        Returns:
            Tuple of (column_names, data_rows)
        """
        try:
            # Decode the CSV content
            csv_text = file_content.getvalue().decode('utf-8')
            
            # Parse CSV
            reader = csv.reader(csv_text.splitlines())
            
            # Get header row (column names)
            column_names = next(reader)
            column_names = [self.clean_column_name(name) for name in column_names]
            
            # Log the cleaned column names
            logger.info(f"CSV column names: {column_names}")
            
            # Extract data rows
            data_rows = []
            row_count = 0
            
            for row in reader:
                if row_count >= self.max_rows:
                    logger.warning(f"Reached maximum row limit ({self.max_rows}). Truncating data.")
                    break
                
                # Add the row
                data_rows.append(row)
                row_count += 1
            
            logger.info(f"Parsed {row_count} rows from CSV")
            return column_names, data_rows
            
        except Exception as e:
            logger.error(f"Error parsing CSV: {e}")
            return [], []
    
    def parse_datetime(self, datetime_str: str) -> Optional[datetime.datetime]:
        """
        Parse a datetime string using multiple formats.
        
        Args:
            datetime_str: Datetime string to parse
            
        Returns:
            Datetime object or None if parsing failed
        """
        if not datetime_str:
            return None
        
        # Handle ISO 8601 format with Z (UTC) timezone indicator
        if datetime_str.endswith('Z'):
            try:
                # Remove the 'Z' and parse as UTC time
                dt_str = datetime_str[:-1]  # Remove the Z
                
                # Try parsing with different ISO formats
                for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f']:
                    try:
                        dt = datetime.datetime.strptime(dt_str, fmt)
                        # Since the Z indicates UTC time, we don't need to adjust for timezones
                        # ClickHouse will store it as local time
                        return dt
                    except ValueError:
                        continue
            except Exception as e:
                logger.warning(f"Error parsing ISO datetime with Z: {datetime_str} - {e}")
        
        # Try different datetime formats
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y',
        ]
        
        for fmt in formats:
            try:
                return datetime.datetime.strptime(datetime_str, fmt)
            except ValueError:
                continue
        
        # If all formats fail, log the error and return None
        logger.warning(f"Failed to parse datetime: {datetime_str}")
        return None
    
    def convert_value(self, value: str, column_type: str) -> Any:
        """
        Convert a value to the appropriate type based on the column type.
        
        Args:
            value: String value to convert
            column_type: ClickHouse column type
            
        Returns:
            Converted value
        """
        if not value:
            return None
        
        try:
            if column_type.startswith('DateTime'):
                return self.parse_datetime(value)
            elif column_type.startswith('Date'):
                dt = self.parse_datetime(value)
                if dt:
                    return dt.date()
                return None
            elif column_type.startswith(('Int', 'UInt')):
                return int(value)
            elif column_type.startswith(('Float', 'Double')):
                return float(value)
            else:
                return value
        except Exception as e:
            logger.warning(f"Error converting value '{value}' to type {column_type}: {e}")
            return None
    
    def filter_columns_and_convert_data(self, csv_columns: List[str], data_rows: List[List[Any]]) -> Tuple[List[str], List[List[Any]]]:
        """
        Filter columns and convert data to match the table structure.
        
        Args:
            csv_columns: Column names from CSV
            data_rows: Data rows from CSV
            
        Returns:
            Tuple of (filtered_columns, filtered_data)
        """
        try:
            # Get table columns with types
            self.column_types = self.get_table_columns_with_types()
            if not self.column_types:
                logger.error("Failed to get table columns with types")
                return csv_columns, data_rows
            
            # Create a mapping of CSV columns to table columns
            column_map = {}
            filtered_columns = []
            column_types_list = []
            
            # First try exact matching
            for i, csv_col in enumerate(csv_columns):
                if csv_col in self.column_types:
                    column_map[i] = len(filtered_columns)
                    filtered_columns.append(csv_col)
                    column_types_list.append(self.column_types[csv_col])
            
            # If no columns match, try case-insensitive matching
            if not column_map:
                table_columns_lower = {col.lower(): col for col in self.column_types.keys()}
                for i, csv_col in enumerate(csv_columns):
                    lower_csv_col = csv_col.lower()
                    if lower_csv_col in table_columns_lower:
                        table_col = table_columns_lower[lower_csv_col]
                        column_map[i] = len(filtered_columns)
                        filtered_columns.append(table_col)
                        column_types_list.append(self.column_types[table_col])
            
            if not column_map:
                logger.warning(f"No matching columns found between CSV and table. CSV: {csv_columns}, Table: {list(self.column_types.keys())}")
                return [], []
            
            # Filter and convert data rows
            filtered_data = []
            for row in data_rows:
                filtered_row = [None] * len(filtered_columns)
                for i, val in enumerate(row):
                    if i in column_map:
                        col_idx = column_map[i]
                        col_type = column_types_list[col_idx]
                        filtered_row[col_idx] = self.convert_value(val, col_type)
                filtered_data.append(filtered_row)
            
            logger.info(f"Filtered columns: {filtered_columns}")
            return filtered_columns, filtered_data
            
        except Exception as e:
            logger.error(f"Error filtering columns and converting data: {e}")
            return [], []
    
    def ingest(self, skip_table_creation: bool = False, **kwargs) -> bool:
        """
        Execute the Google Drive ingestion process:
        1. Download file from Google Drive
        2. Parse CSV
        3. Create table (if needed)
        4. Insert data using native client method
        5. Optimize table (if needed)
        
        Args:
            skip_table_creation: If True, skip the table creation step
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Download file from Google Drive
            file_content = self.download_file()
            if not file_content:
                return False
            
            # Parse CSV
            column_names, data_rows = self.parse_csv(file_content)
            if not column_names or not data_rows:
                return False
            
            # Create table if needed
            if not skip_table_creation:
                create_query = self.load_sql_file(self.create_table_sql)
                logger.info(f"Creating table using {self.create_table_sql}")
                self.client.command(create_query)
            
            # Filter columns and convert data to match table structure
            filtered_columns, filtered_data = self.filter_columns_and_convert_data(column_names, data_rows)
            if not filtered_columns or not filtered_data:
                return False
            
            # Insert data
            logger.info(f"Inserting {len(filtered_data)} rows into {self.table_name}")
            logger.info(f"Using columns: {filtered_columns}")

            # Log a sample of the filtered data
            if filtered_data:
                logger.info(f"Sample row: {filtered_data[0]}")

            count_before = self.get_row_count(self.table_name)
            logger.info(f"Row count before insert in {self.table_name}: {count_before}")

            self.client.insert(self.table_name, filtered_data, column_names=filtered_columns)

            count_after = self.get_row_count(self.table_name)
            rows_inserted = count_after - count_before
            logger.info(f"Row count after insert in {self.table_name}: {count_after}")
            logger.info(f"Rows inserted: {rows_inserted}")

            # Optimize if specified
            if self.optimize_sql:
                optimize_query = self.load_sql_file(self.optimize_sql)
                logger.info(f"Optimizing table using {self.optimize_sql}")
                self.client.command(optimize_query)
                
            return True
            
        except Exception as e:
            logger.error(f"Error in Google Drive ingestion: {e}")
            return False