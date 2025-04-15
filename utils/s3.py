import logging
import boto3
from typing import List, Optional
from datetime import datetime

logger = logging.getLogger("click_runner")

def init_s3_client(access_key: str, secret_key: str, region: str = 'us-east-1'):
    """
    Initialize and return an S3 client.
    
    Args:
        access_key: AWS access key
        secret_key: AWS secret key
        region: AWS region, defaults to us-east-1
        
    Returns:
        boto3 S3 client
    """
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

def list_s3_files(
    bucket: str,
    prefix: str,
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1'
) -> List[str]:
    """
    List all files in an S3 bucket with the given prefix.
    
    Args:
        bucket: S3 bucket name
        prefix: Key prefix to filter by
        access_key: AWS access key
        secret_key: AWS secret key
        region: AWS region
        
    Returns:
        List of file keys
    """
    try:
        s3 = init_s3_client(access_key, secret_key, region)
        paginator = s3.get_paginator('list_objects_v2')
        
        result = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                result.extend([obj['Key'] for obj in page['Contents']])
        
        return result
    except Exception as e:
        logger.error(f"Error listing S3 files: {e}")
        return []

def get_latest_file(
    bucket: str,
    prefix: str,
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1'
) -> Optional[str]:
    """
    Get the most recent file in an S3 bucket with the given prefix.
    Assumes filenames are of format PREFIX/YYYY-MM-DD.parquet
    
    Args:
        bucket: S3 bucket name
        prefix: Key prefix to filter by
        access_key: AWS access key
        secret_key: AWS secret key
        region: AWS region
        
    Returns:
        Key of the most recent file, or None if no files found
    """
    try:
        logger.info(f"Listing files in {bucket}/{prefix}")
        files = list_s3_files(bucket, prefix, access_key, secret_key, region)
        
        if not files:
            logger.warning(f"No files found in S3 path: {bucket}/{prefix}")
            return None
        
        # Filter to only .parquet files
        parquet_files = [f for f in files if f.endswith('.parquet')]
        
        if not parquet_files:
            logger.warning(f"No .parquet files found in S3 path: {bucket}/{prefix}")
            return None
        
        # Log the first few files to help with debugging
        logger.info(f"Found {len(parquet_files)} parquet files. First few: {parquet_files[:3] if len(parquet_files) > 3 else parquet_files}")
        
        # Try to sort by the date in the filename
        # Assuming format: prefix/YYYY-MM-DD.parquet
        def extract_date(filename):
            try:
                date_str = filename.split('/')[-1].split('.')[0]
                return datetime.strptime(date_str, '%Y-%m-%d')
            except (ValueError, IndexError):
                # If we can't parse the date, use a minimal datetime
                logger.warning(f"Couldn't parse date from filename: {filename}")
                return datetime.min
        
        # Sort files by date
        sorted_files = sorted(parquet_files, key=extract_date, reverse=True)
        logger.info(f"Latest file: {sorted_files[0]}")
        return sorted_files[0]
    
    except Exception as e:
        logger.error(f"Error getting latest S3 file: {e}")
        return None