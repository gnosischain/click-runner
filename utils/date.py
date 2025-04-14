from datetime import datetime, timedelta
from typing import Optional, List

def get_yesterday_date() -> str:
    """
    Get yesterday's date in YYYY-MM-DD format.
    
    Returns:
        Yesterday's date as a string
    """
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

def get_date_range(start_date: str, end_date: str) -> List[str]:
    """
    Get a list of dates between start_date and end_date (inclusive).
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        List of dates in YYYY-MM-DD format
    """
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    date_list = []
    current = start
    
    while current <= end:
        date_list.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    
    return date_list

def is_valid_date_format(date_str: str) -> bool:
    """
    Check if a string is in valid YYYY-MM-DD format.
    
    Args:
        date_str: Date string to check
        
    Returns:
        True if valid, False otherwise
    """
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False