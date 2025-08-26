"""Utility functions for data processing and common operations."""
from datetime import date
from typing import Any, Optional


def parse_float(value: Any, default: float = 0.0) -> float:
    """
    Robust float parser for numeric strings like '34.200' or actual numbers.
    
    Args:
        value: Value to parse as float
        default: Default value if parsing fails
        
    Returns:
        Parsed float value or default
    """
    try:
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return float(value)
        return float(str(value).replace(",", "."))
    except Exception:
        return default


def get_today_str() -> str:
    """
    Return today's date as YYYY-MM-DD string.
    
    Returns:
        Today's date in ISO format
    """
    return date.today().isoformat()


def is_date_in_range(start_date: Optional[str], end_date: Optional[str], target_date: Optional[str] = None) -> bool:
    """
    Check if target date (defaults to today) is within the given date range.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        target_date: Target date to check (defaults to today)
        
    Returns:
        True if target date is within range, False otherwise
    """
    if target_date is None:
        target_date = get_today_str()
    
    try:
        start_ok = (start_date or "") <= target_date
        end_ok = target_date <= (end_date or "")
        return start_ok and end_ok
    except Exception:
        return False
