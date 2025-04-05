"""
Global utilities for parsing dates in various formats.
"""

import re
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def parse_spanish_date(text):
    """
    Parse a date in Spanish text format.
    
    Args:
        text (str): Text containing a Spanish date
        
    Returns:
        datetime or None: Parsed date object or None if parsing fails
    """
    if not isinstance(text, str):
        return None
    
    # Spanish month names to numbers mapping
    month_mapping = {
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
        'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
    }
    
    # Format: "viernes 15 de octubre de 2021" or similar
    pattern1 = r'(?:lunes|martes|miércoles|jueves|viernes|sábado|domingo)?\s*(\d{1,2})\s+de\s+([a-zé]+)\s+(?:de\s+)?(\d{4})'
    match = re.search(pattern1, text.lower())
    if match:
        day, month_name, year = match.groups()
        month = month_mapping.get(month_name, None)
        if month:
            try:
                return datetime(int(year), month, int(day))
            except ValueError as e:
                logger.warning(f"Invalid date components: {day}/{month}/{year}, Error: {e}")
    
    # Format: "15/10/2021" or "15-10-2021"
    pattern2 = r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})'
    match = re.search(pattern2, text)
    if match:
        day, month, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError as e:
            logger.warning(f"Invalid date components: {day}/{month}/{year}, Error: {e}")
    
    # Format: "2021/10/15" or "2021-10-15"
    pattern3 = r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})'
    match = re.search(pattern3, text)
    if match:
        year, month, day = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError as e:
            logger.warning(f"Invalid date components: {day}/{month}/{year}, Error: {e}")
    
    # If all parsing attempts fail, return None
    return None

def format_date(date_obj, format="%Y-%m-%d"):
    """
    Format a date object as a string.
    
    Args:
        date_obj (datetime): Date to format
        format (str): Date format string
        
    Returns:
        str: Formatted date string
    """
    if not date_obj:
        return None
    
    if isinstance(date_obj, str):
        try:
            date_obj = datetime.strptime(date_obj, "%Y-%m-%d")
        except ValueError:
            try:
                date_obj = datetime.strptime(date_obj, "%d/%m/%Y")
            except ValueError:
                return None
    
    return date_obj.strftime(format)