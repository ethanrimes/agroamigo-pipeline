"""
Global utilities for parsing dates in various formats.
"""

import re
from datetime import datetime
import logging
import os

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


def filter_files_by_date(file_paths, start_date=None, end_date=None):
    """
    Filter files by date in filename using the pattern yyyymmdd.
    
    Args:
        file_paths (list): List of file paths to filter
        start_date (datetime): Optional start date for filtering (inclusive)
        end_date (datetime): Optional end date for filtering (inclusive)
        
    Returns:
        list: Filtered list of file paths
    """
    if not start_date and not end_date:
        logger.info("No date filter applied - returning all files")
        return file_paths
    
    start_date_str = start_date.strftime('%Y-%m-%d') if start_date else "earliest"
    end_date_str = end_date.strftime('%Y-%m-%d') if end_date else "latest"
    logger.info(f"Filtering files by date range: {start_date_str} to {end_date_str}")
    
    filtered_files = []
    skipped_files = []
    
    for file_path in file_paths:
        filename = os.path.basename(file_path)
        
        # Look for date pattern in filename (YYYYMMDD)
        date_match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
        
        if date_match:
            year, month, day = date_match.groups()
            try:
                file_date = datetime(int(year), int(month), int(day))
                
                # Check if file is within date range
                if start_date and file_date < start_date:
                    skipped_files.append((filename, file_date, "before start date"))
                    continue
                    
                if end_date and file_date > end_date:
                    skipped_files.append((filename, file_date, "after end date"))
                    continue
                
                # File is within date range, add to filtered list
                filtered_files.append(file_path)
                logger.debug(f"Included file in date range: {filename} ({file_date.strftime('%Y-%m-%d')})")
                
            except ValueError as e:
                # Handle invalid dates
                logger.warning(f"Skipping file with invalid date format: {filename} - {str(e)}")
                skipped_files.append((filename, None, "invalid date format"))
        else:
            # No date pattern found
            logger.debug(f"No date pattern found in filename: {filename}")
            if start_date is None and end_date is None:
                # If no date filters, include all files
                filtered_files.append(file_path)
            else:
                # If date filters are applied, skip files without dates
                skipped_files.append((filename, None, "no date in filename"))
    
    # Log summary
    if skipped_files:
        logger.info(f"Skipped {len(skipped_files)} files outside date range:")
        for filename, date, reason in skipped_files[:5]:  # Log only first 5 to avoid excessive logging
            date_str = date.strftime('%Y-%m-%d') if date else "N/A"
            logger.info(f"  - {filename}: {date_str} ({reason})")
        
        if len(skipped_files) > 5:
            logger.info(f"  ... and {len(skipped_files) - 5} more files")
    
    logger.info(f"Date filtering complete: {len(filtered_files)} files included, {len(skipped_files)} files excluded")
    return filtered_files