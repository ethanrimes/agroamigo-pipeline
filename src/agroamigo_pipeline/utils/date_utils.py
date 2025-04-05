"""
Global utilities for parsing dates in various formats.
"""

import re
from datetime import datetime
import logging
import os
from typing import Tuple

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



def parse_spanish_date_scraper(date_string: str) -> datetime:
    """
    Parse a Spanish date string into a datetime object.
    Handles various formats containing Spanish month names, abbreviations, and ISO dates.
    
    Args:
        date_string (str): Date string containing Spanish month name or date format
        
    Returns:
        datetime: Parsed datetime object
        
    Example inputs:
        "enero 2023"
        "15 de marzo 2023"
        "feb del 2023"
        "20 abr 2023"
        "2023-03-15"
        "octubre_09_2019"
        "nov_04_2022"
    """
    # Remove Spanish day names first
    SPANISH_DAYS = [
        'lunes', 'martes', 'miércoles', 'miercoles', 'jueves', 
        'viernes', 'sábado', 'sabado', 'domingo'
    ]
    cleaned_string = date_string.lower().strip()
    for day in SPANISH_DAYS:
        cleaned_string = cleaned_string.replace(day, '')
    
    # Remove known URL prefixes first
    for prefix in [
        'componente-precios-mayoristas-',
        'componente-precio-mayoristas-', 
        'componente-de-precios-mayoristas-',
        'componente-precios-de-mayoristas-',
        'anex-',
        'mayoristas'
    ]:
        cleaned_string = cleaned_string.replace(prefix, '')
    
    # Replace underscores with spaces for better parsing
    cleaned_string = cleaned_string.replace('_', ' ')
    
    # Dictionary mapping Spanish month names and abbreviations to numbers
    SPANISH_MONTHS = {
        'enero': 1, 'ene': 1,
        'febrero': 2, 'feb': 2,
        'marzo': 3, 'mar': 3,
        'abril': 4, 'abr': 4,
        'mayo': 5, 'may': 5,
        'junio': 6, 'jun': 6,
        'julio': 7, 'jul': 7,
        'agosto': 8, 'ago': 8,
        'septiembre': 9, 'sep': 9, 'sept': 9,
        'octubre': 10, 'oct': 10,
        'noviembre': 11, 'nov': 11,
        'diciembre': 12, 'dic': 12
    }
    
    working_string = cleaned_string
    
    # Try ISO date format first (yyyy-mm-dd)
    iso_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', working_string)
    if iso_match:
        year, month, day = map(int, iso_match.groups())
        return datetime(year, month, day)
    
    # Find the month using full names or abbreviations
    month = None
    matched_month_str = None
    
    # When searching for month names, ensure we match whole words only
    for spanish_month, month_num in SPANISH_MONTHS.items():
        if re.search(rf'\b{spanish_month}\b', working_string):
            month = month_num
            matched_month_str = spanish_month
            break
    
    if month:
        # Remove the matched month from working string
        working_string = working_string.replace(matched_month_str, ' ')
        
        # Find the year (4 digits)
        year_match = re.search(r'\b\d{4}\b', working_string)
        if not year_match:
            raise ValueError(f"No valid year found in: {cleaned_string}")
        year = int(year_match.group())
        
        # Remove the matched year from working string
        working_string = working_string.replace(year_match.group(), ' ')
        
        # Find the day (1 or 2 digits) in remaining string
        day_match = re.search(r'\b(\d{1,2})\b', working_string)
        day = int(day_match.group()) if day_match else 1
        
        # Validate day
        if day < 1 or day > 31:
            raise ValueError(f"Invalid day number: {day}")
        
        return datetime(year, month, day)
    
    # Try to parse date in format "month_dd_yyyy" or similar patterns
    date_pattern = re.search(r'(\w+)\s*(\d{1,2})\s*(\d{4})', working_string)
    if date_pattern:
        month_str, day_str, year_str = date_pattern.groups()
        # Try to find month in Spanish months
        for spanish_month, month_num in SPANISH_MONTHS.items():
            if month_str in spanish_month or spanish_month in month_str:
                month = month_num
                day = int(day_str)
                year = int(year_str)
                
                # Validate day
                if day < 1 or day > 31:
                    raise ValueError(f"Invalid day number: {day}")
                    
                return datetime(year, month, day)
    
    raise ValueError(f"No valid Spanish month or date format found in: {cleaned_string}")

def parse_spanish_month_year(text: str) -> Tuple[int, int]:
    """
    Parse a Spanish text containing month and year.
    
    Args:
        text (str): Text containing Spanish month and year
        
    Returns:
        tuple[int, int]: (month_number, year)
        
    Example inputs:
        "Componente precios mayoristas - Septiembre de 2014"
        "Componente precios mayoristas - Mayo 2016"
    """
    logger = logging.getLogger('scraper')
    
    # Dictionary of Spanish months
    SPANISH_MONTHS = {
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
        'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
        'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
    }
    
    logger.debug(f"Parsing month/year from text: '{text}'")
    
    # Remove "componente precios mayoristas" and convert to lowercase
    cleaned_text = text.lower().replace('componente precios mayoristas', '').replace('componente de precios mayoristas', '').replace('Información boletín diario SIPSA', '').strip(' -')
    logger.debug(f"Cleaned text: '{cleaned_text}'")
    
    # Find the month
    month_num = None
    for spanish_month, num in SPANISH_MONTHS.items():
        if spanish_month in cleaned_text:
            month_num = num
            logger.debug(f"Found month: '{spanish_month}' -> {month_num}")
            break
            
    if not month_num:
        logger.error(f"No valid Spanish month found in text. Valid months are: {list(SPANISH_MONTHS.keys())}")
        raise ValueError(f"No valid Spanish month found in: {cleaned_text}")
    
    # Find the year (4 digits)
    year_match = re.search(r'\b\d{4}\b', cleaned_text)
    if not year_match:
        logger.error(f"No 4-digit year found in text: '{cleaned_text}'")
        raise ValueError(f"No valid year found in: {cleaned_text}")
    year = int(year_match.group())
    logger.debug(f"Found year: {year}")
    
    logger.info(f"Successfully parsed month/year: {month_num}/{year}")
    return month_num, year