"""
Module for extracting dates from Excel content, specifically for agricultural price data.
"""

import pandas as pd
import logging
import re
from datetime import datetime

from agroamigo_pipeline.utils.date_utils import parse_spanish_date

logger = logging.getLogger(__name__)

def extract_date_from_excel_content(df):
    """
    Attempt to find and extract a date from Excel content.
    
    Args:
        df (DataFrame): DataFrame containing the Excel content
        
    Returns:
        datetime or None: Extracted date or None if not found
    """
    # Look through the top rows for a date
    max_rows_to_check = min(20, len(df))
    
    for i in range(max_rows_to_check):
        row = df.iloc[i]
        
        # Check each cell in the row for a date
        for cell in row:
            if pd.isna(cell):
                continue
                
            cell_value = str(cell)
            
            # Try to parse as a date
            parsed_date = parse_spanish_date(cell_value)
            if parsed_date:
                logger.info(f"Found date: {parsed_date} from text: {cell_value}")
                return parsed_date
    
    # If we get here, no date was found
    logger.warning("No date found in Excel content")
    return None

def extract_date_from_raw_excel(file_path):
    """
    Extract date from raw Excel file before any processing.
    
    Args:
        file_path (str): Path to the Excel file
        
    Returns:
        datetime or None: Extracted date or None if not found
    """
    logger.info(f"Attempting to extract date from raw Excel file: {file_path}")
    
    try:
        # Read the first few rows of the Excel file without skipping rows
        df_raw = pd.read_excel(file_path, nrows=10)
        
        # Look for date patterns in the first few rows
        for i in range(min(10, len(df_raw))):
            row = df_raw.iloc[i]
            
            for cell in row:
                if pd.isna(cell):
                    continue
                
                cell_value = str(cell)
                
                # Spanish date pattern: "Lunes 24 de mayo de 2021"
                # Match days of the week followed by a date
                days_of_week = ['lunes', 'martes', 'miércoles', 'miercoles', 'jueves', 'viernes', 'sábado', 'sabado', 'domingo']
                months = ['enero', 'febrero', 'marzo', 'abril', 'mayo', 'junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']
                
                cell_lower = cell_value.lower()
                
                # Check if cell contains day of week and month name
                if any(day in cell_lower for day in days_of_week) and any(month in cell_lower for month in months):
                    # Try to parse with parse_spanish_date
                    parsed_date = parse_spanish_date(cell_value)
                    if parsed_date:
                        logger.info(f"Found date in raw Excel: {parsed_date} from text: {cell_value}")
                        return parsed_date
                    
                    # Additional regex pattern for dates like "Lunes 24 de mayo de 2021"
                    date_match = re.search(r'(\d{1,2})\s+de\s+([^\s]+)\s+de\s+(\d{4})', cell_lower)
                    if date_match:
                        day = int(date_match.group(1))
                        month_name = date_match.group(2)
                        year = int(date_match.group(3))
                        
                        # Map Spanish month names to numbers
                        month_map = {
                            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
                            'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
                            'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
                        }
                        
                        if month_name in month_map:
                            month = month_map[month_name]
                            try:
                                date_obj = datetime(year, month, day)
                                logger.info(f"Extracted date using regex: {date_obj} from text: {cell_value}")
                                return date_obj
                            except ValueError:
                                logger.warning(f"Invalid date components: day={day}, month={month}, year={year}")
                
                # Try to parse as datetime directly
                try:
                    date_obj = pd.to_datetime(cell_value, errors='raise')
                    if not pd.isna(date_obj):
                        logger.info(f"Parsed date directly: {date_obj} from text: {cell_value}")
                        return date_obj.to_pydatetime()
                except:
                    pass
                    
                # Check for date in filename
                if file_path:
                    filename = file_path.split('/')[-1]
                    # Extract date patterns from filename (common format YYYYMMDD)
                    date_match = re.search(r'(\d{8}|\d{6}|\d{4}\d{2}\d{2})', filename)
                    if date_match:
                        date_str = date_match.group(1)
                        try:
                            if len(date_str) == 8:  # YYYYMMDD
                                date_obj = datetime.strptime(date_str, '%Y%m%d')
                                logger.info(f"Extracted date from filename: {date_obj} from pattern: {date_str}")
                                return date_obj
                        except ValueError:
                            pass
        
        logger.warning("No date found in raw Excel file")
        return None
        
    except Exception as e:
        logger.error(f"Error extracting date from raw Excel: {str(e)}")
        return None