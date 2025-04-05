"""
Module for parsing geography information from Excel column headers.
"""

import logging

logger = logging.getLogger(__name__)

def parse_geography_from_header(header_text):
    """
    Parse city and market information from a column header.
    
    Args:
        header_text (str): Column header text
        
    Returns:
        tuple: (city, market) where market may be None
    """
    if not isinstance(header_text, str):
        return None, None
    
    # Clean the header text
    header_text = header_text.strip()
    
    # Check if the header contains a comma, which separates city and market
    if ',' in header_text:
        parts = header_text.split(',', 1)
        city = parts[0].strip()
        market = parts[1].strip() if len(parts) > 1 and parts[1].strip() else None
        return city, market
    
    # If no comma, assume it's just a city
    return header_text, None

def extract_geography_columns(df):
    """
    Identify columns that contain geography information (city/market).
    
    Args:
        df (DataFrame): DataFrame to analyze
        
    Returns:
        list: List of column names that contain geography information
    """
    geography_columns = []
    
    # Check each column
    for col in df.columns:
        col_str = str(col).strip()
        
        # Skip columns that are clearly not geography related
        skip_keywords = ['precio', 'var', 'unnamed', 'fecha', 'date', 'product']
        if any(keyword in col_str.lower() for keyword in skip_keywords):
            continue
        
        # If the column has a comma or looks like a place name, consider it a geography column
        if ',' in col_str or len(col_str) > 3:  # Basic heuristic for place names
            geography_columns.append(col)
    
    logger.info(f"Identified {len(geography_columns)} potential geography columns: {geography_columns}")
    return geography_columns