"""
Shared utility functions for the Colombian Agricultural Price Data Pipeline.
"""

import re
import unicodedata
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def normalize_string(text):
    """
    Normalize string by removing accents, extra spaces, and converting to lowercase.
    
    Args:
        text (str): Input text
        
    Returns:
        str: Normalized text
    """
    if not isinstance(text, str):
        return ""
    
    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text.strip())
    
    # Normalize Unicode characters
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
    
    # Convert to lowercase
    return text.lower()

def extract_date_from_spanish_text(text):
    """
    Extract date from Spanish text.
    
    Args:
        text (str): Text containing a date in Spanish
        
    Returns:
        datetime: Extracted date or None if no date found
    """
    # Map Spanish month names to numbers
    month_mapping = {
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
        'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
    }
    
    # Regular expressions for different date formats
    patterns = [
        # Format: 31 de Marzo de 2021
        r'(\d{1,2})\s+de\s+([a-zA-Záéíóúñ]+)\s+de\s+(\d{4})',
        # Format: Marzo 31 de 2021
        r'([a-zA-Záéíóúñ]+)\s+(\d{1,2})\s+de\s+(\d{4})',
        # Format: 31-03-2021 or 31/03/2021
        r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})',
        # Format: 2021-03-31 or 2021/03/31
        r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})'
    ]
    
    for pattern in patterns:
        matches = re.search(pattern, text, re.IGNORECASE)
        if matches:
            groups = matches.groups()
            
            if len(groups) == 3:
                # Handle different date formats
                if pattern == patterns[0]:  # 31 de Marzo de 2021
                    day = int(groups[0])
                    month_name = normalize_string(groups[1])
                    month = month_mapping.get(month_name, 1)  # Default to January if not found
                    year = int(groups[2])
                    
                elif pattern == patterns[1]:  # Marzo 31 de 2021
                    month_name = normalize_string(groups[0])
                    month = month_mapping.get(month_name, 1)
                    day = int(groups[1])
                    year = int(groups[2])
                    
                elif pattern == patterns[2]:  # 31-03-2021
                    day = int(groups[0])
                    month = int(groups[1])
                    year = int(groups[2])
                    
                elif pattern == patterns[3]:  # 2021-03-31
                    year = int(groups[0])
                    month = int(groups[1])
                    day = int(groups[2])
                
                try:
                    return datetime(year, month, day)
                except ValueError as e:
                    logger.warning(f"Invalid date: {e}")
                    continue
    
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
                date_obj = pd.to_datetime(date_obj)
            except:
                return None
    
    return date_obj.strftime(format)

def clean_text(text):
    """Clean and standardize text fields."""
    if text is None:
        return None
    text = re.sub(r'\s+', ' ', str(text)).strip()
    text = text.replace('Ó', 'O').replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ú', 'U')
    return format_text(text)

def format_text(text, preserve_case=False):
    """Format text to have first letter capitalized and rest lowercase."""
    if not text:
        return None
    text = str(text).strip()
    if len(text) == 0:
        return None
    if preserve_case:
        return text
    return text[0].upper() + text[1:].lower()

def normalize_text(text):
    """Remove accents and convert to lowercase for comparison"""
    if not text:
        return ""
    text = text.lower()
    replacements = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'ü': 'u', 'ñ': 'n'
    }
    for acc, non in replacements.items():
        text = text.replace(acc, non)
    return text

def extract_geography_info(text):
    """
    Extract city and market information from text.
    
    Args:
        text (str): Text containing geography information
        
    Returns:
        tuple: (city, market) or (None, None) if not found
    """
    # Common markets and their cities
    known_markets = {
        'corabastos': 'Bogotá',
        'paloquemao': 'Bogotá',
        'plaza minorista': 'Medellín',
        'plaza de mercado': 'Medellín',
        'cavasa': 'Cali',
        'santa helena': 'Cali',
        'barranquillita': 'Barranquilla',
        'granabastos': 'Barranquilla',
        'centroabastos': 'Bucaramanga',
        'surabastos': 'Neiva',
        'mercar': 'Armenia',
        'mercasa': 'Pereira',
        'central mayorista': 'Antioquia',
    }
    
    # Check if any known market is in the text
    text_lower = normalize_string(text)
    
    found_market = None
    for market, city in known_markets.items():
        if market in text_lower:
            found_market = market
            found_city = city
            break
    
    # If no known market found, try to extract city name
    if not found_market:
        # List of major Colombian cities
        major_cities = [
            'bogota', 'medellin', 'cali', 'barranquilla', 'cartagena', 
            'cucuta', 'bucaramanga', 'pereira', 'santa marta', 'ibague', 
            'pasto', 'manizales', 'neiva', 'villavicencio', 'armenia'
        ]
        
        for city in major_cities:
            if city in text_lower:
                found_city = city.title()
                
                # Try to extract market name from text
                market_pattern = r'(?:mercado|plaza|central|mayorista|abastos)\s+([a-zA-Z0-9\s]+)'
                market_match = re.search(market_pattern, text_lower)
                if market_match:
                    found_market = market_match.group(1).strip()
                else:
                    found_market = "Principal"
                break
    
    if found_market and found_city:
        # Capitalize properly
        found_market = found_market.title()
        found_city = found_city.title()
        return found_city, found_market
    
    return None, None

def detect_price_columns(df):
    """
    Detect columns that contain price data.
    
    Args:
        df (DataFrame): DataFrame to analyze
        
    Returns:
        list: List of column names that likely contain prices
    """
    price_columns = []
    
    # Check column names for price indicators
    price_keywords = ['precio', 'price', 'valor', 'pesos', 'cost', 'min', 'max', 'promedio', 'average']
    
    for col in df.columns:
        col_lower = normalize_string(str(col))
        # Check if column name contains price keywords
        if any(keyword in col_lower for keyword in price_keywords):
            price_columns.append(col)
            continue
        
        # Check if column contains numeric values and potential currency symbols
        if df[col].dtype in ['int64', 'float64'] or (
            df[col].apply(lambda x: isinstance(x, str) and any(c in x for c in ['$', '€', '£', 'COP', 'USD'])).any()
        ):
            sample_values = df[col].dropna().sample(min(5, len(df[col].dropna()))).astype(str).tolist()
            
            # Check if values look like prices
            if any('.' in val or ',' in val or val.isdigit() for val in sample_values):
                price_columns.append(col)
    
    return price_columns

def clean_price_value(price_str):
    """
    Clean price values, removing currency symbols and formatting.
    
    Args:
        price_str: Price value (can be string, int, float, etc.)
        
    Returns:
        float: Cleaned price value or None if invalid
    """
    if price_str is None:
        return None
    
    # If already numeric, return as is
    if isinstance(price_str, (int, float)):
        return float(price_str)
    
    # Convert to string for processing
    price_str = str(price_str).strip()
    
    # Remove currency symbols and thousands separators
    price_str = re.sub(r'[^\d.,]', '', price_str)
    
    # Handle different decimal and thousands separators
    if ',' in price_str and '.' in price_str:
        # If both are present, determine which is the decimal separator
        if price_str.rindex('.') > price_str.rindex(','):
            # Period is the decimal separator
            price_str = price_str.replace(',', '')
        else:
            # Comma is the decimal separator
            price_str = price_str.replace('.', '').replace(',', '.')
    elif ',' in price_str:
        # If only comma is present, assume it's the decimal separator
        price_str = price_str.replace(',', '.')
    
    # Convert to float
    try:
        return float(price_str)
    except ValueError:
        return None


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
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,'sepiembre': 9,
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