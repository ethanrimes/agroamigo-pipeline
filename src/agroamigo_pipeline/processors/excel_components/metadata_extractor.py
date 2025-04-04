"""
Excel metadata extraction module for extracting metadata from Excel files.
"""
import logging
import pandas as pd
from datetime import datetime

from agroamigo_pipeline.utils.utils import normalize_string, extract_date_from_spanish_text, format_date

class MetadataExtractor:
    """Extracts metadata from Excel files."""
    
    def __init__(self, logger=None):
        """
        Initialize the metadata extractor.
        
        Args:
            logger (Logger): Logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def extract_all(self, df, raw_df=None):
        """
        Extract all metadata from the DataFrame.
        
        Args:
            df (DataFrame): The prepared DataFrame
            raw_df (DataFrame, optional): The raw DataFrame for date extraction
            
        Returns:
            dict: Dictionary containing all extracted metadata
        """
        # Use raw_df for date extraction if provided, otherwise use df
        date_df = raw_df if raw_df is not None else df
        
        # Extract metadata
        date_str = self.extract_date_from_header(date_df)
        geography_info = self.extract_geography_from_columns(df)
        major_categories = self.identify_major_categories(df)
        product_category_map = self.map_products_to_categories(df, major_categories)
        
        # Return as dictionary
        return {
            'date': date_str or datetime.now().strftime('%Y-%m-%d'),
            'geography_info': geography_info,
            'major_categories': major_categories,
            'product_category_map': product_category_map
        }
    
    def extract_date_from_header(self, df):
        """
        Extract date from Excel file header according to anexo format.
        
        Args:
            df (DataFrame): DataFrame to check for date
            
        Returns:
            str: Date in YYYY-MM-DD format or None if not found
        """
        try:
            # Look for date in the first 15 rows
            for i in range(min(15, len(df))):
                row = df.iloc[i]
                
                # Convert row to string and check for date patterns
                row_str = ' '.join([str(val) for val in row.values if isinstance(val, (str, int, float))])
                
                date_obj = extract_date_from_spanish_text(row_str)
                if date_obj:
                    return format_date(date_obj)
            
            # If date not found in rows, check column names
            headers_str = ' '.join([str(col) for col in df.columns])
            date_obj = extract_date_from_spanish_text(headers_str)
            if date_obj:
                return format_date(date_obj)
            
            # If still not found, try to extract from filename
            return None
        
        except Exception as e:
            self.logger.warning(f"Error extracting date from header: {str(e)}")
            return None
    
    def extract_geography_from_columns(self, df):
        """
        Extract geography information (city/market) from column names.
        According to the anexo format, the column headers should contain city,market information.
        
        Args:
            df (DataFrame): DataFrame to analyze
            
        Returns:
            list: List of dictionaries with city and market information
        """
        geography_info = []
        
        try:
            # Skip first column (usually product/category)
            columns = df.columns[1:] if len(df.columns) > 1 else df.columns
            
            for col in columns:
                if not isinstance(col, str):
                    continue
                
                # Skip columns that are likely not geography related
                if any(keyword in normalize_string(col) for keyword in ['fecha', 'date', 'producto', 'product', 'var %']):
                    continue
                
                # Check if column contains a comma - this indicates city,market format
                if ',' in col:
                    parts = col.split(',', 1)
                    city = parts[0].strip()
                    market = parts[1].strip() if len(parts) > 1 else ""
                else:
                    # No comma - treat the whole string as city
                    city = col.strip()
                    market = ""
                
                # Skip empty values
                if not city:
                    continue
                
                # Add to geography info
                geography_entry = {'city': city, 'market': market, 'column': col}
                geography_info.append(geography_entry)
            
            # If no geography found, try to extract from other cells
            if not geography_info:
                for i in range(min(5, len(df))):
                    for col in df.columns:
                        cell_value = df.iloc[i][col]
                        if isinstance(cell_value, str) and ',' in cell_value:
                            parts = cell_value.split(',', 1)
                            city = parts[0].strip()
                            market = parts[1].strip() if len(parts) > 1 else ""
                            if city:
                                geography_info.append({'city': city, 'market': market, 'column': None})
            
            # If still no geography found, assign default
            if not geography_info:
                self.logger.warning("No geography information found, using default")
                geography_info.append({'city': 'Unknown', 'market': '', 'column': None})
                
            return geography_info
            
        except Exception as e:
            self.logger.warning(f"Error extracting geography from columns: {str(e)}")
            return [{'city': 'Unknown', 'market': '', 'column': None}]
    
    def identify_major_categories(self, df):
        """
        Identify major product categories from the DataFrame.
        In anexo format, major categories span the table and don't have price values.
        
        Args:
            df (DataFrame): DataFrame to analyze
            
        Returns:
            list: List of major category names
        """
        major_categories = []
        
        try:
            # Get first column, usually contains product names
            if len(df.columns) == 0:
                return []
                
            product_col = df.iloc[:, 0]
            
            # Identify rows that could be category headers
            for i, value in enumerate(product_col):
                if not isinstance(value, str):
                    continue
                
                value_clean = str(value).strip()
                
                # Skip empty values and common headers
                if not value_clean or value_clean.lower() in ['producto', 'product', 'item', 'descripcion', 'description']:
                    continue
                
                # Check if this is a category header (followed by products)
                is_category = False
                
                # If row has no values in other columns, likely a category
                if df.iloc[i, 1:].isna().all():
                    is_category = True
                    
                # If the text is in a category-like format
                category_keywords = ['frutas', 'verduras', 'hortalizas', 'tuberculos', 'platanos', 
                                    'granos', 'cereales', 'lacteos', 'carnes']
                if any(keyword in value_clean.lower() for keyword in category_keywords):
                    is_category = True
                
                if is_category and value_clean not in [cat for cat in major_categories]:
                    major_categories.append(value_clean)
            
            return major_categories
            
        except Exception as e:
            self.logger.warning(f"Error identifying major categories: {str(e)}")
            return []
    
    def map_products_to_categories(self, df, major_categories):
        """
        Map products to their respective categories.
        
        Args:
            df (DataFrame): DataFrame with product data
            major_categories (list): List of major category names
            
        Returns:
            dict: Dictionary mapping products to categories
        """
        product_category_map = {}
        
        try:
            if not major_categories:
                return {}
                
            # Get first column, usually contains product names
            product_col = df.iloc[:, 0]
            
            current_category = None
            
            # Map each product to its category
            for i, value in enumerate(product_col):
                if not isinstance(value, str):
                    continue
                
                value_clean = str(value).strip()
                
                # Skip empty values and common headers
                if not value_clean or value_clean.lower() in ['producto', 'product', 'item', 'descripcion', 'description']:
                    continue
                
                # Check if this is a category
                if value_clean in major_categories:
                    current_category = value_clean
                    continue
                
                # If we have a current category and the row has price values, it's a product
                if current_category and not df.iloc[i, 1:].isna().all():
                    product_category_map[value_clean] = current_category
            
            return product_category_map
            
        except Exception as e:
            self.logger.warning(f"Error mapping products to categories: {str(e)}")
            return {} 