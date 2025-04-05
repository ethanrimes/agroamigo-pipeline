"""
Transform Excel data with the specified format into the target output format.
"""

import pandas as pd
import numpy as np
import logging
import re
from datetime import datetime

from agroamigo_pipeline.processors.excel_components.date_extractor import extract_date_from_excel_content
from agroamigo_pipeline.processors.excel_components.geography_parser import parse_geography_from_header, extract_geography_columns

logger = logging.getLogger(__name__)

class ExcelDataTransformer:
    """
    Transform Excel data from the columnar format where columns are city/market
    into the target output format.
    """
    
    def __init__(self, logger=None):
        """
        Initialize the transformer.
        
        Args:
            logger (Logger): Optional logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info("ExcelDataTransformer initialized")
    
    def identify_table_structure(self, df):
        """
        Identify the key structural elements of the table.
        
        Args:
            df (DataFrame): Input DataFrame
            
        Returns:
            dict: Dictionary with identified structure elements
        """
        structure = {
            'price_row_index': None,
            'first_product_row_index': None,
            'geography_columns': [],
            'price_columns': [],
            'var_columns': [],
            'product_column': None
        }
        
        # Identify the "Precio" row and the first product row
        for i, row in df.iterrows():
            row_values = [str(val).lower() if not pd.isna(val) else '' for val in row]
            row_text = ' '.join(row_values)
            
            # Find the row containing "Precio"
            if 'precio' in row_text:
                structure['price_row_index'] = i
                self.logger.info(f"Found 'Precio' row at index {i}")
                
                # The first product row should be right after the "Precio" row
                structure['first_product_row_index'] = i + 1
                break
        
        # If we couldn't find the price row, try another approach
        if structure['price_row_index'] is None:
            # Look for rows that might be category headers
            for i, row in df.iterrows():
                first_cell = str(row.iloc[0]).strip() if not pd.isna(row.iloc[0]) else ''
                # Categories often span the entire row and are in the first column
                if first_cell and len(first_cell) > 3 and pd.isna(row.iloc[1:]).all():
                    structure['first_product_row_index'] = i + 1
                    break
        
        # Identify geography columns (columns with city/market information)
        structure['geography_columns'] = extract_geography_columns(df)
        
        # When there's a clear price row, use it to identify price and variation columns
        if structure['price_row_index'] is not None:
            price_row = df.iloc[structure['price_row_index']]
            
            for col_idx, col_val in enumerate(price_row):
                if pd.isna(col_val):
                    continue
                
                col_val_str = str(col_val).lower()
                if 'precio' in col_val_str:
                    structure['price_columns'].append(df.columns[col_idx])
                elif 'var' in col_val_str:
                    structure['var_columns'].append(df.columns[col_idx])
        
        # If no price columns identified, assume columns that match geography columns are price columns
        if not structure['price_columns'] and structure['geography_columns']:
            structure['price_columns'] = structure['geography_columns']
        
        # Identify the product column (usually the first column)
        if len(df.columns) > 0:
            structure['product_column'] = df.columns[0]
        
        self.logger.info(f"Table structure identified: {structure}")
        return structure
    
    def extract_major_categories(self, df, structure):
        """
        Extract major category spans from the table.
        
        Args:
            df (DataFrame): Input DataFrame
            structure (dict): Table structure information
            
        Returns:
            dict: Dictionary mapping row indices to major categories
        """
        # Skip rows before the first product row
        if structure['first_product_row_index'] is None:
            self.logger.warning("No product row identified, assuming entire DataFrame contains products")
            return {}
        
        # Create a mapping of row indices to major categories
        row_to_category = {}
        current_category = None
        product_column = structure['product_column']
        
        # Start from the beginning to capture all categories
        for i, row in df.iterrows():
            # Skip rows that are part of the header
            if structure['first_product_row_index'] is not None and i < structure['first_product_row_index']:
                continue
                
            first_cell = row[product_column] if not pd.isna(row[product_column]) else None
            
            # Check if this row is a major category (spans the whole row or has distinct formatting)
            is_major_category = (
                first_cell is not None and 
                isinstance(first_cell, str) and
                len(first_cell.strip()) > 0 and
                pd.isna(row.iloc[1:]).all()  # All other cells in the row are empty
            )
            
            if is_major_category:
                current_category = first_cell.strip()
                self.logger.info(f"Found major category at row {i}: {current_category}")
            elif current_category is not None and i >= structure['first_product_row_index']:
                row_to_category[i] = current_category
        
        self.logger.info(f"Extracted {len(set(row_to_category.values()))} major categories")
        return row_to_category
    
    def transform_to_output_format(self, df, structure, row_to_category, date):
        """
        Transform the data into the target output format.
        
        Args:
            df (DataFrame): Input DataFrame
            structure (dict): Table structure information
            row_to_category (dict): Mapping of row indices to major categories
            date (datetime): Observation date
            
        Returns:
            DataFrame: Transformed data in target format
        """
        output_data = []
        
        # Get necessary structure elements
        product_column = structure['product_column']
        first_product_row = structure['first_product_row_index'] or 0
        geography_columns = structure['price_columns'] or structure['geography_columns']
        
        # Process each row that contains product data
        for i, row in df.iloc[first_product_row:].iterrows():
            # Skip rows that might be major categories or non-data rows
            product_name = row[product_column]
            if pd.isna(product_name) or (isinstance(product_name, str) and product_name.strip() == ''):
                continue
                
            # Skip rows with metadata or footnotes
            if isinstance(product_name, str) and any(x in product_name.lower() for x in ['*var', 'n.d', 'predominante']):
                continue
            
            # Get the major category for this row
            major_category = row_to_category.get(i, "")
            
            # Process each geographic column (city/market combination)
            for geo_col in geography_columns:
                # Skip if the column is not valid
                if geo_col not in df.columns:
                    continue
                
                # Parse city and market from column header
                city, market = parse_geography_from_header(str(geo_col))
                if not city:
                    continue
                
                # Get the price value
                price_value = row[geo_col]
                
                # Skip non-numeric or missing prices
                if pd.isna(price_value) or not self._is_valid_price(price_value):
                    continue
                
                # Convert price to float
                try:
                    price = float(price_value)
                except (ValueError, TypeError):
                    # Try to extract numeric part
                    price_str = str(price_value)
                    price_match = re.search(r'[\d.,]+', price_str)
                    if price_match:
                        price_str = price_match.group().replace(',', '.')
                        try:
                            price = float(price_str)
                        except ValueError:
                            continue
                    else:
                        continue
                
                # Create output row
                output_row = {
                    'date': date.strftime('%Y-%m-%d') if date else None,
                    'city': city,
                    'market': market or '',
                    'major_category': major_category,
                    'sub_category': '',  # No subcategory information available
                    'product': product_name,
                    'presentation': '',  # Presentation column should be blank
                    'units': '1 kilogramo',  # Units should be "1 kilogramo"
                    'daily_min': price,  # Same price for min and max
                    'daily_max': price,  # Same price for min and max
                    'source': 'anexo'  # Source should be "anexo"
                }
                
                output_data.append(output_row)
        
        # Create output DataFrame
        output_df = pd.DataFrame(output_data)
        
        # Ensure all required columns are present
        required_columns = [
            'date', 'city', 'market', 'major_category', 'sub_category', 
            'product', 'presentation', 'units', 'daily_min', 'daily_max', 'source'
        ]
        
        for col in required_columns:
            if col not in output_df.columns:
                output_df[col] = ''
        
        # Reorder columns to match required schema
        output_df = output_df[required_columns]
        
        self.logger.info(f"Transformed data to output format with {len(output_df)} rows")
        return output_df
    
    def _is_valid_price(self, value):
        """
        Check if a value is a valid price (numeric and not n.d.).
        
        Args:
            value: Value to check
            
        Returns:
            bool: True if value is a valid price, False otherwise
        """
        if pd.isna(value):
            return False
            
        # Check for n.d. and similar values
        if isinstance(value, str) and ('n.d' in value.lower() or value.strip() == ''):
            return False
            
        # Try to convert to float
        try:
            float_val = float(str(value).replace(',', '.'))
            return True
        except (ValueError, TypeError):
            return False
    
    def transform(self, df, file_path=None):
        """
        Main transformation method.
        
        Args:
            df (DataFrame): Input DataFrame
            file_path (str): Optional file path for logging
            
        Returns:
            DataFrame: Transformed data in target format
        """
        self.logger.info(f"Starting data transformation for {file_path or 'input DataFrame'}")
        
        # First, check if the DataFrame has the extracted_date attribute (set by the reader)
        date = None
        if hasattr(df, 'attrs') and 'extracted_date' in df.attrs:
            date = df.attrs['extracted_date']
            self.logger.info(f"Using pre-extracted date: {date}")
        
        # If not, try to extract date from the Excel content
        if not date:
            date = extract_date_from_excel_content(df)
            
        if not date:
            self.logger.warning("No date found in Excel content, using current date")
            date = datetime.now()
        
        # Identify table structure
        structure = self.identify_table_structure(df)
        
        # Extract major categories
        row_to_category = self.extract_major_categories(df, structure)
        
        # Transform data to output format
        output_df = self.transform_to_output_format(df, structure, row_to_category, date)
        
        self.logger.info(f"Transformation complete: {len(output_df)} rows generated")
        return output_df