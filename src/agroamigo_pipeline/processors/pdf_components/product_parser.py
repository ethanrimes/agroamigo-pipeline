"""
Product data parsing utilities for PDF files.

This module provides functions for parsing product data from PDF text,
using a stack-based approach to track hierarchical category relationships.
"""

import re
import time
import pandas as pd
import logging
import traceback
import os

from agroamigo_pipeline.utils.utils import clean_price_value

class ProductParser:
    """Parses product data from PDF text using stack-based category tracking."""
    
    def __init__(self, logger=None, timing_stats=None):
        """
        Initialize the product parser.
        
        Args:
            logger (Logger): Logger instance
            timing_stats (dict): Dictionary to store timing statistics
        """
        self.logger = logger or logging.getLogger(__name__)
        self.timing_stats = timing_stats or {
            'data_processing': []
        }
    
    def process_product_data(self, text):
        """
        Process the text to extract product data using the stack-based approach.
        
        Args:
            text (str): Extracted text from PDF
            
        Returns:
            list: List of dictionaries with product data
        """
        product_data = []
        
        try:
            # Split text into lines
            lines = text.split('\n')
            
            # Stack to track category hierarchy
            category_stack = ["", ""]  # [major_category, sub_category]
            
            # Common category keywords
            category_keywords = ['frutas', 'verduras', 'hortalizas', 'tuberculos', 'raices', 'platanos', 
                               'granos', 'cereales', 'lacteos', 'carnes', 'leguminosas', 'citricos']
            
            # Identify table headers
            table_header_pattern = r'(producto\s+presentaci[oó]n\s+unidades\s+(?:ronda.*?)?m[ií]nimo\s+m[aá]ximo)'
            header_found = False
            
            for i, line in enumerate(lines):
                line = line.strip()
                
                # Skip empty lines and headers
                if not line or line.lower() in ['producto', 'presentacion', 'unidades']:
                    continue
                
                # Check if this is a table header
                if re.search(table_header_pattern, line.lower()):
                    header_found = True
                    continue
                
                # Skip lines before table header is found
                if not header_found:
                    continue
                
                # Check if this is a price data line
                # Pattern: Product [whitespace] Presentation [whitespace] Units [whitespace] Prices
                price_pattern = r'(.+?)\s+(\S+)\s+(\S+)\s+(\d[\d.,]*)\s+(\d[\d.,]*)'
                extended_price_pattern = r'(.+?)\s+(\S+)\s+(\S+)\s+(\d[\d.,]*)\s+(\d[\d.,]*)\s+(\d[\d.,]*)\s+(\d[\d.,]*)'
                
                # Try extended pattern first (includes Ronda 1 and Ronda 2)
                extended_match = re.search(extended_price_pattern, line)
                if extended_match:
                    product = extended_match.group(1).strip()
                    presentation = extended_match.group(2).strip()
                    units = extended_match.group(3).strip()
                    
                    # Extract prices from Ronda 1 and Ronda 2
                    min1 = clean_price_value(extended_match.group(4))
                    max1 = clean_price_value(extended_match.group(5))
                    min2 = clean_price_value(extended_match.group(6))
                    max2 = clean_price_value(extended_match.group(7))
                    
                    # Calculate daily min and max
                    valid_prices = [p for p in [min1, min2, max1, max2] if p is not None and p > 0]
                    if valid_prices:
                        daily_min = min(valid_prices)
                        daily_max = max(valid_prices)
                        
                        product_data.append({
                            'product': product,
                            'major_category': category_stack[0],
                            'sub_category': category_stack[1],
                            'presentation': presentation,
                            'units': units,
                            'daily_min': daily_min,
                            'daily_max': daily_max
                        })
                    continue
                
                # Try standard pattern (simpler format with just min/max)
                price_match = re.search(price_pattern, line)
                if price_match:
                    product = price_match.group(1).strip()
                    presentation = price_match.group(2).strip()
                    units = price_match.group(3).strip()
                    min_price = clean_price_value(price_match.group(4))
                    max_price = clean_price_value(price_match.group(5))
                    
                    if min_price is not None and max_price is not None:
                        product_data.append({
                            'product': product,
                            'major_category': category_stack[0],
                            'sub_category': category_stack[1],
                            'presentation': presentation,
                            'units': units,
                            'daily_min': min_price,
                            'daily_max': max_price
                        })
                    continue
                
                # If not a price line, check if it's a category header
                # Category headers typically don't have price data and might contain category keywords
                is_category = False
                for keyword in category_keywords:
                    if keyword in line.lower():
                        is_category = True
                        break
                
                if is_category:
                    # Check if it's a major or sub category
                    # If all caps or very short, it's likely a major category
                    if line.isupper() or len(line) < 20:
                        category_stack[0] = line  # Set major category
                        category_stack[1] = ""    # Reset sub category
                    else:
                        category_stack[1] = line  # Set sub category
                
                # Check for "Other" category patterns (Otras frutas, Otros tuberculos, etc.)
                elif line.lower().startswith('otro') and len(line) < 30:
                    category_stack[1] = line  # Set as sub category
            
            return product_data
            
        except Exception as e:
            self.logger.warning(f"Error processing product data: {str(e)}")
            return []
    
    def process_extracted_text(self, file_path, text, metadata):
        """
        Process extracted text and convert to structured data.
        
        Args:
            file_path (str): Path to the PDF file
            text (str): Extracted text
            metadata (dict): Metadata dictionary with date, city, market
            
        Returns:
            DataFrame: Processed data in standardized format
        """
        start_time = time.time()
        self.logger.info(f"Processing extracted text from {os.path.basename(file_path)}")
        
        try:
            # Process product data
            product_data = self.process_product_data(text)
            
            # If no products found, return empty DataFrame
            if not product_data:
                self.logger.warning(f"No product data extracted from PDF")
                return pd.DataFrame()
            
            # Convert to DataFrame
            output_data = []
            
            for product_entry in product_data:
                output_row = {
                    'date': metadata['date'],
                    'city': metadata['city'],
                    'market': metadata['market'],
                    'major_category': product_entry['major_category'],
                    'sub_category': product_entry['sub_category'],
                    'product': product_entry['product'],
                    'presentation': product_entry['presentation'],
                    'units': product_entry['units'],
                    'daily_min': product_entry['daily_min'],
                    'daily_max': product_entry['daily_max'],
                    'source': metadata['source']
                }
                
                output_data.append(output_row)
            
            # Create DataFrame from output data
            output_df = pd.DataFrame(output_data)
            
            # Ensure all required columns exist
            required_columns = [
                'date', 'city', 'market', 'major_category', 'sub_category', 
                'product', 'presentation', 'units', 'daily_min', 'daily_max', 'source'
            ]
            
            for col in required_columns:
                if col not in output_df.columns:
                    output_df[col] = None if col in ['daily_min', 'daily_max'] else ""
            
            duration = time.time() - start_time
            self.timing_stats['data_processing'].append({
                'pdf': os.path.basename(file_path),
                'method': 'text_processing',
                'rows': len(output_df),
                'duration': duration
            })
            self.logger.info(f"Text processing completed in {duration:.2f} seconds, extracted {len(output_df)} rows")
            
            return output_df
            
        except Exception as e:
            self.logger.error(f"Error processing extracted text from PDF {file_path}: {str(e)}")
            self.logger.debug(f"Error details: {traceback.format_exc()}")
            return pd.DataFrame()
