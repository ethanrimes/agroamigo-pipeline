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
        # Log the first 500 characters of received text
        self.logger.info(f"Received text (first 500 chars): {text[:500]}")
        
        product_data = []
        
        try:
            # Split text into lines
            lines = text.split('\n')
            self.logger.info(f"Split text into {len(lines)} lines")
            
            # Stack to track category hierarchy
            category_stack = ["", ""]  # [major_category, sub_category]
            self.logger.info("Initialized category stack")
            
            # Common category keywords - added more based on the PDF
            category_keywords = [
                'frutas', 'verduras', 'hortalizas', 'tuberculos', 'raices', 'platanos', 
                'granos', 'cereales', 'lacteos', 'carnes', 'leguminosas', 'citricos',
                'papa', 'yuca', 'pescados', 'tomates', 'cebollas', 'zanahorias'
            ]
            
            # Identify table headers - improved pattern to match the PDF format
            table_header_pattern = r'(producto\s+presentaci[oó]n\s+unidades\s+(?:ronda.*?)?m[ií]nimo\s+m[aá]ximo)'
            header_found = False
            current_major_category = ""
            current_sub_category = ""
            
            self.logger.info("Starting to process lines")
            for i, line in enumerate(lines):
                line = line.strip()
                
                # Skip empty lines and header markers
                if not line or line.lower() in ['producto', 'presentacion', 'unidades', 'mínimo', 'máximo']:
                    continue
                    
                # Check if this is a table header
                if re.search(table_header_pattern, line.lower()):
                    header_found = True
                    self.logger.info(f"Table header found at line {i}: {line}")
                    continue
                
                # Handle lines with just major categories (they're often standalone)
                # Major categories in the PDF are often single words or short phrases
                if len(line.split()) <= 3 and any(keyword in line.lower() for keyword in category_keywords):
                    current_major_category = line.strip()
                    current_sub_category = ""  # Reset sub-category when major category changes
                    category_stack = [current_major_category, current_sub_category]
                    self.logger.info(f"Found major category at line {i}: {current_major_category}")
                    continue
                
                # Check if line is likely a subcategory (indented or following major categories)
                # Subcategories are often formatted differently
                if not line[0].isdigit() and len(line.split()) <= 4 and not re.search(r'\d+\.\d+', line):
                    # Check if this line starts with "Otras" or specific subcategory markers
                    if (line.lower().startswith(('otras', 'otros')) or 
                        any(f"{keyword}" in line.lower() for keyword in category_keywords)):
                        if current_major_category:  # Only set subcategory if we have a major category
                            current_sub_category = line.strip()
                            category_stack = [current_major_category, current_sub_category]
                            self.logger.info(f"Found subcategory at line {i}: {current_sub_category}")
                            continue
                
                # Handle price data lines - Improved to match the PDF format with Ronda columns
                # This matches the standard format seen in the document
                price_pattern = r'(.+?)\s+(\S+)\s+(\S+)\s+(\d[\d.,]*)\s+(\d[\d.,]*)\s+(\d*)\s+(\d*)'
                price_match = re.search(price_pattern, line)
                
                if price_match:
                    product_name = price_match.group(1).strip()
                    presentation = price_match.group(2).strip()
                    units = price_match.group(3).strip()
                    min_price = self.clean_price_value(price_match.group(4))
                    max_price = self.clean_price_value(price_match.group(5))
                    
                    # Only process valid prices
                    if min_price is not None and max_price is not None:
                        # Don't add products that are actually category headers
                        if not any(keyword == product_name.lower() for keyword in category_keywords):
                            product_entry = {
                                'product': product_name,
                                'major_category': category_stack[0],
                                'sub_category': category_stack[1],
                                'presentation': presentation,
                                'units': units,
                                'daily_min': min_price,
                                'daily_max': max_price
                            }
                            product_data.append(product_entry)
                            self.logger.info(f"Added product {product_name} with daily_min={min_price}, daily_max={max_price}")
                    continue
                
                # If we get here, check if the line could be a category that wasn't caught above
                # This handles cases where categories don't precisely match our patterns
                if not header_found and any(keyword in line.lower() for keyword in category_keywords):
                    if line.isupper() or len(line) < 20:
                        current_major_category = line.strip()
                        category_stack[0] = current_major_category
                        self.logger.info(f"Found unlabeled major category: {current_major_category}")
                    else:
                        current_sub_category = line.strip()
                        category_stack[1] = current_sub_category
                        self.logger.info(f"Found unlabeled subcategory: {current_sub_category}")
            
            self.logger.info(f"Finished processing. Extracted {len(product_data)} products")
            return product_data
            
        except Exception as e:
            self.logger.warning(f"Error processing product data: {str(e)}")
            # Log the traceback for more detailed error information
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return []

    def clean_price_value(self, price_str):
        """
        Clean price strings and convert to float values.
        
        Args:
            price_str (str): Price string from PDF
            
        Returns:
            float: Cleaned price value or None if invalid
        """
        try:
            # Handle cases where price is "0" or empty
            if price_str in ["0", ""]:
                return 0.0
            
            # Remove non-numeric characters except decimal point
            cleaned = re.sub(r'[^\d.]', '', price_str.replace(',', '.'))
            if cleaned:
                return float(cleaned)
            return None
        except (ValueError, TypeError):
            return None
    
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
        filename = os.path.basename(file_path)
        self.logger.info(f"      -----> Processing text from: {filename}")
        
        try:
            # Process product data
            self.logger.info(f"      Parsing product data from text ({len(text)} characters)")
            product_data = self.process_product_data(text)
            self.logger.info(f"      Parsed {len(product_data)} product entries from text")
            
            # If no products found, return empty DataFrame
            if not product_data:
                self.logger.warning(f"      No product data extracted from PDF")
                return pd.DataFrame()
            
            # Convert to DataFrame
            self.logger.info(f"      Converting parsed data to DataFrame")
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
            self.logger.debug(f"      Created DataFrame with {len(output_df)} rows")
            
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
            self.logger.info(f"      Text processing completed in {duration:.2f} seconds, extracted {len(output_df)} rows")
            self.logger.info(f"      <----- Completed text processing from: {filename}")
            
            return output_df
            
        except Exception as e:
            self.logger.error(f"      Error processing extracted text from PDF {file_path}: {str(e)}")
            self.logger.debug(f"      Error details: {traceback.format_exc()}")
            self.logger.info(f"      <----- Failed text processing from: {filename}")
            return pd.DataFrame()
