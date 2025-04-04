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


    def process_table_data(self, file_path, tables, metadata):
        """
        Process extracted tables from pdfplumber using a stack-based approach for category tracking.
        
        Args:
            file_path (str): Path to the PDF file
            tables (list): List of tables extracted by pdfplumber
            metadata (dict): Metadata dictionary with date, city, market
            
        Returns:
            DataFrame: Processed data in standardized format
        """
        start_time = time.time()
        filename = os.path.basename(file_path)
        self.logger.info(f"      -----> Processing tables from: {filename}")
        
        try:
            self.logger.info(f"Entered try catch block for table processing")
            
            processed_tables = []
            
            # List of header texts to ignore (case insensitive)
            header_text_to_ignore = ["producto", "precios de venta mayorista", "productos primera calidad"]
            
            # First, find header rows and clean up tables
            for table_idx, table in enumerate(tables):
                self.logger.info(f"Processing table {table_idx+1} of {len(tables)}")
                
                if table is None or len(table) == 0:
                    continue
                    
                # Find the header row (containing "producto")
                header_row_idx = -1
                for idx, value in enumerate(table.iloc[:, 0]):
                    if isinstance(value, str) and "producto" in str(value).lower():
                        header_row_idx = idx
                        self.logger.info(f"      Found header row at index {header_row_idx}")
                        break
                
                if header_row_idx == -1:
                    # No header row found, skip this table
                    self.logger.warning(f"      No header row found in table {table_idx+1}, skipping")
                    continue
                    
                # Extract data starting from the row after the header
                header_names = table.iloc[header_row_idx].tolist()
                # Clean header names
                clean_headers = [str(h).strip() if h is not None else f"Column_{i}" 
                                for i, h in enumerate(header_names)]
                
                # Get data rows after header
                data_df = table.iloc[header_row_idx+1:].copy()
                data_df.columns = clean_headers
                
                # Add to processed tables
                if not data_df.empty:
                    processed_tables.append(data_df)
                    self.logger.info(f"      Added processed table with {len(data_df)} rows")
                else:
                    self.logger.warning(f"      No data rows found after header in table {table_idx+1}")
            
            # Stack all tables with similar schema
            if not processed_tables:
                self.logger.warning(f"      No valid tables found after header detection")
                return pd.DataFrame()
                
            # Combine all tables
            all_data = pd.concat(processed_tables, ignore_index=True)
            self.logger.info(f"      Combined all tables, total rows: {len(all_data)}")
            
            # Process the combined data
            category_stack = []  # Stack to track category hierarchy (max 2 items)
            product_data = []
            previous_row_had_prices = None  # Track if the previous row had prices
            
            # Iterate through data rows
            for i, row in all_data.iterrows():
                # Convert row to a list for easier processing
                row_list = row.tolist()
                
                # Log the current row
                self.logger.debug(f"      Processing row {i}: {row_list}")
                self.logger.debug(f"      Current stack state: {category_stack}")
                self.logger.debug(f"      Previous row had prices: {previous_row_had_prices}")
                
                # Skip empty rows
                if all(pd.isna(cell) or str(cell).strip() == "" for cell in row_list):
                    self.logger.debug(f"      Row {i} is empty, skipping")
                    continue
                
                # Get product name from first column
                product_name = str(row_list[0]).strip() if not pd.isna(row_list[0]) else ""
                
                # Skip empty product names
                if not product_name:
                    self.logger.debug(f"      Row {i} has empty product name, skipping")
                    continue
                
                # Skip header-related text
                if any(text.lower() in product_name.lower() for text in header_text_to_ignore):
                    self.logger.debug(f"      Row {i} contains header text: '{product_name}', skipping")
                    continue
                
                # Check if row has price data (columns 3 and 4 typically)
                has_prices = False
                price_cols = [j for j in range(3, min(len(row_list), 6))]
                for j in price_cols:
                    if j < len(row_list):
                        price_str = str(row_list[j]).strip() if not pd.isna(row_list[j]) else ""
                        if price_str and (price_str.replace('.', '').isdigit() or price_str.replace(',', '').isdigit()):
                            has_prices = True
                            self.logger.debug(f"      Row {i} has price data in column {j}: {price_str}")
                            break
                
                # If no price data, this is a category row - update the stack
                if not has_prices:
                    self.logger.debug(f"      Row {i} has no price data, treating as category: '{product_name}'")
                    
                    # Stack management logic
                    if len(category_stack) == 0:
                        # Empty stack - add as major category
                        category_stack.append(product_name)
                        self.logger.info(f"      Stack was empty -> Added major category: '{product_name}'")
                    elif len(category_stack) == 1:
                        # One item in stack - add as subcategory
                        category_stack.append(product_name)
                        self.logger.info(f"      Stack had one item -> Added subcategory: '{product_name}'")
                    elif len(category_stack) == 2:
                        # Two items in stack already
                        if previous_row_had_prices is True:
                            # Previous row had prices, update subcategory
                            category_stack[1] = product_name
                            self.logger.info(f"      Previous row had prices -> Updated subcategory to: '{product_name}'")
                        else:
                            # Previous row was also a category, shift stack
                            old_major = category_stack.pop(0)  # Remove old major
                            new_major = category_stack[0]      # Current subcategory becomes new major
                            category_stack = [new_major, product_name]  # Set new major and new sub
                            self.logger.info(f"      Previous row was a category -> Shifted stack: '{old_major}' out, '{new_major}' to major, '{product_name}' to sub")
                    
                    self.logger.debug(f"      New stack state: {category_stack}")
                    previous_row_had_prices = False
                    continue
                
                # Get current categories from stack
                major_category = category_stack[0] if len(category_stack) > 0 else ""
                sub_category = category_stack[1] if len(category_stack) > 1 else ""
                
                self.logger.debug(f"      Row {i} has price data, using categories - Major: '{major_category}', Sub: '{sub_category}'")
                
                # Process product row
                presentation = str(row_list[1]).strip() if len(row_list) > 1 and not pd.isna(row_list[1]) else ""
                units = str(row_list[2]).strip() if len(row_list) > 2 and not pd.isna(row_list[2]) else ""
                
                # Extract price values
                min_price = None
                max_price = None
                
                if len(row_list) > 3 and not pd.isna(row_list[3]):
                    min_price = self.clean_price_value(str(row_list[3]))
                    self.logger.debug(f"      Min price: {min_price}")
                
                if len(row_list) > 4 and not pd.isna(row_list[4]):
                    max_price = self.clean_price_value(str(row_list[4]))
                    self.logger.debug(f"      Max price: {max_price}")
                
                # If we don't have both prices, skip this row
                if min_price is None or max_price is None:
                    self.logger.debug(f"      Row {i} missing price data, skipping")
                    continue
                
                # Create product entry
                product_entry = {
                    'date': metadata['date'],
                    'city': metadata['city'],
                    'market': metadata['market'],
                    'major_category': major_category,
                    'sub_category': sub_category,
                    'product': product_name,
                    'presentation': presentation,
                    'units': units,
                    'daily_min': min_price,
                    'daily_max': max_price,
                    'source': metadata['source']
                }
                
                product_data.append(product_entry)
                self.logger.info(f"      Added product: '{product_name}' in category '{major_category}' > '{sub_category}'")
                previous_row_had_prices = True
            
            # Log final state of stack
            self.logger.info(f"      Final stack state: {category_stack}")
            
            # Create DataFrame from collected product data
            if not product_data:
                self.logger.warning(f"      No product data extracted from tables")
                return pd.DataFrame()
                
            result_df = pd.DataFrame(product_data)
            self.logger.info(f"      Created DataFrame with {len(result_df)} products")
            
            # Ensure all required columns exist
            required_columns = [
                'date', 'city', 'market', 'major_category', 'sub_category', 
                'product', 'presentation', 'units', 'daily_min', 'daily_max', 'source'
            ]
            
            for col in required_columns:
                if col not in result_df.columns:
                    if col in ['daily_min', 'daily_max']:
                        result_df[col] = pd.NA
                        self.logger.debug(f"      Added missing numeric column: {col}")
                    else:
                        result_df[col] = ""
                        self.logger.debug(f"      Added missing text column: {col}")
            
            duration = time.time() - start_time
            self.timing_stats['data_processing'].append({
                'pdf': os.path.basename(file_path),
                'method': 'table_processing',
                'rows': len(result_df),
                'duration': duration
            })
            
            # Log category distribution
            if not result_df.empty:
                category_counts = result_df.groupby(['major_category', 'sub_category']).size().reset_index(name='count')
                self.logger.info(f"      Category distribution:")
                for _, row in category_counts.iterrows():
                    self.logger.info(f"        - {row['major_category']} > {row['sub_category']}: {row['count']} products")
            
            self.logger.info(f"      Table processing completed in {duration:.2f} seconds, extracted {len(result_df)} rows")
            self.logger.info(f"      <----- Completed table processing from: {filename}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"      Error processing tables from PDF {file_path}: {str(e)}")
            self.logger.debug(f"      Error details: {traceback.format_exc()}")
            self.logger.info(f"      <----- Failed table processing from: {filename}")
            return pd.DataFrame()