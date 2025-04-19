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
        self.logger = logger or logging.getLogger("pdf_processor")
        self.timing_stats = timing_stats or {
            'data_processing': []
        }

        self.valid_major_categories = [
            'Procesados',
            'Verduras y hortalizas',
            'Frutas',
            'Pescados',
            'Tuberculos, raices y platanos',
            'Granos y cereales',
            'Carnes',
            'Lacteos y huevos'
        ]

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
        self.logger.debug(f"      -----> Processing tables from: {filename}")
        
        try:
            self.logger.debug(f"Entered try catch block for table processing")
            
            processed_tables = []
            
            # List of header texts to ignore (case insensitive)
            header_text_to_ignore = ["producto", "precios de venta mayorista", "productos primera calidad"]
            
            # First, find header rows and clean up tables
            for table_idx, table in enumerate(tables):
                # self.logger.debug(f"Processing table {table_idx+1} of {len(tables)}")
                
                if table is None or len(table) == 0:
                    continue
                    
                # Find the header row (containing "producto")
                header_row_idx = -1
                for idx, value in enumerate(table.iloc[:, 0]):
                    if isinstance(value, str) and "producto" in str(value).lower():
                        header_row_idx = idx
                        # self.logger.debug(f"      Found header row at index {header_row_idx}")
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
                    # self.logger.debug(f"      Added processed table with {len(data_df)} rows")
                else:
                    self.logger.warning(f"      No data rows found after header in table {table_idx+1}")
            
            # Stack all tables with similar schema
            if not processed_tables:
                self.logger.warning(f"      No valid tables found after header detection")
                return pd.DataFrame()
                
            # Combine all tables
            all_data = pd.concat(processed_tables, ignore_index=True)
            self.logger.debug(f"      Combined all tables, total rows: {len(all_data)}")
            
            # Process the combined data
            category_stack = []  # Stack to track category hierarchy (max 2 items)
            product_data = []
            previous_row_had_prices = None  # Track if the previous row had prices
            
            # Iterate through data rows
            for i, row in all_data.iterrows():
                # Convert row to a list for easier processing
                row_list = row.tolist()
                
                # Log the current row
                # self.logger.debug(f"      Processing row {i}: {row_list}")
                # self.logger.debug(f"      Current stack state: {category_stack}")
                # self.logger.debug(f"      Previous row had prices: {previous_row_had_prices}")
                
                # Skip empty rows
                if all(pd.isna(cell) or str(cell).strip() == "" for cell in row_list):
                    # self.logger.debug(f"      Row {i} is empty, skipping")
                    continue
                
                # Get product name from first column
                product_name = str(row_list[0]).strip() if not pd.isna(row_list[0]) else ""
                
                # Skip empty product names
                if not product_name:
                    # self.logger.debug(f"      Row {i} has empty product name, skipping")
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
                            # self.logger.debug(f"      Row {i} has price data in column {j}: {price_str}")
                            break
                
                # If no price data, this is a category row - update the stack
                if not has_prices:
                    self.logger.debug(f"      Row {i} has no price data, treating as category: '{product_name}'")
                    
                    # Stack management logic
                    if len(category_stack) == 0:
                        # Empty stack - add as major category
                        category_stack.append(product_name)
                        # self.logger.debug(f"      Stack was empty -> Added major category: '{product_name}'")
                    elif len(category_stack) == 1:
                        # One item in stack - add as subcategory
                        category_stack.append(product_name)
                        # self.logger.debug(f"      Stack had one item -> Added subcategory: '{product_name}'")
                    elif len(category_stack) == 2:
                        # Two items in stack already
                        if previous_row_had_prices is True:
                            # Previous row had prices, update subcategory
                            category_stack[1] = product_name
                            # self.logger.debug(f"      Previous row had prices -> Updated subcategory to: '{product_name}'")
                        else:
                            # Previous row was also a category, shift stack
                            old_major = category_stack.pop(0)  # Remove old major
                            new_major = category_stack[0]      # Current subcategory becomes new major
                            category_stack = [new_major, product_name]  # Set new major and new sub
                            # self.logger.debug(f"      Previous row was a category -> Shifted stack: '{old_major}' out, '{new_major}' to major, '{product_name}' to sub")
                    
                    # self.logger.debug(f"      New stack state: {category_stack}")
                    previous_row_had_prices = False
                    continue
                
                ## ADD HERE
                # Check if the current row has prices and if the subcategory matches a valid major category
                if has_prices and len(category_stack) == 2:
                    # Normalize and check if the subcategory matches a valid major category
                    normalized_sub = category_stack[1].lower()
                    normalized_sub = re.sub(r'[áàäâ]', 'a', normalized_sub)
                    normalized_sub = re.sub(r'[éèëê]', 'e', normalized_sub)
                    normalized_sub = re.sub(r'[íìïî]', 'i', normalized_sub)
                    normalized_sub = re.sub(r'[óòöô]', 'o', normalized_sub)
                    normalized_sub = re.sub(r'[úùüû]', 'u', normalized_sub)
                    normalized_sub = re.sub(r'[,]', '', normalized_sub)
                    
                    normalized_major_categories = []
                    for category in self.valid_major_categories:
                        norm_cat = category.lower()
                        norm_cat = re.sub(r'[áàäâ]', 'a', norm_cat)
                        norm_cat = re.sub(r'[éèëê]', 'e', norm_cat)
                        norm_cat = re.sub(r'[íìïî]', 'i', norm_cat)
                        norm_cat = re.sub(r'[óòöô]', 'o', norm_cat)
                        norm_cat = re.sub(r'[úùüû]', 'u', norm_cat)
                        norm_cat = re.sub(r'[,]', '', norm_cat)
                        normalized_major_categories.append(norm_cat)
                    
                    if normalized_sub in normalized_major_categories:
                        # If subcategory matches a valid major category, make it the major category
                        # and set subcategory to empty string
                        self.logger.debug(f"      Subcategory '{category_stack[1]}' matches valid major category, promoting it")
                        category_stack = [category_stack[1], ""]
                        self.logger.debug(f"      Updated stack: {category_stack}")

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

                # Process min price from columns 3 and 5
                min_price_candidates = []
                if len(row_list) > 3 and not pd.isna(row_list[3]):
                    col3_price = self.clean_price_value(str(row_list[3]))
                    if col3_price is not None and col3_price >= 1:
                        min_price_candidates.append(col3_price)
                        
                if len(row_list) > 5 and not pd.isna(row_list[5]):
                    col5_price = self.clean_price_value(str(row_list[5]))
                    if col5_price is not None and col5_price >= 1:
                        min_price_candidates.append(col5_price)

                # Set min_price to the minimum of valid candidates if any exist
                if min_price_candidates:
                    min_price = min(min_price_candidates)
                    # self.logger.debug(f"      Min price: {min_price} from candidates: {min_price_candidates}")

                # Process max price from columns 4 and 6
                max_price_candidates = []
                if len(row_list) > 4 and not pd.isna(row_list[4]):
                    col4_price = self.clean_price_value(str(row_list[4]))
                    if col4_price is not None and col4_price >= 1:
                        max_price_candidates.append(col4_price)
                        
                if len(row_list) > 6 and not pd.isna(row_list[6]):
                    col6_price = self.clean_price_value(str(row_list[6]))
                    if col6_price is not None and col6_price >= 1:
                        max_price_candidates.append(col6_price)

                # Set max_price to the maximum of valid candidates if any exist
                if max_price_candidates:
                    max_price = max(max_price_candidates)
                    # self.logger.debug(f"      Max price: {max_price} from candidates: {max_price_candidates}")
                
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
                # self.logger.debug(f"      Added product: '{product_name}' in category '{major_category}' > '{sub_category}'")
                previous_row_had_prices = True
            
            # Log final state of stack
            # self.logger.debug(f"      Final stack state: {category_stack}")
            
            # Create DataFrame from collected product data
            if not product_data:
                self.logger.warning(f"      No product data extracted from tables")
                return pd.DataFrame()
                
            result_df = pd.DataFrame(product_data)
            self.logger.debug(f"      Created DataFrame with {len(result_df)} products")
            
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
                self.logger.debug(f"      Category distribution:")
                for _, row in category_counts.iterrows():
                    self.logger.debug(f"        - {row['major_category']} > {row['sub_category']}: {row['count']} products")
            
            self.logger.debug(f"      Table processing completed in {duration:.2f} seconds, extracted {len(result_df)} rows")
            self.logger.debug(f"      <----- Completed table processing from: {filename}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"      Error processing tables from PDF {file_path}: {str(e)}")
            self.logger.debug(f"      Error details: {traceback.format_exc()}")
            self.logger.debug(f"      <----- Failed table processing from: {filename}")
            return pd.DataFrame()