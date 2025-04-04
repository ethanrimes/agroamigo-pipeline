"""
CSV parsing utilities for GPT-4o extracted data.

This module provides functions for parsing CSV data extracted by GPT-4o mini.
"""

import time
import pandas as pd
import logging
import traceback
import os

from agroamigo_pipeline.utils.utils import clean_price_value

class GPTCSVParser:
    """Parses CSV text extracted by GPT-4o mini into structured data."""
    
    def __init__(self, logger=None, timing_stats=None):
        """
        Initialize the GPT CSV parser.
        
        Args:
            logger (Logger): Logger instance
            timing_stats (dict): Dictionary to store timing statistics
        """
        self.logger = logger or logging.getLogger(__name__)
        self.timing_stats = timing_stats or {
            'data_processing': []
        }
    
    def process_csv_from_gpt4o(self, csv_text, metadata):
        """
        Process CSV text extracted by GPT-4o mini.
        
        Args:
            csv_text (str): CSV text extracted by GPT-4o mini
            metadata (dict): Metadata dictionary with date, city, market
            
        Returns:
            list: List of dictionaries with product data
        """
        product_data = []
        
        try:
            # Split text into lines
            lines = csv_text.strip().split('\n')
            
            # Track category hierarchy
            current_major_category = ""
            current_sub_category = ""
            
            for line in lines:
                # Skip empty lines
                if not line.strip():
                    continue
                
                # Split by semicolon
                parts = line.split(';')
                
                # Skip header line
                if parts[0].lower() == 'producto':
                    continue
                
                # If only one column has data, it might be a category
                if len(parts) > 1 and all(not part.strip() for part in parts[1:]):
                    category_line = parts[0].strip()
                    
                    # Determine if major or sub category
                    if category_line.isupper() or any(keyword in category_line.lower() for keyword in 
                                                    ['frutas', 'verduras', 'hortalizas', 'tuberculos']):
                        current_major_category = category_line
                        current_sub_category = ""
                    else:
                        current_sub_category = category_line
                    
                    continue
                
                # Extract product data
                if len(parts) >= 5:  # Ensure we have enough columns
                    product = parts[0].strip()
                    presentation = parts[1].strip() if len(parts) > 1 else ""
                    units = parts[2].strip() if len(parts) > 2 else ""
                    
                    # Try to extract prices
                    try:
                        # Format with Ronda 1 and Ronda 2
                        if len(parts) >= 7:
                            min1 = clean_price_value(parts[3])
                            max1 = clean_price_value(parts[4])
                            min2 = clean_price_value(parts[5])
                            max2 = clean_price_value(parts[6])
                            
                            # Calculate daily min and max
                            valid_prices = [p for p in [min1, min2, max1, max2] if p is not None and p > 0]
                            if valid_prices:
                                daily_min = min(valid_prices)
                                daily_max = max(valid_prices)
                            else:
                                continue  # Skip if no valid prices
                        
                        # Simple format with just min/max
                        else:
                            daily_min = clean_price_value(parts[3])
                            daily_max = clean_price_value(parts[4])
                            
                            if daily_min is None or daily_max is None:
                                continue  # Skip if no valid prices
                        
                        product_data.append({
                            'product': product,
                            'major_category': current_major_category,
                            'sub_category': current_sub_category,
                            'presentation': presentation,
                            'units': units,
                            'daily_min': daily_min,
                            'daily_max': daily_max
                        })
                    
                    except (ValueError, IndexError) as e:
                        self.logger.warning(f"Error processing price data for {product}: {str(e)}")
            
            return product_data
            
        except Exception as e:
            self.logger.warning(f"Error processing CSV from GPT-4o: {str(e)}")
            return []
    
    def process_gpt4o_results(self, file_path, csv_texts, metadata):
        """
        Process multiple CSV texts extracted by GPT-4o mini and combine into a single DataFrame.
        
        Args:
            file_path (str): Path to the original PDF file
            csv_texts (list): List of CSV texts extracted by GPT-4o mini
            metadata (dict): Metadata dictionary with date, city, market
            
        Returns:
            DataFrame: Processed data in standardized format
        """
        start_time = time.time()
        filename = os.path.basename(file_path)
        self.logger.info(f"      -----> Processing GPT-4o results for: {filename}")
        
        try:
            # Process each CSV text and combine results
            all_product_data = []
            self.logger.debug(f"      Processing {len(csv_texts)} CSV texts from GPT-4o mini")
            
            for i, csv_text in enumerate(csv_texts):
                if csv_text:
                    self.logger.debug(f"      Processing CSV text {i+1}/{len(csv_texts)} ({len(csv_text)} characters)")
                    product_data = self.process_csv_from_gpt4o(csv_text, metadata)
                    self.logger.debug(f"      Extracted {len(product_data)} product entries from CSV text {i+1}")
                    all_product_data.extend(product_data)
            
            # If no products found, return empty DataFrame
            if not all_product_data:
                self.logger.warning(f"      No product data extracted from PDF with GPT-4o mini")
                return pd.DataFrame()
            
            # Convert to DataFrame
            self.logger.debug(f"      Converting parsed data to DataFrame")
            output_data = []
            
            for product_entry in all_product_data:
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
            self.logger.info(f"      GPT-4o results processing completed in {duration:.2f} seconds, extracted {len(output_df)} rows")
            self.logger.info(f"      <----- Completed GPT-4o results processing for: {filename}")
            
            return output_df
            
        except Exception as e:
            self.logger.error(f"      Error processing GPT-4o results for PDF {file_path}: {str(e)}")
            self.logger.debug(f"      Error details: {traceback.format_exc()}")
            self.logger.info(f"      <----- Failed GPT-4o results processing for: {filename}")
            return pd.DataFrame()
