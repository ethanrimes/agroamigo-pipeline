"""
CSV parsing utilities for GPT-4o extracted data.

This module provides functions for parsing CSV data extracted by GPT-4.1 nano.
"""

import time
import pandas as pd
import logging
import traceback
import os
import json
import re

from agroamigo_pipeline.utils.utils import clean_price_value

class GPTCSVParser:
    """Parses CSV text extracted by GPT-4.1 nano into structured data."""
    
    def __init__(self, logger=None, timing_stats=None, save_csv_data=False):
        """
        Initialize the GPT CSV parser.
        
        Args:
            logger (Logger): Logger instance
            timing_stats (dict): Dictionary to store timing statistics
            save_csv_data (bool): Whether to save CSV data to a permanent directory
        """
        self.logger = logger or logging.getLogger("pdf_processor")
        self.timing_stats = timing_stats or {
            'data_processing': []
        }
        self.save_csv_data = save_csv_data
        
        # Create permanent directory for saved CSV data if requested
        if self.save_csv_data:
            self.csv_data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                           "logs", "permanent_csv_data")
            os.makedirs(self.csv_data_dir, exist_ok=True)
            self.logger.debug(f"Will save CSV data to permanent directory: {self.csv_data_dir}")
    
    def process_csv_from_gpt4o(self, csv_text, metadata, file_path=None):
        """
        Process CSV text extracted by GPT-4.1 nano using stack-based category tracking.
        
        Args:
            csv_text (str): CSV text extracted by GPT-4.1 nano
            metadata (dict): Metadata dictionary with date, city, market
            file_path (str): Optional, path to the original file for logging
        
        Returns:
            list: List of dictionaries with product data
        """
        product_data = []
        
        # Log the input CSV text
        if file_path:
            self.logger.debug(f"{file_path} - Processing CSV text from GPT-4.1 nano - {len(csv_text)} characters")
            self.logger.debug(f"{file_path} - CSV text to process:\n{csv_text}")
        else:
            self.logger.debug(f"Processing CSV text from GPT-4.1 nano - {len(csv_text)} characters")
            self.logger.debug(f"CSV text to process:\n{csv_text}")
        
        if csv_text:
            # Remove opening triple backticks or "plaintext"/"text"
            csv_text = re.sub(r'^(?:```|plaintext|text)\s*\n', '', csv_text)
            # Remove closing triple backticks or "plaintext"/"text"
            csv_text = re.sub(r'\n(?:```|plaintext|text)\s*$', '', csv_text)

        num_lines = csv_text.count('\n') + 1 if csv_text else 0
        if file_path:
            self.logger.debug(f"{file_path} - Number of lines in CSV text after cleaning: {num_lines}")
        else:
            self.logger.debug(f"Number of lines in CSV text after cleaning: {num_lines}")
        
        # Save to permanent directory if requested
        if self.save_csv_data:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            permanent_csv_file = os.path.join(self.csv_data_dir, f"permanent_csv_text_{timestamp}.txt")
            try:
                with open(permanent_csv_file, "w", encoding="utf-8") as f:
                    f.write(f"CSV text ({len(csv_text)} characters):\n")
                    f.write(f"Lines: {num_lines}\n\n")
                    f.write(csv_text)
                if file_path:
                    self.logger.info(f"{file_path} - Saved permanent copy of CSV text to: {permanent_csv_file}")
                else:
                    self.logger.info(f"Saved permanent copy of CSV text to: {permanent_csv_file}")
            except Exception as e:
                if file_path:
                    self.logger.warning(f"{file_path} - Failed to save permanent copy of CSV text: {str(e)}")
                else:
                    self.logger.warning(f"Failed to save permanent copy of CSV text: {str(e)}")

        try:
            # Split text into lines
            lines = csv_text.strip().split('\n')
            if file_path:
                self.logger.info(f"{file_path} - Split CSV text into {len(lines)} lines")
            else:
                self.logger.info(f"Split CSV text into {len(lines)} lines")
            
            # Initialize the category stack (max 2 items)
            # First element is major category, second is subcategory
            category_stack = []
            
            # Track if previous row had price data
            previous_row_had_prices = None
            
            # Processing statistics
            processed_categories = 0
            processed_products = 0
            skipped_lines = 0
            price_errors = 0
            
            for line_idx, line in enumerate(lines):
                if file_path:
                    self.logger.debug(f"{file_path} - Processing line {line_idx+1}: {line}")
                else:
                    self.logger.debug(f"Processing line {line_idx+1}: {line}")
                # Skip empty lines
                if not line.strip():
                    if file_path:
                        self.logger.debug(f"{file_path} - Skipping empty line {line_idx+1}")
                    else:
                        self.logger.debug(f"Skipping empty line {line_idx+1}")
                    skipped_lines += 1
                    continue
                
                # Split by semicolon
                parts = line.split(';')
                
                # Skip header line
                if parts[0].lower() == 'producto':
                    if file_path:
                        self.logger.debug(f"{file_path} - Skipping header line: {line}")
                    else:
                        self.logger.debug(f"Skipping header line: {line}")
                    skipped_lines += 1
                    continue
                
                # Check if this is a category line (only first column has data)
                is_category_line = len(parts) > 1 and all(not part.strip() for part in parts[1:])
                
                if is_category_line:
                    category_name = parts[0].strip()
                    if file_path:
                        self.logger.debug(f"{file_path} - Found category line: '{category_name}'")
                    else:
                        self.logger.debug(f"Found category line: '{category_name}'")
                    
                    # Stack management according to the requirements
                    if len(category_stack) == 0:
                        # Empty stack - add as major category
                        category_stack.append(category_name)
                        if file_path:
                            self.logger.debug(f"{file_path} - Stack was empty -> Added major category: '{category_name}'")
                        else:
                            self.logger.debug(f"Stack was empty -> Added major category: '{category_name}'")
                    elif len(category_stack) == 1:
                        # One item in stack - add as subcategory
                        category_stack.append(category_name)
                        if file_path:
                            self.logger.debug(f"{file_path} - Stack had one item -> Added subcategory: '{category_name}'")
                        else:
                            self.logger.debug(f"Stack had one item -> Added subcategory: '{category_name}'")
                    elif len(category_stack) == 2:
                        # Two items in stack already
                        if previous_row_had_prices is True:
                            # Previous row had prices, update subcategory
                            category_stack[1] = category_name
                            if file_path:
                                self.logger.debug(f"{file_path} - Previous row had prices -> Updated subcategory to: '{category_name}'")
                            else:
                                self.logger.debug(f"Previous row had prices -> Updated subcategory to: '{category_name}'")
                        else:
                            # Previous row was also a category, shift stack
                            old_major = category_stack.pop(0)  # Remove old major
                            new_major = category_stack[0]      # Current subcategory becomes new major
                            category_stack = [new_major, category_name]  # Set new major and new sub
                            if file_path:
                                self.logger.debug(f"{file_path} - Previous row was a category -> Shifted stack: '{old_major}' out, '{new_major}' to major, '{category_name}' to sub")
                            else:
                                self.logger.debug(f"Previous row was a category -> Shifted stack: '{old_major}' out, '{new_major}' to major, '{category_name}' to sub")
                    
                    processed_categories += 1
                    previous_row_had_prices = False
                    continue
                
                # This is a product line with price data
                product = parts[0].strip()
                presentation = parts[1].strip() if len(parts) > 1 else ""
                units = parts[2].strip() if len(parts) > 2 else ""
                
                # Try to extract prices
                try:
                    # Get current categories from stack
                    major_category = category_stack[0] if len(category_stack) > 0 else ""
                    sub_category = category_stack[1] if len(category_stack) > 1 else ""
                    
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
                            if file_path:
                                self.logger.debug(f"{file_path} - Extracted product '{product}' with prices - min1: {min1}, max1: {max1}, min2: {min2}, max2: {max2} -> daily min: {daily_min}, daily max: {daily_max}")
                            else:
                                self.logger.debug(f"Extracted product '{product}' with prices - min1: {min1}, max1: {max1}, min2: {min2}, max2: {max2} -> daily min: {daily_min}, daily max: {daily_max}")
                        else:
                            if file_path:
                                self.logger.warning(f"{file_path} - No valid prices found for product '{product}' in line: {line}")
                            else:
                                self.logger.warning(f"No valid prices found for product '{product}' in line: {line}")
                            price_errors += 1
                            continue  # Skip if no valid prices
                    
                    # Simple format with just min/max
                    else:
                        daily_min = clean_price_value(parts[3])
                        daily_max = clean_price_value(parts[4])
                        
                        if daily_min is None or daily_max is None:
                            if file_path:
                                self.logger.warning(f"{file_path} - Invalid price data for product '{product}' in line: {line}")
                            else:
                                self.logger.warning(f"Invalid price data for product '{product}' in line: {line}")
                            price_errors += 1
                            continue  # Skip if no valid prices
                        
                        if file_path:
                            self.logger.debug(f"{file_path} - Extracted product '{product}' with prices - min: {daily_min}, max: {daily_max}")
                        else:
                            self.logger.debug(f"Extracted product '{product}' with prices - min: {daily_min}, max: {daily_max}")
                    
                    product_data.append({
                        'product': product,
                        'major_category': major_category,
                        'sub_category': sub_category,
                        'presentation': presentation,
                        'units': units,
                        'daily_min': daily_min,
                        'daily_max': daily_max
                    })
                    
                    if file_path:
                        self.logger.debug(f"{file_path} - Added product '{product}' in category '{major_category}' > '{sub_category}'")
                    else:
                        self.logger.debug(f"Added product '{product}' in category '{major_category}' > '{sub_category}'")
                    processed_products += 1
                    previous_row_had_prices = True
                
                except (ValueError, IndexError) as e:
                    if file_path:
                        self.logger.warning(f"{file_path} - Error processing price data for '{product}' in line {line_idx+1}: {str(e)}")
                    else:
                        self.logger.warning(f"Error processing price data for '{product}' in line {line_idx+1}: {str(e)}")
                    price_errors += 1
                    previous_row_had_prices = False
            
            # Log processing summary
            if file_path:
                self.logger.debug(f"{file_path} - CSV processing summary: {processed_products} products, {processed_categories} categories, {skipped_lines} skipped lines, {price_errors} price errors")
                self.logger.debug(f"{file_path} - Final category stack state: {category_stack}")
            else:
                self.logger.debug(f"CSV processing summary: {processed_products} products, {processed_categories} categories, {skipped_lines} skipped lines, {price_errors} price errors")
                self.logger.debug(f"Final category stack state: {category_stack}")
            
            return product_data
            
        except Exception as e:
            if file_path:
                self.logger.error(f"{file_path} - Error processing CSV from GPT-4o: {str(e)}")
                self.logger.debug(f"{file_path} - Error details: {traceback.format_exc()}")
            else:
                self.logger.error(f"Error processing CSV from GPT-4o: {str(e)}")
                self.logger.debug(f"Error details: {traceback.format_exc()}")
            return []

    def process_gpt4o_results(self, file_path, csv_texts, metadata):
        """
        Process multiple CSV texts extracted by GPT-4.1 nano by combining them first,
        then parsing the combined text to maintain continuity between pages.
        
        Args:
            file_path (str): Path to the original PDF file
            csv_texts (list): List of CSV texts extracted by GPT-4.1 nano
            metadata (dict): Metadata dictionary with date, city, market
                
        Returns:
            DataFrame: Processed data in standardized format
        """
        start_time = time.time()
        filename = os.path.basename(file_path)
        self.logger.info(f"{file_path} -----> Processing GPT-4o results for: {filename}")
        
        # Create log directory for detailed processing
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "processed_data")
        os.makedirs(log_dir, exist_ok=True)
        
        try:
            # Save raw CSV texts for debugging
            raw_csv_log_file = os.path.join(log_dir, f"raw_csv_texts_{filename}_{int(time.time())}.json")
            try:
                with open(raw_csv_log_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "file_path": file_path,
                        "metadata": metadata,
                        "csv_texts": csv_texts
                    }, f, indent=2, ensure_ascii=False)
                self.logger.debug(f"{file_path} - Saved raw CSV texts to: {raw_csv_log_file}")
            except Exception as e:
                self.logger.warning(f"{file_path} - Failed to save raw CSV texts to file: {str(e)}")
            
            # Filter out empty CSV texts
            filtered_csv_texts = [text for text in csv_texts if text and len(text.strip()) > 0]
            
            if not filtered_csv_texts:
                self.logger.warning(f"{file_path} - No valid CSV texts found")
                return pd.DataFrame()
            
            # COMBINE ALL CSV TEXTS into a single text before processing
            # Process each CSV text to clean it
            cleaned_texts = []
            for i, csv_text in enumerate(filtered_csv_texts):
                # Remove opening triple backticks or "plaintext"/"text"
                cleaned_text = re.sub(r'^(?:```|plaintext|text)\s*\n', '', csv_text)
                # Remove closing triple backticks or "plaintext"/"text"
                cleaned_text = re.sub(r'\n(?:```|plaintext|text)\s*$', '', cleaned_text)
                
                # Skip header row in all but the first CSV text to avoid duplicating header
                if i > 0:
                    lines = cleaned_text.strip().split('\n')
                    if lines and "producto" in lines[0].lower():
                        cleaned_text = '\n'.join(lines[1:])
                
                cleaned_texts.append(cleaned_text)
            
            # Combine all cleaned texts with newlines between them
            combined_csv_text = '\n'.join(cleaned_texts)
            
            # Save the combined CSV text for debugging
            combined_csv_log_file = os.path.join(log_dir, f"combined_csv_text_{filename}_{int(time.time())}.txt")
            try:
                with open(combined_csv_log_file, "w", encoding="utf-8") as f:
                    f.write(combined_csv_text)
                self.logger.debug(f"{file_path} - Saved combined CSV text to: {combined_csv_log_file}")
            except Exception as e:
                self.logger.warning(f"{file_path} - Failed to save combined CSV text to file: {str(e)}")
            
            # Process the combined CSV text
            self.logger.debug(f"{file_path} - Processing combined CSV text ({len(combined_csv_text)} characters)")
            product_data = self.process_csv_from_gpt4o(combined_csv_text, metadata, file_path=file_path)
            self.logger.debug(f"{file_path} - Extracted {len(product_data)} product entries from combined CSV text")
            
            # If no products found, return empty DataFrame
            if not product_data:
                self.logger.warning(f"{file_path} - No product data extracted from PDF with GPT-4.1 nano")
                return pd.DataFrame()
            
            # Save extracted product data for debugging
            extracted_data_log_file = os.path.join(log_dir, f"extracted_data_{filename}_{int(time.time())}.json")
            try:
                with open(extracted_data_log_file, "w", encoding="utf-8") as f:
                    json.dump(product_data, f, indent=2, ensure_ascii=False)
                self.logger.debug(f"{file_path} - Saved extracted product data to: {extracted_data_log_file}")
            except Exception as e:
                self.logger.warning(f"{file_path} - Failed to save extracted product data to file: {str(e)}")
            
            # Convert to DataFrame
            self.logger.debug(f"{file_path} - Converting parsed data to DataFrame")
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
            self.logger.debug(f"{file_path} - Created DataFrame with {len(output_df)} rows")
            
            # Ensure all required columns exist
            required_columns = [
                'date', 'city', 'market', 'major_category', 'sub_category', 
                'product', 'presentation', 'units', 'daily_min', 'daily_max', 'source'
            ]
            for col in required_columns:
                if col not in output_df.columns:
                    output_df[col] = None if col in ['daily_min', 'daily_max'] else ""
            
            # Save final processed DataFrame
            final_df_log_file = os.path.join(log_dir, f"final_df_{filename}_{int(time.time())}.csv")
            try:
                output_df.to_csv(final_df_log_file, index=False)
                self.logger.debug(f"{file_path} - Saved final processed DataFrame to: {final_df_log_file}")
            except Exception as e:
                self.logger.warning(f"{file_path} - Failed to save final DataFrame to file: {str(e)}")
            
            # Log dataframe statistics
            df_stats = {
                "total_rows": len(output_df),
                "unique_products": output_df['product'].nunique(),
                "unique_categories": output_df['major_category'].nunique(),
                "unique_subcategories": output_df['sub_category'].nunique(),
                "min_price_range": output_df['daily_min'].min() if not output_df.empty else None,
                "max_price_range": output_df['daily_max'].max() if not output_df.empty else None
            }
            self.logger.debug(f"{file_path} - DataFrame statistics: {df_stats}")
            
            duration = time.time() - start_time
            self.logger.debug(f"{file_path} - GPT-4o results processing completed in {duration:.2f} seconds, extracted {len(output_df)} rows")
            self.logger.debug(f"{file_path} - <----- Completed GPT-4o results processing for: {filename}")
            
            return output_df
            
        except Exception as e:
            self.logger.error(f"{file_path} - Error processing GPT-4o results for PDF {file_path}: {str(e)}")
            self.logger.debug(f"{file_path} - Error details: {traceback.format_exc()}")
            self.logger.debug(f"{file_path} - <----- Failed GPT-4o results processing for: {filename}")
            return pd.DataFrame()