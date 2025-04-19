"""
Data validation module for the Colombian Agricultural Price Data Pipeline.

This module provides centralized validation logic for data processed by various components
of the pipeline before updating the registry.
"""

import os
import logging
import pandas as pd
from datetime import datetime
import traceback
import threading

from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.utils.exceptions import (
    ValidationError, NoDataError, NonNumericPriceError, 
    InvalidDateError, NoMajorCategoryError
)

class ValidityChecker:
    """
    Centralized validation for data processed by the pipeline components.
    Performs checks before updating the registry with processing results.
    """
    
    def __init__(self, logger=None):
        """
        Initialize the validity checker.
        
        Args:
            logger (Logger): Logger instance
        """
        # Set up logger if not provided
        if logger is None:
            logging_setup = setup_logging()
            self.logger = logging_setup['error_handler']
        else:
            self.logger = logger
        
        # Initialize the valid major categories list
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
        
        # Required columns for CSV data
        self.required_columns = [
            'date', 'city', 'major_category', 'product', 
            'daily_min', 'daily_max', 'source'
        ]
        
        # Fields to check for max length
        self.length_check_fields = [
            'city', 'market', 'major_category', 'sub_category', 
            'product', 'presentation', 'units'
        ]
        
        # Maximum allowed character length for text fields
        self.max_field_length = 80
        
        self.logger.info("ValidityChecker initialized")
    
    def register_individual_pdf_csv(self, registry, pdf_id, output_df, output_csv, 
                                processing_results, extraction_method, registry_lock):
        """
        Validate and register a processed PDF CSV file in the registry.
        
        Args:
            registry (PDFProcessorRegistry): Registry instance
            pdf_id (str): PDF ID to update
            output_df (DataFrame): Processed data DataFrame
            output_csv (str): Path to the output CSV file
            processing_results (dict): Dict with processing statistics
            extraction_method (str): Method used for extraction
            registry_lock (Lock): Thread lock for registry updates
            
        Returns:
            tuple: (bool, dict) - (validation success, updated processing results)
        """
        self.logger.info(f"Validating PDF result with ID: {pdf_id}")
        
        # Initialize validation results
        validation_results = {
            "passed": True,
            "error_messages": []
        }
        
        # Make a copy of processing results to update
        updated_results = processing_results.copy()
        updated_results['processed_csv_path'] = output_csv
        updated_results['extraction_method'] = extraction_method
        
        try:
            # 1. Check for empty DataFrame
            if output_df is None or output_df.empty:
                validation_results["passed"] = False
                validation_results["error_messages"].append("No data in DataFrame")
                self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Empty DataFrame")
                # No need to log the DataFrame here since it's empty
            else:
                # Calculate metrics if we have data
                updated_results['unique_products'] = len(output_df['product'].unique()) if 'product' in output_df.columns else 0
                updated_results['unique_major_categories'] = len(output_df['major_category'].unique()) if 'major_category' in output_df.columns else 0
                updated_results['unique_subcategories'] = len(output_df['sub_category'].unique()) if 'sub_category' in output_df.columns else 0
                updated_results['rows_processed'] = len(output_df)
                
                if 'city' in output_df.columns and 'market' in output_df.columns:
                    unique_city_markets = output_df[['city', 'market']].drop_duplicates()
                    updated_results['unique_cities_markets'] = len(unique_city_markets)
                
                # 2. Check for missing required columns
                missing_columns = [col for col in self.required_columns if col not in output_df.columns]
                if missing_columns:
                    validation_results["passed"] = False
                    validation_results["error_messages"].append(f"Missing required columns: {', '.join(missing_columns)}")
                    self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Missing columns: {missing_columns}")
                    # Log the entire DataFrame
                    self.logger.info(f"PDF {pdf_id} - DataFrame with missing columns: \n{output_df}")
                
                # Only run these checks if all required columns exist
                if not missing_columns:
                    # 3. Check for invalid price data
                    if 'daily_min' in output_df.columns and 'daily_max' in output_df.columns:
                        # Check if any price is non-numeric
                        non_numeric_prices = (
                            output_df['daily_min'].apply(lambda x: not (isinstance(x, (int, float)) or pd.isna(x))) |
                            output_df['daily_max'].apply(lambda x: not (isinstance(x, (int, float)) or pd.isna(x)))
                        ).any()
                        
                        # Check if any price is less than 1
                        invalid_prices = (
                            ((output_df['daily_min'] < 1) & (~pd.isna(output_df['daily_min']))) |
                            ((output_df['daily_max'] < 1) & (~pd.isna(output_df['daily_max'])))
                        ).any()
                        
                        if non_numeric_prices:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append("Found non-numeric price values")
                            self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Non-numeric price data")
                            # Log the entire DataFrame
                            self.logger.info(f"PDF {pdf_id} - DataFrame with non-numeric price data: \n{output_df}")
                        
                        if invalid_prices:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append("Found price values less than 1")
                            self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Invalid price values")
                            # Log the entire DataFrame
                            self.logger.info(f"PDF {pdf_id} - DataFrame with invalid price values: \n{output_df}")
                    
                    # 4. Check for invalid date format
                    if 'date' in output_df.columns:
                        # Try to convert dates to datetime
                        try:
                            pd.to_datetime(output_df['date'])
                        except:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append("Invalid date format")
                            self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Invalid date format")
                            # Log the entire DataFrame
                            self.logger.info(f"PDF {pdf_id} - DataFrame with invalid date format: \n{output_df}")
                    
                    # 5. Check for major categories in product or subcategory columns
                    if 'product' in output_df.columns and 'sub_category' in output_df.columns:
                        # Check if any major category is found in product names
                        category_in_product = output_df['product'].apply(
                            lambda x: any(cat.lower() == str(x).lower() for cat in self.valid_major_categories)
                        ).any()
                        
                        # Check if any major category is found in subcategory names
                        category_in_subcategory = output_df['sub_category'].apply(
                            lambda x: any(cat.lower() == str(x).lower() for cat in self.valid_major_categories)
                        ).any()
                        
                        if category_in_product:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append("Major category names found in product column")
                            self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Major categories in product column")
                            # Log the entire DataFrame
                            self.logger.info(f"PDF {pdf_id} - DataFrame with major categories in product column: \n{output_df}")
                        
                        if category_in_subcategory:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append("Major category names found in sub_category column")
                            self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Major categories in subcategory column")
                            # Log the entire DataFrame
                            self.logger.info(f"PDF {pdf_id} - DataFrame with major categories in subcategory column: \n{output_df}")
                    
                    # 6. Check for field length exceeding the maximum allowed characters
                    too_long_fields = []
                    for field in self.length_check_fields:
                        if field in output_df.columns:
                            # Check if any field value exceeds max_field_length
                            exceeds_length = output_df[field].astype(str).apply(
                                lambda x: len(str(x)) > self.max_field_length if not pd.isna(x) else False
                            ).any()
                            
                            if exceeds_length:
                                too_long_fields.append(field)
                    
                    if too_long_fields:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append(
                            f"Fields exceeding {self.max_field_length} characters: {', '.join(too_long_fields)}"
                        )
                        self.logger.warning(
                            f"PDF {pdf_id} - {output_csv}: Validation failed - Fields exceeding length limit: {too_long_fields}"
                        )
                        # Log the entire DataFrame
                        self.logger.info(f"PDF {pdf_id} - DataFrame with fields exceeding length limit: \n{output_df}")
            
            # Update processing results based on validation
            updated_results['error_occurred'] = not validation_results["passed"]
            updated_results['error_warnings'] = "; ".join(validation_results["error_messages"]) if validation_results["error_messages"] else ""
            
            # Write to CSV if validation passed
            if validation_results["passed"]:
                # Ensure output directory exists
                os.makedirs(os.path.dirname(output_csv), exist_ok=True)
                
                # Save to CSV
                output_df.to_csv(output_csv, index=False)
                self.logger.info(f"PDF {pdf_id} - {output_csv}: Validation passed - Saved to {output_csv}")
            else:
                self.logger.error(f"PDF {pdf_id} - {output_csv}: Validation failed - {updated_results['error_warnings']}")
            
            # Update registry with processing results - using registry lock
            with registry_lock:
                if updated_results['error_occurred']:
                    registry.update_processing_status(pdf_id, 'error', error=updated_results['error_warnings'])
                else:
                    registry.update_processing_results(pdf_id, updated_results)
            
            self.logger.info(f"PDF {pdf_id} - {output_csv}: Registry updated with processing results")
            
            return validation_results["passed"], updated_results
            
        except Exception as e:
            # Handle any unexpected exceptions during validation
            error_message = f"Validation error: {str(e)}"
            self.logger.error(f"PDF {pdf_id} - {output_csv}: {error_message}")
            self.logger.debug(f"Error details: {traceback.format_exc()}")
            
            # Log the DataFrame if it exists
            if output_df is not None:
                self.logger.info(f"PDF {pdf_id} - DataFrame when exception occurred: \n{output_df}")
            
            # Update processing results with error
            updated_results['error_occurred'] = True
            updated_results['error_warnings'] = error_message
            
            # Update registry with error
            with registry_lock:
                registry.update_processing_status(pdf_id, 'error', error=error_message)
            
            return False, updated_results
        
    def validate_excel_output(self, registry, excel_id, output_df, output_csv, processing_results):
        """
        Validate and register a processed Excel CSV file in the registry.
        
        Args:
            registry (ExcelProcessorRegistry): Registry instance
            excel_id (str): Excel ID to update
            output_df (DataFrame): Processed data DataFrame
            output_csv (str): Path to the output CSV file
            processing_results (dict): Dict with processing statistics
            
        Returns:
            tuple: (bool, dict) - (validation success, updated processing results)
        """
        self.logger.info(f"Validating Excel result with ID: {excel_id}")
        
        # Initialize validation results
        validation_results = {
            "passed": True,
            "error_messages": []
        }
        
        # Make a copy of processing results to update
        updated_results = processing_results.copy()
        updated_results['processed_csv_path'] = output_csv
        
        try:
            # 1. Check for empty DataFrame
            if output_df is None or output_df.empty:
                validation_results["passed"] = False
                validation_results["error_messages"].append("No data in DataFrame")
                self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Empty DataFrame")
                # No need to log the DataFrame here since it's empty
            else:
                # Calculate metrics if we have data
                updated_results['unique_products'] = len(output_df['product'].unique()) if 'product' in output_df.columns else 0
                updated_results['unique_major_categories'] = len(output_df['major_category'].unique()) if 'major_category' in output_df.columns else 0
                updated_results['unique_subcategories'] = len(output_df['sub_category'].unique()) if 'sub_category' in output_df.columns else 0
                updated_results['rows_processed'] = len(output_df)
                
                if 'city' in output_df.columns and 'market' in output_df.columns:
                    unique_city_markets = output_df[['city', 'market']].drop_duplicates()
                    updated_results['unique_cities_markets'] = len(unique_city_markets)
                
                # 2. Check for missing required columns
                missing_columns = [col for col in self.required_columns if col not in output_df.columns]
                if missing_columns:
                    validation_results["passed"] = False
                    validation_results["error_messages"].append(f"Missing required columns: {', '.join(missing_columns)}")
                    self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Missing columns: {missing_columns}")
                    # Log the entire DataFrame
                    self.logger.info(f"Excel {excel_id} - DataFrame with missing columns: \n{output_df}")
                
                # Only run these checks if all required columns exist
                if not missing_columns:
                    # 3. Check for invalid price data
                    if 'daily_min' in output_df.columns and 'daily_max' in output_df.columns:
                        # Check if any price is non-numeric
                        non_numeric_prices = (
                            output_df['daily_min'].apply(lambda x: not (isinstance(x, (int, float)) or pd.isna(x))) |
                            output_df['daily_max'].apply(lambda x: not (isinstance(x, (int, float)) or pd.isna(x)))
                        ).any()
                        
                        # Check if any price is less than 1
                        invalid_prices = (
                            ((output_df['daily_min'] < 1) & (~pd.isna(output_df['daily_min']))) |
                            ((output_df['daily_max'] < 1) & (~pd.isna(output_df['daily_max'])))
                        ).any()
                        
                        if non_numeric_prices:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append("Found non-numeric price values")
                            self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Non-numeric price data")
                            self.logger.debug(f"Excel {excel_id} - {output_csv}: Non-numeric price data found in columns 'daily_min' or 'daily_max'")
                            # Log the entire DataFrame
                            self.logger.info(f"Excel {excel_id} - DataFrame with non-numeric price data: \n{output_df}")
                        
                        if invalid_prices:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append("Found price values less than 1")
                            self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Invalid price values")
                            self.logger.debug(f"Excel {excel_id} - {output_csv}: Price values less than 1 found in columns 'daily_min' or 'daily_max'")
                            # Log the entire DataFrame
                            self.logger.info(f"Excel {excel_id} - DataFrame with invalid price values: \n{output_df}")

                    # 4. Check for invalid date format
                    if 'date' in output_df.columns:
                        # Try to convert dates to datetime
                        try:
                            pd.to_datetime(output_df['date'])
                        except:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append("Invalid date format")
                            self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Invalid date format")
                            self.logger.debug(f"Excel {excel_id} - {output_csv}: Invalid date format found in column 'date'")
                            # Log the entire DataFrame
                            self.logger.info(f"Excel {excel_id} - DataFrame with invalid date format: \n{output_df}")

                    # 5. Check for major categories in product or subcategory columns
                    if 'product' in output_df.columns and 'sub_category' in output_df.columns:
                        # Check if any major category is found in product names
                        category_in_product = output_df['product'].apply(
                            lambda x: any(cat.lower() == str(x).lower() for cat in self.valid_major_categories)
                        ).any()
                        
                        # Check if any major category is found in subcategory names
                        category_in_subcategory = output_df['sub_category'].apply(
                            lambda x: any(cat.lower() == str(x).lower() for cat in self.valid_major_categories)
                        ).any()
                        
                        if category_in_product:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append("Major category names found in product column")
                            self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Major categories in product column")
                            self.logger.debug(f"Excel {excel_id} - {output_csv}: Major category names found in 'product' column")
                            # Log the entire DataFrame
                            self.logger.info(f"Excel {excel_id} - DataFrame with major categories in product column: \n{output_df}")
                        
                        if category_in_subcategory:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append("Major category names found in sub_category column")
                            self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Major categories in subcategory column")
                            self.logger.debug(f"Excel {excel_id} - {output_csv}: Major category names found in 'sub_category' column")
                            # Log the entire DataFrame
                            self.logger.info(f"Excel {excel_id} - DataFrame with major categories in subcategory column: \n{output_df}")

                    # 6. Check for field length exceeding the maximum allowed characters
                    too_long_fields = []
                    for field in self.length_check_fields:
                        if field in output_df.columns:
                            # Check if any field value exceeds max_field_length
                            exceeds_length = output_df[field].astype(str).apply(
                                lambda x: len(str(x)) > self.max_field_length if not pd.isna(x) else False
                            ).any()
                            
                            if exceeds_length:
                                too_long_fields.append(field)

                    if too_long_fields:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append(
                            f"Fields exceeding {self.max_field_length} characters: {', '.join(too_long_fields)}"
                        )
                        self.logger.warning(
                            f"Excel {excel_id} - {output_csv}: Validation failed - Fields exceeding length limit: {too_long_fields}"
                        )
                        self.logger.debug(
                            f"Excel {excel_id} - {output_csv}: The following fields exceed {self.max_field_length} characters: {', '.join(too_long_fields)}"
                        )
                        # Log the entire DataFrame
                        self.logger.info(f"Excel {excel_id} - DataFrame with fields exceeding length limit: \n{output_df}")
            
            # Update processing results based on validation
            updated_results['error_occurred'] = not validation_results["passed"]
            updated_results['error_warnings'] = "; ".join(validation_results["error_messages"]) if validation_results["error_messages"] else ""
            
            # Write to CSV if validation passed
            if validation_results["passed"]:
                # Ensure output directory exists
                os.makedirs(os.path.dirname(output_csv), exist_ok=True)
                
                # Save to CSV
                output_df.to_csv(output_csv, index=False)
                self.logger.info(f"Excel {excel_id} - {output_csv}: Validation passed - Saved to {output_csv}")
            else:
                self.logger.error(f"Excel {excel_id} - {output_csv}: Validation failed - {updated_results['error_warnings']}")
            
            # Update registry with processing results
            if updated_results['error_occurred']:
                registry.update_processing_status(excel_id, 'error', error=updated_results['error_warnings'])
            else:
                registry.update_processing_results(excel_id, updated_results)
            
            self.logger.info(f"Excel {excel_id} - {output_csv}: Registry updated with processing results")
            
            return validation_results["passed"], updated_results
            
        except Exception as e:
            # Handle any unexpected exceptions during validation
            error_message = f"Validation error: {str(e)}"
            self.logger.error(f"Excel {excel_id} - {output_csv}: {error_message}")
            self.logger.debug(f"Error details: {traceback.format_exc()}")
            
            # Log the DataFrame if it exists
            if output_df is not None:
                self.logger.info(f"Excel {excel_id} - DataFrame when exception occurred: \n{output_df}")
            
            # Update processing results with error
            updated_results['error_occurred'] = True
            updated_results['error_warnings'] = error_message
            
            # Update registry with error
            registry.update_processing_status(excel_id, 'error', error=error_message)
            
            return False, updated_results