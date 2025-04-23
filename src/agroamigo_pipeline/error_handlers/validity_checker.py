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
        
        # Invalid string values that should be considered null
        self.invalid_string_values = ['nan', 'unknown', '', 'none', 'null']
        
        self.logger.info("ValidityChecker initialized")
    

    def log_dataframe(self, logger, message, df):
        """
        Helper method to log a DataFrame without hiding rows or columns.
        
        Args:
            logger: Logger instance
            message: Log message prefix
            df: DataFrame to log
        """
        with pd.option_context('display.max_rows', None, 
                            'display.max_columns', None,
                            'display.width', None):
            logger.info(f"{message}\n{df}")

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
                    self.log_dataframe(self.logger, f"PDF {pdf_id} - DataFrame with missing columns:", output_df)
                
                # Only run these checks if all required columns exist
                if not missing_columns:
                    # 3. Perform per-column validations
                    # Date validation - must be non-null and valid datetime
                    invalid_dates = False
                    null_dates = output_df['date'].isna().any()
                    
                    if null_dates:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Null values found in date column")
                        self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Null dates")
                        # self.logger.info(f"PDF {pdf_id} - DataFrame with null dates: \n{output_df[output_df['date'].isna()]}")
                        self.log_dataframe(self.logger, f"PDF {pdf_id} - DataFrame with null dates:", output_df)
                    
                    # Try to convert dates to datetime
                    try:
                        pd.to_datetime(output_df['date'])
                    except:
                        invalid_dates = True
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Invalid date format")
                        self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Invalid date format")
                        # self.logger.info(f"PDF {pdf_id} - DataFrame with invalid date format: \n{output_df}")
                        self.log_dataframe(self.logger, f"PDF {pdf_id} - DataFrame with invalid date format:", output_df)

                    # Price validation - must be non-null, numeric, and > 0
                    null_min_prices = output_df['daily_min'].isna().any()
                    null_max_prices = output_df['daily_max'].isna().any()
                    
                    if null_min_prices:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Null values found in daily_min column")
                        self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Null min prices")
                        # self.logger.info(f"PDF {pdf_id} - DataFrame with null min prices: \n{output_df[output_df['daily_min'].isna()]}")
                        self.log_dataframe(self.logger, f"PDF {pdf_id} - DataFrame with null min prices:", output_df)

                    if null_max_prices:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Null values found in daily_max column")
                        self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Null max prices")
                        # self.logger.info(f"PDF {pdf_id} - DataFrame with null max prices: \n{output_df[output_df['daily_max'].isna()]}")
                        self.log_dataframe(self.logger, f"PDF {pdf_id} - DataFrame with null max prices:", output_df)

                    # Check if prices are numeric
                    non_numeric_min = output_df['daily_min'].apply(lambda x: not (isinstance(x, (int, float)) or pd.isna(x))).any()
                    non_numeric_max = output_df['daily_max'].apply(lambda x: not (isinstance(x, (int, float)) or pd.isna(x))).any()
                    
                    if non_numeric_min:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Non-numeric values found in daily_min column")
                        self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Non-numeric min prices")
                        self.logger.info(f"PDF {pdf_id} - DataFrame with non-numeric min prices: \n{output_df}")
                        self.log_dataframe(self.logger, f"PDF {pdf_id} - DataFrame with non-numeric min prices:", output_df)
                    
                    if non_numeric_max:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Non-numeric values found in daily_max column")
                        self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Non-numeric max prices")
                        self.logger.info(f"PDF {pdf_id} - DataFrame with non-numeric max prices: \n{output_df}")
                        self.log_dataframe(self.logger, f"PDF {pdf_id} - DataFrame with non-numeric max prices:", output_df)
                    
                    # Check if prices are > 0
                    invalid_min_prices = ((output_df['daily_min'] <= 0) & (~pd.isna(output_df['daily_min']))).any()
                    invalid_max_prices = ((output_df['daily_max'] <= 0) & (~pd.isna(output_df['daily_max']))).any()
                    
                    if invalid_min_prices:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Values <= 0 found in daily_min column")
                        self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Invalid min prices")
                        self.logger.info(f"PDF {pdf_id} - DataFrame with invalid min prices: \n{output_df[((output_df['daily_min'] <= 0) & (~pd.isna(output_df['daily_min'])))]}")
                        self.log_dataframe(self.logger, f"PDF {pdf_id} - DataFrame with invalid min prices:", output_df)
                    
                    if invalid_max_prices:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Values <= 0 found in daily_max column")
                        self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Invalid max prices")
                        self.logger.info(f"PDF {pdf_id} - DataFrame with invalid max prices: \n{output_df[((output_df['daily_max'] <= 0) & (~pd.isna(output_df['daily_max'])))]}")
                        self.log_dataframe(self.logger, f"PDF {pdf_id} - DataFrame with invalid max prices:", output_df)
                    
                    # String field validation - major_category, city, product must be non-null, 
                    # strings, and < 80 chars, and not invalid string values
                    for field in ['major_category', 'city', 'product']:
                        # Check for nulls
                        null_values = output_df[field].isna().any()
                        
                        if null_values:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append(f"Null values found in {field} column")
                            self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Null {field}")
                            self.logger.info(f"PDF {pdf_id} - DataFrame with null {field}: \n{output_df[output_df[field].isna()]}")
                            self.log_dataframe(self.logger, f"PDF {pdf_id} - DataFrame with null field {field}:", output_df)
                        
                        # Check for invalid string values (case insensitive)
                        invalid_strings = output_df[field].fillna('').astype(str).apply(
                            lambda x: x.lower().strip() in self.invalid_string_values
                        ).any()
                        
                        if invalid_strings:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append(f"Invalid string values found in {field} column")
                            self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Invalid {field} strings")
                            self.log_dataframe(self.logger, f"PDF {pdf_id} - Invalid {field} strings:", output_df)
                            
                            # Get rows with invalid strings
                            invalid_mask = output_df[field].fillna('').astype(str).apply(
                                lambda x: x.lower().strip() in self.invalid_string_values
                            )
                            self.logger.info(f"PDF {pdf_id} - DataFrame with invalid {field} strings: \n{output_df[invalid_mask]}")
                        
                        # Check field length
                        if field in self.length_check_fields:
                            exceeds_length = output_df[field].astype(str).apply(
                                lambda x: len(str(x)) > self.max_field_length if not pd.isna(x) else False
                            ).any()
                            
                            if exceeds_length:
                                validation_results["passed"] = False
                                validation_results["error_messages"].append(f"Values exceeding {self.max_field_length} characters found in {field} column")
                                self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - {field} exceeds length limit")
                                self.log_dataframe(self.logger, f"PDF {pdf_id} - {field} exceeds length:", output_df)
                                # Get rows with field values exceeding length limit
                                length_mask = output_df[field].astype(str).apply(
                                    lambda x: len(str(x)) > self.max_field_length if not pd.isna(x) else False
                                )
                                self.logger.info(f"PDF {pdf_id} - DataFrame with {field} exceeding length limit: \n{output_df[length_mask]}")
                    
                    # Check in subcategory, city, and product if there is an exact match of any major category
                    for field in ['sub_category', 'city', 'product']:
                        if field in output_df.columns:
                            # Check for exact matches with major categories (case-insensitive)
                            category_in_field = output_df[field].fillna('').astype(str).apply(
                                lambda x: any(cat.lower() == x.lower().strip() for cat in self.valid_major_categories)
                            ).any()
                            
                            if category_in_field:
                                validation_results["passed"] = False
                                validation_results["error_messages"].append(f"Major category names found in {field} column")
                                self.logger.warning(f"PDF {pdf_id} - {output_csv}: Validation failed - Major categories in {field} column")
                                self.log_dataframe(self.logger, f"PDF {pdf_id} - Major categories in {field} column:", output_df)
                                
                                # Get rows with major categories in the field
                                category_mask = output_df[field].fillna('').astype(str).apply(
                                    lambda x: any(cat.lower() == x.lower().strip() for cat in self.valid_major_categories)
                                )
                                # self.logger.info(f"PDF {pdf_id} - DataFrame with major categories in {field} column: \n{output_df[category_mask]}")
            
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
                    # 3. Perform per-column validations
                    # Date validation - must be non-null and valid datetime
                    invalid_dates = False
                    null_dates = output_df['date'].isna().any()
                    
                    if null_dates:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Null values found in date column")
                        self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Null dates")
                        self.logger.info(f"Excel {excel_id} - DataFrame with null dates: \n{output_df[output_df['date'].isna()]}")
                    
                    # Try to convert dates to datetime
                    try:
                        pd.to_datetime(output_df['date'])
                    except:
                        invalid_dates = True
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Invalid date format")
                        self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Invalid date format")
                        self.logger.info(f"Excel {excel_id} - DataFrame with invalid date format: \n{output_df}")
                    
                    # Price validation - must be non-null, numeric, and > 0
                    null_min_prices = output_df['daily_min'].isna().any()
                    null_max_prices = output_df['daily_max'].isna().any()
                    
                    if null_min_prices:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Null values found in daily_min column")
                        self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Null min prices")
                        self.logger.info(f"Excel {excel_id} - DataFrame with null min prices: \n{output_df[output_df['daily_min'].isna()]}")
                    
                    if null_max_prices:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Null values found in daily_max column")
                        self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Null max prices")
                        self.logger.info(f"Excel {excel_id} - DataFrame with null max prices: \n{output_df[output_df['daily_max'].isna()]}")
                    
                    # Check if prices are numeric
                    non_numeric_min = output_df['daily_min'].apply(lambda x: not (isinstance(x, (int, float)) or pd.isna(x))).any()
                    non_numeric_max = output_df['daily_max'].apply(lambda x: not (isinstance(x, (int, float)) or pd.isna(x))).any()
                    
                    if non_numeric_min:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Non-numeric values found in daily_min column")
                        self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Non-numeric min prices")
                        self.logger.info(f"Excel {excel_id} - DataFrame with non-numeric min prices: \n{output_df}")
                    
                    if non_numeric_max:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Non-numeric values found in daily_max column")
                        self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Non-numeric max prices")
                        self.logger.info(f"Excel {excel_id} - DataFrame with non-numeric max prices: \n{output_df}")
                    
                    # Check if prices are > 0
                    invalid_min_prices = ((output_df['daily_min'] <= 0) & (~pd.isna(output_df['daily_min']))).any()
                    invalid_max_prices = ((output_df['daily_max'] <= 0) & (~pd.isna(output_df['daily_max']))).any()
                    
                    if invalid_min_prices:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Values <= 0 found in daily_min column")
                        self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Invalid min prices")
                        self.logger.info(f"Excel {excel_id} - DataFrame with invalid min prices: \n{output_df[((output_df['daily_min'] <= 0) & (~pd.isna(output_df['daily_min'])))]}")
                    
                    if invalid_max_prices:
                        validation_results["passed"] = False
                        validation_results["error_messages"].append("Values <= 0 found in daily_max column")
                        self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Invalid max prices")
                        self.logger.info(f"Excel {excel_id} - DataFrame with invalid max prices: \n{output_df[((output_df['daily_max'] <= 0) & (~pd.isna(output_df['daily_max'])))]}")
                    
                    # String field validation - major_category, city, product must be non-null, 
                    # strings, and < 80 chars, and not invalid string values
                    for field in ['major_category', 'city', 'product']:
                        # Check for nulls
                        null_values = output_df[field].isna().any()
                        
                        if null_values:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append(f"Null values found in {field} column")
                            self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Null {field}")
                            self.logger.info(f"Excel {excel_id} - DataFrame with null {field}: \n{output_df[output_df[field].isna()]}")
                        
                        # Check for invalid string values (case insensitive)
                        invalid_strings = output_df[field].fillna('').astype(str).apply(
                            lambda x: x.lower().strip() in self.invalid_string_values
                        ).any()
                        
                        if invalid_strings:
                            validation_results["passed"] = False
                            validation_results["error_messages"].append(f"Invalid string values found in {field} column")
                            self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Invalid {field} strings")
                            
                            # Get rows with invalid strings
                            invalid_mask = output_df[field].fillna('').astype(str).apply(
                                lambda x: x.lower().strip() in self.invalid_string_values
                            )
                            self.logger.info(f"Excel {excel_id} - DataFrame with invalid {field} strings: \n{output_df[invalid_mask]}")
                        
                        # Check field length
                        if field in self.length_check_fields:
                            exceeds_length = output_df[field].astype(str).apply(
                                lambda x: len(str(x)) > self.max_field_length if not pd.isna(x) else False
                            ).any()
                            
                            if exceeds_length:
                                validation_results["passed"] = False
                                validation_results["error_messages"].append(f"Values exceeding {self.max_field_length} characters found in {field} column")
                                self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - {field} exceeds length limit")
                                
                                # Get rows with field values exceeding length limit
                                length_mask = output_df[field].astype(str).apply(
                                    lambda x: len(str(x)) > self.max_field_length if not pd.isna(x) else False
                                )
                                self.logger.info(f"Excel {excel_id} - DataFrame with {field} exceeding length limit: \n{output_df[length_mask]}")
                    
                    # Check in subcategory, city, and product if there is an exact match of any major category
                    for field in ['sub_category', 'city', 'product']:
                        if field in output_df.columns:
                            # Check for exact matches with major categories (case-insensitive)
                            category_in_field = output_df[field].fillna('').astype(str).apply(
                                lambda x: any(cat.lower() == x.lower().strip() for cat in self.valid_major_categories)
                            ).any()
                            
                            if category_in_field:
                                validation_results["passed"] = False
                                validation_results["error_messages"].append(f"Major category names found in {field} column")
                                self.logger.warning(f"Excel {excel_id} - {output_csv}: Validation failed - Major categories in {field} column")
                                
                                # Get rows with major categories in the field
                                category_mask = output_df[field].fillna('').astype(str).apply(
                                    lambda x: any(cat.lower() == x.lower().strip() for cat in self.valid_major_categories)
                                )
                                self.logger.info(f"Excel {excel_id} - DataFrame with major categories in {field} column: \n{output_df[category_mask]}")
            
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