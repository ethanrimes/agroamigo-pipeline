"""
CSV data validation module for Colombian agricultural price data.

This module provides functions for validating processed CSV data
and ensuring it meets quality requirements for downstream processing.
"""

import os
import pandas as pd
import logging
import traceback
from datetime import datetime
from typing import List, Dict, Union, Tuple, Optional

from agroamigo_pipeline.utils.exceptions import (
    ValidationError, NoMajorCategoryError, InvalidDateError, 
    NonNumericPriceError, NoDataError, InvalidMajorCategoryError
)

class IndividualCSVValidator:
    """Validates processed CSV data for quality and consistency."""
    
    # List of valid major categories for validation
    VALID_MAJOR_CATEGORIES = [
        'Procesados',
        'Verduras y hortalizas',
        'Frutas',
        'Pescados',
        'Tuberculos, raices y platanos',
        'Granos y cereales',
        'Carnes',
        'Lacteos y huevos'
    ]
    
    def __init__(self, logger=None):
        """
        Initialize the CSV validator.
        
        Args:
            logger (Logger): Logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info("IndividualCSVValidator initialized")
    
    def validate_csv_file(self, csv_path: str) -> Tuple[bool, Dict[str, Union[bool, str, List[str]]]]:
        """
        Validate a CSV file for data quality issues.
        
        Args:
            csv_path (str): Path to the CSV file
            
        Returns:
            tuple: (success, validation_results)
                success (bool): True if validation passed, False otherwise
                validation_results (dict): Dictionary with validation results and errors
        """
        filename = os.path.basename(csv_path)
        self.logger.info(f"Validating CSV file: {filename}")
        
        # Initialize validation results
        validation_results = {
            'error_occurred': False,
            'error_warnings': "",
            'error_details': [],
        }
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_path)
            
            # Check if the dataframe is empty
            if df.empty:
                error_msg = f"CSV file {filename} is empty"
                self.logger.error(error_msg)
                validation_results['error_occurred'] = True
                validation_results['error_warnings'] = error_msg
                validation_results['error_details'].append(error_msg)
                raise NoDataError(error_msg)
            
            # Validate the data
            errors = self._validate_dataframe(df, filename)
            
            # If errors found, update validation results
            if errors:
                validation_results['error_occurred'] = True
                validation_results['error_warnings'] = f"Found {len(errors)} validation errors in {filename}"
                validation_results['error_details'] = errors
                self.logger.error(f"Validation failed for {filename} with {len(errors)} errors")
            else:
                self.logger.info(f"Validation passed for {filename}")
            
            return not validation_results['error_occurred'], validation_results
            
        except Exception as e:
            error_msg = f"Error validating CSV file {filename}: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(f"Error details: {traceback.format_exc()}")
            
            validation_results['error_occurred'] = True
            validation_results['error_warnings'] = error_msg
            validation_results['error_details'].append(error_msg)
            
            return False, validation_results
    
    def _validate_dataframe(self, df: pd.DataFrame, filename: str) -> List[str]:
        """
        Validate a DataFrame for data quality issues.
        
        Args:
            df (DataFrame): DataFrame to validate
            filename (str): Name of the file (for logging)
            
        Returns:
            list: List of error messages
        """
        errors = []
        
        # Check required columns
        required_columns = [
            'date', 'city', 'market', 'major_category', 'sub_category', 
            'product', 'presentation', 'units', 'daily_min', 'daily_max', 'source'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            error_msg = f"Missing required columns: {', '.join(missing_columns)}"
            errors.append(error_msg)
            self.logger.error(error_msg)
        
        # Validate date format
        if 'date' in df.columns:
            date_errors = self._validate_dates(df)
            errors.extend(date_errors)
        
        # Validate price data
        if 'daily_min' in df.columns and 'daily_max' in df.columns:
            price_errors = self._validate_prices(df)
            errors.extend(price_errors)
        
        # Validate major categories
        if 'major_category' in df.columns:
            category_errors = self._validate_major_categories(df)
            errors.extend(category_errors)
        
        # Check for empty city or market
        if 'city' in df.columns:
            empty_cities = df['city'].isna().sum() + (df['city'] == '').sum()
            if empty_cities > 0:
                error_msg = f"Found {empty_cities} rows with missing city values"
                errors.append(error_msg)
                self.logger.warning(error_msg)
        
        # Check for empty product names
        if 'product' in df.columns:
            empty_products = df['product'].isna().sum() + (df['product'] == '').sum()
            if empty_products > 0:
                error_msg = f"Found {empty_products} rows with missing product values"
                errors.append(error_msg)
                self.logger.warning(error_msg)
        
        return errors
    
    def _validate_dates(self, df: pd.DataFrame) -> List[str]:
        """
        Validate date values in the DataFrame.
        
        Args:
            df (DataFrame): DataFrame to validate
            
        Returns:
            list: List of error messages
        """
        errors = []
        
        # Check for missing dates
        missing_dates = df['date'].isna().sum() + (df['date'] == '').sum()
        if missing_dates > 0:
            error_msg = f"Found {missing_dates} rows with missing date values"
            errors.append(error_msg)
            self.logger.warning(error_msg)
        
        # Check date format and validity
        try:
            # Convert dates to datetime
            date_series = pd.to_datetime(df['date'], errors='coerce')
            invalid_dates = date_series.isna().sum()
            
            if invalid_dates > 0:
                error_msg = f"Found {invalid_dates} rows with invalid date formats"
                errors.append(error_msg)
                self.logger.warning(error_msg)
                raise InvalidDateError(f"Invalid date formats detected in {invalid_dates} rows")
                
            # Check if dates are in the future
            future_dates = (date_series > pd.Timestamp.now()).sum()
            if future_dates > 0:
                error_msg = f"Found {future_dates} rows with future dates"
                errors.append(error_msg)
                self.logger.warning(error_msg)
            
            # Check if dates are too old (before 2010)
            old_dates = (date_series < pd.Timestamp('2010-01-01')).sum()
            if old_dates > 0:
                error_msg = f"Found {old_dates} rows with dates before 2010"
                errors.append(error_msg)
                self.logger.warning(error_msg)
                
        except Exception as e:
            error_msg = f"Error validating dates: {str(e)}"
            errors.append(error_msg)
            self.logger.error(error_msg)
        
        return errors
    
    def _validate_prices(self, df: pd.DataFrame) -> List[str]:
        """
        Validate price values in the DataFrame.
        
        Args:
            df (DataFrame): DataFrame to validate
            
        Returns:
            list: List of error messages
        """
        errors = []
        
        # Check for non-numeric prices
        try:
            # Convert to numeric, coercing errors to NaN
            min_prices = pd.to_numeric(df['daily_min'], errors='coerce')
            max_prices = pd.to_numeric(df['daily_max'], errors='coerce')
            
            # Check for NaN values (non-numeric)
            non_numeric_min = min_prices.isna().sum()
            non_numeric_max = max_prices.isna().sum()
            
            if non_numeric_min > 0 or non_numeric_max > 0:
                error_msg = f"Found non-numeric prices: {non_numeric_min} min prices, {non_numeric_max} max prices"
                errors.append(error_msg)
                self.logger.warning(error_msg)
                raise NonNumericPriceError(f"Non-numeric prices detected: {non_numeric_min} min, {non_numeric_max} max")
            
            # Check for negative prices
            negative_min = (min_prices < 0).sum()
            negative_max = (max_prices < 0).sum()
            
            if negative_min > 0 or negative_max > 0:
                error_msg = f"Found negative prices: {negative_min} min prices, {negative_max} max prices"
                errors.append(error_msg)
                self.logger.warning(error_msg)
            
            # Check for min price > max price
            min_greater_than_max = (min_prices > max_prices).sum()
            if min_greater_than_max > 0:
                error_msg = f"Found {min_greater_than_max} rows where min price > max price"
                errors.append(error_msg)
                self.logger.warning(error_msg)
            
            # Check for extremely high prices (potential data errors)
            # Threshold: 1,000,000 (1 million) - adjust as needed
            high_price_threshold = 1000000
            high_min_prices = (min_prices > high_price_threshold).sum()
            high_max_prices = (max_prices > high_price_threshold).sum()
            
            if high_min_prices > 0 or high_max_prices > 0:
                error_msg = f"Found potentially erroneous high prices: {high_min_prices} min prices, {high_max_prices} max prices > {high_price_threshold}"
                errors.append(error_msg)
                self.logger.warning(error_msg)
                
        except Exception as e:
            error_msg = f"Error validating prices: {str(e)}"
            errors.append(error_msg)
            self.logger.error(error_msg)
        
        return errors
    
    def _validate_major_categories(self, df: pd.DataFrame) -> List[str]:
        """
        Validate major category values in the DataFrame.
        
        Args:
            df (DataFrame): DataFrame to validate
            
        Returns:
            list: List of error messages
        """
        errors = []
        
        # Check for missing major categories
        missing_categories = df['major_category'].isna().sum() + (df['major_category'] == '').sum()
        
        if missing_categories > 0:
            error_msg = f"Found {missing_categories} rows with missing major category values"
            errors.append(error_msg)
            self.logger.warning(error_msg)
            raise NoMajorCategoryError(f"Missing major category in {missing_categories} rows")
        
        # Check for invalid major categories
        if not missing_categories:
            unique_categories = df['major_category'].unique()
            invalid_categories = [cat for cat in unique_categories 
                                if cat not in self.VALID_MAJOR_CATEGORIES and cat != ""]
            
            if invalid_categories:
                error_msg = f"Found {len(invalid_categories)} invalid major categories: {', '.join(invalid_categories)}"
                errors.append(error_msg)
                self.logger.warning(error_msg)
                
                # Get count of rows with invalid categories
                invalid_cat_rows = df[df['major_category'].isin(invalid_categories)].shape[0]
                raise InvalidMajorCategoryError(
                    f"Invalid major categories detected: {', '.join(invalid_categories)} in {invalid_cat_rows} rows"
                )
        
        return errors
    
    def validate_and_update_registry(self, csv_path: str, registry, entry_id: str) -> bool:
        """
        Validate a CSV file and update the registry with validation results.
        
        Args:
            csv_path (str): Path to the CSV file
            registry: Registry instance (PDF or Excel)
            entry_id (str): ID for the registry entry
            
        Returns:
            bool: True if validation passed, False otherwise
        """
        filename = os.path.basename(csv_path)
        self.logger.info(f"Validating and updating registry for: {filename}")
        
        # Validate the CSV file
        success, validation_results = self.validate_csv_file(csv_path)
        
        # Update the registry with validation results
        try:
            if success:
                self.logger.info(f"Validation passed for {filename}")
                registry.update_validation_status(entry_id, 'valid')
            else:
                self.logger.warning(f"Validation failed for {filename}")
                error_warnings = validation_results['error_warnings']
                error_details = '\n'.join(validation_results['error_details'])
                
                registry.update_validation_status(
                    entry_id, 
                    'invalid', 
                    error=error_warnings,
                    validation_details=error_details
                )
                
                # Add the errors to the registry's processing_results
                registry.update_processing_field(
                    entry_id, 
                    'error_occurred', 
                    True
                )
                
                registry.update_processing_field(
                    entry_id, 
                    'error_warnings', 
                    error_warnings
                )
            
            return success
            
        except Exception as e:
            error_msg = f"Error updating registry for {filename}: {str(e)}"
            self.logger.error(error_msg)
            self.logger.debug(f"Error details: {traceback.format_exc()}")
            
            # Try to update registry with the error
            try:
                registry.update_validation_status(
                    entry_id, 
                    'error', 
                    error=f"Validation error: {str(e)}"
                )
            except:
                pass
            
            return False
    
    def batch_validate(self, csv_dir: str, registry, id_field: str = 'pdf_id', filter_status: Optional[str] = None) -> Dict:
        """
        Batch validate all CSV files in a directory and update registry.
        
        Args:
            csv_dir (str): Directory containing CSV files
            registry: Registry instance (PDF or Excel)
            id_field (str): Field name for the ID in the registry (pdf_id or excel_id)
            filter_status (str, optional): Only validate files with this status
            
        Returns:
            dict: Validation statistics
        """
        self.logger.info(f"Batch validating CSV files in: {csv_dir}")
        
        # Statistics
        stats = {
            'total_files': 0,
            'valid_files': 0,
            'invalid_files': 0,
            'error_files': 0,
            'skipped_files': 0
        }
        
        # Get all CSV files in the directory
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
        stats['total_files'] = len(csv_files)
        
        self.logger.info(f"Found {len(csv_files)} CSV files to validate")
        
        for csv_file in csv_files:
            csv_path = os.path.join(csv_dir, csv_file)
            
            # Extract base filename (without extension)
            base_filename = os.path.splitext(csv_file)[0]
            
            # Try to find entry in registry
            if id_field == 'pdf_id':
                entry = registry.get_pdf_by_filename(base_filename)
            else:  # excel_id
                entry = registry.get_excel_by_filename(base_filename)
            
            if entry is None or entry.empty:
                self.logger.warning(f"No registry entry found for {csv_file}, skipping validation")
                stats['skipped_files'] += 1
                continue
            
            # Get the ID from the entry
            entry_id = entry.iloc[0][id_field]
            
            # Check if we should filter by status
            if filter_status is not None:
                status = entry.iloc[0]['processing_status']
                if status != filter_status:
                    self.logger.info(f"Skipping {csv_file} (status: {status}, filter: {filter_status})")
                    stats['skipped_files'] += 1
                    continue
            
            # Validate and update registry
            try:
                success = self.validate_and_update_registry(csv_path, registry, entry_id)
                
                if success:
                    stats['valid_files'] += 1
                else:
                    stats['invalid_files'] += 1
                    
            except Exception as e:
                error_msg = f"Error validating {csv_file}: {str(e)}"
                self.logger.error(error_msg)
                stats['error_files'] += 1
        
        # Log summary
        self.logger.info(f"Validation summary: {stats['valid_files']} valid, "
                      f"{stats['invalid_files']} invalid, {stats['error_files']} errors, "
                      f"{stats['skipped_files']} skipped out of {stats['total_files']} total files")
        
        return stats