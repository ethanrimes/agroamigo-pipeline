"""
Updated Excel processor for Colombian agricultural price data.
Uses the new registry system for tracking Excel processing.
"""

import os
import logging
import traceback
from datetime import datetime
import time
from pathlib import Path
import pandas as pd

from agroamigo_pipeline.config.settings import (
    SOURCE_DIR, PROCESSED_DIR, PIPELINE_RUN_ID
)
from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.utils.exceptions import ExcelProcessingError
from agroamigo_pipeline.registry.webscraper_registry import WebScraperRegistry
from agroamigo_pipeline.registry.excel_registry import ExcelProcessorRegistry
from agroamigo_pipeline.processors.excel_components.excel_reader import ExcelFileReader
from agroamigo_pipeline.processors.excel_components.excel_transformer import ExcelDataTransformer

class ExcelProcessor:
    """
    Process Excel files containing Colombian agricultural price data.
    """
    
    def __init__(self, logger=None):
        """
        Initialize the Excel processor.
        
        Args:
            logger (Logger): Logger instance
        """
        # Set up logger if not provided
        if logger is None:
            logging_setup = setup_logging()
            self.logger = logging_setup['excel_processor']
        else:
            self.logger = logger
            
        # Initialize registries
        self.webscraper_registry = WebScraperRegistry(logger=self.logger)
        self.registry = ExcelProcessorRegistry(logger=self.logger)
        
        # Initialize component modules
        self.reader = ExcelFileReader(logger=self.logger)
        self.transformer = ExcelDataTransformer(logger=self.logger)
        
        # Initialize statistics
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'total_rows_processed': 0
        }
        
        self.logger.info("ExcelProcessor initialized")
    
    def process_excel_file(self, excel_id, file_path, output_dir=None, force_reprocess=False):
        """
        Process a single Excel file and save results to CSV.
        
        Args:
            excel_id (str): Excel ID from the registry
            file_path (str): Path to the Excel file
            output_dir (str): Directory to save processed data
            force_reprocess (bool): If True, reprocess file even if already processed
            
        Returns:
            dict: Processing results
        """
        filename = os.path.basename(file_path)
        self.logger.info(f"Processing Excel file: {filename}")
        
        # Check if file has already been processed
        excel_info = self.registry.get_excel_by_id(excel_id)
        
        if excel_info and excel_info['processing_status'] == 'processed' and not force_reprocess:
            self.logger.info(f"File already processed: {filename}")
            return None
        
        # Set default output directory
        output_dir = output_dir or PROCESSED_DIR
        os.makedirs(output_dir, exist_ok=True)
        
        # Update registry status to 'processing'
        self.registry.update_processing_status(excel_id, 'processing')
        
        # Processing results dictionary
        processing_results = {
            'unique_products': 0,
            'unique_major_categories': 0,
            'unique_subcategories': 0,
            'rows_processed': 0,
            'error_occurred': False,
            'error_warnings': "",
            'processing_duration_seconds': 0.0,
            'extraction_method': "excel_parsing",
            'processed_csv_path': "",
            'unique_cities_markets': 0,
            'pipeline_run_id': PIPELINE_RUN_ID or ""
        }
        
        start_time = time.time()
        
        try:
            # Read the Excel file using the reader component
            df = self.reader.read_and_prepare(file_path)
            
            # Get the extracted date from the reader and store it
            extracted_date = self.reader.get_extracted_date()
            if extracted_date:
                # Convert to pandas datetime if it's a string
                if isinstance(extracted_date, str):
                    extracted_date = pd.to_datetime(extracted_date)
                
                # Store in processing results as parsed_date
                processing_results['parsed_date'] = extracted_date
                self.logger.info(f"Extracted date from Excel content: {extracted_date}")
            else:
                # No content date found, we'll keep using the file_date from registry
                self.logger.info("No date extracted from Excel content, using file_date from filename")
            
            # Transform the data using the transformer component
            output_df = self.transformer.transform(df, file_path)
            
            # Create output path
            base_filename = os.path.splitext(filename)[0]
            output_csv = os.path.join(output_dir, f"{base_filename}.csv")
            
            # Save to CSV
            output_df.to_csv(output_csv, index=False)
            self.logger.info(f"Saved output to: {output_csv}")
            
            # Validate the output CSV
            validation_success = self.validate_output_csv(output_csv, excel_id)
            self.logger.info(f"Validation {'passed' if validation_success else 'failed'} for: {os.path.basename(output_csv)}")
            
            # Update processing results
            processing_results['unique_products'] = len(output_df['product'].unique()) if 'product' in output_df.columns else 0
            processing_results['unique_major_categories'] = len(output_df['major_category'].unique()) if 'major_category' in output_df.columns else 0
            processing_results['unique_subcategories'] = len(output_df['sub_category'].unique()) if 'sub_category' in output_df.columns else 0
            processing_results['rows_processed'] = len(output_df)
            processing_results['processed_csv_path'] = output_csv
            
            # If validation failed, update error status
            if not validation_success:
                processing_results['error_occurred'] = True
                processing_results['error_warnings'] = "CSV validation failed"
            
            # Count unique cities and markets
            if 'city' in output_df.columns and 'market' in output_df.columns:
                unique_city_markets = output_df[['city', 'market']].drop_duplicates()
                processing_results['unique_cities_markets'] = len(unique_city_markets)
            
            # Update statistics
            self.stats['processed_files'] += 1
            self.stats['total_rows_processed'] += processing_results['rows_processed']
            if validation_success:
                self.stats['validation_passed'] += 1
            else:
                self.stats['validation_failed'] += 1
            
            self.logger.info(
                f"Successfully processed {filename}: "
                f"{processing_results['rows_processed']} rows, "
                f"{processing_results['unique_products']} products"
            )
            
        except Exception as e:
            processing_results['error_occurred'] = True
            processing_results['error_warnings'] = str(e)
            
            self.logger.error(f"Error processing Excel file {filename}: {str(e)}")
            self.logger.debug(f"Error details: {traceback.format_exc()}")
            
            self.stats['failed_files'] += 1
        
        # Calculate processing time
        processing_results['processing_duration_seconds'] = time.time() - start_time
        
        # Update registry with processing results
        if processing_results['error_occurred']:
            self.registry.update_processing_status(excel_id, 'error', error=processing_results['error_warnings'])
        else:
            self.registry.update_processing_results(excel_id, processing_results)
        
        return processing_results
    
    def process_excel_file(self, excel_id, file_path, output_dir=None, force_reprocess=False):
        """
        Process a single Excel file and save results to CSV.
        
        Args:
            excel_id (str): Excel ID from the registry
            file_path (str): Path to the Excel file
            output_dir (str): Directory to save processed data
            force_reprocess (bool): If True, reprocess file even if already processed
            
        Returns:
            dict: Processing results
        """
        filename = os.path.basename(file_path)
        self.logger.info(f"Processing Excel file: {filename}")
        
        # Check if file has already been processed
        excel_info = self.registry.get_excel_by_id(excel_id)
        
        if excel_info and excel_info['processing_status'] == 'processed' and not force_reprocess:
            self.logger.info(f"File already processed: {filename}")
            return None
        
        # Set default output directory
        output_dir = output_dir or PROCESSED_DIR
        os.makedirs(output_dir, exist_ok=True)
        
        # Update registry status to 'processing'
        self.registry.update_processing_status(excel_id, 'processing')
        
        # Processing results dictionary
        processing_results = {
            'unique_products': 0,
            'unique_major_categories': 0,
            'unique_subcategories': 0,
            'rows_processed': 0,
            'error_occurred': False,
            'error_warnings': "",
            'processing_duration_seconds': 0.0,
            'extraction_method': "excel_parsing",
            'processed_csv_path': "",
            'unique_cities_markets': 0,
            'pipeline_run_id': PIPELINE_RUN_ID or ""
        }
        
        start_time = time.time()
        
        try:
            # Read the Excel file using the reader component
            df = self.reader.read_and_prepare(file_path)
            
            # Get the extracted date from the reader and store it
            extracted_date = self.reader.get_extracted_date()
            if extracted_date:
                # Convert to pandas datetime if it's a string
                if isinstance(extracted_date, str):
                    extracted_date = pd.to_datetime(extracted_date)
                
                # Store in processing results as parsed_date
                processing_results['parsed_date'] = extracted_date
                self.logger.info(f"Extracted date from Excel content: {extracted_date}")
            else:
                # No content date found, we'll keep using the file_date from registry
                self.logger.info("No date extracted from Excel content, using file_date from filename")
            
            # Transform the data using the transformer component
            output_df = self.transformer.transform(df, file_path)
            
            # Create output path
            base_filename = os.path.splitext(filename)[0]
            output_csv = os.path.join(output_dir, f"{base_filename}.csv")
            
            # Update processing duration
            processing_results['processing_duration_seconds'] = time.time() - start_time
            
            # Import ValidityChecker if not already initialized
            if not hasattr(self, 'validator'):
                from agroamigo_pipeline.processors.validator import ValidityChecker
                self.validator = ValidityChecker(logger=self.logger)
            
            # Validate the output and update the registry
            validation_passed, updated_results = self.validator.validate_excel_output(
                self.registry, excel_id, output_df, output_csv, processing_results
            )
            
            # Update processing_results with results from validation
            processing_results = updated_results
            
            # Update statistics
            self.stats['processed_files'] += 1
            self.stats['total_rows_processed'] += processing_results['rows_processed']
            if validation_passed:
                self.stats['validation_passed'] += 1
                self.logger.info(
                    f"Successfully processed {filename}: "
                    f"{processing_results['rows_processed']} rows, "
                    f"{processing_results['unique_products']} products"
                )
            else:
                self.stats['validation_failed'] += 1
                self.logger.warning(f"Validation failed for {filename}: {processing_results['error_warnings']}")
            
        except Exception as e:
            processing_results['error_occurred'] = True
            processing_results['error_warnings'] = str(e)
            
            self.logger.error(f"Error processing Excel file {filename}: {str(e)}")
            self.logger.debug(f"Error details: {traceback.format_exc()}")
            
            self.stats['failed_files'] += 1
            
            # Update registry with error
            self.registry.update_processing_status(excel_id, 'error', error=processing_results['error_warnings'])
        
        return processing_results
    
    def validate_output_csv(self, output_csv, excel_id, output_df=None):
        """
        Validate the output CSV file and update the registry with validation results.
        
        Args:
            output_csv (str): Path to the output CSV file
            excel_id (str): Excel ID for the registry
            output_df (DataFrame, optional): DataFrame to validate, if None will read from output_csv
        
        Returns:
            bool: True if validation passed, False otherwise
        """
        # Import ValidityChecker if not already initialized
        if not hasattr(self, 'validator'):
            from agroamigo_pipeline.processors.validator import ValidityChecker
            self.validator = ValidityChecker(logger=self.logger)
            
        # If DataFrame not provided, read from CSV
        if output_df is None:
            try:
                output_df = pd.read_csv(output_csv)
            except Exception as e:
                self.logger.error(f"Error reading output CSV: {str(e)}")
                self.registry.update_processing_status(excel_id, 'error', error=f"Error reading output CSV: {str(e)}")
                return False
        
        # Prepare processing results
        processing_results = {
            'unique_products': 0,
            'unique_major_categories': 0,
            'unique_subcategories': 0,
            'rows_processed': len(output_df) if output_df is not None else 0,
            'error_occurred': False,
            'error_warnings': "",
            'processing_duration_seconds': 0.0,
            'extraction_method': "excel_parsing",
            'processed_csv_path': output_csv,
            'unique_cities_markets': 0,
            'pipeline_run_id': PIPELINE_RUN_ID or ""
        }
        
        # Validate using the ValidityChecker
        validation_passed, updated_results = self.validator.validate_excel_output(
            self.registry, excel_id, output_df, output_csv, processing_results
        )
        
        return validation_passed