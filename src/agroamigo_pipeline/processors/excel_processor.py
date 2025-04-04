import os
import pandas as pd
import numpy as np
import re
from pathlib import Path
import logging
from datetime import datetime
import concurrent.futures
import traceback

from agroamigo_pipeline.config.settings import SOURCE_DIR, PROCESSED_DIR, MIN_UNIQUE_PRODUCTS, MAX_UNIQUE_PRODUCTS
from agroamigo_pipeline.utils.logger import setup_logging, log_function_call, log_dataframe_info
from agroamigo_pipeline.utils.path_manager import ensure_directory, get_files_by_extension, generate_output_path
from agroamigo_pipeline.utils.file_registry import FileRegistry
from agroamigo_pipeline.utils.utils import normalize_string, extract_date_from_spanish_text, format_date, extract_geography_info, clean_price_value
from agroamigo_pipeline.utils.exceptions import ExcelProcessingError
from agroamigo_pipeline.processors.excel_components.excel_reader import ExcelFileReader
from agroamigo_pipeline.processors.excel_components.metadata_extractor import MetadataExtractor
from agroamigo_pipeline.processors.excel_components.data_transformer import ExcelDataTransformer

class ExcelProcessor:
    """
    Process Excel files containing agricultural price data according to the 'anexo' format.
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
            
        # Initialize file registry
        self.registry = FileRegistry()
        
        # Initialize components
        self.reader = ExcelFileReader(logger=self.logger)
        self.metadata_extractor = MetadataExtractor(logger=self.logger)
        self.data_transformer = ExcelDataTransformer(logger=self.logger)
        
        # Initialize statistics dictionary
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'total_rows_processed': 0,
            'warnings': 0
        }
        
        self.logger.info("ExcelProcessor initialized")
    
    def _process_excel_to_dataframe(self, file_path):
        """
        Process an Excel file and extract structured data.
        
        Args:
            file_path (str): Path to the Excel file
            
        Returns:
            DataFrame: Processed data in standardized format
        """
        try:
            # Read and prepare the Excel file
            df = self.reader.read_and_prepare(file_path)
            
            # Also read the raw file for date extraction (without skiprows)
            raw_df = None
            try:
                raw_df = pd.read_excel(file_path)
            except Exception:
                # If we can't read the raw file, we'll use the prepared one
                pass
            
            # Extract metadata
            metadata = self.metadata_extractor.extract_all(df, raw_df)
            
            # Transform the data
            price_df = self.data_transformer.transform(df, metadata)
            
            return price_df
            
        except Exception as e:
            self.logger.error(f"Error processing Excel file {file_path}: {str(e)}")
            self.logger.debug(f"Error details: {traceback.format_exc()}")
            raise ExcelProcessingError(f"Failed to process Excel file: {str(e)}")
    
    def process_excel_file(self, file_path, output_dir=None, force_reprocess=False):
        """
        Process a single Excel file and save results to CSV.
        
        Args:
            file_path (str): Path to the Excel file
            output_dir (str): Directory to save processed data (default: PROCESSED_DIR)
            force_reprocess (bool): If True, reprocess file even if already processed
            
        Returns:
            dict: Statistics about the processing
        """
        filename = os.path.basename(file_path)
        self.logger.info(f"Processing Excel file: {filename}")
        
        # Check if file has already been processed
        if not force_reprocess and self.registry.file_processed(file_path):
            self.logger.info(f"File already processed: {filename}")
            return self.registry.get_file_info(file_path)['processing_history'][-1]['stats']
        
        # Set default output directory
        output_dir = output_dir or PROCESSED_DIR
        ensure_directory(output_dir)
        
        # Statistics dictionary
        stats = {
            'filename': filename,
            'file_path': file_path,
            'unique_products': 0,
            'unique_major_categories': 0,
            'unique_subcategories': 0,
            'rows_processed': 0,
            'error_occurred': False,
            'errors_warnings': "",
            'processing_time_seconds': 0
        }
        
        start_time = datetime.now()
        
        try:
            # Process Excel file
            output_df = self._process_excel_to_dataframe(file_path)
            
            # Update statistics
            stats['unique_products'] = len(output_df['product'].unique()) if 'product' in output_df.columns else 0
            stats['unique_major_categories'] = len(output_df['major_category'].unique()) if 'major_category' in output_df.columns else 0
            stats['unique_subcategories'] = len(output_df['sub_category'].unique()) if 'sub_category' in output_df.columns and not output_df['sub_category'].isna().all() else 0
            stats['rows_processed'] = len(output_df)
            
            # Check if we have enough unique products
            if stats['unique_products'] < MIN_UNIQUE_PRODUCTS:
                warning_msg = f"Warning: File has only {stats['unique_products']} unique products, which is below the minimum threshold of {MIN_UNIQUE_PRODUCTS}"
                self.logger.warning(warning_msg)
                stats['errors_warnings'] += warning_msg + "; "
            
            # Check if we have too many unique products (might indicate parsing error)
            if stats['unique_products'] > MAX_UNIQUE_PRODUCTS:
                warning_msg = f"Warning: File has {stats['unique_products']} unique products, which exceeds the maximum threshold of {MAX_UNIQUE_PRODUCTS}"
                self.logger.warning(warning_msg)
                stats['errors_warnings'] += warning_msg + "; "
            
            # Generate output path
            output_path = generate_output_path(file_path, output_dir, extension='.csv')
            
            # Save to CSV
            output_df.to_csv(output_path, index=False)
            
            # Update overall statistics
            self.stats['processed_files'] += 1
            self.stats['total_rows_processed'] += stats['rows_processed']
            
            self.logger.info(f"Successfully processed Excel file {filename}: {stats['rows_processed']} rows, {stats['unique_products']} products")
            
        except Exception as e:
            stats['error_occurred'] = True
            stats['errors_warnings'] += str(e)
            
            self.logger.error(f"Error processing Excel file {filename}: {str(e)}")
            self.logger.debug(f"Error details: {traceback.format_exc()}")
            
            # Update overall statistics
            self.stats['failed_files'] += 1
            output_path = None
        
        # Calculate processing time
        stats['processing_time_seconds'] = (datetime.now() - start_time).total_seconds()
        
        # Register processed file
        self.registry.register_file(
            file_path,
            output_path=output_path,
            status="processed" if not stats['error_occurred'] else "error",
            error=stats['errors_warnings'] if stats['error_occurred'] else None,
            stats=stats
        )
        
        return stats
    
    def process_excel_files(self, source_dir=None, output_dir=None, start_date=None, end_date=None, force_reprocess=False, max_workers=None):
        """
        Process all Excel files in the source directory.
        
        Args:
            source_dir (str): Directory containing Excel files
            output_dir (str): Directory to save processed data
            start_date (datetime): Start date for filtering files
            end_date (datetime): End date for filtering files
            force_reprocess (bool): If True, reprocess files even if already processed
            max_workers (int): Maximum number of worker threads
            
        Returns:
            dict: Statistics about the processing
        """
        # Set default directories
        source_dir = source_dir or SOURCE_DIR
        output_dir = output_dir or PROCESSED_DIR
        
        # Ensure directories exist
        ensure_directory(source_dir)
        ensure_directory(output_dir)
        
        self.logger.info(f"Processing Excel files in {source_dir}")
        
        # Reset statistics
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'total_rows_processed': 0,
            'warnings': 0
        }
        
        # Get Excel files
        excel_files = get_files_by_extension(source_dir, ['.xls', '.xlsx'], recursive=True)
        self.stats['total_files'] = len(excel_files)
        
        self.logger.info(f"Found {len(excel_files)} Excel files in source directory")
        
        # Set up parallel processing
        max_workers = max_workers or min(os.cpu_count(), len(excel_files))
        
        # Determine if we can use concurrent processing (more than one file and multiple CPUs)
        use_concurrent = max_workers > 1 and len(excel_files) > 1
        
        if use_concurrent:
            self.logger.info(f"Using {max_workers} workers for parallel processing")
            
            # Process files concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {
                    executor.submit(self.process_excel_file, file_path, output_dir, force_reprocess): file_path
                    for file_path in excel_files
                }
                
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path = future_to_file[future]
                    filename = os.path.basename(file_path)
                    
                    try:
                        stats = future.result()
                        # Stats are already aggregated in self.stats by each process_excel_file call
                    except Exception as e:
                        self.logger.error(f"Error processing {filename}: {str(e)}")
                        self.stats['failed_files'] += 1
        else:
            # Process files sequentially
            for file_path in excel_files:
                try:
                    self.process_excel_file(file_path, output_dir, force_reprocess)
                except Exception as e:
                    filename = os.path.basename(file_path)
                    self.logger.error(f"Error processing {filename}: {str(e)}")
                    self.stats['failed_files'] += 1
        
        # Log summary
        self.logger.info(f"Excel processing summary: {self.stats['processed_files']} processed, "
                        f"{self.stats['failed_files']} failed, {self.stats['skipped_files']} skipped "
                        f"out of {self.stats['total_files']} total files")
        self.logger.info(f"Processed {self.stats['total_rows_processed']} total rows")
        
        return self.stats