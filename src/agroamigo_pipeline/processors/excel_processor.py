"""
Processor for Excel files with the specified format where columns are city/market combinations.
"""

import os
import pandas as pd
import logging
from datetime import datetime
import traceback

from agroamigo_pipeline.config.settings import SOURCE_DIR, PROCESSED_DIR
from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.utils.path_manager import ensure_directory, get_files_by_extension, generate_output_path
from agroamigo_pipeline.utils.parquet_registry import ParquetRegistry  # Changed from FileRegistry
from agroamigo_pipeline.utils.exceptions import ExcelProcessingError
from agroamigo_pipeline.utils.date_utils import filter_files_by_date
from agroamigo_pipeline.processors.excel_components.excel_reader import ExcelFileReader
from agroamigo_pipeline.processors.excel_components.excel_transformer import ExcelDataTransformer

class ExcelProcessor:
    """
    Process Excel files containing agricultural price data.
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
            
        # Initialize registry - changed to ParquetRegistry
        self.registry = ParquetRegistry()
        
        # Initialize components
        self.reader = ExcelFileReader(logger=self.logger)
        self.transformer = ExcelDataTransformer(logger=self.logger)
        
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
            'rows_processed': 0,
            'error_occurred': False,
            'errors_warnings': "",
            'processing_time_seconds': 0,
            'extracted_date': None
        }
        
        start_time = datetime.now()
        
        try:
            # Read the Excel file using the reader component
            df = self.reader.read_and_prepare(file_path)
            
            # Get the extracted date from the reader
            extracted_date = self.reader.get_extracted_date()
            stats['extracted_date'] = extracted_date.strftime('%Y-%m-%d') if extracted_date else None
            
            # Transform the data using the transformer component
            output_df = self.transformer.transform(df, file_path)
            
            # Update statistics
            stats['unique_products'] = len(output_df['product'].unique()) if 'product' in output_df.columns else 0
            stats['unique_major_categories'] = len(output_df['major_category'].unique()) if 'major_category' in output_df.columns else 0
            stats['rows_processed'] = len(output_df)
            
            # Generate output path
            output_path = generate_output_path(file_path, output_dir, extension='.csv')
            
            # Save to CSV
            output_df.to_csv(output_path, index=False)
            
            # Update overall statistics
            self.stats['processed_files'] += 1
            self.stats['total_rows_processed'] += stats['rows_processed']
            
            self.logger.info(f"Successfully processed Excel file {filename}: {stats['rows_processed']} rows, {stats['unique_products']} products")
            self.logger.info(f"Extracted date: {stats['extracted_date']}")
            
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
            start_date (datetime): Start date for filtering files by filename
            end_date (datetime): End date for filtering files by filename
            force_reprocess (bool): If True, reprocess files even if already processed
            max_workers (int): Maximum number of worker threads (not used in this implementation)
            
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
        self.logger.info(f"Date range filter: {start_date} to {end_date}")
        
        # Reset statistics
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'filtered_by_date': 0,
            'total_rows_processed': 0,
            'warnings': 0,
            'files_with_dates': 0,
            'files_without_dates': 0
        }
        
        # Get Excel files
        excel_files = get_files_by_extension(source_dir, ['.xls', '.xlsx'], recursive=True)
        self.logger.info(f"Found {len(excel_files)} Excel files in source directory")
        
        # Filter files by date in filename
        from agroamigo_pipeline.utils.date_utils import filter_files_by_date
        filtered_excel_files = filter_files_by_date(excel_files, start_date, end_date)
        
        # Update statistics with filtered count
        self.stats['total_files'] = len(excel_files)
        self.stats['filtered_by_date'] = len(excel_files) - len(filtered_excel_files)
        
        self.logger.info(f"After date filtering: {len(filtered_excel_files)} Excel files to process")
        
        # Process files sequentially (for simplicity and reliability)
        for file_path in filtered_excel_files:
            try:
                stats = self.process_excel_file(file_path, output_dir, force_reprocess)
                # Track date extraction success
                if stats.get('extracted_date'):
                    self.stats['files_with_dates'] += 1
                else:
                    self.stats['files_without_dates'] += 1
            except Exception as e:
                filename = os.path.basename(file_path)
                self.logger.error(f"Error processing {filename}: {str(e)}")
                self.stats['failed_files'] += 1
        
        # Log summary
        self.logger.info(f"Excel processing summary: {self.stats['processed_files']} processed, "
                        f"{self.stats['failed_files']} failed, {self.stats['skipped_files']} skipped, "
                        f"{self.stats['filtered_by_date']} filtered by date "
                        f"out of {self.stats['total_files']} total files")
        self.logger.info(f"Processed {self.stats['total_rows_processed']} total rows")
        self.logger.info(f"Successfully extracted dates from {self.stats['files_with_dates']} files, "
                        f"failed to extract dates from {self.stats['files_without_dates']} files")
        
        return self.stats

    def get_statistics(self):
        """
        Get the current processing statistics.
        
        Returns:
            dict: Dictionary containing processing statistics
        """
        return self.stats.copy()

    def clear_statistics(self):
        """
        Reset the processing statistics.
        """
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'total_rows_processed': 0,
            'warnings': 0
        }
        self.logger.info("Processing statistics cleared")