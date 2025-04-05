"""
Updated import section for the PDF processor orchestrator to use ParquetRegistry
"""

# Original imports
import os
import logging
import traceback
import concurrent.futures
from datetime import datetime
from queue import Queue
import copy
import threading
from functools import partial
from pathlib import Path
import pandas as pd

from agroamigo_pipeline.config.settings import (
    PDF_DIR, PROCESSED_DIR, MIN_UNIQUE_PRODUCTS, MAX_UNIQUE_PRODUCTS
)
from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.utils.path_manager import ensure_directory, get_files_by_extension
from agroamigo_pipeline.utils.parquet_registry import ParquetRegistry  # Changed from FileRegistry
from agroamigo_pipeline.utils.exceptions import PDFProcessingError

from agroamigo_pipeline.processors.pdf_components.text_extraction import TextExtractor
from agroamigo_pipeline.processors.pdf_components.metadata_extraction import MetadataExtractor
from agroamigo_pipeline.processors.pdf_components.product_parser import ProductParser
from agroamigo_pipeline.processors.pdf_components.image_handler import ImageHandler
from agroamigo_pipeline.processors.pdf_components.gpt_client import GPTClient
from agroamigo_pipeline.processors.pdf_components.gpt_csv_parser import GPTCSVParser


# Global lock for coordinating directory creation
dir_creation_lock = threading.Lock()

# Thread local storage for thread-specific loggers
thread_local = threading.local()

class PDFProcessorOrchestrator:
    """
    Orchestrates the PDF processing workflow for Colombian agricultural price data.
    """
    
    def __init__(self, logger=None, save_debug_data=False):
        """
        Initialize the PDF processor orchestrator.
        
        Args:
            logger (Logger): Logger instance
            save_debug_data (bool): Whether to save debug data (images, GPT responses, CSV data)
        """
        # Set up logger if not provided
        if logger is None:
            logging_setup = setup_logging()
            self.logger = logging_setup['pdf_processor']
        else:
            self.logger = logger
            
        # Debug flag
        self.save_debug_data = save_debug_data
        if self.save_debug_data:
            self.logger.info("Debug mode enabled - will save intermediate data to permanent directories")
            
        # Initialize registry - use it for reference only in multi-processing mode
        # Changed to use ParquetRegistry instead of FileRegistry
        self.registry = ParquetRegistry()
        
        # Initialize statistics dictionary
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'total_rows_processed': 0,
            'warnings': 0,
            'gpt4o_calls': 0,
            'files_using_gpt_extraction': 0
        }
        
        # Timing statistics
        self.timing_stats = {
            'pdf_text_extraction': [],
            'pdf_to_images': [],
            'image_encoding': [],
            'gpt4_query': [],
            'data_processing': []
        }
        
        # Initialize component modules with debug flag and the parent logger level
        self.text_extractor = TextExtractor(logger=self.logger, timing_stats=self.timing_stats)
        self.metadata_extractor = MetadataExtractor(logger=self.logger)
        self.product_parser = ProductParser(logger=self.logger, timing_stats=self.timing_stats)
        self.image_handler = ImageHandler(logger=self.logger, timing_stats=self.timing_stats, save_images=self.save_debug_data)
        self.gpt_client = GPTClient(logger=self.logger, timing_stats=self.timing_stats, save_responses=self.save_debug_data)
        self.gpt_csv_parser = GPTCSVParser(logger=self.logger, timing_stats=self.timing_stats, save_csv_data=self.save_debug_data)
        
        # Create necessary directories upfront to avoid race conditions
        self._ensure_all_directories()
        
        self.logger.info("PDFProcessorOrchestrator initialized")
        self.logger.debug(f"Logger level: {logging.getLevelName(self.logger.level)}")
    
    def _ensure_all_directories(self):
        """Create all required directories upfront to avoid race conditions."""
        with dir_creation_lock:
            # Core directories
            ensure_directory(PROCESSED_DIR)
            
            # Component specific directories
            log_dirs = [
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "gpt_responses"),
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "processed_data"),
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "permanent_images"),
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "permanent_gpt_responses"),
                os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "permanent_csv_data")
            ]
            
            for directory in log_dirs:
                ensure_directory(directory)
    
    def process_pdf_with_text_extraction(self, file_path, text, thread_logger, thread_timing_stats):
        """
        Process PDF using direct text extraction methods.
        
        Args:
            file_path (str): Path to the PDF file
            text (str): Extracted text
            thread_logger (Logger): Thread-specific logger
            thread_timing_stats (dict): Thread-specific timing statistics
                
        Returns:
            DataFrame: Processed data in standardized format
        """
        filename = os.path.basename(file_path)
        thread_logger.info(f"----> Text extraction flow for: {filename}")
        
        # Create thread-local component instances
        metadata_extractor = MetadataExtractor(logger=thread_logger)
        product_parser = ProductParser(logger=thread_logger, timing_stats=thread_timing_stats)
        text_extractor = TextExtractor(logger=thread_logger, timing_stats=thread_timing_stats)
        
        # Extract metadata from text
        thread_logger.info(f"      Extracting metadata from text")
        metadata = metadata_extractor.extract_metadata_from_pdf(file_path, text)
        thread_logger.info(f"      Metadata extracted: date={metadata['date']}, city={metadata['city']}, market={metadata['market']}")
        
        # Try table extraction first
        thread_logger.info(f"      Attempting to extract tables with pdfplumber")
        tables = text_extractor.extract_table_with_pdfplumber(file_path)
        
        if tables and len(tables) > 0:
            thread_logger.info(f"      Successfully extracted {len(tables)} tables, processing table data")
            result = product_parser.process_table_data(file_path, tables, metadata)
            if result is not None and not result.empty:
                thread_logger.info(f"      Table extraction successful, extracted {len(result)} rows")
                thread_logger.info(f"<---- Completed table extraction flow for: {filename}")
                return result
            else:
                thread_logger.info(f"      Table extraction failed to produce results, falling back to text processing")
        else:
            thread_logger.info(f"      No tables extracted, falling back to text processing")
        
        thread_logger.info(f"<---- Completed text extraction flow for: {filename}")
        return pd.DataFrame()  # Return empty DataFrame if processing failed
    
    def process_pdf_with_gpt4o(self, file_path, thread_logger, thread_timing_stats):
        """
        Process PDF using GPT-4o mini for image-based extraction.
        
        Args:
            file_path (str): Path to the PDF file
            thread_logger (Logger): Thread-specific logger
            thread_timing_stats (dict): Thread-specific timing statistics
            
        Returns:
            tuple: (DataFrame, int) - Processed data and number of GPT4o calls made
        """
        filename = os.path.basename(file_path)
        thread_logger.info(f"----> GPT-4o vision flow for: {filename}")
        gpt4o_calls = 0
        
        try:
            # Create thread-local component instances
            text_extractor = TextExtractor(logger=thread_logger, timing_stats=thread_timing_stats)
            metadata_extractor = MetadataExtractor(logger=thread_logger)
            image_handler = ImageHandler(logger=thread_logger, timing_stats=thread_timing_stats, save_images=self.save_debug_data)
            gpt_client = GPTClient(logger=thread_logger, timing_stats=thread_timing_stats, save_responses=self.save_debug_data)
            gpt_csv_parser = GPTCSVParser(logger=thread_logger, timing_stats=thread_timing_stats, save_csv_data=self.save_debug_data)
            
            # First extract text to get metadata
            thread_logger.info(f"      Extracting text for metadata")
            text_to_use = text_extractor.extract_text_with_pdfplumber(file_path)
            thread_logger.info(f"      Using text from pdfplumber for metadata")
            
            # Extract metadata from text
            thread_logger.info(f"      Extracting metadata from text")
            metadata = metadata_extractor.extract_metadata_from_pdf(file_path, text_to_use)
            thread_logger.info(f"      Metadata extracted: date={metadata['date']}, city={metadata['city']}, market={metadata['market']}")
            
            # Convert PDF to images
            thread_logger.info(f"      Converting PDF to images")
            image_paths = image_handler.convert_pdf_to_images(file_path, save_copies=True)
            thread_logger.info(f"      PDF converted to {len(image_paths)} images")
            
            # Check if we need to extract header metadata
            need_header_extraction = metadata['city'] == 'Unknown' or metadata['market'] == 'Unknown'
            
            # Encode all images to base64
            thread_logger.info(f"      Encoding all images to base64")
            base64_images = []
            for img_path in image_paths:
                base64_images.append(image_handler.encode_image(img_path))
            thread_logger.info(f"      All {len(base64_images)} images encoded")
            
            # Process images for tables using synchronous requests
            csv_texts = []
            thread_logger.info(f"      Processing images for table extraction")
            
            for i, (image_path, base64_image) in enumerate(zip(image_paths, base64_images)):
                thread_logger.info(f"      Processing image {i+1}/{len(image_paths)} for table extraction")
                csv_text = gpt_client.query_gpt4o_with_image(image_path, base64_image)
                if csv_text:
                    thread_logger.info(f"      Successfully extracted table from image {i+1}")
                    csv_texts.append(csv_text)
                    gpt4o_calls += 1
                else:
                    thread_logger.warning(f"      Failed to extract table from image {i+1}")
            
            # Process header if needed
            if need_header_extraction and len(image_paths) > 0:
                thread_logger.info(f"      Extracting header data from first image")
                header_text = gpt_client.query_gpt4o_with_image(
                    image_paths[0], 
                    base64_images[0], 
                    prompt_type="header"
                )
                gpt4o_calls += 1
                
                if header_text:
                    thread_logger.info(f"      Successfully extracted header: {header_text[:100]}...")
                    
                    # Update metadata using the header text
                    thread_logger.info(f"      Updating metadata using extracted header")
                    updated_metadata = metadata_extractor.extract_metadata_from_pdf(file_path, header_text)
                    
                    # Only update missing fields
                    if metadata['city'] == 'Unknown' and updated_metadata['city'] != 'Unknown':
                        metadata['city'] = updated_metadata['city']
                        thread_logger.info(f"      Updated city to: {metadata['city']}")
                    
                    if metadata['market'] == 'Unknown' and updated_metadata['market'] != 'Unknown':
                        metadata['market'] = updated_metadata['market']
                        thread_logger.info(f"      Updated market to: {metadata['market']}")
                    
                    if metadata['date'] == datetime.now().strftime('%Y-%m-%d') and updated_metadata['date'] != metadata['date']:
                        metadata['date'] = updated_metadata['date']
                        thread_logger.info(f"      Updated date to: {metadata['date']}")
            
            # Process the CSV texts
            thread_logger.info(f"      Processing {len(csv_texts)} CSV texts from GPT-4o mini")
            result = gpt_csv_parser.process_gpt4o_results(file_path, csv_texts, metadata)
            thread_logger.info(f"      CSV processing complete, extracted {len(result) if result is not None else 0} rows")
            
            thread_logger.info(f"<---- Completed GPT-4o vision flow for: {filename}")
            return result, gpt4o_calls
            
        except Exception as e:
            thread_logger.error(f"Error processing PDF with GPT-4o mini: {str(e)}")
            thread_logger.info(f"Error details: {traceback.format_exc()}")
            thread_logger.info(f"<---- Failed GPT-4o vision flow for: {filename}")
            return pd.DataFrame(), gpt4o_calls  # Return empty DataFrame with the number of calls made
    
    def process_single_pdf_file(self, file_path, output_dir, force_reprocess, registry_info):
        """
        Process a single PDF file in a thread-safe manner.
        
        Args:
            file_path (str): Path to the PDF file
            output_dir (str): Directory to save processed data
            force_reprocess (bool): If True, reprocess file even if already processed
            registry_info (dict): Pre-checked registry information
            
        Returns:
            dict: Result information including stats and file registry data
        """
        # Create thread-specific logger
        thread_id = threading.get_ident()
        thread_logger = logging.getLogger(f'pdf_processor_{thread_id}')
        thread_logger.setLevel(self.logger.level)  # Use the same level as parent logger
        
        # Add handlers to the logger
        for handler in self.logger.handlers:
            # Create a copy of the handler to avoid sharing handler instances
            if isinstance(handler, logging.FileHandler):
                file_handler = logging.FileHandler(
                    handler.baseFilename, mode='a', encoding='utf-8'
                )
                file_handler.setLevel(self.logger.level)  # Use parent logger's level
                file_handler.setFormatter(handler.formatter)
                thread_logger.addHandler(file_handler)
            elif isinstance(handler, logging.StreamHandler):
                console_handler = logging.StreamHandler()
                console_handler.setLevel(max(logging.INFO, self.logger.level))  # Console shows INFO or higher
                console_handler.setFormatter(handler.formatter)
                thread_logger.addHandler(console_handler)
        
        # Create thread-specific timing stats
        thread_timing_stats = {
            'pdf_text_extraction': [],
            'pdf_to_images': [],
            'image_encoding': [],
            'gpt4_query': [],
            'data_processing': []
        }
        
        filename = os.path.basename(file_path)
        thread_logger.info(f"========== STARTING PDF PROCESSING: {filename} ==========")
        thread_logger.debug(f"Thread logger using level: {logging.getLevelName(thread_logger.level)}")
        
        # Rest of the method would follow...
        
        # Check if file should be processed based on pre-checked registry info
        if not force_reprocess and not registry_info['process']:
            thread_logger.info(f"File already processed: {filename}")
            return {
                'stats': registry_info['stats'],
                'already_processed': True,
                'registry_update': None,
                'output_path': None
            }
        
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
            'processing_time_seconds': 0,
            'extraction_method': "",
            'text_is_valid': False,
            'used_gpt_extraction': False
        }
        
        start_time = datetime.now()
        thread_logger.info(f"Starting processing at: {start_time}")
        
        try:
            # Create thread-local TextExtractor for text extraction
            text_extractor = TextExtractor(logger=thread_logger, timing_stats=thread_timing_stats)
            
            # Flag to track if text extraction had errors
            text_extraction_error = False
            text_to_use = ""
            
            try:
                thread_logger.info(f"Step 1: Attempting text extraction from PDF")
                # Use only pdfplumber for text extraction
                thread_logger.info(f"Extracting text using pdfplumber")
                text_to_use = text_extractor.extract_text_with_pdfplumber(file_path)
                thread_logger.info(f"pdfplumber extraction complete, extracted {len(text_to_use)} characters")
            except Exception as e:
                thread_logger.error(f"Error during text extraction: {str(e)}")
                text_extraction_error = True
                stats['errors_warnings'] += f"Text extraction error: {str(e)}; "
            
            # Assess text validity only if extraction was successful
            text_is_valid = False
            if not text_extraction_error:
                thread_logger.info(f"Step 2: Checking if text is valid")
                text_is_valid = text_extractor.assess_text_quality(text_to_use)
                stats['text_is_valid'] = text_is_valid
                thread_logger.info(f"Text is valid: {text_is_valid}")
            
            # Processing strategy based on text validity
            if not text_extraction_error and text_is_valid:
                # Valid text - process directly
                thread_logger.info(f"Step 3A: Text is valid, processing with direct text extraction")
                stats['extraction_method'] = "text_extraction"
                stats['used_gpt_extraction'] = False  # Text extraction was used
                thread_logger.info(f"Extracting metadata from text")
                output_df = self.process_pdf_with_text_extraction(file_path, text_to_use, thread_logger, thread_timing_stats)
                thread_logger.info(f"Text extraction processing complete, extracted {len(output_df) if output_df is not None else 0} rows")
            else:
                # Invalid text or extraction error - use GPT-4o mini with image
                if text_extraction_error:
                    thread_logger.info(f"Step 3B: Text extraction failed, processing with GPT-4o vision")
                else:
                    thread_logger.info(f"Step 3B: Text is not valid, processing with GPT-4o vision")
                    
                stats['extraction_method'] = "gpt4o_vision"
                stats['used_gpt_extraction'] = True  # GPT image extraction was used
                thread_logger.info(f"Converting PDF to images and processing with GPT-4o")
                output_df, gpt4o_calls = self.process_pdf_with_gpt4o(file_path, thread_logger, thread_timing_stats)
                thread_logger.info(f"GPT-4o vision processing complete, extracted {len(output_df) if output_df is not None else 0} rows")
            
            # Check if we have results
            if output_df is not None and not output_df.empty:
                # Create output path
                base_filename = os.path.splitext(filename)[0]
                output_csv = os.path.join(output_dir, f"{base_filename}.csv")
                
                # Save to CSV - use a lock to prevent contention during file writing
                thread_logger.info(f"Step 4: Saving extracted data to CSV")
                output_df.to_csv(output_csv, index=False)
                thread_logger.info(f"Saved output to: {output_csv}")
                
                # Update statistics
                stats['unique_products'] = len(output_df['product'].unique())
                stats['unique_major_categories'] = len(output_df['major_category'].unique())
                stats['unique_subcategories'] = len(output_df['sub_category'].unique())
                stats['rows_processed'] = len(output_df)
                
                thread_logger.info(f"Data statistics: {stats['rows_processed']} rows, {stats['unique_products']} products, " + 
                               f"{stats['unique_major_categories']} major categories, {stats['unique_subcategories']} subcategories")
                thread_logger.info(f"Used GPT extraction: {stats['used_gpt_extraction']}")
                
                output_path = output_csv
                
            else:
                thread_logger.warning(f"No data extracted from PDF, no output file created")
                stats['error_occurred'] = True
                stats['errors_warnings'] += "No data extracted from PDF; "
                output_path = None
            
        except Exception as e:
            stats['error_occurred'] = True
            stats['errors_warnings'] += str(e)
            
            thread_logger.error(f"Error processing PDF file {filename}: {str(e)}")
            thread_logger.error(f"Error details: {traceback.format_exc()}")
            
            output_path = None
        
        # Calculate processing time
        stats['processing_time_seconds'] = (datetime.now() - start_time).total_seconds()
        thread_logger.info(f"Total processing time: {stats['processing_time_seconds']:.2f} seconds")
        thread_logger.info(f"Used GPT extraction: {stats['used_gpt_extraction']}")
        
        # Prepare registry update information
        registry_update = {
            'file_path': file_path,
            'output_path': output_path,
            'status': "processed" if not stats['error_occurred'] else "error",
            'error': stats['errors_warnings'] if stats['error_occurred'] else None,
            'stats': stats
        }
        
        thread_logger.info(f"========== COMPLETED PDF PROCESSING: {filename} ==========")
        
        # Return the stats, timing stats, and registry update info
        return {
            'stats': stats,
            'timing_stats': thread_timing_stats,
            'registry_update': registry_update,
            'output_path': output_path,
            'already_processed': False,
            'gpt4o_calls': gpt4o_calls if 'gpt4o_calls' in locals() else 0
        }
    
    def process_pdf_files(self, pdf_files=None, output_dir=None, start_date=None, end_date=None, max_workers=None, force_reprocess=False):
        """
        Process multiple PDF files with multithreading.
        
        Args:
            pdf_files (list): List of PDF file paths to process
            output_dir (str): Directory to save processed data
            start_date (datetime): Start date for filtering files by filename
            end_date (datetime): End date for filtering files by filename
            max_workers (int): Maximum number of worker threads
            force_reprocess (bool): If True, reprocess files even if already processed
            
        Returns:
            dict: Statistics about the processing
        """
        # Set default directories
        output_dir = output_dir or PROCESSED_DIR
        ensure_directory(output_dir)
        
        # After getting PDF files but before filtering
        if pdf_files is None:
            self.logger.info(f"Finding PDF files in {PDF_DIR}")
            pdf_files = get_files_by_extension(PDF_DIR, ['.pdf'], recursive=True)
            # Convert PosixPath objects to strings if necessary
            pdf_files = [str(file_path) for file_path in pdf_files]
        
        self.logger.info(f"Found {len(pdf_files)} PDF files")
        self.logger.info(f"Date range filter: {start_date} to {end_date}")
        
        # Filter files by date in filename
        from agroamigo_pipeline.utils.date_utils import filter_files_by_date
        filtered_pdf_files = filter_files_by_date(pdf_files, start_date, end_date)
        filtered_pdf_files = [str(file_path) for file_path in filtered_pdf_files]
        self.logger.info(f"After date filtering: {len(filtered_pdf_files)} PDF files to process")
        
        # Reset statistics
        self.stats = {
            'total_files': len(pdf_files),
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'filtered_by_date': len(pdf_files) - len(filtered_pdf_files),
            'total_rows_processed': 0,
            'warnings': 0,
            'gpt4o_calls': 0,
            'files_using_gpt_extraction': 0
        }
        
        # Pre-check registry to determine which files need processing
        registry_info = {}
        for file_path in filtered_pdf_files:
            if not force_reprocess and self.registry.file_processed(file_path):
                file_info = self.registry.get_file_info(file_path)
                registry_info[file_path] = {
                    'process': False,
                    'stats': file_info['processing_history'][-1]['stats'] if file_info else None
                }
                self.stats['skipped_files'] += 1
            else:
                registry_info[file_path] = {'process': True, 'stats': None}
        
        # Set maximum number of workers
        max_workers = max_workers or min(os.cpu_count(), len(filtered_pdf_files))
        
        # Determine if we can use concurrent processing
        use_concurrent = max_workers > 1 and len(filtered_pdf_files) > 1
        
        results = []
        
        if use_concurrent:
            self.logger.info(f"Using {max_workers} workers for parallel processing")
            
            # Process files concurrently using ProcessPoolExecutor
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit tasks
                future_to_file = {}
                for file_path in filtered_pdf_files:
                    if registry_info[file_path]['process'] or force_reprocess:
                        future = executor.submit(
                            self.process_single_pdf_file,
                            file_path,
                            output_dir,
                            force_reprocess,
                            registry_info[file_path]
                        )
                        future_to_file[future] = file_path
                
                # Collect results as they complete
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path = future_to_file[future]
                    filename = os.path.basename(file_path)
                    
                    try:
                        result = future.result()
                        results.append(result)
                        
                        if result['already_processed']:
                            # File was already processed and we're using cached results
                            continue
                        
                        # Update stats based on result
                        if result['stats']['error_occurred']:
                            self.stats['failed_files'] += 1
                        else:
                            self.stats['processed_files'] += 1
                            self.stats['total_rows_processed'] += result['stats']['rows_processed']
                            
                        # Track GPT usage
                        if result['stats']['used_gpt_extraction']:
                            self.stats['files_using_gpt_extraction'] += 1
                            
                        if 'gpt4o_calls' in result:
                            self.stats['gpt4o_calls'] += result['gpt4o_calls']
                            
                    except Exception as e:
                        self.logger.error(f"Error processing {filename}: {str(e)}")
                        self.stats['failed_files'] += 1
        else:
            # Process files sequentially
            for file_path in filtered_pdf_files:
                try:
                    result = self.process_single_pdf_file(
                        file_path,
                        output_dir,
                        force_reprocess,
                        registry_info[file_path]
                    )
                    results.append(result)
                    
                    if result['already_processed']:
                        # File was already processed and we're using cached results
                        continue
                        
                    # Update stats
                    if result['stats']['error_occurred']:
                        self.stats['failed_files'] += 1
                    else:
                        self.stats['processed_files'] += 1
                        self.stats['total_rows_processed'] += result['stats']['rows_processed']
                    
                    # Track GPT usage
                    if result['stats']['used_gpt_extraction']:
                        self.stats['files_using_gpt_extraction'] += 1
                        
                    if 'gpt4o_calls' in result:
                        self.stats['gpt4o_calls'] += result['gpt4o_calls']
                        
                except Exception as e:
                    filename = os.path.basename(file_path)
                    self.logger.error(f"Error processing {filename}: {str(e)}")
                    self.stats['failed_files'] += 1
        
        # Update registry with all results
        for result in results:
            if result['registry_update']:
                self.registry.register_file(
                    result['registry_update']['file_path'],
                    output_path=result['registry_update']['output_path'],
                    status=result['registry_update']['status'],
                    error=result['registry_update']['error'],
                    stats=result['registry_update']['stats']
                )
        
        # Log summary
        self.logger.info(f"PDF processing summary: {self.stats['processed_files']} processed, "
                      f"{self.stats['failed_files']} failed, {self.stats['skipped_files']} skipped, "
                      f"{self.stats['filtered_by_date']} filtered by date "
                      f"out of {self.stats['total_files']} total files")
        self.logger.info(f"Processed {self.stats['total_rows_processed']} total rows")
        self.logger.info(f"Files using GPT extraction: {self.stats['files_using_gpt_extraction']} out of {self.stats['processed_files']} processed files")
        self.logger.info(f"Made {self.stats['gpt4o_calls']} GPT-4o calls")
        
        # Merge timing statistics from all threads
        for result in results:
            if 'timing_stats' in result:
                for key, values in result['timing_stats'].items():
                    if key in self.timing_stats:
                        self.timing_stats[key].extend(values)
        
        return self.stats