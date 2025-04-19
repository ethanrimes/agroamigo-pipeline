"""
PDF processor orchestrator component.
Handles the detailed orchestration of PDF processing tasks.
"""

import os
import logging
import traceback
import concurrent.futures
from datetime import datetime
import threading
from pathlib import Path
import pandas as pd
import time
import queue
import re
from tqdm import tqdm

from agroamigo_pipeline.config.settings import (
    PDF_DIR, PROCESSED_DIR, MIN_UNIQUE_PRODUCTS, MAX_UNIQUE_PRODUCTS, PIPELINE_RUN_ID
)
from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.utils.path_manager import ensure_directory, get_files_by_extension
from agroamigo_pipeline.registry.pdf_registry import PDFProcessorRegistry
from agroamigo_pipeline.utils.exceptions import PDFProcessingError, EmptyProductDataError

from agroamigo_pipeline.processors.pdf_components.text_extraction import TextExtractor
from agroamigo_pipeline.processors.pdf_components.metadata_extraction import MetadataExtractor
from agroamigo_pipeline.processors.pdf_components.product_parser import ProductParser
from agroamigo_pipeline.processors.pdf_components.image_handler import ImageHandler
from agroamigo_pipeline.processors.pdf_components.gpt_client import GPTClient
from agroamigo_pipeline.processors.pdf_components.gpt_csv_parser import GPTCSVParser
from agroamigo_pipeline.error_handlers.validity_checker import ValidityChecker

# Implement thread locking 
dir_creation_lock = threading.Lock() # Global lock for coordinating directory creation
thread_local = threading.local() # Thread local storage for thread-specific loggers
global_stats_lock = threading.Lock() # Global stats dictionary for thread-safe updates
registry_lock = threading.Lock() # Global registry lock for thread-safe registry operations
output_dir_lock = threading.Lock() # Global output directory lock for thread-safe output directory creation

class PDFProcessorOrchestrator:
    """
    Orchestrates the PDF processing workflow for Colombian agricultural price data.
    """
    
    def __init__(self, logger=None, save_debug_data=False, registry=None):
        """
        Initialize the PDF processor orchestrator.
        
        Args:
            logger (Logger): Logger instance
            save_debug_data (bool): Whether to save debug data (images, GPT responses, CSV data)
            registry (PDFProcessorRegistry): Registry instance for PDF tracking
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
            
        # Initialize registry - use provided registry or create new one
        self.registry = registry or PDFProcessorRegistry(logger=self.logger)
        
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
        
        # Initialize the validity checker
        self.validity_checker = ValidityChecker(logger=self.logger)
        
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
        thread_logger.debug(f"{file_path} -----> Text extraction flow for: {filename}")
        
        # Create thread-local component instances
        metadata_extractor = MetadataExtractor(logger=thread_logger)
        product_parser = ProductParser(logger=thread_logger, timing_stats=thread_timing_stats)
        text_extractor = TextExtractor(logger=thread_logger, timing_stats=thread_timing_stats)
        
        # Extract metadata from text
        thread_logger.debug(f"{file_path} -      Extracting metadata from text")
        metadata = metadata_extractor.extract_metadata_from_pdf(file_path, text)
        thread_logger.debug(f"{file_path} -      Metadata extracted: date={metadata['date']}, city={metadata['city']}, market={metadata['market']}")
        
        # Try table extraction first
        thread_logger.debug(f"{file_path} -      Attempting to extract tables with pdfplumber")
        tables = text_extractor.extract_table_with_pdfplumber(file_path)
        
        if tables and len(tables) > 0:
            thread_logger.debug(f"{file_path} -      Successfully extracted {len(tables)} tables, processing table data")
            result = product_parser.process_table_data(file_path, tables, metadata)
            if result is not None and not result.empty:
                thread_logger.debug(f"{file_path} -      Table extraction successful, extracted {len(result)} rows")
                thread_logger.debug(f"{file_path} -<---- Completed table extraction flow for: {filename}")
                return result
            else:
                thread_logger.debug(f"{file_path} -      Table extraction failed to produce results, falling back to text processing")
        else:
            thread_logger.debug(f"{file_path} -      No tables extracted, error")
        
        # Process text directly if table extraction failed
        thread_logger.debug(f"{file_path} -      Table extraction failed")
        return pd.DataFrame()  # Return empty DataFrame if processing failed
    
    def process_pdf_with_gpt4o(self, file_path, thread_logger, thread_timing_stats):
        """
        Process PDF using GPT-4.1 nano for image-based extraction.
        
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
            thread_logger.debug(f"      Extracting text for metadata")
            text_to_use = text_extractor.extract_text_with_pdfplumber(file_path)
            thread_logger.debug(f"      Using text from pdfplumber for metadata")
            
            # Extract metadata from text
            thread_logger.debug(f"      Extracting metadata from text")
            metadata = metadata_extractor.extract_metadata_from_pdf(file_path, text_to_use)
            thread_logger.debug(f"      Metadata extracted: date={metadata['date']}, city={metadata['city']}, market={metadata['market']}")
            
            # Convert PDF to images
            thread_logger.debug(f"      Converting PDF to images")
            image_paths = image_handler.convert_pdf_to_images(file_path, save_copies=True)
            thread_logger.debug(f"      PDF converted to {len(image_paths)} images")
            
            # Check if we need to extract header metadata
            need_header_extraction = metadata['city'] == 'Unknown' or metadata['market'] == 'Unknown'
            
            # Encode all images to base64
            thread_logger.debug(f"      Encoding all images to base64")
            base64_images = []
            for img_path in image_paths:
                base64_images.append(image_handler.encode_image(img_path))
            thread_logger.debug(f"      All {len(base64_images)} images encoded")
            
            # Process images for tables using synchronous requests
            csv_texts = []
            thread_logger.debug(f"      Processing images for table extraction")
            
            for i, (image_path, base64_image) in enumerate(zip(image_paths, base64_images)):
                thread_logger.debug(f"      Processing image {i+1}/{len(image_paths)} for table extraction")
                csv_text = gpt_client.query_gpt4o_with_image(image_path, base64_image, source_pdf_path=file_path)
                if csv_text:
                    thread_logger.debug(f"      Successfully extracted table from image {i+1}")
                    csv_texts.append(csv_text)
                    gpt4o_calls += 1
                else:
                    thread_logger.warning(f"      Failed to extract table from image {i+1}")
            
            # Process header if needed
            if need_header_extraction and len(image_paths) > 0:
                thread_logger.debug(f"      Extracting header data from first image")
                header_text = gpt_client.query_gpt4o_with_image(
                    image_paths[0], 
                    base64_images[0], 
                    prompt_type="header",
                    source_pdf_path=file_path
                )
                gpt4o_calls += 1
                
                if header_text:
                    thread_logger.debug(f"      Successfully extracted header: {header_text[:100]}...")
                    
                    # Update metadata using the header text
                    thread_logger.debug(f"      Updating metadata using extracted header")
                    updated_metadata = metadata_extractor.extract_metadata_from_pdf(file_path, header_text)
                    
                    # Only update missing fields
                    if metadata['city'] == 'Unknown' and updated_metadata['city'] != 'Unknown':
                        metadata['city'] = updated_metadata['city']
                        thread_logger.debug(f"      Updated city to: {metadata['city']}")
                    
                    if metadata['market'] == 'Unknown' and updated_metadata['market'] != 'Unknown':
                        metadata['market'] = updated_metadata['market']
                        thread_logger.debug(f"      Updated market to: {metadata['market']}")
                    
                    if metadata['date'] == datetime.now().strftime('%Y-%m-%d') and updated_metadata['date'] != metadata['date']:
                        metadata['date'] = updated_metadata['date']
                        thread_logger.debug(f"      Updated date to: {metadata['date']}")
            
            # Process the CSV texts
            thread_logger.debug(f"      Processing {len(csv_texts)} CSV texts from GPT-4.1 nano")
            result = gpt_csv_parser.process_gpt4o_results(file_path, csv_texts, metadata)
            thread_logger.debug(f"      CSV processing complete, extracted {len(result) if result is not None else 0} rows")
            
            thread_logger.debug(f"<---- Completed GPT-4o vision flow for: {filename}")
            return result, gpt4o_calls
            
        except Exception as e:
            thread_logger.error(f"Error processing PDF with GPT-4.1 nano: {str(e)}")
            thread_logger.info(f"Error details: {traceback.format_exc()}")
            thread_logger.info(f"<---- Failed GPT-4o vision flow for: {filename}")
            return pd.DataFrame(), gpt4o_calls  # Return empty DataFrame with the number of calls made
    
    def handle_output_csv(self, output_df, output_csv, pdf_id, extraction_method, processing_results):
        """
        Handle validation and writing of the output CSV using the validity checker.
        
        Args:
            output_df (DataFrame): DataFrame to validate and write
            output_csv (str): Path to output CSV file
            pdf_id (str): PDF ID from registry
            extraction_method (str): Method used for extraction
            processing_results (dict): Processing statistics
            
        Returns:
            tuple: (bool, dict) - (validation success, updated processing results)
        """
        # Use the validity checker to validate and register the CSV
        validation_success, updated_results = self.validity_checker.register_individual_pdf_csv(
            registry=self.registry,
            pdf_id=pdf_id,
            output_df=output_df,
            output_csv=output_csv,
            processing_results=processing_results,
            extraction_method=extraction_method,
            registry_lock=registry_lock  # Pass the registry_lock from the module level
        )
        
        return validation_success, updated_results
    
    def _process_pdf_worker(self, pdf_id, file_path, output_dir, force_reprocess, result_queue):
        """
        Worker function for processing a single PDF in a thread pool.
        
        Args:
            pdf_id (str): PDF ID from registry
            file_path (str): Path to PDF file
            output_dir (str): Output directory for results
            force_reprocess (bool): Whether to force reprocessing
            result_queue (Queue): Queue to collect results
            
        Returns:
            None: Results are placed in the queue
        """
        try:
            # Create thread-specific logger
            thread_name = threading.current_thread().name
            thread_logger = logging.getLogger(f"pdf_processor.{thread_name}")
            thread_logger.setLevel(self.logger.level)
            
            # Create thread-specific timing stats dict
            thread_timing_stats = {
                'pdf_text_extraction': [],
                'pdf_to_images': [],
                'image_encoding': [],
                'gpt4_query': [],
                'data_processing': []
            }
            
            # Statistics dictionary
            processing_results = {
                'unique_products': 0,
                'unique_major_categories': 0,
                'unique_subcategories': 0,
                'rows_processed': 0,
                'error_occurred': False,
                'error_warnings': "",
                'processing_duration_seconds': 0.0,
                'extraction_method': "",
                'processed_csv_path': "",
                'unique_cities_markets': 0,
                'pipeline_run_id': PIPELINE_RUN_ID or ""
            }
            
            start_time = datetime.now()
            thread_logger.debug(f"Starting processing {os.path.basename(file_path)} at: {start_time}")
            
            try:
                # Try text extraction first
                thread_logger.debug(f"Attempting text extraction for {os.path.basename(file_path)}")
                
                text = self.text_extractor.extract_text_with_pdfplumber(file_path)
                text_is_valid = self.text_extractor.assess_text_quality(text)
                
                if text_is_valid:
                    # Process with text extraction
                    output_df = self.process_pdf_with_text_extraction(file_path, text, thread_logger, thread_timing_stats)
                    extraction_method = "text_extraction"
                    
                    # If text extraction failed, try GPT vision
                    if output_df is None or output_df.empty:
                        thread_logger.debug(f"Text extraction didn't produce results, falling back to GPT vision")
                        output_df, gpt4o_calls = self.process_pdf_with_gpt4o(file_path, thread_logger, thread_timing_stats)
                        extraction_method = "gpt_vision"
                        
                        # Update global stats safely
                        with global_stats_lock:
                            self.stats['gpt4o_calls'] += gpt4o_calls
                            self.stats['files_using_gpt_extraction'] += 1
                else:
                    thread_logger.debug(f"Text extraction not valid, using GPT vision")
                    # Use GPT vision
                    output_df, gpt4o_calls = self.process_pdf_with_gpt4o(file_path, thread_logger, thread_timing_stats)
                    extraction_method = "gpt_vision"
                    
                    # Update global stats safely
                    with global_stats_lock:
                        self.stats['gpt4o_calls'] += gpt4o_calls
                        self.stats['files_using_gpt_extraction'] += 1
                
                # Check if we have results with valid product data
                if output_df is not None and not output_df.empty:
                    # Check for zero unique products
                    unique_products = len(output_df['product'].unique()) if 'product' in output_df.columns else 0
                    if unique_products == 0:
                        raise EmptyProductDataError("DataFrame contains zero unique products")
                        
                    # Create output path
                    base_filename = os.path.splitext(os.path.basename(file_path))[0]
                    output_csv = os.path.join(output_dir, f"{base_filename}.csv")
                    
                    # Create output directory if it doesn't exist (with lock)
                    with output_dir_lock:
                        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
                    
                    # Use handle_output_csv instead of validate_output_csv
                    validation_success, updated_results = self.handle_output_csv(
                        output_df=output_df,
                        output_csv=output_csv,
                        pdf_id=pdf_id,
                        extraction_method=extraction_method,
                        processing_results=processing_results
                    )
                    
                    # Update processing_results with the updated values
                    processing_results = updated_results
                    
                    # Update global stats safely
                    with global_stats_lock:
                        self.stats['processed_files'] += 1
                        self.stats['total_rows_processed'] += processing_results['rows_processed']
                    
                    thread_logger.info(
                        f"Successfully processed {os.path.basename(file_path)}: "
                        f"{processing_results['rows_processed']} rows, "
                        f"{processing_results['unique_products']} products"
                    )
                else:
                    # Raise exception for empty DataFrame
                    raise EmptyProductDataError("No data extracted from PDF, DataFrame is empty")
                    
            except Exception as e:
                processing_results['error_occurred'] = True
                processing_results['error_warnings'] = str(e)
                
                thread_logger.error(f"Error processing PDF file {os.path.basename(file_path)}: {str(e)}")
                thread_logger.debug(f"Error details: {traceback.format_exc()}")
                
                # Update global stats safely
                with global_stats_lock:
                    self.stats['failed_files'] += 1
            
            # Calculate processing time
            processing_results['processing_duration_seconds'] = (datetime.now() - start_time).total_seconds()
            
            # Place results in queue
            result_queue.put({
                'success': True,
                'pdf_id': pdf_id,
                'file_path': file_path,
                'results': processing_results,
                'status': 'processed' if not processing_results['error_occurred'] else 'error'
            })
            
        except Exception as e:
            # Handle errors and put in queue
            self.logger.error(f"Error in worker processing {os.path.basename(file_path)}: {str(e)}")
            self.logger.debug(f"Error details: {traceback.format_exc()}")
            
            # Put error in queue
            result_queue.put({
                'success': False,
                'pdf_id': pdf_id,
                'file_path': file_path,
                'error': str(e),
                'status': 'error'
            })

    
    def process_pdf_files(self, output_dir=None, start_date=None, end_date=None, max_workers=None, force_reprocess=False):
        """
        Process all PDF files in the registry.
        
        Args:
            output_dir (str): Directory to save processed data
            start_date (datetime): Start date for filtering files
            end_date (datetime): End date for filtering files
            max_workers (int): Maximum number of worker threads
            force_reprocess (bool): If True, reprocess files even if already processed
            
        Returns:
            dict: Processing statistics
        """
        # Set default values
        output_dir = output_dir or PROCESSED_DIR
        
        # Ensure output directory exists (with lock)
        with output_dir_lock:
            os.makedirs(output_dir, exist_ok=True)
        
        # Reset statistics
        self.stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'total_rows_processed': 0,
            'files_using_gpt_extraction': 0,
            'gpt4o_calls': 0,
            'validation_passed': 0,
            'validation_failed': 0,
            'start_time': datetime.now(),
            'end_time': None,
            'total_duration_seconds': 0
        }
        
        # Get PDF files from registry that need to be processed - with registry lock
        with registry_lock:
            if force_reprocess:
                # Get all PDFs
                pdf_df = self.registry.read()
            else:
                # Get only unprocessed PDFs or PDFs with errors
                pdf_df = self.registry.read()
                pdf_df = pdf_df[(pdf_df['processing_status'] == 'not_processed') | 
                                (pdf_df['processing_status'] == 'error')]
            
            # Filter by date if needed
            if start_date or end_date:
                if start_date:
                    pdf_df = pdf_df[pdf_df['file_date'] >= pd.Timestamp(start_date)]
                if end_date:
                    pdf_df = pdf_df[pdf_df['file_date'] <= pd.Timestamp(end_date)]
            
            # Make a local copy to avoid registry locking during processing
            pdf_df = pdf_df.copy()
        
        total_pdfs = len(pdf_df)
        self.logger.debug(f"Found {total_pdfs} PDF files to process")
        self.stats['total_files'] = total_pdfs
        
        # No files to process
        if total_pdfs == 0:
            self.logger.debug("No PDF files to process")
            
            # Update final statistics
            self.stats['end_time'] = datetime.now()
            self.stats['total_duration_seconds'] = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
            
            # Export registry to CSV
            with registry_lock:
                self.registry.export_to_csv()
            
            return self.stats
        
        # Determine number of workers
        if max_workers is None or max_workers <= 0:
            # Default to number of CPUs if not specified
            max_workers = os.cpu_count()
        
        # Cap max_workers to the number of PDFs
        max_workers = min(max_workers, total_pdfs)
        self.logger.info(f"Using {max_workers} workers for parallel processing")
        
        # Create all required directories upfront to avoid race conditions
        self._ensure_all_directories()
        
        # Create a queue for collecting results from workers
        result_queue = queue.Queue()
        
        # Create thread pool and submit tasks
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks to the pool
            futures = []
            for row in pdf_df.itertuples():
                pdf_id = row.pdf_id
                file_path = row.file_path
                
                # Update registry status to 'processing' before submitting
                with registry_lock:
                    self.registry.update_processing_status(pdf_id, 'processing')
                
                future = executor.submit(
                    self._process_pdf_worker,
                    pdf_id,
                    file_path,
                    output_dir,
                    force_reprocess,
                    result_queue
                )
                futures.append(future)
            
            with tqdm(total=total_pdfs, desc="Processing PDFs", unit="file") as pbar:
                # Monitor the queue and update progress
                completed = 0
                while completed < total_pdfs:
                    try:
                        # Check if any result is available with a small timeout
                        result = result_queue.get(timeout=0.1)
                        
                        # Process the result
                        if result['success']:
                            pdf_id = result['pdf_id']
                            file_path = result['file_path']
                            filename = os.path.basename(file_path)
                            results = result['results']
                            
                            if result['status'] == 'skipped':
                                pbar.set_postfix(status="Skipped")
                            elif result['status'] == 'error':
                                pbar.set_postfix(status="Failed")
                                # Update validation statistics
                                with global_stats_lock:
                                    self.stats['validation_failed'] += 1
                            else:
                                if results:
                                    # Successfully processed with validation
                                    if not results.get('error_occurred', False):
                                        with global_stats_lock:
                                            self.stats['validation_passed'] += 1
                                            
                                        # Add debug level logs for the saved DataFrame
                                        output_csv = results.get('processed_csv_path', '')
                                        if output_csv and os.path.exists(output_csv):
                                            try:
                                                # Read the saved DataFrame
                                                df = pd.read_csv(output_csv)
                                                self.logger.debug(f"DEBUG: Saved DataFrame for PDF {filename} from {file_path}:")
                                                self.logger.debug(f"DEBUG: CSV path: {output_csv}")
                                                self.logger.debug(f"DEBUG: DataFrame shape: {df.shape}")
                                                self.logger.debug(f"DEBUG: DataFrame columns: {df.columns.tolist()}")
                                                self.logger.debug(f"DEBUG: DataFrame head (5 rows):\n{df.head(5)}")
                                                self.logger.debug(f"DEBUG: Unique products: {df['product'].nunique() if 'product' in df.columns else 0}")
                                                self.logger.debug(f"DEBUG: Unique categories: {df['major_category'].nunique() if 'major_category' in df.columns else 0}")
                                                self.logger.debug(f"DEBUG: Unique subcategories: {df['subcategory'].nunique() if 'subcategory' in df.columns else 0}")
                                                self.logger.debug(f"DEBUG: Source PDF path: {file_path}")
                                            except Exception as e:
                                                self.logger.debug(f"DEBUG: Error reading saved DataFrame for {filename}: {str(e)}")
                                    else:
                                        with global_stats_lock:
                                            self.stats['validation_failed'] += 1
                                    
                                    pbar.set_postfix(
                                        rows=results.get('rows_processed', 0),
                                        method=results.get('extraction_method', 'unknown'),
                                        valid="✓" if not results.get('error_occurred', False) else "✗"
                                    )
                        else:
                            # Handle error case
                            pbar.set_postfix(status="Error")
                            with global_stats_lock:
                                self.stats['validation_failed'] += 1
                                
                            # Log debug info for error case
                            self.logger.debug(f"DEBUG: Processing failed for PDF {os.path.basename(result['file_path'])} from {result['file_path']}")
                            self.logger.debug(f"DEBUG: Error: {result.get('error', 'Unknown error')}")
                        
                        # Update progress
                        completed += 1
                        pbar.update(1)
                        
                        # Update description to show statistics
                        pbar.set_description(
                            f"Processing PDFs [Processed: {self.stats['processed_files']}, "
                            f"Failed: {self.stats['failed_files']}, "
                            f"Validated: {self.stats['validation_passed']}/{self.stats['processed_files']}]"
                        )
                    except queue.Empty:
                        # No results available yet, check if any futures are done
                        done_futures = [f for f in futures if f.done()]
                        if len(done_futures) > completed:
                            # Some futures are done but didn't put result in queue
                            # This could happen if there was an exception in _process_pdf_worker
                            missing = len(done_futures) - completed
                            self.logger.warning(f"{missing} tasks completed but didn't report results")
                            
                            # Update progress for these tasks
                            completed += missing
                            pbar.update(missing)
                            
                            # Update description
                            with global_stats_lock:
                                self.stats['failed_files'] += missing
                                self.stats['validation_failed'] += missing
                            
                            pbar.set_description(
                                f"Processing PDFs [Processed: {self.stats['processed_files']}, "
                                f"Failed: {self.stats['failed_files']}, "
                                f"Validated: {self.stats['validation_passed']}/{self.stats['processed_files']}]"
                            )
                        
                        # Check if all futures are done
                        if all(f.done() for f in futures):
                            # Ensure we've completed all tasks
                            remaining = total_pdfs - completed
                            if remaining > 0:
                                self.logger.warning(f"{remaining} tasks may have failed silently")
                                pbar.update(remaining)
                                with global_stats_lock:
                                    self.stats['failed_files'] += remaining
                                    self.stats['validation_failed'] += remaining
                            
                            completed = total_pdfs  # Force exit from loop
                            
                        # Small sleep to avoid busy waiting
                        time.sleep(0.01)
        
        # Update final statistics
        self.stats['end_time'] = datetime.now()
        self.stats['total_duration_seconds'] = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        # Force a final read and write to ensure all records are properly saved
        self.logger.info("Performing final registry synchronization...")
        with registry_lock:
            df = self.registry.read()
            self.registry.write(df)
            
            # Export registry to CSV
            self.logger.info("Exporting registry to CSV...")
            self.registry.export_to_csv()
        
        # Log summary
        self.logger.info(f"PDF processing summary: {self.stats['processed_files']} processed, "
                    f"{self.stats['failed_files']} failed, {self.stats['skipped_files']} skipped "
                    f"out of {self.stats['total_files']} total files")
        self.logger.info(f"Validation results: {self.stats['validation_passed']} passed, {self.stats['validation_failed']} failed")
        self.logger.info(f"Processed {self.stats['total_rows_processed']} total rows")
        self.logger.info(f"Files using GPT extraction: {self.stats['files_using_gpt_extraction']}")
        self.logger.info(f"Total GPT calls: {self.stats['gpt4o_calls']}")
        self.logger.info(f"Total processing time: {self.stats['total_duration_seconds']:.2f} seconds")
        
        if self.stats['total_files'] > 0:
            avg_time_per_file = self.stats['total_duration_seconds'] / self.stats['total_files']
            self.logger.info(f"Average time per file: {avg_time_per_file:.2f} seconds")
            
            if self.stats['processed_files'] > 0:
                success_rate = (self.stats['validation_passed'] / self.stats['processed_files']) * 100
                self.logger.info(f"Validation success rate: {success_rate:.2f}%")
        
        return self.stats