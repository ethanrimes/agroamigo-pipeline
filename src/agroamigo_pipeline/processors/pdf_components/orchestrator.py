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

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

from agroamigo_pipeline.config.settings import (
    PDF_DIR, PROCESSED_DIR, MIN_UNIQUE_PRODUCTS, MAX_UNIQUE_PRODUCTS, PIPELINE_RUN_ID
)
from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.utils.path_manager import ensure_directory, get_files_by_extension
from agroamigo_pipeline.registry.pdf_registry import PDFProcessorRegistry
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

# Global stats dictionary for thread-safe updates
global_stats_lock = threading.Lock()

# Global registry lock for thread-safe registry operations
registry_lock = threading.Lock()

# Global output directory lock for thread-safe output directory creation
output_dir_lock = threading.Lock()

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
        
        # Process text directly if table extraction failed
        thread_logger.info(f"      Processing extracted text directly")
        result = product_parser.process_extracted_text(file_path, text, metadata)
        
        if result is not None and not result.empty:
            thread_logger.info(f"      Text processing successful, extracted {len(result)} rows")
            thread_logger.info(f"<---- Completed text extraction flow for: {filename}")
            return result
        else:
            thread_logger.info(f"      Text processing failed to produce results")
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
                csv_text = gpt_client.query_gpt4o_with_image(image_path, base64_image, source_pdf_path=file_path)
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
                    prompt_type="header",
                    source_pdf_path=file_path
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
            thread_logger = logging.getLogger(f"{self.logger.name}.{thread_name}")
            thread_logger.setLevel(self.logger.level)
            
            # Create thread-specific timing stats dict
            thread_timing_stats = {
                'pdf_text_extraction': [],
                'pdf_to_images': [],
                'image_encoding': [],
                'gpt4_query': [],
                'data_processing': []
            }
            
            # Check if file has already been processed - using registry lock
            with registry_lock:
                pdf_info = self.registry.get_pdf_by_id(pdf_id)
                if pdf_info and pdf_info['processing_status'] == 'processed' and not force_reprocess:
                    thread_logger.info(f"File already processed: {os.path.basename(file_path)}")
                    with global_stats_lock:
                        self.stats['skipped_files'] += 1
                    
                    # Put results in queue
                    result_queue.put({
                        'success': True,
                        'pdf_id': pdf_id,
                        'file_path': file_path,
                        'results': None,
                        'status': 'skipped'
                    })
                    return
                
                # Update registry status to 'processing'
                self.registry.update_processing_status(pdf_id, 'processing')
            
            # Ensure output directory exists with lock
            with output_dir_lock:
                os.makedirs(output_dir, exist_ok=True)
            
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
            thread_logger.info(f"Starting processing {os.path.basename(file_path)} at: {start_time}")
            
            try:
                # Try text extraction first
                thread_logger.info(f"Attempting text extraction for {os.path.basename(file_path)}")
                
                text = self.text_extractor.extract_text_with_pdfplumber(file_path)
                text_is_valid = self.text_extractor.assess_text_quality(text)
                
                if text_is_valid:
                    # Process with text extraction
                    output_df = self.process_pdf_with_text_extraction(file_path, text, thread_logger, thread_timing_stats)
                    extraction_method = "text_extraction"
                    
                    # If text extraction failed, try GPT vision
                    if output_df is None or output_df.empty:
                        thread_logger.info(f"Text extraction didn't produce results, falling back to GPT vision")
                        output_df, gpt4o_calls = self.process_pdf_with_gpt4o(file_path, thread_logger, thread_timing_stats)
                        extraction_method = "gpt_vision"
                        
                        # Update global stats safely
                        with global_stats_lock:
                            self.stats['gpt4o_calls'] += gpt4o_calls
                            self.stats['files_using_gpt_extraction'] += 1
                else:
                    thread_logger.info(f"Text extraction not valid, using GPT vision")
                    # Use GPT vision
                    output_df, gpt4o_calls = self.process_pdf_with_gpt4o(file_path, thread_logger, thread_timing_stats)
                    extraction_method = "gpt_vision"
                    
                    # Update global stats safely
                    with global_stats_lock:
                        self.stats['gpt4o_calls'] += gpt4o_calls
                        self.stats['files_using_gpt_extraction'] += 1
                
                # Check if we have results
                if output_df is not None and not output_df.empty:
                    # Create output path
                    base_filename = os.path.splitext(os.path.basename(file_path))[0]
                    output_csv = os.path.join(output_dir, f"{base_filename}.csv")
                    
                    # Create output directory if it doesn't exist (with lock)
                    with output_dir_lock:
                        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
                    
                    # Save to CSV
                    output_df.to_csv(output_csv, index=False)
                    thread_logger.info(f"Saved output to: {output_csv}")
                    
                    # Update processing results
                    processing_results['unique_products'] = len(output_df['product'].unique()) if 'product' in output_df.columns else 0
                    processing_results['unique_major_categories'] = len(output_df['major_category'].unique()) if 'major_category' in output_df.columns else 0
                    processing_results['unique_subcategories'] = len(output_df['sub_category'].unique()) if 'sub_category' in output_df.columns else 0
                    processing_results['rows_processed'] = len(output_df)
                    processing_results['extraction_method'] = extraction_method
                    processing_results['processed_csv_path'] = output_csv
                    
                    # Count unique cities and markets
                    if 'city' in output_df.columns and 'market' in output_df.columns:
                        unique_city_markets = output_df[['city', 'market']].drop_duplicates()
                        processing_results['unique_cities_markets'] = len(unique_city_markets)
                    
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
                    processing_results['error_occurred'] = True
                    processing_results['error_warnings'] = "No data extracted from PDF"
                    thread_logger.warning(f"No data extracted from {os.path.basename(file_path)}")
                    
                    # Update global stats safely
                    with global_stats_lock:
                        self.stats['failed_files'] += 1
                
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
            
            # Update registry with processing results - using registry lock
            with registry_lock:
                if processing_results['error_occurred']:
                    self.registry.update_processing_status(pdf_id, 'error', error=processing_results['error_warnings'])
                else:
                    self.registry.update_processing_results(pdf_id, processing_results)
            
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
            
            # Update registry with error - using registry lock
            with registry_lock:
                try:
                    self.registry.update_processing_status(pdf_id, 'error', error=str(e))
                except Exception as reg_err:
                    self.logger.error(f"Failed to update registry for {pdf_id}: {str(reg_err)}")
            
            # Put error in queue
            result_queue.put({
                'success': False,
                'pdf_id': pdf_id,
                'file_path': file_path,
                'error': str(e),
                'status': 'error'
            })
    
    def process_single_pdf_file(self, pdf_id, file_path, output_dir=None, force_reprocess=False):
        """
        Process a single PDF file and save results to CSV.
        
        Args:
            pdf_id (str): PDF ID from the registry
            file_path (str): Path to the PDF file
            output_dir (str): Directory to save processed data
            force_reprocess (bool): If True, reprocess file even if already processed
            
        Returns:
            dict: Processing results
        """
        filename = os.path.basename(file_path)
        self.logger.info(f"Processing PDF file: {filename}")
        
        # Check if file has already been processed
        pdf_info = self.registry.get_pdf_by_id(pdf_id)
        
        if pdf_info and pdf_info['processing_status'] == 'processed' and not force_reprocess:
            self.logger.info(f"File already processed: {filename}")
            
            # Update global stats safely
            with global_stats_lock:
                self.stats['skipped_files'] += 1
                
            return None
        
        # Set default output directory
        output_dir = output_dir or PROCESSED_DIR
        os.makedirs(output_dir, exist_ok=True)
        
        # Update registry status to 'processing'
        with registry_lock:
            self.registry.update_processing_status(pdf_id, 'processing')
        
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
        self.logger.info(f"Starting processing at: {start_time}")
        
        try:
            # Try text extraction first
            self.logger.info(f"Attempting text extraction for {filename}")
            
            text = self.text_extractor.extract_text_with_pdfplumber(file_path)
            text_is_valid = self.text_extractor.assess_text_quality(text)
            
            if text_is_valid:
                # Extract metadata from text
                self.logger.info(f"Extracting metadata from text")
                metadata = self.metadata_extractor.extract_metadata_from_pdf(file_path, text)
                
                # Try table extraction first
                self.logger.info(f"Attempting to extract tables with pdfplumber")
                tables = self.text_extractor.extract_table_with_pdfplumber(file_path)
                
                if tables and len(tables) > 0:
                    self.logger.info(f"Processing {len(tables)} extracted tables")
                    output_df = self.product_parser.process_table_data(file_path, tables, metadata)
                    extraction_method = "text_extraction"
                else:
                    self.logger.info(f"No tables found, falling back to GPT extraction")
                    # Use GPT vision
                    output_df, gpt4o_calls = self.process_pdf_with_gpt4o(file_path, self.logger, self.timing_stats)
                    extraction_method = "gpt_vision"
                    
                    # Update global stats safely
                    with global_stats_lock:
                        self.stats['gpt4o_calls'] += gpt4o_calls
                        self.stats['files_using_gpt_extraction'] += 1
            else:
                self.logger.info(f"Text extraction not valid, using GPT vision")
                # Use GPT vision
                output_df, gpt4o_calls = self.process_pdf_with_gpt4o(file_path, self.logger, self.timing_stats)
                extraction_method = "gpt_vision"
                
                # Update global stats safely
                with global_stats_lock:
                    self.stats['gpt4o_calls'] += gpt4o_calls
                    self.stats['files_using_gpt_extraction'] += 1
            
            # Check if we have results
            if output_df is not None and not output_df.empty:
                # Create output path
                base_filename = os.path.splitext(filename)[0]
                output_csv = os.path.join(output_dir, f"{base_filename}.csv")
                
                # Ensure output directory exists
                with output_dir_lock:
                    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
                
                # Save to CSV
                output_df.to_csv(output_csv, index=False)
                self.logger.info(f"Saved output to: {output_csv}")
                
                # Update processing results
                processing_results['unique_products'] = len(output_df['product'].unique()) if 'product' in output_df.columns else 0
                processing_results['unique_major_categories'] = len(output_df['major_category'].unique()) if 'major_category' in output_df.columns else 0
                processing_results['unique_subcategories'] = len(output_df['sub_category'].unique()) if 'sub_category' in output_df.columns else 0
                processing_results['rows_processed'] = len(output_df)
                processing_results['extraction_method'] = extraction_method
                processing_results['processed_csv_path'] = output_csv
                
                # Count unique cities and markets
                if 'city' in output_df.columns and 'market' in output_df.columns:
                    unique_city_markets = output_df[['city', 'market']].drop_duplicates()
                    processing_results['unique_cities_markets'] = len(unique_city_markets)
                
                # Update global stats safely
                with global_stats_lock:
                    self.stats['processed_files'] += 1
                    self.stats['total_rows_processed'] += processing_results['rows_processed']
                
                self.logger.info(
                    f"Successfully processed {filename}: "
                    f"{processing_results['rows_processed']} rows, "
                    f"{processing_results['unique_products']} products"
                )
            else:
                processing_results['error_occurred'] = True
                processing_results['error_warnings'] = "No data extracted from PDF"
                self.logger.warning(f"No data extracted from {filename}")
                
                # Update global stats safely
                with global_stats_lock:
                    self.stats['failed_files'] += 1
            
        except Exception as e:
            processing_results['error_occurred'] = True
            processing_results['error_warnings'] = str(e)
            
            self.logger.error(f"Error processing PDF file {filename}: {str(e)}")
            self.logger.debug(f"Error details: {traceback.format_exc()}")
            
            # Update global stats safely
            with global_stats_lock:
                self.stats['failed_files'] += 1
        
        # Calculate processing time
        processing_results['processing_duration_seconds'] = (datetime.now() - start_time).total_seconds()
        
        # Update registry with processing results
        with registry_lock:
            if processing_results['error_occurred']:
                self.registry.update_processing_status(pdf_id, 'error', error=processing_results['error_warnings'])
            else:
                self.registry.update_processing_results(pdf_id, processing_results)
        
        return processing_results
    
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
        self.logger.info(f"Found {total_pdfs} PDF files to process")
        self.stats['total_files'] = total_pdfs
        
        # No files to process
        if total_pdfs == 0:
            self.logger.info("No PDF files to process")
            
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
            max_workers = min(os.cpu_count() or 4, 8)  # Cap at 8 to avoid too many threads
        
        # Cap max_workers to the number of PDFs
        max_workers = min(max_workers, total_pdfs)
        
        # Create all required directories upfront to avoid race conditions
        self._ensure_all_directories()
        
        # Sequential processing if only 1 worker or 1 file
        if max_workers == 1 or total_pdfs == 1:
            self.logger.info("Using sequential processing")
            
            # Process files sequentially with progress bar
            if TQDM_AVAILABLE:
                # Use tqdm for progress tracking
                pbar = tqdm(total=total_pdfs, desc="Processing PDFs", unit="file")
                
                for i, row in enumerate(pdf_df.itertuples()):
                    pdf_id = row.pdf_id
                    file_path = row.file_path
                    filename = os.path.basename(file_path)
                    
                    self.logger.info(f"Processing {i+1}/{total_pdfs}: {filename}")
                    
                    try:
                        results = self.process_single_pdf_file(pdf_id, file_path, output_dir, force_reprocess)
                        if results is None:
                            pbar.set_postfix(status="Skipped")
                        elif results['error_occurred']:
                            pbar.set_postfix(status="Failed")
                        else:
                            pbar.set_postfix(
                                rows=results['rows_processed'],
                                method=results['extraction_method']
                            )
                    except Exception as e:
                        self.logger.error(f"Error processing {filename}: {str(e)}")
                        self.logger.debug(f"Error details: {traceback.format_exc()}")
                        pbar.set_postfix(status="Error")
                    
                    # Update progress bar
                    pbar.update(1)
                
                pbar.close()
            else:
                # Without tqdm, use simple progress reporting
                for i, row in enumerate(pdf_df.itertuples()):
                    pdf_id = row.pdf_id
                    file_path = row.file_path
                    filename = os.path.basename(file_path)
                    
                    self.logger.info(f"Processing {i+1}/{total_pdfs}: {filename}")
                    
                    try:
                        results = self.process_single_pdf_file(pdf_id, file_path, output_dir, force_reprocess)
                        
                        # Log progress
                        progress_msg = f"Progress: {i+1}/{total_pdfs} ({((i+1)/total_pdfs)*100:.1f}%)"
                        if results is None:
                            self.logger.info(f"{progress_msg} - Skipped: {filename}")
                        elif results['error_occurred']:
                            self.logger.info(f"{progress_msg} - Failed: {filename}")
                        else:
                            self.logger.info(
                                f"{progress_msg} - Processed: {filename} "
                                f"({results['rows_processed']} rows via {results['extraction_method']})"
                            )
                    except Exception as e:
                        self.logger.error(f"Error processing {filename}: {str(e)}")
                        self.logger.debug(f"Error details: {traceback.format_exc()}")
                        self.logger.info(f"{progress_msg} - Error: {filename}")
        else:
            # Parallel processing with thread pool
            self.logger.info(f"Using {max_workers} workers for parallel processing")
            
            # Create a queue for collecting results from workers
            result_queue = queue.Queue()
            
            # Create thread pool and submit tasks
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks to the pool
                futures = []
                for row in pdf_df.itertuples():
                    pdf_id = row.pdf_id
                    file_path = row.file_path
                    
                    future = executor.submit(
                        self._process_pdf_worker,
                        pdf_id,
                        file_path,
                        output_dir,
                        force_reprocess,
                        result_queue
                    )
                    futures.append(future)
                
                # Track progress with tqdm if available
                if TQDM_AVAILABLE:
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
                                    else:
                                        if results:
                                            pbar.set_postfix(
                                                rows=results.get('rows_processed', 0),
                                                method=results.get('extraction_method', 'unknown')
                                            )
                                else:
                                    # Handle error case
                                    pbar.set_postfix(status="Error")
                                
                                # Update progress
                                completed += 1
                                pbar.update(1)
                                
                                # Update description to show statistics
                                pbar.set_description(
                                    f"Processing PDFs [Processed: {self.stats['processed_files']}, "
                                    f"Failed: {self.stats['failed_files']}, "
                                    f"Skipped: {self.stats['skipped_files']}]"
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
                                    
                                    pbar.set_description(
                                        f"Processing PDFs [Processed: {self.stats['processed_files']}, "
                                        f"Failed: {self.stats['failed_files']}, "
                                        f"Skipped: {self.stats['skipped_files']}]"
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
                                    
                                    completed = total_pdfs  # Force exit from loop
                                    
                                # Small sleep to avoid busy waiting
                                time.sleep(0.01)
                else:
                    # Without tqdm, use manual progress tracking
                    completed = 0
                    start_time = time.time()
                    
                    # Wait for all futures to complete
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
                                    status = "Skipped"
                                elif result['status'] == 'error':
                                    status = "Failed"
                                else:
                                    if results:
                                        rows = results.get('rows_processed', 0)
                                        method = results.get('extraction_method', 'unknown')
                                        status = f"Processed ({rows} rows via {method})"
                                    else:
                                        status = "Processed (0 rows)"
                            else:
                                # Handle error case
                                status = "Error"
                            
                            # Update progress
                            completed += 1
                            
                            # Log progress every 5% or for each file if total is small
                            log_interval = max(1, int(total_pdfs / 20))
                            if completed % log_interval == 0 or completed == total_pdfs:
                                elapsed = time.time() - start_time
                                eta = (elapsed / completed) * (total_pdfs - completed) if completed > 0 else 0
                                
                                self.logger.info(
                                    f"Progress: {completed}/{total_pdfs} ({(completed/total_pdfs)*100:.1f}%) "
                                    f"[P:{self.stats['processed_files']} F:{self.stats['failed_files']} "
                                    f"S:{self.stats['skipped_files']}] ETA: {eta:.1f}s"
                                )
                                
                        except queue.Empty:
                            # No results available yet, check if any futures are done
                            done_futures = [f for f in futures if f.done()]
                            if len(done_futures) > completed:
                                # Some futures are done but didn't put result in queue
                                missing = len(done_futures) - completed
                                self.logger.warning(f"{missing} tasks completed but didn't report results")
                                
                                # Update progress for these tasks
                                completed += missing
                                
                                # Count as failed
                                with global_stats_lock:
                                    self.stats['failed_files'] += missing
                            
                            # Check if all futures are done
                            if all(f.done() for f in futures):
                                # Ensure we've completed all tasks
                                remaining = total_pdfs - completed
                                if remaining > 0:
                                    self.logger.warning(f"{remaining} tasks may have failed silently")
                                    with global_stats_lock:
                                        self.stats['failed_files'] += remaining
                                
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
        self.logger.info(f"Processed {self.stats['total_rows_processed']} total rows")
        self.logger.info(f"Files using GPT extraction: {self.stats['files_using_gpt_extraction']}")
        self.logger.info(f"Total GPT calls: {self.stats['gpt4o_calls']}")
        self.logger.info(f"Total processing time: {self.stats['total_duration_seconds']:.2f} seconds")
        
        if self.stats['total_files'] > 0:
            avg_time_per_file = self.stats['total_duration_seconds'] / self.stats['total_files']
            self.logger.info(f"Average time per file: {avg_time_per_file:.2f} seconds")
            
            if self.stats['processed_files'] > 0:
                success_rate = (self.stats['processed_files'] / self.stats['total_files']) * 100
                self.logger.info(f"Success rate: {success_rate:.2f}%")
        
        return self.stats