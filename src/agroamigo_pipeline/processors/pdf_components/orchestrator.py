"""
PDF processing orchestration module.

This module coordinates the PDF processing workflow, including text extraction,
metadata extraction, product parsing, and GPT-4o integration when needed.
"""

import os
import logging
import traceback
import concurrent.futures
from datetime import datetime

from agroamigo_pipeline.config.settings import (
    PDF_DIR, PROCESSED_DIR, MIN_UNIQUE_PRODUCTS, MAX_UNIQUE_PRODUCTS
)
from agroamigo_pipeline.utils.logger import setup_logging, log_function_call, log_dataframe_info
from agroamigo_pipeline.utils.path_manager import ensure_directory, get_files_by_extension, generate_output_path
from agroamigo_pipeline.utils.file_registry import FileRegistry
from agroamigo_pipeline.utils.exceptions import PDFProcessingError

from agroamigo_pipeline.processors.pdf_components.text_extraction import TextExtractor
from agroamigo_pipeline.processors.pdf_components.metadata_extraction import MetadataExtractor
from agroamigo_pipeline.processors.pdf_components.product_parser import ProductParser
from agroamigo_pipeline.processors.pdf_components.image_handler import ImageHandler
from agroamigo_pipeline.processors.pdf_components.gpt_client import GPTClient
from agroamigo_pipeline.processors.pdf_components.gpt_csv_parser import GPTCSVParser

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
            
        # Initialize file registry
        self.registry = FileRegistry()
        
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
        
        # Initialize component modules with debug flag
        self.text_extractor = TextExtractor(logger=self.logger, timing_stats=self.timing_stats)
        self.metadata_extractor = MetadataExtractor(logger=self.logger)
        self.product_parser = ProductParser(logger=self.logger, timing_stats=self.timing_stats)
        self.image_handler = ImageHandler(logger=self.logger, timing_stats=self.timing_stats, save_images=self.save_debug_data)
        self.gpt_client = GPTClient(logger=self.logger, timing_stats=self.timing_stats, save_responses=self.save_debug_data)
        self.gpt_csv_parser = GPTCSVParser(logger=self.logger, timing_stats=self.timing_stats, save_csv_data=self.save_debug_data)
        
        self.logger.info("PDFProcessorOrchestrator initialized")
    
    def process_pdf_with_text_extraction(self, file_path, text):
        """
        Process PDF using direct text extraction methods.
        
        Args:
            file_path (str): Path to the PDF file
            text (str): Extracted text
                
        Returns:
            DataFrame: Processed data in standardized format
        """
        filename = os.path.basename(file_path)
        self.logger.info(f"----> Text extraction flow for: {filename}")
        
        # Extract metadata from text
        self.logger.info(f"      Extracting metadata from text")
        metadata = self.metadata_extractor.extract_metadata_from_pdf(file_path, text)
        self.logger.info(f"      Metadata extracted: date={metadata['date']}, city={metadata['city']}, market={metadata['market']}")
        
        # Try table extraction first
        self.logger.info(f"      Attempting to extract tables with pdfplumber")
        tables = self.text_extractor.extract_table_with_pdfplumber(file_path)
        
        if tables and len(tables) > 0:
            self.logger.info(f"      Successfully extracted {len(tables)} tables, processing table data")
            result = self.product_parser.process_table_data(file_path, tables, metadata)
            if result is not None and not result.empty:
                self.logger.info(f"      Table extraction successful, extracted {len(result)} rows")
                self.logger.info(f"<---- Completed table extraction flow for: {filename}")
                return result
            else:
                self.logger.info(f"      Table extraction failed to produce results, falling back to text processing")
        else:
            self.logger.info(f"      No tables extracted, falling back to text processing")
        
        # Fall back to text processing if table extraction fails
        # self.logger.info(f"      Processing text to extract product data")
        # result = self.product_parser.process_extracted_text(file_path, text, metadata)
        # self.logger.info(f"      Product data extraction complete, extracted {len(result) if result is not None else 0} rows")
        
        self.logger.info(f"<---- Completed text extraction flow for: {filename}")
        return result
    
    async def process_pdf_with_gpt4o_async(self, file_path):
        """
        Process PDF using GPT-4o mini for image-based extraction.
        
        Args:
            file_path (str): Path to the PDF file
            
        Returns:
            DataFrame: Processed data in standardized format
        """
        filename = os.path.basename(file_path)
        self.logger.info(f"----> GPT-4o vision flow for: {filename}")
        
        try:
            # First extract text to get metadata
            self.logger.info(f"      Extracting text for metadata")
            text_to_use = self.text_extractor.extract_text_with_pdfplumber(file_path)
            self.logger.info(f"      Using text from pdfplumber for metadata")
            
            # Extract metadata from text
            self.logger.info(f"      Extracting metadata from text")
            metadata = self.metadata_extractor.extract_metadata_from_pdf(file_path, text_to_use)
            self.logger.info(f"      Metadata extracted: date={metadata['date']}, city={metadata['city']}, market={metadata['market']}")
            
            # Convert PDF to images
            self.logger.info(f"      Converting PDF to images")
            image_paths = self.image_handler.convert_pdf_to_images(file_path, save_copies=True)
            self.logger.info(f"      PDF converted to {len(image_paths)} images")
            
            # Check if we need to extract header metadata
            extract_header = metadata['city'] == 'Unknown' or metadata['market'] == 'Unknown'
            
            # Prepare image encoding and API requests
            self.logger.info(f"      Preparing to process {len(image_paths)} images with GPT-4o mini")
            
            # Create tasks for async processing
            import asyncio
            import concurrent.futures
            
            async def process_all_images():
                # First encode all images to base64
                self.logger.info(f"      Encoding all images to base64")
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    base64_images = list(await asyncio.get_event_loop().run_in_executor(
                        executor, 
                        lambda: [self.image_handler.encode_image(img_path) for img_path in image_paths]
                    ))
                
                self.logger.info(f"      All images encoded to base64")
                
                # Prepare tasks for all pages
                tasks = []
                for i, (image_path, base64_image) in enumerate(zip(image_paths, base64_images)):
                    self.logger.info(f"      Creating GPT-4o task for image {i+1}/{len(image_paths)}")
                    task = self.gpt_client.query_gpt4o_with_image_async(image_path, base64_image)
                    tasks.append(task)
                
                # Add header extraction task if needed
                header_task = None
                if extract_header and len(image_paths) > 0:
                    self.logger.info(f"      Creating additional task to extract header metadata from first page")
                    header_task = self.gpt_client.query_gpt4o_with_image_async(
                        image_paths[0], 
                        base64_images[0], 
                        prompt_type="header"
                    )
                    tasks.append(header_task)
                
                # Execute all tasks simultaneously
                self.logger.info(f"      Executing {len(tasks)} GPT-4o requests asynchronously")
                responses = await asyncio.gather(*tasks)
                
                # Separate the header response if it exists
                if header_task:
                    header_text = responses[-1]
                    csv_texts = responses[:-1]
                    self.logger.info(f"      Received header text: {header_text[:100]}...")
                    self.logger.info(f"      Received {len(csv_texts)} CSV responses")
                    
                    # Update metadata using the header text if needed
                    if metadata['city'] == 'Unknown' or metadata['market'] == 'Unknown':
                        self.logger.info(f"      Updating metadata using extracted header")
                        updated_metadata = self.metadata_extractor.extract_metadata_from_pdf(file_path, header_text)
                        
                        # Only update missing fields
                        if metadata['city'] == 'Unknown' and updated_metadata['city'] != 'Unknown':
                            metadata['city'] = updated_metadata['city']
                            self.logger.info(f"      Updated city to: {metadata['city']}")
                        
                        if metadata['market'] == 'Unknown' and updated_metadata['market'] != 'Unknown':
                            metadata['market'] = updated_metadata['market']
                            self.logger.info(f"      Updated market to: {metadata['market']}")
                        
                        if metadata['date'] == datetime.now().strftime('%Y-%m-%d') and updated_metadata['date'] != metadata['date']:
                            metadata['date'] = updated_metadata['date']
                            self.logger.info(f"      Updated date to: {metadata['date']}")
                else:
                    csv_texts = responses
                    self.logger.info(f"      Received {len(csv_texts)} CSV responses")
                
                return csv_texts, metadata
            
            # Run async processing
            loop = asyncio.get_event_loop()
            csv_texts, updated_metadata = loop.run_until_complete(process_all_images())
            
            self.stats['gpt4o_calls'] += len(csv_texts) + (1 if extract_header else 0)
            
            # Process the CSV texts
            self.logger.info(f"      Processing {len(csv_texts)} CSV texts from GPT-4o mini")
            result = self.gpt_csv_parser.process_gpt4o_results(file_path, csv_texts, updated_metadata)
            self.logger.info(f"      CSV processing complete, extracted {len(result) if result is not None else 0} rows")
            
            self.logger.info(f"<---- Completed GPT-4o vision flow for: {filename}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing PDF with GPT-4o mini: {str(e)}")
            self.logger.info(f"Error details: {traceback.format_exc()}")
            self.logger.info(f"<---- Failed GPT-4o vision flow for: {filename}")
            return None


    def process_pdf_with_gpt4o(self, file_path):
        """
        Process PDF using GPT-4o mini for image-based extraction.
        
        Args:
            file_path (str): Path to the PDF file
            
        Returns:
            DataFrame: Processed data in standardized format
        """
        filename = os.path.basename(file_path)
        self.logger.info(f"----> GPT-4o vision flow for: {filename}")
        
        try:
            # First extract text to get metadata
            self.logger.info(f"      Extracting text for metadata")
            text_to_use = self.text_extractor.extract_text_with_pdfplumber(file_path)
            self.logger.info(f"      Using text from pdfplumber for metadata")
            
            # Extract metadata from text
            self.logger.info(f"      Extracting metadata from text")
            metadata = self.metadata_extractor.extract_metadata_from_pdf(file_path, text_to_use)
            self.logger.info(f"      Metadata extracted: date={metadata['date']}, city={metadata['city']}, market={metadata['market']}")
            
            # Convert PDF to images
            self.logger.info(f"      Converting PDF to images")
            image_paths = self.image_handler.convert_pdf_to_images(file_path, save_copies=True)
            self.logger.info(f"      PDF converted to {len(image_paths)} images")
            
            # Check if we need to extract header metadata
            need_header_extraction = metadata['city'] == 'Unknown' or metadata['market'] == 'Unknown'
            
            # Encode all images to base64
            self.logger.info(f"      Encoding all images to base64")
            base64_images = []
            for img_path in image_paths:
                base64_images.append(self.image_handler.encode_image(img_path))
            self.logger.info(f"      All {len(base64_images)} images encoded")
            
            # Process images for tables using synchronous requests
            csv_texts = []
            self.logger.info(f"      Processing images for table extraction")
            
            for i, (image_path, base64_image) in enumerate(zip(image_paths, base64_images)):
                self.logger.info(f"      Processing image {i+1}/{len(image_paths)} for table extraction")
                csv_text = self.gpt_client.query_gpt4o_with_image(image_path, base64_image)
                if csv_text:
                    self.logger.info(f"      Successfully extracted table from image {i+1}")
                    csv_texts.append(csv_text)
                else:
                    self.logger.warning(f"      Failed to extract table from image {i+1}")
            
            # Process header if needed
            if need_header_extraction and len(image_paths) > 0:
                self.logger.info(f"      Extracting header data from first image")
                header_text = self.gpt_client.query_gpt4o_with_image(
                    image_paths[0], 
                    base64_images[0], 
                    prompt_type="header"
                )
                
                if header_text:
                    self.logger.info(f"      Successfully extracted header: {header_text[:100]}...")
                    
                    # Update metadata using the header text
                    self.logger.info(f"      Updating metadata using extracted header")
                    updated_metadata = self.metadata_extractor.extract_metadata_from_pdf(file_path, header_text)
                    
                    # Only update missing fields
                    if metadata['city'] == 'Unknown' and updated_metadata['city'] != 'Unknown':
                        metadata['city'] = updated_metadata['city']
                        self.logger.info(f"      Updated city to: {metadata['city']}")
                    
                    if metadata['market'] == 'Unknown' and updated_metadata['market'] != 'Unknown':
                        metadata['market'] = updated_metadata['market']
                        self.logger.info(f"      Updated market to: {metadata['market']}")
                    
                    if metadata['date'] == datetime.now().strftime('%Y-%m-%d') and updated_metadata['date'] != metadata['date']:
                        metadata['date'] = updated_metadata['date']
                        self.logger.info(f"      Updated date to: {metadata['date']}")
            
            # Track GPT-4o calls
            self.stats['gpt4o_calls'] += len(csv_texts) + (1 if need_header_extraction else 0)
            
            # Process the CSV texts
            self.logger.info(f"      Processing {len(csv_texts)} CSV texts from GPT-4o mini")
            result = self.gpt_csv_parser.process_gpt4o_results(file_path, csv_texts, metadata)
            self.logger.info(f"      CSV processing complete, extracted {len(result) if result is not None else 0} rows")
            
            self.logger.info(f"<---- Completed GPT-4o vision flow for: {filename}")
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing PDF with GPT-4o mini: {str(e)}")
            self.logger.info(f"Error details: {traceback.format_exc()}")
            self.logger.info(f"<---- Failed GPT-4o vision flow for: {filename}")
            return None
    
    def process_pdf_file(self, file_path, output_dir=None, force_reprocess=False):
        """
        Process a single PDF file and save results to CSV.
        
        Args:
            file_path (str): Path to the PDF file
            output_dir (str): Directory to save processed data (default: PROCESSED_DIR)
            force_reprocess (bool): If True, reprocess file even if already processed
            
        Returns:
            dict: Statistics about the processing
        """
        filename = os.path.basename(file_path)
        self.logger.info(f"========== STARTING PDF PROCESSING: {filename} ==========")
        
        # Check if file has already been processed
        if not force_reprocess and self.registry.file_processed(file_path):
            self.logger.info(f"File already processed: {filename}")
            return self.registry.get_file_info(file_path)['processing_history'][-1]['stats']
        
        # Set default output directory
        output_dir = output_dir or PROCESSED_DIR
        ensure_directory(output_dir)
        self.logger.info(f"Output directory: {output_dir}")
        
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
            'used_gpt_extraction': False  # Added new field to track if GPT image extraction was used
        }
        
        start_time = datetime.now()
        self.logger.info(f"Starting processing at: {start_time}")
        
        try:
            # Flag to track if text extraction had errors
            text_extraction_error = False
            text_to_use = ""
            
            try:
                self.logger.info(f"Step 1: Attempting text extraction from PDF")
                # Use only pdfplumber for text extraction
                self.logger.info(f"Extracting text using pdfplumber")
                text_to_use = self.text_extractor.extract_text_with_pdfplumber(file_path)
                self.logger.info(f"pdfplumber extraction complete, extracted {len(text_to_use)} characters")
            except Exception as e:
                self.logger.error(f"Error during text extraction: {str(e)}")
                text_extraction_error = True
                stats['errors_warnings'] += f"Text extraction error: {str(e)}; "
            
            # Assess text validity only if extraction was successful
            text_is_valid = False
            if not text_extraction_error:
                self.logger.info(f"Step 2: Checking if text is valid")
                text_is_valid = self.text_extractor.assess_text_quality(text_to_use)
                stats['text_is_valid'] = text_is_valid
                self.logger.info(f"Text is valid: {text_is_valid}")
            
            # Processing strategy based on text validity
            if not text_extraction_error and text_is_valid:
                # Valid text - process directly
                self.logger.info(f"Step 3A: Text is valid, processing with direct text extraction")
                stats['extraction_method'] = "text_extraction"
                stats['used_gpt_extraction'] = False  # Text extraction was used
                self.logger.info(f"Extracting metadata from text")
                output_df = self.process_pdf_with_text_extraction(file_path, text_to_use)
                self.logger.info(f"Text extraction processing complete, extracted {len(output_df) if output_df is not None else 0} rows")
            else:
                # Invalid text or extraction error - use GPT-4o mini with image
                if text_extraction_error:
                    self.logger.info(f"Step 3B: Text extraction failed, processing with GPT-4o vision")
                else:
                    self.logger.info(f"Step 3B: Text is not valid, processing with GPT-4o vision")
                    
                stats['extraction_method'] = "gpt4o_vision"
                stats['used_gpt_extraction'] = True  # GPT image extraction was used
                self.logger.info(f"Converting PDF to images and processing with GPT-4o")
                output_df = self.process_pdf_with_gpt4o(file_path)
                self.stats['gpt4o_calls'] += 1
                self.logger.info(f"GPT-4o vision processing complete, extracted {len(output_df) if output_df is not None else 0} rows")
            
            # Check if we have results
            if output_df is not None and not output_df.empty:
                # Save to CSV
                self.logger.info(f"Step 4: Saving extracted data to CSV")
                base_filename = os.path.splitext(filename)[0]
                output_csv = os.path.join(output_dir, f"{base_filename}.csv")
                output_df.to_csv(output_csv, index=False)
                self.logger.info(f"Saved output to: {output_csv}")
                
                # Update statistics
                stats['unique_products'] = len(output_df['product'].unique())
                stats['unique_major_categories'] = len(output_df['major_category'].unique())
                stats['unique_subcategories'] = len(output_df['sub_category'].unique())
                stats['rows_processed'] = len(output_df)
                
                self.logger.info(f"Data statistics: {stats['rows_processed']} rows, {stats['unique_products']} products, " + 
                               f"{stats['unique_major_categories']} major categories, {stats['unique_subcategories']} subcategories")
                self.logger.info(f"Used GPT extraction: {stats['used_gpt_extraction']}")
                
                # Update overall statistics
                self.stats['processed_files'] += 1
                self.stats['total_rows_processed'] += stats['rows_processed']
                
                output_path = output_csv
                
            else:
                self.logger.warning(f"No data extracted from PDF, no output file created")
                stats['error_occurred'] = True
                stats['errors_warnings'] += "No data extracted from PDF; "
                output_path = None
                
                # Update overall statistics
                self.stats['failed_files'] += 1
            
        except Exception as e:
            stats['error_occurred'] = True
            stats['errors_warnings'] += str(e)
            
            self.logger.error(f"Error processing PDF file {filename}: {str(e)}")
            self.logger.error(f"Error details: {traceback.format_exc()}")
            
            # Update overall statistics
            self.stats['failed_files'] += 1
            output_path = None
        
        # Calculate processing time
        stats['processing_time_seconds'] = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Total processing time: {stats['processing_time_seconds']:.2f} seconds")
        self.logger.info(f"Used GPT extraction: {stats['used_gpt_extraction']}")
        
        # Register processed file
        self.logger.info(f"Step 5: Registering processing results")
        self.registry.register_file(
            file_path,
            output_path=output_path,
            status="processed" if not stats['error_occurred'] else "error",
            error=stats['errors_warnings'] if stats['error_occurred'] else None,
            stats=stats
        )
        
        self.logger.info(f"========== COMPLETED PDF PROCESSING: {filename} ==========")
        
        return stats
    
    def process_pdf_files(self, pdf_files=None, output_dir=None, start_date=None, end_date=None, max_workers=None, force_reprocess=False):
        """
        Process multiple PDF files with multithreading.
        
        Args:
            pdf_files (list): List of PDF file paths to process
            output_dir (str): Directory to save processed data
            start_date (datetime): Start date for filtering files
            end_date (datetime): End date for filtering files
            max_workers (int): Maximum number of worker threads
            force_reprocess (bool): If True, reprocess files even if already processed
            
        Returns:
            dict: Statistics about the processing
        """
        # Set default directories
        output_dir = output_dir or PROCESSED_DIR
        ensure_directory(output_dir)
        
        # Get PDF files if not provided
        if pdf_files is None:
            self.logger.info(f"Finding PDF files in {PDF_DIR}")
            pdf_files = get_files_by_extension(PDF_DIR, ['.pdf'], recursive=True)
        
        self.logger.info(f"Processing {len(pdf_files)} PDF files")
        
        # Reset statistics
        self.stats = {
            'total_files': len(pdf_files),
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'total_rows_processed': 0,
            'warnings': 0,
            'gpt4o_calls': 0,
            'files_using_gpt_extraction': 0
        }
        
        # Set maximum number of workers
        max_workers = max_workers or min(os.cpu_count(), len(pdf_files))
        
        # Determine if we can use concurrent processing
        use_concurrent = max_workers > 1 and len(pdf_files) > 1
        
        if use_concurrent:
            self.logger.info(f"Using {max_workers} workers for parallel processing")
            
            # Process files concurrently
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {
                    executor.submit(self.process_pdf_file, file_path, output_dir, force_reprocess): file_path
                    for file_path in pdf_files
                }
                
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path = future_to_file[future]
                    filename = os.path.basename(file_path)
                    
                    try:
                        stats = future.result()
                        # Update overall stats if GPT extraction was used
                        if stats.get('used_gpt_extraction', False):
                            self.stats['files_using_gpt_extraction'] += 1
                    except Exception as e:
                        self.logger.error(f"Error processing {filename}: {str(e)}")
                        self.stats['failed_files'] += 1
        else:
            # Process files sequentially
            for file_path in pdf_files:
                try:
                    stats = self.process_pdf_file(file_path, output_dir, force_reprocess)
                    # Update overall stats if GPT extraction was used
                    if stats.get('used_gpt_extraction', False):
                        self.stats['files_using_gpt_extraction'] += 1
                except Exception as e:
                    filename = os.path.basename(file_path)
                    self.logger.error(f"Error processing {filename}: {str(e)}")
                    self.stats['failed_files'] += 1
        
        # Log summary
        self.logger.info(f"PDF processing summary: {self.stats['processed_files']} processed, "
                        f"{self.stats['failed_files']} failed, {self.stats['skipped_files']} skipped "
                        f"out of {self.stats['total_files']} total files")
        self.logger.info(f"Processed {self.stats['total_rows_processed']} total rows")
        self.logger.info(f"Files using GPT extraction: {self.stats['files_using_gpt_extraction']} out of {self.stats['processed_files']} processed files")
        self.logger.info(f"Made {self.stats['gpt4o_calls']} GPT-4o calls")
        
        return self.stats
