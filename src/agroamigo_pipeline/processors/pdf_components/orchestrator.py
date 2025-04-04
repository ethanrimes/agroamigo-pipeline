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
    PDF_DIR, PROCESSED_DIR, MIN_UNIQUE_PRODUCTS, MAX_UNIQUE_PRODUCTS,
    PDF_TEXT_QUALITY_THRESHOLD
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
    
    def __init__(self, logger=None):
        """
        Initialize the PDF processor orchestrator.
        
        Args:
            logger (Logger): Logger instance
        """
        # Set up logger if not provided
        if logger is None:
            logging_setup = setup_logging()
            self.logger = logging_setup['pdf_processor']
        else:
            self.logger = logger
            
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
            'gpt4o_calls': 0
        }
        
        # Timing statistics
        self.timing_stats = {
            'pdf_text_extraction': [],
            'pdf_to_images': [],
            'image_encoding': [],
            'gpt4_query': [],
            'data_processing': []
        }
        
        # Initialize component modules
        self.text_extractor = TextExtractor(logger=self.logger, timing_stats=self.timing_stats)
        self.metadata_extractor = MetadataExtractor(logger=self.logger)
        self.product_parser = ProductParser(logger=self.logger, timing_stats=self.timing_stats)
        self.image_handler = ImageHandler(logger=self.logger, timing_stats=self.timing_stats)
        self.gpt_client = GPTClient(logger=self.logger, timing_stats=self.timing_stats)
        self.gpt_csv_parser = GPTCSVParser(logger=self.logger, timing_stats=self.timing_stats)
        
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
        # Extract metadata from text
        metadata = self.metadata_extractor.extract_metadata_from_pdf(file_path, text)
        
        # Process the text to extract product data
        return self.product_parser.process_extracted_text(file_path, text, metadata)
    
    def process_pdf_with_gpt4o(self, file_path):
        """
        Process PDF using GPT-4o mini for image-based extraction.
        
        Args:
            file_path (str): Path to the PDF file
            
        Returns:
            DataFrame: Processed data in standardized format
        """
        try:
            # First extract text to get metadata
            text_pypdf = self.text_extractor.extract_text_with_pypdf(file_path)
            text_pdfplumber = self.text_extractor.extract_text_with_pdfplumber(file_path)
            
            # Use the text with more content for metadata extraction
            text_to_use = text_pypdf if len(text_pypdf) > len(text_pdfplumber) else text_pdfplumber
            
            # Extract metadata from text
            metadata = self.metadata_extractor.extract_metadata_from_pdf(file_path, text_to_use)
            
            # Convert PDF to images
            image_paths = self.image_handler.convert_pdf_to_images(file_path)
            
            # Process each page with GPT-4o mini
            csv_texts = []
            
            for image_path in image_paths:
                # Encode the image to base64
                base64_image = self.image_handler.encode_image(image_path)
                
                # Query GPT-4o mini with the image
                csv_text = self.gpt_client.query_gpt4o_with_image(image_path, base64_image)
                
                if csv_text:
                    csv_texts.append(csv_text)
            
            # Process the CSV texts
            return self.gpt_csv_parser.process_gpt4o_results(file_path, csv_texts, metadata)
            
        except Exception as e:
            self.logger.error(f"Error processing PDF with GPT-4o mini: {str(e)}")
            self.logger.debug(f"Error details: {traceback.format_exc()}")
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
        self.logger.info(f"Processing PDF file: {filename}")
        
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
            'processing_time_seconds': 0,
            'extraction_method': "",
            'text_quality_score': 0.0
        }
        
        start_time = datetime.now()
        
        try:
            # Try direct text extraction first
            text_pypdf = self.text_extractor.extract_text_with_pypdf(file_path)
            text_pdfplumber = self.text_extractor.extract_text_with_pdfplumber(file_path)
            
            # Use the text with more content
            text_to_use = text_pypdf if len(text_pypdf) > len(text_pdfplumber) else text_pdfplumber
            
            # Assess text quality
            text_quality = self.text_extractor.assess_text_quality(text_to_use)
            stats['text_quality_score'] = text_quality
            
            # Processing strategy based on text quality
            if text_quality >= PDF_TEXT_QUALITY_THRESHOLD:
                # Good quality text - process directly
                stats['extraction_method'] = "text_extraction"
                output_df = self.process_pdf_with_text_extraction(file_path, text_to_use)
            else:
                # Poor quality text - use GPT-4o mini with image
                stats['extraction_method'] = "gpt4o_vision"
                output_df = self.process_pdf_with_gpt4o(file_path)
                self.stats['gpt4o_calls'] += 1
            
            # Update statistics
            if output_df is not None and not output_df.empty:
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
                self.logger.info(f"Saved processed data to {output_path}")
                
                # Update overall statistics
                self.stats['processed_files'] += 1
                self.stats['total_rows_processed'] += stats['rows_processed']
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
            'gpt4o_calls': 0
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
                        # Statistics are updated by the individual process_pdf_file calls
                    except Exception as e:
                        self.logger.error(f"Error processing {filename}: {str(e)}")
                        self.stats['failed_files'] += 1
        else:
            # Process files sequentially
            for file_path in pdf_files:
                try:
                    self.process_pdf_file(file_path, output_dir, force_reprocess)
                except Exception as e:
                    filename = os.path.basename(file_path)
                    self.logger.error(f"Error processing {filename}: {str(e)}")
                    self.stats['failed_files'] += 1
        
        # Log summary
        self.logger.info(f"PDF processing summary: {self.stats['processed_files']} processed, "
                        f"{self.stats['failed_files']} failed, {self.stats['skipped_files']} skipped "
                        f"out of {self.stats['total_files']} total files")
        self.logger.info(f"Processed {self.stats['total_rows_processed']} total rows")
        self.logger.info(f"Made {self.stats['gpt4o_calls']} GPT-4o calls")
        
        return self.stats
