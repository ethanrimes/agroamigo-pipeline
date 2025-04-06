"""
PDF processor for Colombian agricultural price data.
Entry point for PDF processing functionality.
"""

import os
import logging
from datetime import datetime

from agroamigo_pipeline.config.settings import (
    PDF_DIR, PROCESSED_DIR
)
from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.processors.pdf_components.orchestrator import PDFProcessorOrchestrator
from agroamigo_pipeline.registry.pdf_registry import PDFProcessorRegistry

class PDFProcessor:
    """
    Process PDF files containing Colombian agricultural price data.
    Acts as an entry point to the PDF processing functionality.
    """
    
    def __init__(self, logger=None, save_debug_data=False):
        """
        Initialize the PDF processor.
        
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
            
        # Initialize registry
        self.registry = PDFProcessorRegistry(logger=self.logger)
        
        # Initialize the orchestrator that handles the actual processing
        self.orchestrator = PDFProcessorOrchestrator(
            logger=self.logger,
            save_debug_data=save_debug_data,
            registry=self.registry
        )
        
        self.logger.info("PDFProcessor initialized")
    
    def process_pdf_file(self, pdf_id, file_path, output_dir=None, force_reprocess=False):
        """
        Process a single PDF file and save results to CSV.
        
        Args:
            pdf_id (str): PDF ID from the registry
            file_path (str): Path to the PDF file
            output_dir (str): Directory to save processed data
            force_reprocess (bool): If True, reprocess file even if already processed
            
        Returns:
            dict: Processing statistics
        """
        # Delegate to orchestrator
        return self.orchestrator.process_single_pdf_file(
            pdf_id=pdf_id,
            file_path=file_path,
            output_dir=output_dir,
            force_reprocess=force_reprocess
        )
    
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
        # Delegate to orchestrator
        return self.orchestrator.process_pdf_files(
            output_dir=output_dir,
            start_date=start_date,
            end_date=end_date,
            max_workers=max_workers,
            force_reprocess=force_reprocess
        )
    
    @property
    def stats(self):
        """Get current processing statistics."""
        return self.orchestrator.stats