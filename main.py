"""
Main module for the Colombian Agricultural Price Data Pipeline.
Uses the new registry system for tracking files through the pipeline.
"""

import argparse
import logging
import os
import sys
import uuid
from datetime import datetime

from agroamigo_pipeline.config.settings import (
    ensure_directories, SOURCE_DIR, PDF_DIR, TEMP_DIR, PROCESSED_DIR, 
    CONSOLIDATED_DIR, LOG_LEVEL, MAX_WORKERS, PIPELINE_RUN_ID
)
from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.utils.exceptions import PipelineError

# Import processors
from agroamigo_pipeline.scraping.web_scraper import WebScraper
from agroamigo_pipeline.processors.zip_extractor import ZipExtractor
from agroamigo_pipeline.processors.excel_processor import ExcelProcessor
from agroamigo_pipeline.processors.pdf_processor import PDFProcessor
from agroamigo_pipeline.processors.data_aggregator import DataAggregator
from agroamigo_pipeline.error_handlers.error_gatherer import ErrorGatherer
from agroamigo_pipeline.error_handlers.run_statistics import RunStatistics


class Pipeline:
    """Main pipeline orchestration class."""
    
    def __init__(self, log_level=None):
        """
        Initialize the pipeline with configuration.
        
        Args:
            log_level (int): Logging level (e.g., logging.INFO)
        """
        # Initialize logging
        self.log_level = log_level or getattr(logging, LOG_LEVEL)
        logging_setup = setup_logging(self.log_level)
        self.logger = logging_setup['scraper']  # Main logger
        
        # Ensure all directories exist
        ensure_directories()
        
        # Generate pipeline run ID if not already set
        global PIPELINE_RUN_ID
        if not PIPELINE_RUN_ID:
            PIPELINE_RUN_ID = str(uuid.uuid4())

        # Add timestamp tracking for run statistics
        self.start_timestamp = datetime.now()
        
        # Initialize the run statistics module
        self.run_statistics = RunStatistics(
            logger=self.logger,
            pipeline_run_id=PIPELINE_RUN_ID,
            start_timestamp=self.start_timestamp
        )
        
        self.logger.info(f"Pipeline initialized with run ID: {PIPELINE_RUN_ID}")
        self.logger.debug(f"Log level: {logging.getLevelName(self.log_level)}")
        self.logger.debug(f"Directories: SOURCE={SOURCE_DIR}, PDF={PDF_DIR}, "
                       f"PROCESSED={PROCESSED_DIR}, CONSOLIDATED={CONSOLIDATED_DIR}")
    
    def run(self, start_date=None, end_date=None, components=None, max_workers=None, force_reprocess=False):
        """
        Run the full pipeline or specified components.
        
        Args:
            start_date (datetime): Start date for filtering files
            end_date (datetime): End date for filtering files
            components (list): List of components to run (None for all)
            max_workers (int): Number of parallel workers for processing
            force_reprocess (bool): If True, reprocess files even if they've been processed before
            
        Returns:
            dict: Results from each pipeline component
        """
        self.logger.info(f"Starting pipeline run with components: {components or 'all'}")
        
        # Format date range for logging
        start_date_str = start_date.strftime('%Y-%m-%d') if start_date else "earliest"
        end_date_str = end_date.strftime('%Y-%m-%d') if end_date else "latest"
        self.logger.info(f"Date range for filtering: {start_date_str} to {end_date_str}")
        
        # Use provided max_workers or default from settings
        max_workers = max_workers or MAX_WORKERS
        self.logger.info(f"Using maximum of {max_workers} worker threads")
        
        try:
            # Run pipeline components in sequence
            results = {}
            
            # Step 0: Web scraping
            if not components or 'scraper' in components:
                self.logger.info("Running web scraper...")
                scraper_stats = self._run_web_scraper(start_date, end_date, max_workers, force_reprocess)
                results['scraper'] = scraper_stats
                self.logger.info("Completed web scraping")
            
            # Step 1: ZIP extraction
            if not components or 'extractor' in components:
                self.logger.info("Running ZIP extraction...")
                extractor_stats = self._run_extractor(start_date, end_date, force_reprocess)
                results['extractor'] = extractor_stats
                self.logger.info("Completed ZIP extraction")
            
            # Step 2: Excel processing
            if not components or 'excel' in components:
                self.logger.info("Running Excel processing...")
                excel_stats = self._run_excel_processor(start_date, end_date, max_workers, force_reprocess)
                results['excel'] = excel_stats
                self.logger.info("Completed Excel processing")
            
            # Step 3: PDF processing
            if not components or 'pdf' in components:
                self.logger.info("Running PDF processing...")
                pdf_stats = self._run_pdf_processor(start_date, end_date, max_workers, force_reprocess)
                results['pdf'] = pdf_stats
                self.logger.info("Completed PDF processing")
            
            # Step 4: Data aggregation
            if not components or 'aggregator' in components:
                self.logger.info("Running data aggregation...")
                aggregator_stats = self._run_data_aggregation(start_date, end_date)
                results['aggregator'] = aggregator_stats
                self.logger.info("Completed data aggregation")
            
            # Step 5: Error aggregation
            if not components or 'error' in components:
                self.logger.info("Running error aggregation...")
                error_stats = self._run_error_aggregation()
                results['error'] = error_stats
                self.logger.info("Completed error aggregation")
            
            # At the end of run before returning results, add:
            self.logger.info("Generating run statistics...")
            stats_results = self.run_statistics.process_end_of_run(components)
            results['run_statistics'] = stats_results

            self.logger.info("Pipeline execution completed successfully")
            return results
            
        except Exception as e:
            # Try to generate run statistics even if there was an error
            try:
                self.logger.info("Generating run statistics after error...")
                stats_results = self.run_statistics.process_end_of_run(components)
                if 'run_statistics' not in results:
                    results['run_statistics'] = stats_results
            except Exception as stats_error:
                self.logger.error(f"Failed to generate run statistics: {str(stats_error)}")
            
            # Original error handling
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise
    
    def _run_web_scraper(self, start_date, end_date, max_workers, force_reprocess):
        """
        Run the web scraper component.
        
        Args:
            start_date (datetime): Start date for filtering 
            end_date (datetime): End date for filtering
            max_workers (int): Number of worker threads
            force_reprocess (bool): Whether to force reprocessing
            
        Returns:
            dict: Scraping statistics
        """
        try:
            # Initialize web scraper
            scraper = WebScraper(source_dir=SOURCE_DIR, max_workers=max_workers, logger=self.logger)
            
            # Run scraper
            stats = scraper.run(start_date, end_date, max_workers, force_reprocess)
            
            return stats
        except Exception as e:
            self.logger.error(f"Web scraping failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise
    
    def _run_extractor(self, start_date, end_date, force_reprocess):
        """
        Run the ZIP extractor component.
        
        Args:
            start_date (datetime): Start date for filtering
            end_date (datetime): End date for filtering
            force_reprocess (bool): Whether to force reprocessing
            
        Returns:
            dict: Extraction statistics
        """
        try:
            # Initialize ZIP extractor
            extractor = ZipExtractor(
                source_dir=SOURCE_DIR,
                pdf_dir=PDF_DIR,
                temp_dir=TEMP_DIR,
                logger=self.logger
            )
            
            # Process ZIP files
            stats = extractor.process_all_zips(start_date, end_date, force_reprocess)
            
            return stats
        except Exception as e:
            self.logger.error(f"ZIP extraction failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise
    
    def _run_excel_processor(self, start_date, end_date, max_workers, force_reprocess):
        """
        Run the Excel processor component.
        
        Args:
            start_date (datetime): Start date for filtering
            end_date (datetime): End date for filtering
            max_workers (int): Number of worker threads
            force_reprocess (bool): Whether to force reprocessing
            
        Returns:
            dict: Processing statistics
        """
        try:
            # Initialize Excel processor
            processor = ExcelProcessor(logger=self.logger)
            
            # Process Excel files
            stats = processor.process_excel_files(
                source_dir=SOURCE_DIR,  # Not used but kept for compatibility
                output_dir=PROCESSED_DIR,
                start_date=start_date,
                end_date=end_date,
                force_reprocess=force_reprocess
            )
            
            return stats
        except Exception as e:
            self.logger.error(f"Excel processing failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise
    
    def _run_pdf_processor(self, start_date, end_date, max_workers, force_reprocess):
        """
        Run the PDF processor component.
        
        Args:
            start_date (datetime): Start date for filtering
            end_date (datetime): End date for filtering
            max_workers (int): Number of worker threads
            force_reprocess (bool): Whether to force reprocessing
            
        Returns:
            dict: Processing statistics
        """
        try:
            # Use MAX_WORKERS from config if no max_workers specified
            max_workers = max_workers or MAX_WORKERS
            
            # Initialize PDF processor
            processor = PDFProcessor(logger=self.logger)
            
            # Process PDF files
            stats = processor.process_pdf_files(
                output_dir=PROCESSED_DIR,
                start_date=start_date,
                end_date=end_date,
                max_workers=max_workers,
                force_reprocess=force_reprocess
            )
            
            return stats
        except Exception as e:
            self.logger.error(f"PDF processing failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise
    
    def _run_data_aggregation(self, start_date, end_date):
        """
        Run the data aggregation component.
        
        Args:
            start_date (datetime): Start date for filtering
            end_date (datetime): End date for filtering
            
        Returns:
            dict: Aggregation statistics
        """
        try:
            # Initialize data aggregator
            aggregator = DataAggregator(
                input_dir=PROCESSED_DIR,
                output_dir=CONSOLIDATED_DIR,
                logger=self.logger
            )
            
            # Aggregate data
            aggregator.aggregate_data(
                filter_date_start=start_date.strftime('%Y-%m-%d') if start_date else None,
                filter_date_end=end_date.strftime('%Y-%m-%d') if end_date else None,
                update_registries=True
            )
            
            return aggregator.stats
        except Exception as e:
            self.logger.error(f"Data aggregation failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise
    
    def _run_error_aggregation(self):
        """
        Run the error aggregation component.
        
        Returns:
            dict: Error statistics
        """
        try:
            # Initialize error gatherer
            error_gatherer = ErrorGatherer(logger=self.logger)
            
            # Collect errors
            error_df, summary_path = error_gatherer.collect_all_errors()
            
            error_count = len(error_df) if error_df is not None else 0
            
            return {
                'error_count': error_count,
                'summary_path': summary_path
            }
        except Exception as e:
            self.logger.error(f"Error aggregation failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='Colombian Agricultural Price Data Pipeline')
    
    parser.add_argument('--start-date', type=str, default='2000-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default='2100-12-31', help='End date (YYYY-MM-DD)')
    parser.add_argument('--components', type=str, nargs='+', 
                      choices=['scraper', 'extractor', 'excel', 'pdf', 'aggregator', 'error'],
                      help='Components to run (default: all)')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS,
                      help=f'Number of worker processes (default: {MAX_WORKERS})')
    parser.add_argument('--log-level', type=str, default=LOG_LEVEL,
                      choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                      help=f'Logging level (default: {LOG_LEVEL})')
    parser.add_argument('--force-reprocess', action='store_true',
                      help='Force reprocessing of files even if already processed')
    
    return parser.parse_args()

def main():
    """Main entry point for the pipeline."""
    args = parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d') if args.start_date else None
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else None
    
    # Convert log level string to constant
    log_level = getattr(logging, args.log_level)
    
    # Initialize and run pipeline
    pipeline = Pipeline(log_level=log_level)
    pipeline.run(
        start_date=start_date,
        end_date=end_date,
        components=args.components,
        max_workers=args.workers,
        force_reprocess=args.force_reprocess
    )

if __name__ == "__main__":
    main()