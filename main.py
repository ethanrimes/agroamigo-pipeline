import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from agroamigo_pipeline.config.settings import (
    ensure_directories, SOURCE_DIR, PDF_DIR, TEMP_DIR, PROCESSED_DIR, 
    CONSOLIDATED_DIR, LOG_LEVEL, MAX_WORKERS, PROCESSING_REGISTRY
)
from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.processors.zip_extractor import ZipExtractor
from agroamigo_pipeline.processors.excel_processor import ExcelProcessor
from agroamigo_pipeline.processors.pdf_processor import PDFProcessor
from agroamigo_pipeline.processors.data_aggregator import DataAggregator
from agroamigo_pipeline.error_handlers.error_gatherer import ErrorGatherer
from agroamigo_pipeline.utils.parquet_registry import ParquetRegistry
from agroamigo_pipeline.scraping.web_scraper import WebScraper
from agroamigo_pipeline.utils.web_scraper_registry import WebScraperRegistry
from agroamigo_pipeline.processors.pdf_components.orchestrator import PDFProcessorOrchestrator

class Pipeline:
    """Main pipeline orchestration class."""
    
    def __init__(self, log_level=None):
        """Initialize the pipeline with configuration."""
        # Initialize logging
        self.log_level = log_level or getattr(logging, LOG_LEVEL)
        logging_setup = setup_logging(self.log_level)
        self.logger = logging_setup['scraper']  # Main logger
        
        # Ensure all directories exist
        ensure_directories()
        
        # Initialize registry - use ParquetRegistry instead of FileRegistry
        self.registry = ParquetRegistry()
        
        # Initialize web scraper registry
        self.web_scraper_registry = WebScraperRegistry()
        
        # Check if old JSON registry exists and migrate if needed
        self._migrate_registry_if_needed()
        
        self.logger.info("Pipeline initialized")
        self.logger.debug(f"Log level: {logging.getLevelName(self.log_level)}")
        self.logger.debug(f"Directories: SOURCE={SOURCE_DIR}, PDF={PDF_DIR}, "
                       f"PROCESSED={PROCESSED_DIR}, CONSOLIDATED={CONSOLIDATED_DIR}")
    
    def _run_extractor(self, start_date=None, end_date=None, force_reprocess=False):
        """
        Run the ZIP extractor component.
        
        Args:
            start_date (datetime): Start date for filtering files
            end_date (datetime): End date for filtering files
            force_reprocess (bool): If True, reprocess files even if already processed
        
        Returns:
            dict: Processing statistics
        """
        try:
            self.logger.info("Starting ZIP extraction process")
            
            # Format date range for logging
            start_date_str = start_date.strftime('%Y-%m-%d') if start_date else "earliest"
            end_date_str = end_date.strftime('%Y-%m-%d') if end_date else "latest"
            self.logger.info(f"Date range for ZIP extraction: {start_date_str} to {end_date_str}")
            
            # Create and configure the ZipExtractor
            extractor = ZipExtractor(
                source_dir=SOURCE_DIR,
                pdf_dir=PDF_DIR, 
                temp_dir=TEMP_DIR,
                logger=self.logger  # Pass the main logger to ensure consistent log level
            )
            
            # Process all ZIP files with date filtering
            stats = extractor.process_all_zips(
                start_date=start_date,
                end_date=end_date,
                force_reprocess=force_reprocess
            )
            
            self.logger.info(f"ZIP extraction completed: {stats.get('success', 0)} successful, "
                            f"{stats.get('failed', 0)} failed, {stats.get('skipped', 0)} skipped due to date filter")
            return stats
        except Exception as e:
            self.logger.error(f"ZIP extraction failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise

    def _run_error_aggregation(self):
        """Run the error aggregation to collect all errors from pipeline components."""
        try:
            self.logger.info("Starting error aggregation process")
            
            # Create error gatherer
            error_gatherer = ErrorGatherer(logger=self.logger)  # Pass logger to ensure consistent log level
            
            # Collect all errors from component registries
            error_df, summary_path = error_gatherer.collect_all_errors()
            
            error_count = len(error_df) if error_df is not None else 0
            self.logger.info(f"Error aggregation completed: {error_count} errors found")
            self.logger.info(f"Error summary written to: {summary_path}")
            
            return {
                'error_count': error_count,
                'summary_path': summary_path
            }
        except Exception as e:
            self.logger.error(f"Error aggregation failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise

    def _migrate_registry_if_needed(self):
        """Migrate from JSON registry to Parquet if needed."""
        if os.path.exists(PROCESSING_REGISTRY):
            self.logger.info("Found existing JSON registry, attempting migration")
            if self.registry.migrate_from_json(PROCESSING_REGISTRY):
                self.logger.info("Successfully migrated registry data from JSON to Parquet")
                # Optionally rename old JSON file to prevent future migrations
                backup_path = f"{PROCESSING_REGISTRY}.bak"
                try:
                    os.rename(PROCESSING_REGISTRY, backup_path)
                    self.logger.info(f"Renamed old JSON registry to {backup_path}")
                except Exception as e:
                    self.logger.warning(f"Could not rename old JSON registry: {str(e)}")
            else:
                self.logger.warning("Migration from JSON registry failed or had no data")
    
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
        
        try:
            # Run pipeline components in sequence
            results = {}
            
            # Step 0: Web scraping (new component)
            if not components or 'scraper' in components:
                self.logger.info("Running web scraper...")
                scraper_stats = self._run_web_scraper(start_date, end_date, max_workers)
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
                excel_stats = self._run_excel_processor(start_date, end_date, force_reprocess)
                results['excel'] = excel_stats
                self.logger.info("Completed Excel processing")
            
            # Step 3: PDF processing
            if not components or 'pdf' in components:
                self.logger.info("Running PDF processing...")
                pdf_stats = self._run_pdf_processor(start_date, end_date, max_workers, force_reprocess)
                results['pdf'] = pdf_stats
                self.logger.info("Completed PDF processing")
            
            # Step 4: Error aggregation
            if not components or 'error' in components:
                self.logger.info("Running error aggregation...")
                error_stats = self._run_error_aggregation()
                results['error'] = error_stats
                self.logger.info("Completed error aggregation")
            
            # Step 5: Data aggregation
            if not components or 'aggregator' in components:
                self.logger.info("Running data aggregation...")
                aggregator_stats = self._run_data_aggregation(start_date, end_date)
                results['aggregator'] = aggregator_stats
                self.logger.info("Completed data aggregation")
            
            # Export registry data to CSV at the end of the run
            self.logger.info("Exporting registry data to CSV...")
            csv_path = self.registry.export_to_csv()
            if csv_path:
                self.logger.info(f"Registry data exported to {csv_path}")
            
            # Export web scraper registry data to CSV
            self.logger.info("Exporting web scraper registry data to CSV...")
            scraper_csv_path = self.web_scraper_registry.export_to_csv()
            if scraper_csv_path:
                self.logger.info(f"Web scraper registry data exported to {scraper_csv_path}")
            
            self.logger.info("Pipeline execution completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise
    
    def _run_web_scraper(self, start_date, end_date, max_workers=None):
        """Run the web scraper component."""
        try:
            # Pass the log level to the web scraper
            scraper = WebScraper(source_dir=SOURCE_DIR, max_workers=max_workers, logger=self.logger)
            stats = scraper.run(start_date=start_date, end_date=end_date, max_workers=max_workers)
            return stats
        except Exception as e:
            self.logger.error(f"Web scraping failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise

    def _run_excel_processor(self, start_date=None, end_date=None, force_reprocess=False):
        """Run the Excel processor component."""
        try:
            self.logger.info("Starting Excel processing")
            
            # Create and configure the ExcelProcessor - pass our logger to ensure consistent log level
            processor = ExcelProcessor(logger=self.logger)
            
            # Process Excel files
            stats = processor.process_excel_files(
                source_dir=SOURCE_DIR,
                output_dir=PROCESSED_DIR,
                start_date=start_date,
                end_date=end_date,
                force_reprocess=force_reprocess
            )
            
            self.logger.info(f"Excel processing completed: {stats['processed_files']} succeeded, {stats['failed_files']} failed")
            return stats
        except Exception as e:
            self.logger.error(f"Excel processing failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise

    def _run_pdf_processor(self, start_date=None, end_date=None, max_workers=None, force_reprocess=False):
        """Run the PDF processor component."""
        try:
            self.logger.info("Starting PDF processing")
            
            # Create and configure the PDFProcessor - pass our logger to ensure consistent log level
            processor = PDFProcessorOrchestrator(logger=self.logger)
            
            # Process all PDF files
            stats = processor.process_pdf_files(
                output_dir=PROCESSED_DIR,
                start_date=start_date,
                end_date=end_date,
                max_workers=max_workers,
                force_reprocess=force_reprocess
            )
            
            self.logger.info(f"PDF processing completed: {stats['processed_files']} succeeded, {stats['failed_files']} failed")
            return stats
        except Exception as e:
            self.logger.error(f"PDF processing failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise

    def _run_data_aggregation(self, start_date=None, end_date=None):
        """Run the data aggregation component."""
        try:
            self.logger.info("Starting data aggregation process")
            
            # Create and configure the DataAggregator - pass our logger to ensure consistent log level
            aggregator = DataAggregator(
                input_dir=PROCESSED_DIR,
                output_dir=CONSOLIDATED_DIR,
                logger=self.logger
            )
            
            # Generate consolidated dataset
            result = aggregator.aggregate_data(
                filter_date_start=start_date.strftime('%Y-%m-%d') if start_date else None,
                filter_date_end=end_date.strftime('%Y-%m-%d') if end_date else None
            )
            
            # Get statistics
            stats = {
                'files_processed': aggregator.stats['files_processed'],
                'rows_processed': aggregator.stats['rows_processed'],
                'rows_output': aggregator.stats['rows_output'],
                'duplicates_removed': aggregator.stats['duplicates_removed']
            }
            
            self.logger.info(f"Data aggregation completed: {stats['files_processed']} files processed, "
                            f"{stats['rows_output']} rows in final dataset")
            
            return stats
        except Exception as e:
            self.logger.error(f"Data aggregation failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='Colombian Agricultural Price Data Pipeline')
    
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--components', type=str, nargs='+', 
                      choices=['extractor', 'excel', 'pdf', 'error', 'aggregator', 'scraper'],
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