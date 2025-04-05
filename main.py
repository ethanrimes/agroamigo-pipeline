import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from agroamigo_pipeline.config.settings import (
    ensure_directories, SOURCE_DIR, PDF_DIR, PROCESSED_DIR, 
    CONSOLIDATED_DIR, LOG_LEVEL, MAX_WORKERS, PROCESSING_REGISTRY
)
from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.processors.zip_extractor import ZipExtractor
from agroamigo_pipeline.processors.excel_processor import ExcelProcessor
from agroamigo_pipeline.processors.pdf_processor import PDFProcessor
from agroamigo_pipeline.processors.data_aggregator import DataAggregator
from agroamigo_pipeline.error_handlers.error_gatherer import ErrorGatherer
from agroamigo_pipeline.utils.parquet_registry import ParquetRegistry

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
        
        # Check if old JSON registry exists and migrate if needed
        self._migrate_registry_if_needed()
        
        self.logger.info("Pipeline initialized")
        self.logger.debug(f"Log level: {logging.getLevelName(self.log_level)}")
        self.logger.debug(f"Directories: SOURCE={SOURCE_DIR}, PDF={PDF_DIR}, "
                       f"PROCESSED={PROCESSED_DIR}, CONSOLIDATED={CONSOLIDATED_DIR}")
    
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
        self.logger.info(f"Date range: {start_date} to {end_date}")
        
        # Use provided max_workers or default from settings
        max_workers = max_workers or MAX_WORKERS
        
        try:
            # Run pipeline components in sequence
            results = {}
            
            # Step 1: ZIP extraction
            if not components or 'extractor' in components:
                self.logger.info("Running ZIP extraction...")
                extractor_stats = self._run_extractor(force_reprocess)
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
            
            self.logger.info("Pipeline execution completed successfully")
            return results
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise
    
    # The rest of the class remains the same, just replace any remaining FileRegistry references with ParquetRegistry
    # No other changes are needed in the methods below as they use the registry through the same interface
    
    def _run_extractor(self, force_reprocess=False):
        """Run the ZIP extractor component."""
        try:
            extractor = ZipExtractor()
            stats = extractor.process_zip_files(force_reprocess=force_reprocess)
            return stats
        except Exception as e:
            self.logger.error(f"ZIP extraction failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise
    
    def _run_excel_processor(self, start_date, end_date, force_reprocess=False):
        """Run the Excel processor component."""
        try:
            processor = ExcelProcessor()
            stats = processor.process_excel_files(
                source_dir=SOURCE_DIR,
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
    
    def _run_pdf_processor(self, start_date, end_date, max_workers, force_reprocess=False):
        """Run the PDF processor component."""
        try:
            # Get list of PDF files
            pdf_files = list(Path(PDF_DIR).glob('**/*.pdf'))
            self.logger.info(f"Found {len(pdf_files)} PDF files to process")
            
            processor = PDFProcessor()
            stats = processor.process_pdf_files(
                pdf_files=pdf_files,
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
    
    def _run_error_aggregation(self):
        """Run the error aggregation component."""
        try:
            aggregator = ErrorGatherer()
            df = aggregator.consolidate_error_files()
            return {
                "total_errors": len(df),
                "error_stats": aggregator.stats
            }
        except Exception as e:
            self.logger.error(f"Error aggregation failed: {str(e)}")
            self.logger.exception("Exception details:")
            raise
    
    def _run_data_aggregation(self, start_date, end_date):
        """Run the data aggregation component."""
        try:
            aggregator = DataAggregator()
            df = aggregator.aggregate_data(
                filter_date_start=start_date.strftime('%Y-%m-%d') if start_date else None,
                filter_date_end=end_date.strftime('%Y-%m-%d') if end_date else None
            )
            return {
                "rows_output": len(df),
                "aggregation_stats": aggregator.stats
            }
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
                      choices=['extractor', 'excel', 'pdf', 'error', 'aggregator'],
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