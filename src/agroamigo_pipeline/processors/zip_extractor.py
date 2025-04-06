"""
Updated ZIP extractor for processing ZIP files.
Uses the new registry system for tracking extractions.
Also handles PDF files directly downloaded from the web scraper.
"""

import os
import zipfile
import logging
import shutil
from pathlib import Path
from datetime import datetime
import time
import pandas as pd
import mimetypes

from agroamigo_pipeline.config.settings import (
    SOURCE_DIR, PDF_DIR, TEMP_DIR
)
from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.utils.date_utils import parse_spanish_date, filter_files_by_date
from agroamigo_pipeline.utils.exceptions import ExtractorError
from agroamigo_pipeline.registry.webscraper_registry import WebScraperRegistry
from agroamigo_pipeline.registry.extractor_registry import ZipExtractorRegistry
from agroamigo_pipeline.registry.pdf_registry import PDFProcessorRegistry

class ZipExtractor:
    """Extract and process ZIP files containing PDFs and other documents.
    Also handles standalone PDF files from the web scraper."""
    
    def __init__(self, source_dir=None, pdf_dir=None, temp_dir=None, logger=None):
        """
        Initialize the ZIP extractor.
        
        Args:
            source_dir (str): Directory with source ZIP files
            pdf_dir (str): Directory for extracted PDF files
            temp_dir (str): Temporary directory for processing
            logger (Logger): Logger instance
        """
        # Set up directories
        self.source_dir = Path(source_dir) if source_dir else Path(SOURCE_DIR)
        self.pdf_dir = Path(pdf_dir) if pdf_dir else Path(PDF_DIR)
        self.temp_dir = Path(temp_dir) if temp_dir else Path(TEMP_DIR)
        
        # Ensure directories exist
        os.makedirs(self.source_dir, exist_ok=True)
        os.makedirs(self.pdf_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Set up logger
        if logger is None:
            logging_setup = setup_logging()
            self.logger = logging_setup['file_extractor']
        else:
            self.logger = logger
            
        # Initialize registries
        self.webscraper_registry = WebScraperRegistry(logger=self.logger)
        self.extractor_registry = ZipExtractorRegistry(logger=self.logger)
        self.pdf_registry = PDFProcessorRegistry(logger=self.logger) 
        
        # Statistics tracking
        self.total_stats = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "pdf_extracted": 0,
            "direct_pdfs_processed": 0,
            "excel_retained": 0
        }
        
        # Monthly statistics
        self.monthly_stats = {}
        
        self.logger.info("ZipExtractor initialized")
        self.logger.debug(f"Source directory: {self.source_dir}")
        self.logger.debug(f"PDF directory: {self.pdf_dir}")
        self.logger.debug(f"Temp directory: {self.temp_dir}")
    
    def _get_date_from_filename(self, filename):
        """
        Extract date from filename in format YYYYMMDD.
        
        Args:
            filename (str): Filename to extract date from
            
        Returns:
            str: Date string in YYYYMMDD format or None if not found
            datetime: Parsed date object or None if not found
        """
        import re
        date_match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
        if date_match:
            date_str = ''.join(date_match.groups())
            try:
                year = int(date_match.group(1))
                month = int(date_match.group(2))
                day = int(date_match.group(3))
                return date_str, datetime(year, month, day)
            except ValueError:
                self.logger.warning(f"Invalid date in filename {filename}: {date_match.groups()}")
                return date_str, None
        return None, None
    
    def process_all_zips(self, start_date=None, end_date=None, force_reprocess=False):
        """
        Process all ZIP files in the source directory within the specified date range.
        Also processes direct PDF files downloaded by the web scraper.
        
        Args:
            start_date (datetime): Optional start date for filtering files (inclusive)
            end_date (datetime): Optional end date for filtering files (inclusive)
            force_reprocess (bool): If True, reprocess files even if already processed
            
        Returns:
            dict: Processing statistics
        """
        self.logger.info(f"Processing ZIP files from {self.source_dir}")
        
        # Format date range for logging
        start_date_str = start_date.strftime('%Y-%m-%d') if start_date else "earliest"
        end_date_str = end_date.strftime('%Y-%m-%d') if end_date else "latest"
        self.logger.info(f"Date filter: {start_date_str} to {end_date_str}")
        
        # Reset statistics
        self.total_stats = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "pdf_extracted": 0,
            "direct_pdfs_processed": 0,
            "excel_retained": 0
        }
        self.monthly_stats = {}
        
        # Get ZIP files from WebScraperRegistry
        zip_files_df = self.webscraper_registry.get_files_by_type("zip", with_errors=False)
        
        # Get PDF files directly from WebScraperRegistry (for direct PDF handling)
        pdf_files_df = self.webscraper_registry.get_files_by_type("pdf", with_errors=False)
        
        # Filter by date if needed
        if start_date or end_date:
            if start_date:
                zip_files_df = zip_files_df[zip_files_df['file_date'] >= pd.Timestamp(start_date)]
                pdf_files_df = pdf_files_df[pdf_files_df['file_date'] >= pd.Timestamp(start_date)]
            if end_date:
                zip_files_df = zip_files_df[zip_files_df['file_date'] <= pd.Timestamp(end_date)]
                pdf_files_df = pdf_files_df[pdf_files_df['file_date'] <= pd.Timestamp(end_date)]
        
        # Filter out files with empty download paths
        zip_files_df = zip_files_df[zip_files_df['download_path'] != '']
        pdf_files_df = pdf_files_df[pdf_files_df['download_path'] != '']
        
        self.logger.info(f"Found {len(zip_files_df)} ZIP files to process")
        self.logger.info(f"Found {len(pdf_files_df)} direct PDF files to process")
        
        # Process each ZIP file
        for _, row in zip_files_df.iterrows():
            download_id = row['download_id']
            file_path = row['download_path']
            file_date = row['file_date']
            
            # Check if already processed in extractor registry unless force_reprocess is True
            existing_extractions = self.extractor_registry.get_extractions_by_download_id(download_id)
            if not existing_extractions.empty and not force_reprocess:
                # Check if any extraction is completed successfully
                if any(existing_extractions['extraction_complete'] & ~existing_extractions['expansion_errors']):
                    self.logger.info(f"Skipping {os.path.basename(file_path)} - already extracted successfully")
                    self.total_stats["skipped"] += 1
                    continue
                    
                # If all extractions failed, reprocess
                self.logger.info(f"Re-extracting {os.path.basename(file_path)} due to previous errors")
            
            # Process the ZIP file
            self._process_single_zip(download_id, file_path, file_date)
        
        # Process each direct PDF file
        for _, row in pdf_files_df.iterrows():
            download_id = row['download_id']
            file_path = row['download_path']
            file_date = row['file_date']
            
            # Check if already processed in extractor registry unless force_reprocess is True
            existing_extractions = self.extractor_registry.get_extractions_by_download_id(download_id)
            if not existing_extractions.empty and not force_reprocess:
                # Check if any extraction is completed successfully
                if any(existing_extractions['extraction_complete'] & ~existing_extractions['expansion_errors']):
                    self.logger.info(f"Skipping PDF {os.path.basename(file_path)} - already processed successfully")
                    self.total_stats["skipped"] += 1
                    continue
            
            # Process the PDF file directly
            self._process_single_pdf(download_id, file_path, file_date)
        
        # Export registries to CSV
        self.extractor_registry.export_to_csv()
        self.pdf_registry.export_to_csv()
        
        # Log statistics
        self._log_statistics()
        
        return self.total_stats
    
    def _process_single_pdf(self, download_id, pdf_path, file_date):
        """
        Process a single PDF file directly downloaded from web scraper.
        
        Args:
            download_id (str): Download ID from WebScraperRegistry
            pdf_path (str): Path to the PDF file
            file_date (datetime): Date associated with the file from WebScraperRegistry
        """
        pdf_path = Path(pdf_path)
        pdf_filename = pdf_path.name
        self.logger.info(f"Processing direct PDF: {pdf_filename}")
        
        # Extract date from filename
        date_from_filename, date_obj = self._get_date_from_filename(pdf_filename)
        
        # Use the date from filename if available, otherwise use the file_date from WebScraper
        filename_date = date_obj if date_obj is not None else file_date
        
        # Initialize month_key for statistics
        month_key = "unknown"
        if date_from_filename:
            try:
                year = date_from_filename[:4]
                month = date_from_filename[4:6]
                month_key = f"{year}-{month}"
            except:
                pass
        
        # Initialize statistics for this month if not exists
        if month_key not in self.monthly_stats:
            self.monthly_stats[month_key] = {
                "processed": 0,
                "success": 0,
                "failed": 0,
                "skipped": 0,
                "pdf_extracted": 0,
                "direct_pdfs_processed": 0,
                "excel_retained": 0
            }
        
        # Increment processed count
        self.total_stats["processed"] += 1
        self.monthly_stats[month_key]["processed"] += 1
        
        try:
            # Start timing
            start_time = time.time()
            
            # Verify it's actually a PDF
            if not self._is_pdf_file(pdf_path):
                raise ExtractorError(f"Not a valid PDF file: {pdf_filename}")
            
            # Destination path in PDF directory
            dest_path = self.pdf_dir / pdf_filename
            
            # Copy the PDF to the PDF directory
            shutil.copy2(pdf_path, dest_path)
            
            # Calculate extraction duration
            extraction_duration = time.time() - start_time
            
            # Register extraction in registry with the filename_date
            extraction_id = self.extractor_registry.register_extraction(
                download_id=download_id,
                file_date=filename_date,  # Use date from filename or from WebScraper
                zip_file_path=str(pdf_path),  # Original path
                extraction_dir_path=str(self.pdf_dir),
                extracted_files=[str(dest_path)],
                is_valid_zip=True,  # Not a ZIP but successful process
                duration_seconds=extraction_duration
            )
            
            # Register in PDF registry with the filename_date
            self.pdf_registry.register_pdf(
                extraction_id=extraction_id,
                file_path=str(dest_path),
                pdf_filename=pdf_filename,
                file_date=filename_date,  # Use same date as extraction
                date_expanded=datetime.now()
            )
            
            # Update statistics
            self.total_stats["success"] += 1
            self.total_stats["direct_pdfs_processed"] += 1
            
            self.monthly_stats[month_key]["success"] += 1
            self.monthly_stats[month_key]["direct_pdfs_processed"] += 1
            
            self.logger.info(f"Successfully processed direct PDF: {pdf_filename}")
            
        except Exception as e:
            # Register error in registry
            self.extractor_registry.register_extraction(
                download_id=download_id,
                file_date=filename_date,  # Use same date even for error cases
                zip_file_path=str(pdf_path),
                extraction_dir_path=str(self.pdf_dir),
                error=str(e),
                is_valid_zip=False,
                duration_seconds=0.0
            )
            
            # Update statistics
            self.total_stats["failed"] += 1
            self.monthly_stats[month_key]["failed"] += 1
            
            self.logger.error(f"Error processing PDF {pdf_filename}: {str(e)}")
            self.logger.debug("Error details:", exc_info=True)
    
    def _process_single_zip(self, download_id, zip_path, file_date):
        """
        Process a single ZIP file.
        
        Args:
            download_id (str): Download ID from WebScraperRegistry
            zip_path (str): Path to the ZIP file
            file_date (datetime): Date associated with the file from WebScraperRegistry
        """
        zip_path = Path(zip_path)
        zip_filename = zip_path.name
        self.logger.info(f"Processing: {zip_filename}")
        
        # Extract date from filename
        date_from_filename, date_obj = self._get_date_from_filename(zip_filename)
        
        # Use the date from filename if available, otherwise use the file_date from WebScraper
        filename_date = date_obj if date_obj is not None else file_date
        
        # Initialize month_key for statistics
        month_key = "unknown"
        if date_from_filename:
            try:
                year = date_from_filename[:4]
                month = date_from_filename[4:6]
                month_key = f"{year}-{month}"
            except:
                pass
        
        # Initialize statistics for this month if not exists
        if month_key not in self.monthly_stats:
            self.monthly_stats[month_key] = {
                "processed": 0,
                "success": 0,
                "failed": 0,
                "skipped": 0,
                "pdf_extracted": 0,
                "direct_pdfs_processed": 0,
                "excel_retained": 0
            }
        
        # Increment processed count
        self.total_stats["processed"] += 1
        self.monthly_stats[month_key]["processed"] += 1
        
        try:
            # Clean up temp directory
            self._clean_temp_directory()
            
            # Start timing
            start_time = time.time()
            
            # Process the ZIP file
            extracted_files = self._extract_zip(zip_path, date_from_filename)
            
            # Calculate extraction duration
            extraction_duration = time.time() - start_time
            
            # Register extraction in registry - pass the filename_date
            extraction_id = self.extractor_registry.register_extraction(
                download_id=download_id,
                file_date=filename_date,  # Use the date from filename or from WebScraper
                zip_file_path=str(zip_path),
                extraction_dir_path=str(self.pdf_dir),
                extracted_files=extracted_files['file_paths'],
                is_valid_zip=True,
                duration_seconds=extraction_duration
            )
            
            # Register each extracted PDF file in the PDF registry
            for pdf_path in extracted_files['pdf_paths']:
                pdf_filename = os.path.basename(pdf_path)
                self.pdf_registry.register_pdf(
                    extraction_id=extraction_id,
                    file_path=pdf_path,
                    pdf_filename=pdf_filename,
                    file_date=filename_date,  # Use the same date as extraction
                    date_expanded=datetime.now()
                )
            
            # Update statistics
            self.total_stats["success"] += 1
            self.total_stats["pdf_extracted"] += extracted_files['pdf']
            self.total_stats["excel_retained"] += extracted_files['excel']
            
            self.monthly_stats[month_key]["success"] += 1
            self.monthly_stats[month_key]["pdf_extracted"] += extracted_files['pdf']
            self.monthly_stats[month_key]["excel_retained"] += extracted_files['excel']
            
            self.logger.info(f"Successfully processed {zip_filename} - extracted {extracted_files['total']} files")
            
        except Exception as e:
            # Register error in registry
            self.extractor_registry.register_extraction(
                download_id=download_id,
                file_date=filename_date,  # Use the same date even for error cases
                zip_file_path=str(zip_path),
                extraction_dir_path=str(self.pdf_dir),
                error=str(e),
                is_valid_zip=False,
                duration_seconds=0.0
            )
            
            # Update statistics
            self.total_stats["failed"] += 1
            self.monthly_stats[month_key]["failed"] += 1
            
            self.logger.error(f"Error processing {zip_filename}: {str(e)}")
            self.logger.debug("Error details:", exc_info=True)
            
        except Exception as e:
            # Register error in registry
            self.extractor_registry.register_extraction(
                download_id=download_id,
                file_date=file_date,
                zip_file_path=str(zip_path),
                extraction_dir_path=str(self.pdf_dir),
                error=str(e),
                is_valid_zip=False,
                duration_seconds=0.0
            )
            
            # Update statistics
            self.total_stats["failed"] += 1
            self.monthly_stats[month_key]["failed"] += 1
            
            self.logger.error(f"Error processing {zip_filename}: {str(e)}")
            self.logger.debug("Error details:", exc_info=True)
    
    def _clean_temp_directory(self):
        """Clean up the temporary directory."""
        try:
            # Remove all files in temp directory
            for item in self.temp_dir.iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            self.logger.debug("Cleaned temporary directory")
        except Exception as e:
            self.logger.warning(f"Error cleaning temporary directory: {str(e)}")
    
    def _extract_zip(self, zip_path, date_prefix=None):
        """
        Extract files from a ZIP archive.
        
        Args:
            zip_path (Path): Path to the ZIP file
            date_prefix (str): Date string to prepend to extracted filenames
            
        Returns:
            dict: Statistics on extracted files
        """
        stats = {
            'total': 0, 
            'pdf': 0, 
            'excel': 0, 
            'other': 0,
            'file_paths': [],
            'pdf_paths': [],
            'excel_paths': []
        }
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Extract all files to temp directory
                zip_ref.extractall(self.temp_dir)
                
                # Add date prefix if available
                date_prefix_str = ""
                if date_prefix:
                    date_prefix_str = f"{date_prefix}-"
                
                # Process extracted files
                for item in self.temp_dir.glob('**/*'):
                    if item.is_file():
                        stats['total'] += 1

                        # Always prepend date prefix to filename
                        date_prefix_str = f"{date_prefix}-" if date_prefix else ""
                        new_filename = f"{date_prefix_str}{item.name}"

                        # Process based on file type
                        if item.suffix.lower() == '.pdf':
                            # Move to PDF directory
                            dest_path = self.pdf_dir / new_filename
                            shutil.move(item, dest_path)
                            stats['pdf'] += 1
                            stats['file_paths'].append(str(dest_path))
                            stats['pdf_paths'].append(str(dest_path))
                            self.logger.debug(f"Moved PDF to: {dest_path}")

                        elif item.suffix.lower() in ['.xlsx', '.xls']:
                            # Move Excel files back to source directory
                            dest_path = self.source_dir / new_filename
                            shutil.move(item, dest_path)
                            stats['excel'] += 1
                            stats['file_paths'].append(str(dest_path))
                            stats['excel_paths'].append(str(dest_path))
                            self.logger.debug(f"Moved Excel to: {dest_path}")

                        else:
                            # Move other files to temp root (flattening structure)
                            dest_path = self.temp_dir / new_filename
                            if item != dest_path:
                                shutil.move(item, dest_path)
                            stats['other'] += 1
                            stats['file_paths'].append(str(dest_path))
                            self.logger.debug(f"Moved other file to: {dest_path}")

                
                return stats
                
        except zipfile.BadZipFile:
            # If not a valid ZIP, check if it's a PDF that was incorrectly named
            if zip_path.suffix.lower() == '.pdf' or self._is_pdf_file(zip_path):
                # Just move it to PDF directory
                dest_filename = zip_path.name
                if date_prefix and not self._get_date_from_filename(dest_filename)[0]:
                    dest_filename = f"{date_prefix}-{dest_filename}"
                
                dest_path = self.pdf_dir / dest_filename
                shutil.copy2(zip_path, dest_path)
                
                stats = {
                    'total': 1, 
                    'pdf': 1, 
                    'excel': 0, 
                    'other': 0,
                    'file_paths': [str(dest_path)],
                    'pdf_paths': [str(dest_path)],
                    'excel_paths': []
                }
                self.logger.info(f"Found PDF file with .zip extension: {zip_path.name}")
                return stats
            else:
                raise ExtractorError(f"Not a valid ZIP file: {zip_path.name}")
    
    def _is_pdf_file(self, file_path):
        """
        Check if a file is a PDF by examining its header.
        
        Args:
            file_path (Path): Path to the file
            
        Returns:
            bool: True if file is a PDF, False otherwise
        """
        try:
            with open(file_path, 'rb') as f:
                header = f.read(5)
                return header == b'%PDF-'
        except:
            return False
    
    def _log_statistics(self):
        """Log processing statistics."""
        self.logger.info("\n===== ZIP Processing Statistics =====")
        
        # Log total statistics
        self.logger.info(f"\nTotal Statistics:")
        self.logger.info(f"Total files processed: {self.total_stats['processed']}")
        self.logger.info(f"Successfully processed: {self.total_stats['success']}")
        self.logger.info(f"Failed to process: {self.total_stats['failed']}")
        self.logger.info(f"Skipped due to date filter: {self.total_stats['skipped']}")
        self.logger.info(f"PDF files extracted: {self.total_stats['pdf_extracted']}")
        self.logger.info(f"Direct PDF files processed: {self.total_stats['direct_pdfs_processed']}")
        self.logger.info(f"Excel files retained: {self.total_stats['excel_retained']}")
        
        if self.total_stats['processed'] > 0:
            success_rate = (self.total_stats['success'] / self.total_stats['processed']) * 100
            self.logger.info(f"Overall success rate: {success_rate:.2f}%")
        
        # Log statistics by month
        if self.monthly_stats:
            self.logger.info(f"\nStatistics by Month:")
            for month, stats in sorted(self.monthly_stats.items()):
                if month == "unknown":
                    continue
                    
                self.logger.info(f"\n{month}:")
                self.logger.info(f"  Files processed: {stats['processed']}")
                self.logger.info(f"  Successfully processed: {stats['success']}")
                self.logger.info(f"  Failed to process: {stats['failed']}")
                self.logger.info(f"  Skipped: {stats.get('skipped', 0)}")
                self.logger.info(f"  PDF files extracted: {stats['pdf_extracted']}")
                self.logger.info(f"  Direct PDF files processed: {stats['direct_pdfs_processed']}")
                self.logger.info(f"  Excel files retained: {stats['excel_retained']}")
                
                if stats['processed'] > 0:
                    success_rate = (stats['success'] / stats['processed']) * 100
                    self.logger.info(f"  Success rate: {success_rate:.2f}%")