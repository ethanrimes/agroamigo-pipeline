"""
Process and extract zip files for the Colombian Agricultural Price Data Pipeline.
"""

import os
import zipfile
import logging
import shutil
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from agroamigo_pipeline.config.settings import (
    SOURCE_DIR, PDF_DIR, TEMP_DIR, REGISTRY_DIR,
    EXTRACTOR_LOG_DIR
)
from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.utils.date_utils import parse_spanish_date, filter_files_by_date
from agroamigo_pipeline.utils.exceptions import ExtractorError

class ZipExtractor:
    """Extract and process ZIP files containing PDFs and other documents."""
    
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
            
        # Set up error tracking
        self.registry_dir = Path(REGISTRY_DIR)
        self.registry_path = self.registry_dir / 'zip_registry.parquet'
        self.csv_path = self.registry_dir / 'zip_summary.csv'
        
        # Create error log directory
        self.log_dir = Path(EXTRACTOR_LOG_DIR)
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Statistics tracking
        self.total_stats = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "pdf_extracted": 0,
            "excel_retained": 0
        }
        
        # Monthly statistics
        self.monthly_stats = {}
        
        # Initialize registry
        self._init_registry()
        
        self.logger.info("ZipExtractor initialized")
        self.logger.debug(f"Source directory: {self.source_dir}")
        self.logger.debug(f"PDF directory: {self.pdf_dir}")
        self.logger.debug(f"Temp directory: {self.temp_dir}")
    
    def _init_registry(self):
        """Initialize the registry schema and file."""
        # Define schema for the registry
        self.registry_schema = pa.schema([
            ('filename', pa.string()),  # ZIP filename
            ('source_path', pa.string()),  # Full source path
            ('date_processed', pa.timestamp('ns')),  # When processed
            ('date_from_filename', pa.string()),  # Date extracted from filename (YYYYMMDD)
            ('status', pa.string()),  # 'success', 'error', 'skipped', etc.
            ('error_occurred', pa.bool_()),  # Whether an error occurred
            ('error_message', pa.string()),  # Error message if any
            ('destination_path', pa.string()),  # Where file was saved (if success)
            ('files_extracted', pa.int32()),  # Number of files extracted
            ('pdf_count', pa.int32()),  # Number of PDFs extracted
            ('excel_count', pa.int32()),  # Number of Excel files retained
            ('month_key', pa.string())  # Month key for grouping (YYYY-MM)
        ])
        
        # Create registry directory if it doesn't exist
        os.makedirs(self.registry_dir, exist_ok=True)
        
        # Create empty registry if it doesn't exist
        if not os.path.exists(self.registry_path):
            self._create_empty_registry()
    
    def _create_empty_registry(self):
        """Create an empty Parquet registry file with the schema."""
        # Create empty DataFrame with proper column types
        empty_df = pd.DataFrame({
            'filename': pd.Series([], dtype='string'),
            'source_path': pd.Series([], dtype='string'),
            'date_processed': pd.Series([], dtype='datetime64[ns]'),
            'date_from_filename': pd.Series([], dtype='string'),
            'status': pd.Series([], dtype='string'),
            'error_occurred': pd.Series([], dtype='bool'),
            'error_message': pd.Series([], dtype='string'),
            'destination_path': pd.Series([], dtype='string'),
            'files_extracted': pd.Series([], dtype='int32'),
            'pdf_count': pd.Series([], dtype='int32'),
            'excel_count': pd.Series([], dtype='int32'),
            'month_key': pd.Series([], dtype='string')
        })
        
        # Convert to Arrow table and write to Parquet
        table = pa.Table.from_pandas(empty_df, schema=self.registry_schema)
        pq.write_table(table, self.registry_path)
        
        self.logger.info(f"Created empty registry at {self.registry_path}")
    
    def _read_registry(self):
        """Read the registry from Parquet file."""
        if not os.path.exists(self.registry_path):
            self._create_empty_registry()
            return pd.DataFrame(columns=self.registry_schema.names)
        
        try:
            return pd.read_parquet(self.registry_path)
        except Exception as e:
            self.logger.error(f"Error reading registry: {str(e)}")
            self._create_empty_registry()
            return pd.DataFrame(columns=self.registry_schema.names)
    
    def _write_registry(self, df):
        """Write the registry DataFrame to Parquet file."""
        # Ensure all required columns exist with correct types
        for col in self.registry_schema.names:
            if col not in df.columns:
                # Add missing columns with appropriate default values
                if 'int' in str(self.registry_schema.field(col).type):
                    df[col] = 0
                elif 'bool' in str(self.registry_schema.field(col).type):
                    df[col] = False
                elif 'timestamp' in str(self.registry_schema.field(col).type):
                    df[col] = pd.NaT
                else:
                    df[col] = ''
        
        # Convert timestamp columns to correct format
        for col in ['date_processed']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Ensure integer columns are correctly typed
        for col in ['files_extracted', 'pdf_count', 'excel_count']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int32')
        
        # Ensure we have only the defined columns in the correct order
        df = df[self.registry_schema.names]
        
        # Write to Parquet
        table = pa.Table.from_pandas(df, schema=self.registry_schema)
        pq.write_table(table, self.registry_path)
    
    def export_to_csv(self):
        """Export the registry data to CSV."""
        df = self._read_registry()
        if not df.empty:
            # Format dates for CSV
            export_df = df.copy()
            
            # Convert timestamps to string format for readability
            if 'date_processed' in export_df.columns:
                export_df['date_processed'] = export_df['date_processed'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            export_df.to_csv(self.csv_path, index=False)
            self.logger.info(f"Exported ZIP extractor registry to CSV: {self.csv_path}")
            return self.csv_path
        else:
            self.logger.warning("No ZIP extractor data to export to CSV")
            return None
    
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
        
        # Get list of ZIP files
        zip_files = list(self.source_dir.glob('*.zip'))
        self.logger.info(f"Found {len(zip_files)} ZIP files")
        
        # Reset statistics
        self.total_stats = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "pdf_extracted": 0,
            "excel_retained": 0
        }
        self.monthly_stats = {}
        
        # Filter files by date using our date_utils function
        filtered_zip_files = []
        
        for zip_path in zip_files:
            # Extract date from filename
            date_str, file_date = self._get_date_from_filename(zip_path.name)
            
            # Skip files without a valid date if date filtering is enabled
            if (start_date or end_date) and not file_date:
                self.logger.debug(f"Skipping {zip_path.name}: No valid date in filename")
                continue
                
            # Check if file is within date range
            if start_date and file_date and file_date < start_date:
                self.logger.debug(f"Skipping {zip_path.name}: Date {file_date.strftime('%Y-%m-%d')} is before start date {start_date_str}")
                self.total_stats["skipped"] += 1
                continue
                
            if end_date and file_date and file_date > end_date:
                self.logger.debug(f"Skipping {zip_path.name}: Date {file_date.strftime('%Y-%m-%d')} is after end date {end_date_str}")
                self.total_stats["skipped"] += 1
                continue
                
            # File is within date range, add to filtered list
            filtered_zip_files.append(zip_path)
            self.logger.debug(f"Included file in date range: {zip_path.name}")
        
        # Log filtered files count
        self.logger.info(f"After date filtering: {len(filtered_zip_files)} ZIP files to process, {self.total_stats['skipped']} skipped")
        
        # Process each filtered ZIP file
        for zip_path in filtered_zip_files:
            self._process_single_zip(zip_path, force_reprocess)
        
        # Export to CSV
        self.export_to_csv()
        
        # Log statistics
        self._log_statistics()
        
        return self.total_stats
    
    def _process_single_zip(self, zip_path, force_reprocess=False):
        """
        Process a single ZIP file.
        
        Args:
            zip_path (Path): Path to the ZIP file
            force_reprocess (bool): If True, reprocess files even if already processed
        """
        zip_filename = zip_path.name
        self.logger.info(f"Processing: {zip_filename}")
        
        # Check if already processed and not forcing reprocess
        registry_df = self._read_registry()
        existing_entry = registry_df[registry_df['filename'] == zip_filename]
        if not existing_entry.empty and not force_reprocess:
            if not existing_entry.iloc[0]['error_occurred']:
                self.logger.info(f"Skipping {zip_filename} - already processed successfully")
                return
            else:
                self.logger.info(f"Reprocessing {zip_filename} due to previous error")
        
        # Extract date from filename
        date_from_filename, file_date = self._get_date_from_filename(zip_filename)
        
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
                "excel_retained": 0
            }
        
        # Increment processed count
        self.total_stats["processed"] += 1
        self.monthly_stats[month_key]["processed"] += 1
        
        # Initialize record
        record = {
            'filename': zip_filename,
            'source_path': str(zip_path),
            'date_processed': datetime.now(),
            'date_from_filename': date_from_filename or '',
            'status': 'processing',
            'error_occurred': False,
            'error_message': '',
            'destination_path': '',
            'files_extracted': 0,
            'pdf_count': 0,
            'excel_count': 0,
            'month_key': month_key
        }
        
        try:
            # Clean up temp directory
            self._clean_temp_directory()
            
            # Process the ZIP file
            extracted_files = self._extract_zip(zip_path, date_from_filename)
            
            # Update record with success
            record['status'] = 'success'
            record['files_extracted'] = extracted_files['total']
            record['pdf_count'] = extracted_files['pdf']
            record['excel_count'] = extracted_files['excel']
            record['destination_path'] = str(self.pdf_dir)
            
            # Update statistics
            self.total_stats["success"] += 1
            self.total_stats["pdf_extracted"] += extracted_files['pdf']
            self.total_stats["excel_retained"] += extracted_files['excel']
            
            self.monthly_stats[month_key]["success"] += 1
            self.monthly_stats[month_key]["pdf_extracted"] += extracted_files['pdf']
            self.monthly_stats[month_key]["excel_retained"] += extracted_files['excel']
            
            self.logger.info(f"Successfully processed {zip_filename} - extracted {extracted_files['total']} files")
            
        except Exception as e:
            # Update record with error
            record['status'] = 'error'
            record['error_occurred'] = True
            record['error_message'] = str(e)
            
            # Update statistics
            self.total_stats["failed"] += 1
            self.monthly_stats[month_key]["failed"] += 1
            
            self.logger.error(f"Error processing {zip_filename}: {str(e)}")
            self.logger.debug("Error details:", exc_info=True)
        
        # Update registry with this record
        self._update_registry(record)
    
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
        stats = {'total': 0, 'pdf': 0, 'excel': 0, 'other': 0}
        
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
                            self.logger.debug(f"Moved PDF to: {dest_path}")

                        elif item.suffix.lower() in ['.xlsx', '.xls']:
                            # Move Excel files back to source directory
                            dest_path = self.source_dir / new_filename
                            shutil.move(item, dest_path)
                            stats['excel'] += 1
                            self.logger.debug(f"Moved Excel to: {dest_path}")

                        else:
                            # Move other files to temp root (flattening structure)
                            dest_path = self.temp_dir / new_filename
                            if item != dest_path:
                                shutil.move(item, dest_path)
                            stats['other'] += 1
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
                
                stats = {'total': 1, 'pdf': 1, 'excel': 0, 'other': 0}
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
    
    def _update_registry(self, record):
        """
        Update the registry with a new record.
        
        Args:
            record (dict): Record to add to registry
        """
        registry_df = self._read_registry()
        
        # Remove existing entry if any
        filename = record['filename']
        registry_df = registry_df[registry_df['filename'] != filename]
        
        # Add new record
        new_df = pd.DataFrame([record])
        updated_df = pd.concat([registry_df, new_df], ignore_index=True)
        
        # Write back to registry
        self._write_registry(updated_df)
    
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
                self.logger.info(f"  Excel files retained: {stats['excel_retained']}")
                
                if stats['processed'] > 0:
                    success_rate = (stats['success'] / stats['processed']) * 100
                    self.logger.info(f"  Success rate: {success_rate:.2f}%")
                    
        # Write the detailed summary to a log file
        log_path = self.log_dir / f"zip_extraction_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        try:
            with open(log_path, 'w') as f:
                f.write("=== ZIP EXTRACTION SUMMARY ===\n\n")
                f.write(f"Total files processed: {self.total_stats['processed']}\n")
                f.write(f"Successfully processed: {self.total_stats['success']}\n")
                f.write(f"Failed to process: {self.total_stats['failed']}\n")
                f.write(f"Skipped due to date filter: {self.total_stats['skipped']}\n\n")
                
                f.write("=== MONTHLY BREAKDOWN ===\n")
                for month, stats in sorted(self.monthly_stats.items()):
                    if month == "unknown":
                        continue
                    f.write(f"\n{month}:\n")
                    f.write(f"  Files processed: {stats['processed']}\n")
                    f.write(f"  Successfully processed: {stats['success']}\n")
                    f.write(f"  Failed to process: {stats['failed']}\n")
                    f.write(f"  PDF files extracted: {stats['pdf_extracted']}\n")
                    f.write(f"  Excel files retained: {stats['excel_retained']}\n")
                    
                    if stats['processed'] > 0:
                        success_rate = (stats['success'] / stats['processed']) * 100
                        f.write(f"  Success rate: {success_rate:.2f}%\n")
            
            self.logger.info(f"Detailed statistics written to {log_path}")
        except Exception as e:
            self.logger.warning(f"Failed to write detailed statistics to log file: {str(e)}")