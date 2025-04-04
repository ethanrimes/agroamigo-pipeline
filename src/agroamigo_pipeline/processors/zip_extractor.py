import os
import zipfile
import shutil
import re
from pathlib import Path
import logging
from datetime import datetime

from agroamigo_pipeline.config.settings import SOURCE_DIR, PDF_DIR, TEMP_DIR
from agroamigo_pipeline.utils.logger import setup_logging, log_function_call
from agroamigo_pipeline.utils.path_manager import ensure_directory, get_files_by_extension
from agroamigo_pipeline.utils.file_registry import FileRegistry

class ZipExtractor:
    """
    Extract files from ZIP archives and organize them based on type.
    """
    
    def __init__(self, source_dir=None, pdf_dir=None, temp_dir=None, logger=None):
        """
        Initialize the ZIP extractor.
        
        Args:
            source_dir (str): Directory containing source files
            pdf_dir (str): Directory to save extracted PDF files
            temp_dir (str): Temporary directory for extraction
            logger (Logger): Logger instance
        """
        # Set up logger if not provided
        if logger is None:
            logging_setup = setup_logging()
            self.logger = logging_setup['file_extractor']
        else:
            self.logger = logger
            
        # Use provided paths or defaults from settings
        self.source_dir = source_dir or SOURCE_DIR
        self.pdf_dir = pdf_dir or PDF_DIR
        self.temp_dir = temp_dir or TEMP_DIR
        
        # Ensure directories exist
        ensure_directory(self.source_dir)
        ensure_directory(self.pdf_dir)
        ensure_directory(self.temp_dir)
        
        # Initialize statistics dictionary
        self.stats = {
            'total_files': 0,
            'zip_files': 0,
            'excel_files': 0,
            'pdf_files': 0,
            'other_files': 0,
            'extracted_files': 0,
            'failed_files': 0,
            'warnings': 0
        }
        
        # Initialize file registry
        self.registry = FileRegistry()
        
        self.logger.info("ZipExtractor initialized")
        self.logger.debug(f"Using source_dir: {self.source_dir}")
        self.logger.debug(f"Using pdf_dir: {self.pdf_dir}")
        self.logger.debug(f"Using temp_dir: {self.temp_dir}")
    
    def _get_output_filename(self, zip_filename, internal_filename):
        """
        Generate an output filename by combining ZIP filename and internal filename.
        
        Args:
            zip_filename (str): Name of the ZIP file
            internal_filename (str): Name of the file inside the ZIP
            
        Returns:
            str: Combined filename
        """
        # Get base names without extension
        zip_base = Path(zip_filename).stem
        internal_base = Path(internal_filename).name
        
        # Combine names
        return f"{zip_base}_{internal_base}"
    
    def process_zip_file(self, zip_path):
        """
        Process a single ZIP file, extracting contents based on file type.
        
        Args:
            zip_path (str): Path to the ZIP file
            
        Returns:
            dict: Statistics about the extraction process
        """
        zip_filename = os.path.basename(zip_path)
        self.logger.info(f"Processing ZIP file: {zip_filename}")
        
        # Check if file has already been processed
        if self.registry.file_processed(zip_path):
            self.logger.info(f"File already processed: {zip_filename}")
            return self.registry.get_file_info(zip_path)
        
        stats = {
            'files_extracted': 0,
            'excel_files': 0,
            'pdf_files': 0,
            'other_files': 0,
            'errors': [],
            'warnings': []
        }
        
        try:
            # Create temporary extraction directory
            temp_extract_dir = os.path.join(self.temp_dir, f"extract_{Path(zip_filename).stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}")
            os.makedirs(temp_extract_dir, exist_ok=True)
            self.logger.debug(f"Created temporary extraction directory: {temp_extract_dir}")
            
            # Extract all files to temporary directory
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_extract_dir)
                self.logger.debug(f"Extracted {len(zip_ref.namelist())} files from {zip_filename}")
            
            # Process extracted files
            for root, dirs, files in os.walk(temp_extract_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    file_ext = os.path.splitext(file)[1].lower()
                    
                    # Generate output filename
                    output_filename = self._get_output_filename(zip_filename, file_path.replace(temp_extract_dir + os.sep, ''))
                    
                    try:
                        # Route files based on extension
                        if file_ext in ['.pdf']:
                            # Move PDFs to PDF directory
                            pdf_output_path = os.path.join(self.pdf_dir, output_filename)
                            shutil.copy2(file_path, pdf_output_path)
                            self.logger.debug(f"Copied PDF to: {pdf_output_path}")
                            stats['pdf_files'] += 1
                            stats['files_extracted'] += 1
                            
                        elif file_ext in ['.xls', '.xlsx']:
                            # Keep Excel files in source directory
                            excel_output_path = os.path.join(self.source_dir, output_filename)
                            shutil.copy2(file_path, excel_output_path)
                            self.logger.debug(f"Copied Excel to: {excel_output_path}")
                            stats['excel_files'] += 1
                            stats['files_extracted'] += 1
                            
                        else:
                            # Log other file types
                            self.logger.debug(f"Skipping unsupported file type: {file} ({file_ext})")
                            stats['other_files'] += 1
                            stats['warnings'].append(f"Unsupported file type: {file_ext}")
                            
                    except Exception as e:
                        error_msg = f"Error processing {file}: {str(e)}"
                        self.logger.error(error_msg)
                        stats['errors'].append(error_msg)
            
            # Clean up temporary directory
            shutil.rmtree(temp_extract_dir)
            self.logger.debug(f"Removed temporary extraction directory: {temp_extract_dir}")
            
            # Update overall statistics
            self.stats['zip_files'] += 1
            self.stats['extracted_files'] += stats['files_extracted']
            self.stats['excel_files'] += stats['excel_files']
            self.stats['pdf_files'] += stats['pdf_files']
            self.stats['other_files'] += stats['other_files']
            self.stats['warnings'] += len(stats['warnings'])
            
            if stats['errors']:
                self.stats['failed_files'] += 1
            
            # Log extraction summary
            self.logger.info(f"Extracted {stats['files_extracted']} files from {zip_filename} "
                           f"({stats['excel_files']} Excel, {stats['pdf_files']} PDF, "
                           f"{stats['other_files']} other)")
            
            if stats['errors']:
                self.logger.warning(f"{len(stats['errors'])} errors occurred during extraction")
            
            # Register processed file
            self.registry.register_file(
                zip_path, 
                status="processed" if not stats['errors'] else "error",
                error=stats['errors'] if stats['errors'] else None,
                stats=stats
            )
            
            return stats
            
        except Exception as e:
            error_msg = f"Failed to extract ZIP file {zip_filename}: {str(e)}"
            self.logger.error(error_msg)
            stats['errors'].append(error_msg)
            
            self.stats['failed_files'] += 1
            
            # Register failed file
            self.registry.register_file(
                zip_path, 
                status="error",
                error=stats['errors'],
                stats=stats
            )
            
            return stats
    
    def process_non_zip_file(self, file_path):
        """
        Process a non-ZIP file, routing it based on extension.
        
        Args:
            file_path (str): Path to the file
            
        Returns:
            dict: Statistics about the processing
        """
        filename = os.path.basename(file_path)
        file_ext = os.path.splitext(filename)[1].lower()
        
        self.logger.info(f"Processing non-ZIP file: {filename}")
        
        # Check if file has already been processed
        if self.registry.file_processed(file_path):
            self.logger.info(f"File already processed: {filename}")
            return self.registry.get_file_info(file_path)
        
        stats = {
            'file_type': file_ext,
            'moved': False,
            'errors': [],
            'warnings': []
        }
        
        try:
            if file_ext in ['.pdf']:
                # Move PDF to PDF directory
                output_path = os.path.join(self.pdf_dir, filename)
                shutil.copy2(file_path, output_path)
                self.logger.debug(f"Copied PDF to: {output_path}")
                stats['moved'] = True
                self.stats['pdf_files'] += 1
                
            elif file_ext in ['.xls', '.xlsx']:
                # Keep Excel files in source directory
                self.logger.debug(f"Keeping Excel file in source directory: {filename}")
                self.stats['excel_files'] += 1
                
            else:
                # Log other file types
                self.logger.debug(f"Skipping unsupported file type: {filename} ({file_ext})")
                stats['warnings'].append(f"Unsupported file type: {file_ext}")
                self.stats['other_files'] += 1
            
            # Update overall statistics
            self.stats['total_files'] += 1
            
            # Register processed file
            self.registry.register_file(
                file_path, 
                status="processed",
                stats=stats
            )
            
            return stats
            
        except Exception as e:
            error_msg = f"Failed to process file {filename}: {str(e)}"
            self.logger.error(error_msg)
            stats['errors'].append(error_msg)
            
            self.stats['failed_files'] += 1
            
            # Register failed file
            self.registry.register_file(
                file_path, 
                status="error",
                error=stats['errors'],
                stats=stats
            )
            
            return stats
    
    def process_zip_files(self, source_dir=None, force_reprocess=False):
        """
        Process all ZIP files in the source directory.
        
        Args:
            source_dir (str): Directory containing source files (default: self.source_dir)
            force_reprocess (bool): If True, reprocess files even if they've been processed before
            
        Returns:
            dict: Statistics about the extraction process
        """
        source_dir = source_dir or self.source_dir
        self.logger.info(f"Processing ZIP files in {source_dir}")
        
        # Reset statistics
        self.stats = {
            'total_files': 0,
            'zip_files': 0,
            'excel_files': 0,
            'pdf_files': 0,
            'other_files': 0,
            'extracted_files': 0,
            'failed_files': 0,
            'warnings': 0
        }
        
        # Get all files in source directory
        all_files = [os.path.join(source_dir, f) for f in os.listdir(source_dir) 
                    if os.path.isfile(os.path.join(source_dir, f))]
        
        self.logger.info(f"Found {len(all_files)} files in source directory")
        
        # Process each file
        for file_path in all_files:
            filename = os.path.basename(file_path)
            file_ext = os.path.splitext(filename)[1].lower()
            
            # Skip already processed files unless force_reprocess is True
            if not force_reprocess and self.registry.file_processed(file_path):
                self.logger.debug(f"Skipping already processed file: {filename}")
                continue
            
            if file_ext == '.zip':
                self.process_zip_file(file_path)
            else:
                self.process_non_zip_file(file_path)
        
        # Log summary
        self.logger.info(f"Processed {self.stats['total_files']} files "
                       f"({self.stats['zip_files']} ZIP, {self.stats['excel_files']} Excel, "
                       f"{self.stats['pdf_files']} PDF, {self.stats['other_files']} other)")
        self.logger.info(f"Extracted {self.stats['extracted_files']} files from ZIP archives")
        self.logger.info(f"{self.stats['failed_files']} files failed, {self.stats['warnings']} warnings")
        
        return self.stats