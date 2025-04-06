"""
Registry for tracking ZIP file extractions.
Updated to include error_occurred and timestamp columns.
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import logging
import uuid

from agroamigo_pipeline.registry.base_registry import BaseRegistry

class ZipExtractorRegistry(BaseRegistry):
    """Registry to track ZIP file extractions."""
    
    def __init__(self, logger=None):
        """Initialize the extractor registry."""
        # Define schema for extractor registry
        schema = pa.schema([
            ('extraction_id', pa.string()),  # Primary key
            ('download_id', pa.string()),    # Foreign key to WebScraperRegistry
            ('file_date', pa.timestamp('ns')),
            ('zip_file_path', pa.string()),
            ('extraction_dir_path', pa.string()),
            ('extracted_files', pa.list_(pa.string())),
            ('extraction_complete', pa.bool_()),
            ('date_expanded', pa.timestamp('ns')),
            ('is_valid_zip', pa.bool_()),
            ('extraction_duration_seconds', pa.float64()),
            ('num_contained_files', pa.int32()),
            ('expansion_errors', pa.bool_()),  # Keep for backward compatibility
            ('error_message', pa.string()),
            ('error_occurred', pa.bool_()),    # New column that replaces expansion_errors
            ('timestamp', pa.timestamp('ns'))  # Timestamp for when extraction was performed
        ])
        
        # Initialize base registry
        super().__init__("extractor", schema, logger)
        self.logger.debug("ZipExtractorRegistry initialized")
    
    def register_extraction(self, download_id, file_date, zip_file_path, extraction_dir_path,
                          extracted_files=None, is_valid_zip=False, duration_seconds=0.0,
                          error=None):
        """
        Register a ZIP file extraction.
        
        Args:
            download_id (str): Download ID from WebScraperRegistry
            file_date (datetime): Date associated with the file
            zip_file_path (str): Path to the ZIP file
            extraction_dir_path (str): Directory where files were extracted
            extracted_files (list): List of paths to extracted files
            is_valid_zip (bool): Whether the ZIP file was valid
            duration_seconds (float): Time taken for extraction in seconds
            error (str): Error message if extraction failed
            
        Returns:
            str: Extraction ID
        """
        df = self.read()
        
        # Generate new extraction ID
        extraction_id = str(uuid.uuid4())
        
        # Ensure extracted_files is a list
        if extracted_files is None:
            extracted_files = []
        
        # Count contained files
        num_contained_files = len(extracted_files)
        
        # Set the current time
        current_time = datetime.now()
        
        # Create a new record
        new_record = {
            'extraction_id': extraction_id,
            'download_id': download_id,
            'file_date': file_date if isinstance(file_date, pd.Timestamp) else pd.Timestamp(file_date),
            'zip_file_path': zip_file_path,
            'extraction_dir_path': extraction_dir_path,
            'extracted_files': extracted_files,
            'extraction_complete': error is None,
            'date_expanded': current_time,
            'is_valid_zip': is_valid_zip,
            'extraction_duration_seconds': duration_seconds,
            'num_contained_files': num_contained_files,
            'expansion_errors': error is not None,  # Keep for backward compatibility
            'error_message': error or '',
            'error_occurred': error is not None,    # New field
            'timestamp': current_time              # New field
        }
        
        # Append new record
        new_df = pd.DataFrame([new_record])
        updated_df = pd.concat([df, new_df], ignore_index=True)
        
        # Write updated DataFrame
        self.write(updated_df)
        self.logger.debug(f"Registered extraction for {os.path.basename(zip_file_path)}")
        
        return extraction_id
    
    def get_extractions_by_download_id(self, download_id):
        """
        Get extractions by download ID.
        
        Args:
            download_id (str): Download ID from WebScraperRegistry
            
        Returns:
            pd.DataFrame: Matching extraction records
        """
        df = self.read()
        return df[df['download_id'] == download_id]
    
    def get_extraction_by_id(self, extraction_id):
        """
        Get extraction record by extraction ID.
        
        Args:
            extraction_id (str): Extraction ID
            
        Returns:
            dict: Extraction record or None if not found
        """
        df = self.read()
        matches = df[df['extraction_id'] == extraction_id]
        
        if matches.empty:
            return None
        
        # Convert to dictionary
        return matches.iloc[0].to_dict()
    
    def get_extractions_by_date_range(self, start_date=None, end_date=None):
        """
        Get extractions within a date range.
        
        Args:
            start_date (datetime): Start date (inclusive)
            end_date (datetime): End date (inclusive)
            
        Returns:
            pd.DataFrame: Extractions within the date range
        """
        df = self.read()
        
        # Apply date filters if specified
        if start_date:
            df = df[df['file_date'] >= pd.Timestamp(start_date)]
        
        if end_date:
            df = df[df['file_date'] <= pd.Timestamp(end_date)]
        
        return df
    
    def update_extraction_status(self, extraction_id, status, error=None):
        """
        Update the status of an extraction.
        
        Args:
            extraction_id (str): Extraction ID
            status (bool): Whether extraction is complete
            error (str): Error message if status is False
            
        Returns:
            bool: True if successful, False if extraction not found
        """
        df = self.read()
        
        # Find the extraction
        extraction_idx = df.index[df['extraction_id'] == extraction_id].tolist()
        
        if not extraction_idx:
            self.logger.warning(f"Extraction with ID {extraction_id} not found in registry")
            return False
        
        # Update status
        df.at[extraction_idx[0], 'extraction_complete'] = status
        
        # Update error fields if applicable
        if not status and error:
            df.at[extraction_idx[0], 'expansion_errors'] = True    # Keep for backward compatibility
            df.at[extraction_idx[0], 'error_occurred'] = True      # New field
            df.at[extraction_idx[0], 'error_message'] = error
            df.at[extraction_idx[0], 'timestamp'] = datetime.now()  # Update timestamp
        
        # Write updated DataFrame
        self.write(df)
        self.logger.debug(f"Updated extraction {extraction_id} status to {status}")
        
        return True
    
    def get_statistics(self):
        """
        Get extraction statistics.
        
        Returns:
            dict: Dictionary of statistics
        """
        df = self.read()
        
        if df.empty:
            return {
                'total_extractions': 0,
                'successful_extractions': 0,
                'failed_extractions': 0,
                'total_files_extracted': 0,
                'avg_files_per_extraction': 0,
                'avg_extraction_time': 0,
                'error_rate': 0
            }
        
        # Calculate statistics
        total = len(df)
        successful = df['extraction_complete'].sum()
        failed = total - successful
        total_files = df['num_contained_files'].sum()
        
        # Calculate averages
        avg_files = total_files / successful if successful > 0 else 0
        avg_time = df[df['extraction_complete']]['extraction_duration_seconds'].mean()
        error_rate = (failed / total * 100) if total > 0 else 0
        
        return {
            'total_extractions': total,
            'successful_extractions': successful,
            'failed_extractions': failed,
            'total_files_extracted': total_files,
            'avg_files_per_extraction': avg_files,
            'avg_extraction_time': avg_time,
            'error_rate': error_rate
        }
    
    def get_pending_extractions(self):
        """
        Get extractions that have not been completed.
        
        Returns:
            pd.DataFrame: Pending extraction records
        """
        df = self.read()
        return df[~df['extraction_complete']]
    
    def get_successful_extractions(self):
        """
        Get successful extractions.
        
        Returns:
            pd.DataFrame: Successful extraction records
        """
        df = self.read()
        return df[df['extraction_complete'] & ~df['error_occurred']]