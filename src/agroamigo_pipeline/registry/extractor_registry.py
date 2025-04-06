"""
Registry for tracking ZIP file extractions.
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
        """Initialize the ZIP extractor registry."""
        # Define schema for ZIP extractor registry
        schema = pa.schema([
            ('extraction_id', pa.string()),  # Primary key
            ('download_id', pa.string()),  # Foreign key to WebScraperRegistry
            ('file_date', pa.timestamp('ns')),
            ('zip_file_path', pa.string()),
            ('extraction_dir_path', pa.string()),
            ('extracted_file_paths', pa.list_(pa.string())),
            ('last_expanded', pa.timestamp('ns')),
            ('extraction_duration_seconds', pa.float64()),
            ('expansion_errors', pa.bool_()),
            ('expansion_error_messages', pa.string()),
            ('num_contained_files', pa.int32()),
            ('is_valid_zip', pa.bool_()),
            ('extraction_complete', pa.bool_())
        ])
        
        # Initialize base registry
        super().__init__("extractor", schema, logger)
        self.logger.debug("ZipExtractorRegistry initialized")
    
    def get_extraction_by_id(self, extraction_id):
        """
        Get an extraction record by its ID.
        
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
    
    def get_extractions_by_download_id(self, download_id):
        """
        Get extraction records by download ID.
        
        Args:
            download_id (str): Download ID from WebScraperRegistry
            
        Returns:
            pd.DataFrame: Matching extraction records
        """
        df = self.read()
        return df[df['download_id'] == download_id]
    
    def get_extractions_by_path(self, zip_file_path):
        """
        Get extraction records by ZIP file path.
        
        Args:
            zip_file_path (str): Path to the ZIP file
            
        Returns:
            pd.DataFrame: Matching extraction records
        """
        df = self.read()
        return df[df['zip_file_path'] == zip_file_path]
    
    def register_extraction(self, download_id, file_date, zip_file_path, extraction_dir_path, 
                          extracted_files=None, error=None, is_valid_zip=True, duration_seconds=0.0):
        """
        Register a ZIP file extraction.
        
        Args:
            download_id (str): Download ID from WebScraperRegistry
            file_date (datetime): Date associated with the file
            zip_file_path (str): Path to the ZIP file
            extraction_dir_path (str): Directory where files were extracted
            extracted_files (list, optional): List of paths to extracted files
            error (str, optional): Error message if extraction failed
            is_valid_zip (bool): Whether the file is a valid ZIP
            duration_seconds (float): Duration of extraction in seconds
            
        Returns:
            str: Extraction ID
        """
        df = self.read()
        
        current_time = datetime.now()
        
        # Check if file has already been extracted
        existing_extractions = df[df['zip_file_path'] == zip_file_path]
        
        if not existing_extractions.empty:
            # Get the existing extraction_id
            extraction_id = existing_extractions.iloc[0]['extraction_id']
            
            # Remove old record(s)
            df = df[df['zip_file_path'] != zip_file_path]
        else:
            # Generate a new extraction_id
            extraction_id = str(uuid.uuid4())
        
        # Ensure extracted_files is a list
        if extracted_files is None:
            extracted_files = []
        
        # Create a new record
        new_record = {
            'extraction_id': extraction_id,
            'download_id': download_id,
            'file_date': file_date,
            'zip_file_path': zip_file_path,
            'extraction_dir_path': extraction_dir_path,
            'extracted_file_paths': extracted_files,
            'last_expanded': current_time,
            'extraction_duration_seconds': duration_seconds,
            'expansion_errors': error is not None,
            'expansion_error_messages': error or '',
            'num_contained_files': len(extracted_files),
            'is_valid_zip': is_valid_zip,
            'extraction_complete': (error is None) and is_valid_zip
        }
        
        # Append new record
        new_df = pd.DataFrame([new_record])
        updated_df = pd.concat([df, new_df], ignore_index=True)
        
        # Write updated DataFrame
        self.write(updated_df)
        self.logger.debug(f"Registered extraction for: {os.path.basename(zip_file_path)}")
        
        return extraction_id
    
    def get_pending_extractions(self):
        """
        Get ZIP files that need to be extracted.
        
        Returns:
            pd.DataFrame: Records of ZIP files that need extraction
        """
        df = self.read()
        return df[~df['extraction_complete']]
    
    def get_successful_extractions(self):
        """
        Get successfully extracted ZIP files.
        
        Returns:
            pd.DataFrame: Successfully extracted ZIP files
        """
        df = self.read()
        return df[df['extraction_complete'] & ~df['expansion_errors']]
    
    def get_extracted_files(self, with_file_types=None):
        """
        Get all extracted files, optionally filtering by file type.
        
        Args:
            with_file_types (list, optional): List of file extensions to filter by (e.g., ['.pdf', '.xlsx'])
            
        Returns:
            list: Paths to extracted files
        """
        df = self.read()
        successful = df[df['extraction_complete'] & ~df['expansion_errors']]
        
        all_files = []
        for _, row in successful.iterrows():
            file_paths = row['extracted_file_paths']
            all_files.extend(file_paths)
        
        # Filter by file type if specified
        if with_file_types:
            filtered_files = []
            for file_path in all_files:
                ext = os.path.splitext(file_path)[1].lower()
                if ext in with_file_types:
                    filtered_files.append(file_path)
            return filtered_files
        
        return all_files
    
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
                'success_rate': 0,
                'total_files_extracted': 0,
                'avg_files_per_zip': 0
            }
        
        successful = (df['extraction_complete'] & ~df['expansion_errors']).sum()
        failed = df['expansion_errors'].sum()
        total = len(df)
        
        # Total files extracted
        total_files = df['num_contained_files'].sum()
        
        return {
            'total_extractions': total,
            'successful_extractions': successful,
            'failed_extractions': failed,
            'success_rate': (successful / total * 100) if total > 0 else 0,
            'total_files_extracted': total_files,
            'avg_files_per_zip': (total_files / total) if total > 0 else 0
        }
    
    def get_extractions_by_date_range(self, start_date=None, end_date=None, with_errors=False):
        """
        Get extractions within a date range.
        
        Args:
            start_date (datetime, optional): Start date (inclusive)
            end_date (datetime, optional): End date (inclusive)
            with_errors (bool): If True, include extractions with errors
            
        Returns:
            pd.DataFrame: Extractions within the date range
        """
        df = self.read()
        
        # Apply error filter
        if not with_errors:
            df = df[~df['expansion_errors']]
        
        # Apply date filters if specified
        if start_date:
            df = df[df['file_date'] >= pd.Timestamp(start_date)]
        
        if end_date:
            df = df[df['file_date'] <= pd.Timestamp(end_date)]
        
        return df