"""
Registry for tracking web scraper downloads.
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import logging
import uuid

from agroamigo_pipeline.registry.base_registry import BaseRegistry

class WebScraperRegistry(BaseRegistry):
    """Registry to track web scraper downloads."""
    
    def __init__(self, logger=None):
        """Initialize the web scraper registry."""
        # Define schema for web scraper registry
        schema = pa.schema([
            ('download_id', pa.string()),  # Primary key
            ('filename', pa.string()),
            ('file_date', pa.timestamp('ns')),
            ('file_type', pa.string()),  # "zip" or "excel"
            ('error_occurred', pa.bool_()),
            ('error_message', pa.string()),
            ('download_path', pa.string()),
            ('url', pa.string()),
            ('last_processed', pa.timestamp('ns')),
            ('month_key', pa.string()),  # YYYY-MM format
            ('download_attempt_count', pa.int32()),
            ('download_duration_seconds', pa.float64())
        ])
        
        # Initialize base registry
        super().__init__("webscraper", schema, logger)
        self.logger.debug("WebScraperRegistry initialized")
    
    def file_exists(self, filename):
        """
        Check if a file has been previously registered.
        
        Args:
            filename (str): Filename to check
            
        Returns:
            bool: True if file exists in registry, False otherwise
        """
        df = self.read()
        return not df[df['filename'] == filename].empty
    
    def get_download_by_id(self, download_id):
        """
        Get a download record by its ID.
        
        Args:
            download_id (str): Download ID
            
        Returns:
            dict: Download record or None if not found
        """
        df = self.read()
        matches = df[df['download_id'] == download_id]
        
        if matches.empty:
            return None
        
        # Convert to dictionary
        return matches.iloc[0].to_dict()
    
    def register_download_attempt(self, filename, file_date, url, file_type, error=None, 
                               download_path=None, month_key=None, duration_seconds=0.0):
        """
        Register a download attempt in the registry.
        
        Args:
            filename (str): Name of the file
            file_date (datetime): Date associated with the file
            url (str): URL that was attempted to download
            file_type (str): Type of file ("zip" or "excel")
            error (str, optional): Error message if download failed
            download_path (str, optional): Path where file was saved if successful
            month_key (str, optional): Month key for grouping (e.g., '2023-04')
            duration_seconds (float, optional): Duration of download in seconds
            
        Returns:
            str: Download ID
        """
        df = self.read()
        
        current_time = datetime.now()
        
        # Check if file exists
        file_records = df[df['filename'] == filename]
        
        if not file_records.empty:
            # Get the existing download_id
            download_id = file_records.iloc[0]['download_id']
            # Increment attempt count
            attempt_count = file_records.iloc[0]['download_attempt_count'] + 1
            
            # Remove old record(s)
            df = df[df['filename'] != filename]
        else:
            # Generate a new download_id
            download_id = str(uuid.uuid4())
            attempt_count = 1
        
        # Create a new record
        new_record = {
            'download_id': download_id,
            'filename': filename,
            'file_date': file_date,
            'file_type': file_type.lower(),
            'error_occurred': error is not None,
            'error_message': error or '',
            'download_path': download_path or '',
            'url': url,
            'last_processed': current_time,
            'month_key': month_key or '',
            'download_attempt_count': attempt_count,
            'download_duration_seconds': duration_seconds
        }
        
        # Append new record
        new_df = pd.DataFrame([new_record])
        updated_df = pd.concat([df, new_df], ignore_index=True)
        
        # Write updated DataFrame
        self.write(updated_df)
        self.logger.debug(f"Registered download attempt for: {filename}, attempt #{attempt_count}")
        
        return download_id
    
    def get_files_by_type(self, file_type, with_errors=False):
        """
        Get all files of a specific type.
        
        Args:
            file_type (str): Type of file ("zip" or "excel")
            with_errors (bool): If True, include files with errors
            
        Returns:
            pd.DataFrame: Files of the specified type
        """
        df = self.read()
        
        # First filter by file type
        result = df[df['file_type'] == file_type.lower()]
        
        # Then filter by error status if requested
        if not with_errors:
            result = result[~result['error_occurred']]
        
        return result
    
    def get_files_by_date_range(self, file_type=None, start_date=None, end_date=None, with_errors=False):
        """
        Get files within a date range.
        
        Args:
            file_type (str, optional): Type of file ("zip" or "excel")
            start_date (datetime, optional): Start date (inclusive)
            end_date (datetime, optional): End date (inclusive)
            with_errors (bool): If True, include files with errors
            
        Returns:
            pd.DataFrame: Files within the date range
        """
        df = self.read()
        
        # Apply file type filter if specified
        if file_type:
            df = df[df['file_type'] == file_type.lower()]
        
        # Apply error filter
        if not with_errors:
            df = df[~df['error_occurred']]
        
        # Apply date filters if specified
        if start_date:
            df = df[df['file_date'] >= pd.Timestamp(start_date)]
        
        if end_date:
            df = df[df['file_date'] <= pd.Timestamp(end_date)]
        
        return df
    
    def get_successful_downloads(self):
        """
        Get list of successfully downloaded files.
        
        Returns:
            pd.DataFrame: DataFrame of successful downloads
        """
        df = self.read()
        return df[~df['error_occurred'] & (df['download_path'] != '')]
    
    def get_failed_downloads(self):
        """
        Get list of failed downloads.
        
        Returns:
            pd.DataFrame: DataFrame of failed downloads
        """
        df = self.read()
        return df[df['error_occurred']]
    
    def get_statistics(self):
        """
        Get download statistics.
        
        Returns:
            dict: Dictionary of statistics
        """
        df = self.read()
        
        if df.empty:
            return {
                'total_files': 0,
                'successful_downloads': 0,
                'failed_downloads': 0,
                'success_rate': 0,
                'download_attempts': 0,
                'unique_months': 0,
                'by_file_type': {}
            }
        
        successful = (~df['error_occurred'] & (df['download_path'] != '')).sum()
        failed = df['error_occurred'].sum()
        total = len(df)
        
        # Statistics by file type
        by_file_type = {}
        for file_type in df['file_type'].unique():
            type_df = df[df['file_type'] == file_type]
            type_success = (~type_df['error_occurred'] & (type_df['download_path'] != '')).sum()
            type_failed = type_df['error_occurred'].sum()
            
            by_file_type[file_type] = {
                'total': len(type_df),
                'successful': type_success,
                'failed': type_failed,
                'success_rate': (type_success / len(type_df) * 100) if len(type_df) > 0 else 0
            }
        
        # Group by month_key and count unique files per month
        month_stats = {}
        if 'month_key' in df.columns:
            month_groups = df.groupby('month_key')
            for month, group in month_groups:
                if month and month.strip():  # Skip empty month_key
                    month_stats[month] = {
                        'total_files': len(group),
                        'successful': (~group['error_occurred'] & (group['download_path'] != '')).sum(),
                        'failed': group['error_occurred'].sum()
                    }
        
        return {
            'total_files': total,
            'successful_downloads': successful,
            'failed_downloads': failed,
            'success_rate': (successful / total * 100) if total > 0 else 0,
            'download_attempts': df['download_attempt_count'].sum(),
            'unique_months': len(month_stats),
            'by_file_type': by_file_type,
            'month_stats': month_stats
        }