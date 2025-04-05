"""
Registry to track web scraper downloads and attempts.
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import logging

from agroamigo_pipeline.config.settings import (
    REGISTRY_DIR, SOURCE_DIR
)

logger = logging.getLogger(__name__)

class WebScraperRegistry:
    """
    Registry to track web scraper downloads and attempts.
    """
    def __init__(self, parquet_path=None, csv_path=None):
        # Set default paths if not provided
        self.parquet_path = parquet_path or os.path.join(REGISTRY_DIR, 'scraper_registry.parquet')
        self.csv_path = csv_path or os.path.join(REGISTRY_DIR, 'scraper_summary.csv')
        self._ensure_directories()
        
        # Define schema
        self.registry_schema = pa.schema([
            ('filename', pa.string()),
            ('file_date', pa.timestamp('ns')),
            ('error_occurred', pa.bool_()),
            ('error_message', pa.string()),
            ('download_path', pa.string()),
            ('url', pa.string()),
            ('last_processed', pa.timestamp('ns')),
            ('month_key', pa.string()),
            ('download_attempt_count', pa.int32())
        ])
        
        # Initialize empty registry if it doesn't exist
        if not os.path.exists(self.parquet_path):
            self._create_empty_parquet()
    
    def _ensure_directories(self):
        """Ensure the parent directories for registry files exist."""
        os.makedirs(os.path.dirname(self.parquet_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
    
    def _create_empty_parquet(self):
        """Create an empty parquet file with the defined schema."""
        # Create empty DataFrame with proper timestamp types
        empty_df = pd.DataFrame({
            'filename': pd.Series([], dtype='string'),
            'file_date': pd.Series([], dtype='datetime64[ns]'),
            'error_occurred': pd.Series([], dtype='bool'),
            'error_message': pd.Series([], dtype='string'),
            'download_path': pd.Series([], dtype='string'),
            'url': pd.Series([], dtype='string'),
            'last_processed': pd.Series([], dtype='datetime64[ns]'),
            'month_key': pd.Series([], dtype='string'),
            'download_attempt_count': pd.Series([], dtype='int32')
        })
        
        table = pa.Table.from_pandas(empty_df, schema=self.registry_schema)
        pq.write_table(table, self.parquet_path)
        logger.info(f"Created empty web scraper registry at {self.parquet_path}")
    
    def _read_parquet(self):
        """Read the Parquet file and return as DataFrame."""
        if not os.path.exists(self.parquet_path):
            self._create_empty_parquet()
            return pd.DataFrame(columns=self.registry_schema.names)
        
        try:
            return pd.read_parquet(self.parquet_path)
        except Exception as e:
            logger.error(f"Error reading web scraper parquet registry: {str(e)}")
            self._create_empty_parquet()
            return pd.DataFrame(columns=self.registry_schema.names)
    
    def _write_parquet(self, df):
        """Write DataFrame to Parquet with the defined schema."""
        # Ensure all columns exist and have the right data types
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
        
        # Convert to proper timestamp format
        for col in ['file_date', 'last_processed']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert to correct types
        for col in ['download_attempt_count']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int32')
        
        for col in ['error_occurred']:
            if col in df.columns:
                df[col] = df[col].astype(bool)
        
        # Ensure we only have the defined columns in the specified order
        df = df[self.registry_schema.names]
        
        # Write to parquet
        table = pa.Table.from_pandas(df, schema=self.registry_schema)
        pq.write_table(table, self.parquet_path)
    
    def export_to_csv(self):
        """Export current registry data to CSV."""
        df = self._read_parquet()
        if not df.empty:
            # Format dates for CSV
            export_df = df.copy()
            
            # Convert timestamps to string format for readability
            if 'file_date' in export_df.columns:
                export_df['file_date'] = export_df['file_date'].dt.strftime('%Y-%m-%d')
            
            if 'last_processed' in export_df.columns:
                export_df['last_processed'] = export_df['last_processed'].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            export_df.to_csv(self.csv_path, index=False)
            logger.info(f"Exported web scraper registry to CSV: {self.csv_path}")
            return self.csv_path
        else:
            logger.warning("No web scraper data to export to CSV")
            return None
    
    def file_exists(self, filename):
        """
        Check if a file has been previously registered.
        
        Args:
            filename (str): Filename to check
            
        Returns:
            bool: True if file exists in registry, False otherwise
        """
        df = self._read_parquet()
        return not df[df['filename'] == filename].empty
    
    def register_download_attempt(self, filename, file_date, url, error=None, download_path=None, month_key=None):
        """
        Register a download attempt in the registry.
        
        Args:
            filename (str): Name of the file
            file_date (datetime): Date associated with the file
            url (str): URL that was attempted to download
            error (str, optional): Error message if download failed
            download_path (str, optional): Path where file was saved if successful
            month_key (str, optional): Month key for grouping (e.g., '2023-04')
            
        Returns:
            bool: True if this was a new file, False if it was an update
        """
        df = self._read_parquet()
        
        current_time = datetime.now()
        is_new_file = True
        
        # Check if file exists
        file_records = df[df['filename'] == filename]
        
        if not file_records.empty:
            is_new_file = False
            # Increment attempt count
            attempt_count = file_records['download_attempt_count'].max() + 1
            
            # Remove old record(s)
            df = df[df['filename'] != filename]
        else:
            attempt_count = 1
        
        # Create a new record
        new_record = {
            'filename': filename,
            'file_date': file_date,
            'error_occurred': error is not None,
            'error_message': error or '',
            'download_path': download_path or '',
            'url': url,
            'last_processed': current_time,
            'month_key': month_key or '',
            'download_attempt_count': attempt_count
        }
        
        # Append new record
        new_df = pd.DataFrame([new_record])
        updated_df = pd.concat([df, new_df], ignore_index=True)
        
        # Write updated DataFrame
        self._write_parquet(updated_df)
        logger.debug(f"Registered download attempt for: {filename}, attempt #{attempt_count}")
        
        return is_new_file
    
    def get_successful_downloads(self):
        """
        Get list of successfully downloaded files.
        
        Returns:
            pd.DataFrame: DataFrame of successful downloads
        """
        df = self._read_parquet()
        return df[~df['error_occurred'] & (df['download_path'] != '')]
    
    def get_failed_downloads(self):
        """
        Get list of failed downloads.
        
        Returns:
            pd.DataFrame: DataFrame of failed downloads
        """
        df = self._read_parquet()
        return df[df['error_occurred']]
    
    def get_statistics(self):
        """
        Get download statistics.
        
        Returns:
            dict: Dictionary of statistics
        """
        df = self._read_parquet()
        
        if df.empty:
            return {
                'total_files': 0,
                'successful_downloads': 0,
                'failed_downloads': 0,
                'success_rate': 0,
                'download_attempts': 0,
                'unique_months': 0
            }
        
        successful = (~df['error_occurred'] & (df['download_path'] != '')).sum()
        failed = df['error_occurred'].sum()
        total = len(df)
        
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
            'month_stats': month_stats
        }
    
    def clean_registry(self, keep_last_n_days=30):
        """
        Clean the registry by removing old entries that are not successful downloads.
        
        Args:
            keep_last_n_days (int): Number of days of history to keep
            
        Returns:
            int: Number of entries removed
        """
        df = self._read_parquet()
        
        if df.empty:
            return 0
        
        # Calculate cutoff date
        cutoff_date = datetime.now() - pd.Timedelta(days=keep_last_n_days)
        
        # Count records before filtering
        old_count = len(df)
        
        # Keep successful downloads and recent entries
        df = df[
            (~df['error_occurred'] & (df['download_path'] != '')) | 
            (df['last_processed'] >= cutoff_date)
        ]
        
        # Count records after filtering
        new_count = len(df)
        removed = old_count - new_count
        
        if removed > 0:
            # Write updated DataFrame
            self._write_parquet(df)
            logger.info(f"Cleaned web scraper registry: removed {removed} old entries")
        
        return removed