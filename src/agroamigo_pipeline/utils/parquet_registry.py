"""
Registry to track processed files using Parquet format for efficient storage and querying.
"""

import os
import json
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
import logging

from agroamigo_pipeline.config.settings import PROCESSING_REGISTRY, REGISTRY_PARQUET, REGISTRY_CSV

logger = logging.getLogger(__name__)

class ParquetRegistry:
    """
    Registry to track processed files and avoid duplicate processing.
    Uses Parquet for storage and provides CSV export functionality.
    """
    def __init__(self, parquet_path=REGISTRY_PARQUET, csv_path=REGISTRY_CSV):
        self.parquet_path = parquet_path
        self.csv_path = csv_path
        self._registry_metadata = self._load_registry_metadata()
        self._ensure_directories()
        
        # Define schema
        self.registry_schema = pa.schema([
            ('filename', pa.string()),
            ('file_path', pa.string()),
            ('output_path', pa.string()),
            ('status', pa.string()),
            ('first_processed', pa.timestamp('ns')),
            ('last_processed', pa.timestamp('ns')),
            ('timestamp', pa.timestamp('ns')),
            ('unique_products', pa.int32()),
            ('unique_major_categories', pa.int32()),
            ('unique_subcategories', pa.int32()),
            ('rows_processed', pa.int32()),
            ('error_occurred', pa.bool_()),
            ('errors_warnings', pa.string()),
            ('processing_time_seconds', pa.float64()),
            ('extraction_method', pa.string()),
            ('text_is_valid', pa.bool_()),
            ('used_gpt_extraction', pa.bool_())
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
        import pandas as pd
        import numpy as np
        
        # Create empty DataFrame with appropriate types
        empty_df = pd.DataFrame({
            'filename': pd.Series([], dtype='string'),
            'file_path': pd.Series([], dtype='string'),
            'output_path': pd.Series([], dtype='string'),
            'status': pd.Series([], dtype='string'),
            'first_processed': pd.Series([], dtype='datetime64[ns]'),
            'last_processed': pd.Series([], dtype='datetime64[ns]'),
            'timestamp': pd.Series([], dtype='datetime64[ns]'),
            'unique_products': pd.Series([], dtype='int32'),
            'unique_major_categories': pd.Series([], dtype='int32'),
            'unique_subcategories': pd.Series([], dtype='int32'),
            'rows_processed': pd.Series([], dtype='int32'),
            'error_occurred': pd.Series([], dtype='bool'),
            'errors_warnings': pd.Series([], dtype='string'),
            'processing_time_seconds': pd.Series([], dtype='float64'),
            'extraction_method': pd.Series([], dtype='string'),
            'text_is_valid': pd.Series([], dtype='bool'),
            'used_gpt_extraction': pd.Series([], dtype='bool')
        })
        
        table = pa.Table.from_pandas(empty_df, schema=self.registry_schema)
        pq.write_table(table, self.parquet_path)
        logger.info(f"Created empty registry at {self.parquet_path}")
    
    def _load_registry_metadata(self):
        """Load the registry metadata or create a new one."""
        # This stores additional metadata about the registry itself
        metadata_path = os.path.join(os.path.dirname(self.parquet_path), 'registry_metadata.json')
        
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, UnicodeDecodeError):
                # If registry metadata is corrupted, create a new one
                return {'last_updated': datetime.now().isoformat()}
        else:
            return {'last_updated': datetime.now().isoformat()}
    
    def _save_registry_metadata(self):
        """Save the registry metadata to disk."""
        self._registry_metadata['last_updated'] = datetime.now().isoformat()
        metadata_path = os.path.join(os.path.dirname(self.parquet_path), 'registry_metadata.json')
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(self._registry_metadata, f, indent=2)
    
    def _read_parquet(self):
        """Read the Parquet file and return as DataFrame."""
        if not os.path.exists(self.parquet_path):
            self._create_empty_parquet()
            return pd.DataFrame(columns=self.registry_schema.names)
        
        try:
            return pd.read_parquet(self.parquet_path)
        except Exception as e:
            logger.error(f"Error reading parquet registry: {str(e)}")
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
        for col in ['first_processed', 'last_processed', 'timestamp']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert to correct types
        for col in ['unique_products', 'unique_major_categories', 'unique_subcategories', 'rows_processed']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int32')
        
        for col in ['error_occurred', 'text_is_valid', 'used_gpt_extraction']:
            if col in df.columns:
                df[col] = df[col].astype(bool)
        
        for col in ['processing_time_seconds']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        # Ensure we only have the defined columns in the specified order
        df = df[self.registry_schema.names]
        
        # Write to parquet
        table = pa.Table.from_pandas(df, schema=self.registry_schema)
        pq.write_table(table, self.parquet_path)
        self._save_registry_metadata()
    
    def export_to_csv(self):
        """Export current registry data to CSV."""
        df = self._read_parquet()
        if not df.empty:
            # Select only the schema fields we want in the CSV
            csv_columns = [
                'filename', 'unique_products', 'unique_major_categories', 
                'unique_subcategories', 'rows_processed', 'error_occurred',
                'errors_warnings', 'extraction_method', 'file_path'
            ]
            
            # Select only columns that exist in the dataframe
            available_columns = [col for col in csv_columns if col in df.columns]
            export_df = df[available_columns].copy()
            
            # Replace any null values
            export_df = export_df.fillna({
                'filename': '',
                'unique_products': 0,
                'unique_major_categories': 0,
                'unique_subcategories': 0,
                'rows_processed': 0,
                'error_occurred': False,
                'errors_warnings': '',
                'extraction_method': '',
                'file_path': ''
            })
            
            export_df.to_csv(self.csv_path, index=False)
            logger.info(f"Exported registry to CSV: {self.csv_path}")
            return self.csv_path
        else:
            logger.warning("No data to export to CSV")
            return None
    
    def file_processed(self, file_path):
        """
        Check if a file has been processed before.
        
        Args:
            file_path (str): Path to the file to check
        
        Returns:
            bool: True if file has been processed, False otherwise
        """
        file_path = str(Path(file_path).resolve())
        df = self._read_parquet()
        return not df[df['file_path'] == file_path].empty
    
    def get_file_info(self, file_path):
        """
        Get information about a processed file.
        
        Args:
            file_path (str): Path to the file to check
        
        Returns:
            dict: File processing information or None if not found
        """
        file_path = str(Path(file_path).resolve())
        df = self._read_parquet()
        filtered_df = df[df['file_path'] == file_path]
        
        if filtered_df.empty:
            return None
        
        row = filtered_df.iloc[-1]  # Get the most recent record
        
        # Construct a result in the same format as the old registry for compatibility
        result = {
            'first_processed': row['first_processed'].isoformat() if pd.notna(row['first_processed']) else None,
            'last_processed': row['last_processed'].isoformat() if pd.notna(row['last_processed']) else None,
            'processing_history': [{
                'timestamp': row['timestamp'].isoformat() if pd.notna(row['timestamp']) else None,
                'status': row['status'],
                'output_path': row['output_path'],
                'error': row['errors_warnings'] if row['error_occurred'] else None,
                'stats': {
                    'filename': row['filename'],
                    'file_path': row['file_path'],
                    'unique_products': int(row['unique_products']),
                    'unique_major_categories': int(row['unique_major_categories']),
                    'unique_subcategories': int(row['unique_subcategories']),
                    'rows_processed': int(row['rows_processed']),
                    'error_occurred': bool(row['error_occurred']),
                    'errors_warnings': row['errors_warnings'],
                    'processing_time_seconds': float(row['processing_time_seconds']),
                    'extraction_method': row['extraction_method'],
                    'text_is_valid': bool(row['text_is_valid']),
                    'used_gpt_extraction': bool(row['used_gpt_extraction'])
                }
            }]
        }
        
        return result
    
    def register_file(self, file_path, output_path=None, status="processed", error=None, stats=None):
        """
        Register a file as processed.
        
        Args:
            file_path (str): Path to the file being registered
            output_path (str): Path to the output file (if any)
            status (str): Processing status (processed, error, etc.)
            error (str): Error message if processing failed
            stats (dict): Processing statistics
        """
        file_path = str(Path(file_path).resolve())
        stats = stats or {}
        current_time = datetime.now()
        
        # Read current registry
        df = self._read_parquet()
        
        # Check if file already exists in registry
        filtered_df = df[df['file_path'] == file_path]
        
        if filtered_df.empty:
            # First time processing this file
            first_processed = current_time
            last_processed = current_time
        else:
            # File has been processed before
            first_processed = filtered_df.iloc[0]['first_processed']
            last_processed = current_time
        
        # Create a record to append
        record = {
            'filename': stats.get('filename', os.path.basename(file_path)),
            'file_path': file_path,
            'output_path': output_path or '',
            'status': status,
            'first_processed': first_processed,
            'last_processed': last_processed,
            'timestamp': current_time,
            'unique_products': stats.get('unique_products', 0),
            'unique_major_categories': stats.get('unique_major_categories', 0),
            'unique_subcategories': stats.get('unique_subcategories', 0),
            'rows_processed': stats.get('rows_processed', 0),
            'error_occurred': stats.get('error_occurred', False),
            'errors_warnings': error or stats.get('errors_warnings', ''),
            'processing_time_seconds': stats.get('processing_time_seconds', 0.0),
            'extraction_method': stats.get('extraction_method', ''),
            'text_is_valid': stats.get('text_is_valid', False),
            'used_gpt_extraction': stats.get('used_gpt_extraction', False)
        }
        
        # Append new row
        new_df = pd.DataFrame([record])
        updated_df = pd.concat([df, new_df], ignore_index=True)
        
        # Write updated dataframe back to parquet
        self._write_parquet(updated_df)
        logger.debug(f"Registered file: {file_path} with status: {status}")
    
    def get_all_files(self, status=None):
        """
        Get all registered files, optionally filtered by status.
        
        Args:
            status (str): Filter by processing status
        
        Returns:
            pd.DataFrame: DataFrame of files and their info
        """
        df = self._read_parquet()
        
        if status is not None:
            return df[df['status'] == status]
        
        return df
    
    def migrate_from_json(self, json_path=PROCESSING_REGISTRY):
        """
        Migrate data from the old JSON registry to the new Parquet format.
        
        Args:
            json_path (str): Path to the old JSON registry
            
        Returns:
            bool: True if migration was successful, False otherwise
        """
        if not os.path.exists(json_path):
            logger.warning(f"JSON registry not found at {json_path}")
            return False
            
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                old_registry = json.load(f)
                
            if 'files' not in old_registry:
                logger.error("Invalid JSON registry format")
                return False
                
            records = []
            
            for file_path, file_info in old_registry['files'].items():
                for history in file_info.get('processing_history', []):
                    stats = history.get('stats', {})
                    
                    record = {
                        'filename': stats.get('filename', os.path.basename(file_path)),
                        'file_path': file_path,
                        'output_path': history.get('output_path', ''),
                        'status': history.get('status', 'unknown'),
                        'first_processed': pd.to_datetime(file_info.get('first_processed')),
                        'last_processed': pd.to_datetime(file_info.get('last_processed')),
                        'timestamp': pd.to_datetime(history.get('timestamp')),
                        'unique_products': stats.get('unique_products', 0),
                        'unique_major_categories': stats.get('unique_major_categories', 0),
                        'unique_subcategories': stats.get('unique_subcategories', 0),
                        'rows_processed': stats.get('rows_processed', 0),
                        'error_occurred': stats.get('error_occurred', False),
                        'errors_warnings': history.get('error', '') or stats.get('errors_warnings', ''),
                        'processing_time_seconds': stats.get('processing_time_seconds', 0.0),
                        'extraction_method': stats.get('extraction_method', ''),
                        'text_is_valid': stats.get('text_is_valid', False),
                        'used_gpt_extraction': stats.get('used_gpt_extraction', False)
                    }
                    
                    records.append(record)
            
            # Create DataFrame and save to parquet
            if records:
                migration_df = pd.DataFrame(records)
                self._write_parquet(migration_df)
                logger.info(f"Successfully migrated {len(records)} records from JSON to Parquet")
                
                # Export to CSV as well
                self.export_to_csv()
                return True
            else:
                logger.warning("No records to migrate from JSON registry")
                return False
                
        except Exception as e:
            logger.error(f"Error migrating from JSON: {str(e)}")
            return False