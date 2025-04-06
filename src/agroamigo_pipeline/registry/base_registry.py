"""
Base registry class providing common functionality for all registry types.
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import logging
from pathlib import Path
import shutil

from agroamigo_pipeline.config.settings import REGISTRY_DIR

class BaseRegistry:
    """Base class for all pipeline registries with common functionality."""
    
    def __init__(self, registry_name, schema=None, logger=None):
        """
        Initialize a base registry.
        
        Args:
            registry_name (str): Name of the registry (used for filenames)
            schema (pa.Schema): PyArrow schema for the registry
            logger (logging.Logger): Logger instance
        """
        self.registry_name = registry_name
        self.logger = logger or logging.getLogger(f'registry.{registry_name}')
        
        # Set up file paths
        self.registry_dir = Path(REGISTRY_DIR)
        
        # Create parquet subdirectory and always store parquet files there
        self.parquet_dir = self.registry_dir / 'parquet'
        os.makedirs(self.parquet_dir, exist_ok=True)
        
        # Set the parquet file path to always point to the subdirectory
        self.parquet_path = self.parquet_dir / f"{registry_name}_registry.parquet"
        
        # CSV path remains at the root of the registry directory for easy access
        self.csv_path = self.registry_dir / f"{registry_name}_summary.csv"
        
        # Ensure registry directory exists
        os.makedirs(self.registry_dir, exist_ok=True)
        
        # Initialize schema
        self.schema = schema
        
        # Create empty registry if it doesn't exist
        if not os.path.exists(self.parquet_path) and self.schema:
            self._create_empty_registry()
        
        # Check for legacy location and migrate if needed
        self._migrate_legacy_parquet()
    
    def _migrate_legacy_parquet(self):
        """Check for parquet file in legacy location and migrate if needed."""
        legacy_path = self.registry_dir / f"{self.registry_name}_registry.parquet"
        
        # If the file exists in the old location but not in the new location, move it
        if os.path.exists(legacy_path) and not os.path.exists(self.parquet_path):
            try:
                # Ensure the parquet directory exists
                os.makedirs(self.parquet_dir, exist_ok=True)
                
                # Move the file from legacy to new location
                shutil.move(legacy_path, self.parquet_path)
                self.logger.info(f"Migrated registry from {legacy_path} to {self.parquet_path}")
            except Exception as e:
                self.logger.error(f"Error migrating registry file: {str(e)}")
        
        # If both legacy and new location have files, use the newer one and delete the older one
        elif os.path.exists(legacy_path) and os.path.exists(self.parquet_path):
            try:
                # Compare modification times to determine which is newer
                legacy_mtime = os.path.getmtime(legacy_path)
                new_mtime = os.path.getmtime(self.parquet_path)
                
                if legacy_mtime > new_mtime:
                    # Legacy is newer, so replace the new one
                    shutil.move(legacy_path, self.parquet_path)
                    self.logger.info(f"Replaced registry with newer version from {legacy_path}")
                else:
                    # New one is newer, so remove the legacy one
                    os.remove(legacy_path)
                    self.logger.info(f"Removed outdated legacy registry from {legacy_path}")
            except Exception as e:
                self.logger.error(f"Error resolving registry file conflict: {str(e)}")
    
    def _create_empty_registry(self):
        """Create an empty Parquet registry file with the defined schema."""
        # Create empty DataFrame
        empty_data = {}
        for field in self.schema:
            name = field.name
            dtype = self._get_pandas_dtype(field.type)
            empty_data[name] = pd.Series([], dtype=dtype)
            
        empty_df = pd.DataFrame(empty_data)
        
        # Convert to Arrow table and write to Parquet
        table = pa.Table.from_pandas(empty_df, schema=self.schema)
        
        # Ensure the parquet directory exists
        os.makedirs(os.path.dirname(self.parquet_path), exist_ok=True)
        
        # Write to the correct location
        pq.write_table(table, self.parquet_path)
        
        self.logger.info(f"Created empty registry at {self.parquet_path}")
    
    def _get_pandas_dtype(self, pa_type):
        """Convert PyArrow type to Pandas dtype."""
        if pa.types.is_string(pa_type):
            return 'string'
        elif pa.types.is_boolean(pa_type):
            return 'bool'
        elif pa.types.is_integer(pa_type):
            return 'int32'
        elif pa.types.is_floating(pa_type):
            return 'float64'
        elif pa.types.is_timestamp(pa_type):
            return 'datetime64[ns]'
        else:
            return 'object'
    
    def read(self):
        """
        Read the registry from Parquet file.
        
        Returns:
            pd.DataFrame: Registry as DataFrame
        """
        if not os.path.exists(self.parquet_path):
            if self.schema:
                self._create_empty_registry()
            return pd.DataFrame(columns=[field.name for field in self.schema])
        
        try:
            return pd.read_parquet(self.parquet_path)
        except Exception as e:
            self.logger.error(f"Error reading registry: {str(e)}")
            if self.schema:
                self._create_empty_registry()
            return pd.DataFrame(columns=[field.name for field in self.schema])
    
    def write(self, df):
        """
        Write DataFrame to the registry.
        
        Args:
            df (pd.DataFrame): DataFrame to write
        """
        # Ensure all required columns exist with correct types
        if self.schema:
            for field in self.schema:
                col = field.name
                pa_type = field.type  # Get the type for this field
                
                if col not in df.columns:
                    # Add missing columns with appropriate default values
                    if pa.types.is_integer(pa_type):
                        df[col] = 0
                    elif pa.types.is_boolean(pa_type):
                        df[col] = False
                    elif pa.types.is_timestamp(pa_type):
                        df[col] = pd.NaT
                    elif pa.types.is_floating(pa_type):
                        df[col] = 0.0
                    else:
                        df[col] = ''
                
                # Ensure correct data types
                if pa.types.is_timestamp(pa_type):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif pa.types.is_integer(pa_type):
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int32')
                elif pa.types.is_floating(pa_type):
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                elif pa.types.is_boolean(pa_type):
                    df[col] = df[col].astype(bool)
                elif pa.types.is_string(pa_type):
                    df[col] = df[col].astype(str)
            
            # Ensure we have only the defined columns in the correct order
            df = df[[field.name for field in self.schema]]
            
            # Ensure the parquet directory exists
            os.makedirs(os.path.dirname(self.parquet_path), exist_ok=True)
            
            # Write to Parquet
            table = pa.Table.from_pandas(df, schema=self.schema)
            pq.write_table(table, self.parquet_path)
        else:
            # If no schema defined, just write the DataFrame as is
            # Ensure the parquet directory exists
            os.makedirs(os.path.dirname(self.parquet_path), exist_ok=True)
            df.to_parquet(self.parquet_path)
        
        self.logger.debug(f"Updated registry with {len(df)} records")
    
    def export_to_csv(self):
        """
        Export the registry data to CSV.
        
        Returns:
            str: Path to the CSV file or None if empty
        """
        df = self.read()
        if not df.empty:
            # Format dates for CSV
            export_df = df.copy()
            
            # Convert timestamps to string format for readability
            for col in export_df.columns:
                if pd.api.types.is_datetime64_dtype(export_df[col]):
                    export_df[col] = export_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Remove any legacy parquet files at the root level
            legacy_path = self.registry_dir / f"{self.registry_name}_registry.parquet"
            if os.path.exists(legacy_path):
                try:
                    os.remove(legacy_path)
                    self.logger.info(f"Removed legacy registry file: {legacy_path}")
                except Exception as e:
                    self.logger.warning(f"Could not remove legacy registry file: {str(e)}")
            
            # Export the CSV to the registry root for easy access
            export_df.to_csv(self.csv_path, index=False)
            self.logger.info(f"Exported registry to CSV: {self.csv_path}")
            return self.csv_path
        else:
            self.logger.warning(f"No data to export to CSV for {self.registry_name}")
            return None
    
    def get_by_id(self, id_field, id_value):
        """
        Get records by ID field.
        
        Args:
            id_field (str): Name of the ID field
            id_value (any): Value to match
            
        Returns:
            pd.DataFrame: Matching records
        """
        df = self.read()
        return df[df[id_field] == id_value]
    
    def add_or_update(self, record, id_field):
        """
        Add or update a record in the registry.
        
        Args:
            record (dict): Record to add or update
            id_field (str): Name of the ID field
            
        Returns:
            bool: True if new record was added, False if updated
        """
        df = self.read()
        
        # Check if record exists
        if id_field in record and id_field in df.columns:
            matches = df[df[id_field] == record[id_field]]
            
            if not matches.empty:
                # Remove existing record
                df = df[df[id_field] != record[id_field]]
                is_new = False
            else:
                is_new = True
        else:
            is_new = True
        
        # Add new record
        new_df = pd.DataFrame([record])
        updated_df = pd.concat([df, new_df], ignore_index=True)
        
        # Write back to registry
        self.write(updated_df)
        
        return is_new
    
    def query(self, condition_func):
        """
        Query the registry using a condition function.
        
        Args:
            condition_func (callable): Function that takes a DataFrame and returns a DataFrame
            
        Returns:
            pd.DataFrame: Filtered DataFrame
        """
        df = self.read()
        return condition_func(df)
    
    def get_statistics(self):
        """
        Get statistics about the registry.
        
        Returns:
            dict: Statistics
        """
        df = self.read()
        
        stats = {
            'total_records': len(df),
            'last_updated': datetime.now().isoformat()
        }
        
        # Add status counts if status column exists
        if 'status' in df.columns:
            status_counts = df['status'].value_counts().to_dict()
            stats['status_counts'] = status_counts
        
        # Add error counts if error_occurred column exists
        if 'error_occurred' in df.columns:
            error_count = df['error_occurred'].sum()
            stats['error_count'] = int(error_count)
            stats['success_count'] = len(df) - int(error_count)
        
        return stats