"""
Registry for tracking Excel file processing.
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import logging
import uuid

from agroamigo_pipeline.registry.base_registry import BaseRegistry

class ExcelProcessorRegistry(BaseRegistry):
    """Registry to track Excel file processing."""
    
    def __init__(self, logger=None):
        """Initialize the Excel processor registry."""
        # Define schema for Excel processor registry
        schema = pa.schema([
            ('excel_id', pa.string()),  # Primary key
            ('download_id', pa.string()),  # Foreign key to WebScraperRegistry
            ('file_date', pa.timestamp('ns')),
            ('filename', pa.string()),
            ('file_path', pa.string()),
            ('processing_status', pa.string()),  # "not_processed", "processing", "processed", "error"
            ('processed_date', pa.timestamp('ns')),
            ('parsed_date', pa.timestamp('ns')),  # Date extracted from Excel content
            ('processing_duration_seconds', pa.float64()),
            ('unique_products', pa.int32()),
            ('unique_major_categories', pa.int32()),
            ('unique_subcategories', pa.int32()),
            ('rows_processed', pa.int32()),
            ('error_occurred', pa.bool_()),
            ('error_warnings', pa.string()),
            ('extraction_method', pa.string()),  # Usually "excel_parsing"
            ('processed_csv_path', pa.string()),
            ('unique_cities_markets', pa.int32()),
            ('pipeline_run_id', pa.string()),  # ID of the pipeline run
            ('consolidated', pa.bool_())  # Whether this file has been consolidated
        ])
        
        # Initialize base registry
        super().__init__("excel", schema, logger)
        self.logger.debug("ExcelProcessorRegistry initialized")
    
    def get_excel_by_id(self, excel_id):
        """
        Get an Excel record by its ID.
        
        Args:
            excel_id (str): Excel ID
            
        Returns:
            dict: Excel record or None if not found
        """
        df = self.read()
        matches = df[df['excel_id'] == excel_id]
        
        if matches.empty:
            return None
        
        # Convert to dictionary
        return matches.iloc[0].to_dict()
    
    def get_excel_by_download_id(self, download_id):
        """
        Get Excel records by download ID.
        
        Args:
            download_id (str): Download ID from WebScraperRegistry
            
        Returns:
            pd.DataFrame: Matching Excel records
        """
        df = self.read()
        return df[df['download_id'] == download_id]
    
    def get_excel_by_path(self, file_path):
        """
        Get Excel records by file path.
        
        Args:
            file_path (str): Path to the Excel file
            
        Returns:
            pd.DataFrame: Matching Excel records
        """
        df = self.read()
        return df[df['file_path'] == file_path]
    
    def register_excel(self, download_id, file_path, filename, file_date=None):
        """
        Register an Excel file for processing.
        
        Args:
            download_id (str): Download ID from WebScraperRegistry
            file_path (str): Path to the Excel file
            filename (str): Name of the Excel file
            file_date (datetime, optional): Date associated with the file
            
        Returns:
            str: Excel ID
        """
        df = self.read()
        
        current_time = datetime.now()
        
        # Check if file has already been registered
        existing_excels = df[df['file_path'] == file_path]
        
        if not existing_excels.empty:
            # Get the existing excel_id
            excel_id = existing_excels.iloc[0]['excel_id']
            
            # Update status if not already processed
            if existing_excels.iloc[0]['processing_status'] == 'not_processed':
                # Remove old record(s)
                df = df[df['file_path'] != file_path]
            else:
                # Already processed or processing, don't update
                return excel_id
        else:
            # Generate a new excel_id
            excel_id = str(uuid.uuid4())
        
        # Create a new record
        new_record = {
            'excel_id': excel_id,
            'download_id': download_id,
            'file_date': file_date or current_time,
            'filename': filename,
            'file_path': file_path,
            'processing_status': 'not_processed',
            'processed_date': pd.NaT,
            'parsed_date': pd.NaT,
            'processing_duration_seconds': 0.0,
            'unique_products': 0,
            'unique_major_categories': 0,
            'unique_subcategories': 0,
            'rows_processed': 0,
            'error_occurred': False,
            'error_warnings': '',
            'extraction_method': 'excel_parsing',
            'processed_csv_path': '',
            'unique_cities_markets': 0,
            'pipeline_run_id': '',
            'consolidated': False
        }
        
        # Append new record
        new_df = pd.DataFrame([new_record])
        updated_df = pd.concat([df, new_df], ignore_index=True)
        
        # Write updated DataFrame
        self.write(updated_df)
        self.logger.debug(f"Registered Excel for processing: {filename}")
        
        return excel_id
    
    def update_processing_status(self, excel_id, status, error=None):
        """
        Update the processing status of an Excel file.
        
        Args:
            excel_id (str): Excel ID
            status (str): New status ("processing", "processed", "error")
            error (str, optional): Error message if status is "error"
            
        Returns:
            bool: True if successful, False if Excel not found
        """
        df = self.read()
        
        # Find the Excel
        excel_idx = df.index[df['excel_id'] == excel_id].tolist()
        
        if not excel_idx:
            self.logger.warning(f"Excel with ID {excel_id} not found in registry")
            return False
        
        # Update status
        df.at[excel_idx[0], 'processing_status'] = status
        
        # Update error fields if applicable
        if status == 'error' and error:
            df.at[excel_idx[0], 'error_occurred'] = True
            df.at[excel_idx[0], 'error_warnings'] = error
        
        # Update processed_date if status is "processed" or "error"
        if status in ['processed', 'error']:
            df.at[excel_idx[0], 'processed_date'] = datetime.now()
        
        # Write updated DataFrame
        self.write(df)
        self.logger.debug(f"Updated Excel {excel_id} status to {status}")
        
        return True
    
    def update_processing_results(self, excel_id, results):
        """
        Update the processing results of an Excel file.
        
        Args:
            excel_id (str): Excel ID
            results (dict): Processing results containing statistics and output path
            
        Returns:
            bool: True if successful, False if Excel not found
        """
        df = self.read()
        
        # Find the Excel
        excel_idx = df.index[df['excel_id'] == excel_id].tolist()
        
        if not excel_idx:
            self.logger.warning(f"Excel with ID {excel_id} not found in registry")
            return False
        
        # Get current record for the Excel
        idx = excel_idx[0]
        
        # Update statistics fields
        if 'unique_products' in results:
            df.at[idx, 'unique_products'] = results['unique_products']
        
        if 'unique_major_categories' in results:
            df.at[idx, 'unique_major_categories'] = results['unique_major_categories']
        
        if 'unique_subcategories' in results:
            df.at[idx, 'unique_subcategories'] = results['unique_subcategories']
        
        if 'rows_processed' in results:
            df.at[idx, 'rows_processed'] = results['rows_processed']
        
        if 'unique_cities_markets' in results:
            df.at[idx, 'unique_cities_markets'] = results['unique_cities_markets']
        
        if 'extraction_method' in results:
            df.at[idx, 'extraction_method'] = results['extraction_method']
        
        if 'processing_duration_seconds' in results:
            df.at[idx, 'processing_duration_seconds'] = results['processing_duration_seconds']
        
        if 'processed_csv_path' in results:
            df.at[idx, 'processed_csv_path'] = results['processed_csv_path']
        
        if 'parsed_date' in results:
            df.at[idx, 'parsed_date'] = results['parsed_date']
        
        if 'pipeline_run_id' in results:
            df.at[idx, 'pipeline_run_id'] = results['pipeline_run_id']
        
        if 'error_occurred' in results:
            df.at[idx, 'error_occurred'] = results['error_occurred']
        
        if 'error_warnings' in results:
            df.at[idx, 'error_warnings'] = results['error_warnings']
        
        # Update processed_date and status
        df.at[idx, 'processed_date'] = datetime.now()
        df.at[idx, 'processing_status'] = 'processed' if not results.get('error_occurred', False) else 'error'
        
        # Write updated DataFrame
        self.write(df)
        self.logger.debug(f"Updated Excel {excel_id} with processing results")
        
        return True
    
    def mark_as_consolidated(self, excel_id, consolidated=True):
        """
        Mark an Excel file as consolidated.
        
        Args:
            excel_id (str): Excel ID
            consolidated (bool): Whether the Excel has been consolidated
            
        Returns:
            bool: True if successful, False if Excel not found
        """
        df = self.read()
        
        # Find the Excel
        excel_idx = df.index[df['excel_id'] == excel_id].tolist()
        
        if not excel_idx:
            self.logger.warning(f"Excel with ID {excel_id} not found in registry")
            return False
        
        # Update consolidated field
        df.at[excel_idx[0], 'consolidated'] = consolidated
        
        # Write updated DataFrame
        self.write(df)
        self.logger.debug(f"Marked Excel {excel_id} as {'consolidated' if consolidated else 'not consolidated'}")
        
        return True
    
    def get_pending_excels(self):
        """
        Get Excel files that need to be processed.
        
        Returns:
            pd.DataFrame: Records of Excel files that need processing
        """
        df = self.read()
        return df[df['processing_status'] == 'not_processed']
    
    def get_processed_excels(self, include_errors=False):
        """
        Get processed Excel files.
        
        Args:
            include_errors (bool): Whether to include Excel files with errors
            
        Returns:
            pd.DataFrame: Records of processed Excel files
        """
        df = self.read()
        
        if include_errors:
            return df[df['processing_status'].isin(['processed', 'error'])]
        else:
            return df[df['processing_status'] == 'processed']
    
    def get_unconsolidated_excels(self):
        """
        Get processed Excel files that have not been consolidated.
        
        Returns:
            pd.DataFrame: Records of unconsolidated Excel files
        """
        df = self.read()
        return df[(df['processing_status'] == 'processed') & ~df['consolidated']]
    
    def get_statistics(self):
        """
        Get processing statistics.
        
        Returns:
            dict: Dictionary of statistics
        """
        df = self.read()
        
        if df.empty:
            return {
                'total_excels': 0,
                'processed_excels': 0,
                'error_excels': 0,
                'pending_excels': 0,
                'success_rate': 0,
                'total_rows_processed': 0,
                'avg_rows_per_excel': 0,
                'consolidated_excels': 0
            }
        
        processed = (df['processing_status'] == 'processed').sum()
        errors = (df['processing_status'] == 'error').sum()
        pending = (df['processing_status'] == 'not_processed').sum()
        total = len(df)
        
        # Total rows processed
        total_rows = df['rows_processed'].sum()
        
        # Consolidated count
        consolidated = df['consolidated'].sum()
        
        return {
            'total_excels': total,
            'processed_excels': processed,
            'error_excels': errors,
            'pending_excels': pending,
            'success_rate': (processed / total * 100) if total > 0 else 0,
            'total_rows_processed': total_rows,
            'avg_rows_per_excel': (total_rows / processed) if processed > 0 else 0,
            'consolidated_excels': consolidated
        }
    
    def get_excels_by_date_range(self, start_date=None, end_date=None, with_errors=False):
        """
        Get Excel files within a date range.
        
        Args:
            start_date (datetime, optional): Start date (inclusive)
            end_date (datetime, optional): End date (inclusive)
            with_errors (bool): If True, include Excel files with errors
            
        Returns:
            pd.DataFrame: Excel files within the date range
        """
        df = self.read()
        
        # Apply error filter
        if not with_errors:
            df = df[~df['error_occurred']]
        
        # Apply date filters if specified
        if start_date:
            df = df[df['file_date'] >= pd.Timestamp(start_date)]
        
        if end_date:
            df = df[df['file_date'] <= pd.Timestamp(end_date)]
        
        return df