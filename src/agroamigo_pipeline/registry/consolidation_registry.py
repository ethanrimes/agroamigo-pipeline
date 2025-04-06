"""
Registry for tracking data consolidation.
Updated to include the consolidated_id column.
"""

import os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import logging
import uuid

from agroamigo_pipeline.registry.base_registry import BaseRegistry

class ConsolidationRegistry(BaseRegistry):
    """Registry to track data consolidation."""
    
    def __init__(self, logger=None):
        """Initialize the consolidation registry."""
        # Define schema for consolidation registry
        schema = pa.schema([
            ('consolidation_id', pa.string()),  # Primary key
            ('consolidated_id', pa.string()),   # Added ID column for unique identification
            ('pipeline_run_id', pa.string()),   # ID of the pipeline run
            ('consolidation_time', pa.timestamp('ns')),
            ('consolidation_duration_seconds', pa.float64()),
            ('main_csv_path', pa.string()),
            ('month_key', pa.string()),  # YYYY-MM format
            ('num_excel_files', pa.int32()),
            ('num_pdf_files', pa.int32()),
            ('total_rows', pa.int32()),
            ('error_occurred', pa.bool_()),
            ('error_message', pa.string()),
            ('excel_ids', pa.list_(pa.string())),  # Foreign keys to ExcelProcessorRegistry
            ('pdf_ids', pa.list_(pa.string())),  # Foreign keys to PDFProcessorRegistry
            ('filter_date_start', pa.string()),  # Date filter used for consolidation
            ('filter_date_end', pa.string()),  # Date filter used for consolidation
            ('duplicates_removed', pa.int32()),  # Number of duplicate rows removed
            ('data_quality_score', pa.float64())  # Optional quality score (0-100)
        ])
        
        # Initialize base registry
        super().__init__("consolidation", schema, logger)
        self.logger.debug("ConsolidationRegistry initialized")
    
    def get_consolidation_by_id(self, consolidation_id):
        """
        Get a consolidation record by its ID.
        
        Args:
            consolidation_id (str): Consolidation ID
            
        Returns:
            dict: Consolidation record or None if not found
        """
        df = self.read()
        matches = df[df['consolidation_id'] == consolidation_id]
        
        if matches.empty:
            return None
        
        # Convert to dictionary
        return matches.iloc[0].to_dict()
    
    def get_consolidation_by_consolidated_id(self, consolidated_id):
        """
        Get a consolidation record by its consolidated ID.
        
        Args:
            consolidated_id (str): Consolidated ID
            
        Returns:
            dict: Consolidation record or None if not found
        """
        df = self.read()
        matches = df[df['consolidated_id'] == consolidated_id]
        
        if matches.empty:
            return None
        
        # Convert to dictionary
        return matches.iloc[0].to_dict()
    
    def get_consolidation_by_month(self, month_key):
        """
        Get consolidation records for a specific month.
        
        Args:
            month_key (str): Month key in YYYY-MM format
            
        Returns:
            pd.DataFrame: Matching consolidation records
        """
        df = self.read()
        return df[df['month_key'] == month_key]
    
    def get_latest_consolidation(self):
        """
        Get the most recent consolidation record.
        
        Returns:
            dict: Latest consolidation record or None if no records
        """
        df = self.read()
        
        if df.empty:
            return None
        
        # Sort by consolidation time in descending order
        sorted_df = df.sort_values('consolidation_time', ascending=False)
        
        # Return the first record
        return sorted_df.iloc[0].to_dict()
    
    def register_consolidation(self, pipeline_run_id, main_csv_path, month_key, 
                            excel_ids=None, pdf_ids=None, 
                            filter_date_start=None, filter_date_end=None,
                            num_excel_files=0, num_pdf_files=0, total_rows=0,
                            duration_seconds=0.0, duplicates_removed=0,
                            error=None):
        """
        Register a data consolidation.
        
        Args:
            pipeline_run_id (str): ID of the pipeline run
            main_csv_path (str): Path to the consolidated CSV file
            month_key (str): Month key in YYYY-MM format
            excel_ids (list, optional): List of Excel IDs included in consolidation
            pdf_ids (list, optional): List of PDF IDs included in consolidation
            filter_date_start (str, optional): Start date for filtering (YYYY-MM-DD)
            filter_date_end (str, optional): End date for filtering (YYYY-MM-DD)
            num_excel_files (int): Number of Excel files consolidated
            num_pdf_files (int): Number of PDF files consolidated
            total_rows (int): Total rows in the consolidated CSV
            duration_seconds (float): Duration of consolidation in seconds
            duplicates_removed (int): Number of duplicate rows removed
            error (str, optional): Error message if consolidation failed
            
        Returns:
            str: Consolidation ID
        """
        df = self.read()
        
        # Generate new IDs
        consolidation_id = str(uuid.uuid4())
        consolidated_id = str(uuid.uuid4())
        
        # Ensure lists are not None and handle empty arrays safely
        excel_ids = excel_ids if excel_ids is not None else []
        pdf_ids = pdf_ids if pdf_ids is not None else []
        
        # Convert lists to arrays if they're empty
        if len(excel_ids) == 0:
            excel_ids = []
        if len(pdf_ids) == 0:
            pdf_ids = []
            
        # Create a new record
        new_record = {
            'consolidation_id': consolidation_id,
            'consolidated_id': consolidated_id,        # New unique ID
            'pipeline_run_id': pipeline_run_id,
            'consolidation_time': datetime.now(),
            'consolidation_duration_seconds': duration_seconds,
            'main_csv_path': main_csv_path,
            'month_key': month_key,
            'num_excel_files': num_excel_files,
            'num_pdf_files': num_pdf_files,
            'total_rows': total_rows,
            'error_occurred': error is not None,
            'error_message': error or '',
            'excel_ids': excel_ids,
            'pdf_ids': pdf_ids,
            'filter_date_start': filter_date_start or '',
            'filter_date_end': filter_date_end or '',
            'duplicates_removed': duplicates_removed,
            'data_quality_score': 0.0  # Set a default quality score
        }
        
        # Append new record
        new_df = pd.DataFrame([new_record])
        updated_df = pd.concat([df, new_df], ignore_index=True)
        
        # Write updated DataFrame
        self.write(updated_df)
        self.logger.debug(f"Registered consolidation for month: {month_key}")
        
        return consolidation_id
    
    def update_quality_score(self, consolidation_id, quality_score):
        """
        Update the data quality score for a consolidation.
        
        Args:
            consolidation_id (str): Consolidation ID
            quality_score (float): Quality score (0-100)
            
        Returns:
            bool: True if successful, False if consolidation not found
        """
        df = self.read()
        
        # Find the consolidation
        cons_idx = df.index[df['consolidation_id'] == consolidation_id].tolist()
        
        if not cons_idx:
            self.logger.warning(f"Consolidation with ID {consolidation_id} not found in registry")
            return False
        
        # Update quality score
        df.at[cons_idx[0], 'data_quality_score'] = max(0.0, min(100.0, quality_score))
        
        # Write updated DataFrame
        self.write(df)
        self.logger.debug(f"Updated quality score for consolidation {consolidation_id}")
        
        return True
    
    def get_statistics(self):
        """
        Get consolidation statistics.
        
        Returns:
            dict: Dictionary of statistics
        """
        df = self.read()
        
        if df.empty:
            return {
                'total_consolidations': 0,
                'successful_consolidations': 0,
                'failed_consolidations': 0,
                'total_rows_consolidated': 0,
                'total_files_consolidated': 0,
                'avg_quality_score': 0
            }
        
        successful = (~df['error_occurred']).sum()
        failed = df['error_occurred'].sum()
        total = len(df)
        
        # Total rows and files
        total_rows = df['total_rows'].sum()
        total_files = df['num_excel_files'].sum() + df['num_pdf_files'].sum()
        
        # Average quality score
        avg_quality = df['data_quality_score'].mean()
        
        return {
            'total_consolidations': total,
            'successful_consolidations': successful,
            'failed_consolidations': failed,
            'success_rate': (successful / total * 100) if total > 0 else 0,
            'total_rows_consolidated': total_rows,
            'total_files_consolidated': total_files,
            'avg_quality_score': avg_quality
        }
    
    def get_consolidated_files(self):
        """
        Get all Excel and PDF files that have been consolidated.
        
        Returns:
            tuple: (list of Excel IDs, list of PDF IDs)
        """
        df = self.read()
        
        if df.empty:
            return [], []
        
        # Filter for successful consolidations
        successful = df[~df['error_occurred']]
        
        # Collect all Excel and PDF IDs
        excel_ids = []
        pdf_ids = []
        
        for _, row in successful.iterrows():
            # Handle potentially empty arrays safely
            if isinstance(row['excel_ids'], list) and len(row['excel_ids']) > 0:
                excel_ids.extend(row['excel_ids'])
            
            if isinstance(row['pdf_ids'], list) and len(row['pdf_ids']) > 0:
                pdf_ids.extend(row['pdf_ids'])
        
        # Remove duplicates
        excel_ids = list(set(excel_ids))
        pdf_ids = list(set(pdf_ids))
        
        return excel_ids, pdf_ids
    
    def get_consolidations_by_date_range(self, start_date=None, end_date=None):
        """
        Get consolidations with filter dates within a specified range.
        
        Args:
            start_date (str, optional): Start date (YYYY-MM-DD)
            end_date (str, optional): End date (YYYY-MM-DD)
            
        Returns:
            pd.DataFrame: Consolidations within the date range
        """
        df = self.read()
        
        if df.empty:
            return df
        
        filtered_df = df.copy()
        
        # Filter by start_date
        if start_date:
            start_date_pd = pd.to_datetime(start_date)
            # Keep rows with empty filter_date_start or filter_date_start >= start_date
            mask = (filtered_df['filter_date_start'] == '') | \
                   (pd.to_datetime(filtered_df['filter_date_start'], errors='coerce') >= start_date_pd)
            filtered_df = filtered_df[mask]
        
        # Filter by end_date
        if end_date:
            end_date_pd = pd.to_datetime(end_date)
            # Keep rows with empty filter_date_end or filter_date_end <= end_date
            mask = (filtered_df['filter_date_end'] == '') | \
                   (pd.to_datetime(filtered_df['filter_date_end'], errors='coerce') <= end_date_pd)
            filtered_df = filtered_df[mask]
        
        return filtered_df
    
    def export_to_csv(self):
        """
        Export the consolidation registry to CSV without list columns.
        
        Returns:
            str: Path to the CSV file or None if empty
        """
        df = self.read()
        
        if df.empty:
            self.logger.warning(f"No data to export to CSV for {self.registry_name}")
            return None
        
        # Create a copy for export
        export_df = df.copy()
        
        # Specifically drop the list columns
        columns_to_exclude = ['excel_ids', 'pdf_ids']
        for col in columns_to_exclude:
            if col in export_df.columns:
                export_df = export_df.drop(columns=[col])
        
        # Format datetime columns
        for col in export_df.columns:
            if pd.api.types.is_datetime64_dtype(export_df[col]):
                export_df[col] = export_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Export to CSV
        export_df.to_csv(self.csv_path, index=False)
        self.logger.info(f"Exported consolidation registry to CSV: {self.csv_path}")
        
        return self.csv_path