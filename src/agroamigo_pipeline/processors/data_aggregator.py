"""
Updated data aggregator for Colombian agricultural price data.
Uses the new registry system for tracking consolidation.
"""

import os
import logging
import traceback
from datetime import datetime
import time
import uuid
from pathlib import Path
import pandas as pd

from agroamigo_pipeline.config.settings import (
    PROCESSED_DIR, CONSOLIDATED_DIR, PIPELINE_RUN_ID
)
from agroamigo_pipeline.utils.logger import setup_logging
from agroamigo_pipeline.utils.exceptions import DataAggregationError
from agroamigo_pipeline.registry.pdf_registry import PDFProcessorRegistry
from agroamigo_pipeline.registry.excel_registry import ExcelProcessorRegistry
from agroamigo_pipeline.registry.consolidation_registry import ConsolidationRegistry

class DataAggregator:
    """
    Aggregate processed CSV files into a consolidated dataset.
    """
    
    def __init__(self, input_dir=None, output_dir=None, logger=None):
        """
        Initialize the data aggregator.
        
        Args:
            input_dir (str): Directory containing processed CSV files
            output_dir (str): Directory to save consolidated output
            logger (Logger): Logger instance
        """
        # Set up logger if not provided
        if logger is None:
            logging_setup = setup_logging()
            self.logger = logging_setup['data_aggregator']
        else:
            self.logger = logger
            
        # Use provided paths or defaults from settings
        self.input_dir = input_dir or PROCESSED_DIR
        self.output_dir = output_dir or CONSOLIDATED_DIR
        
        # Ensure directories exist
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Initialize registries
        self.pdf_registry = PDFProcessorRegistry(logger=self.logger)
        self.excel_registry = ExcelProcessorRegistry(logger=self.logger)
        self.consolidation_registry = ConsolidationRegistry(logger=self.logger)
        
        # Initialize statistics
        self.stats = {
            'files_processed': 0,
            'rows_processed': 0,
            'rows_output': 0,
            'duplicates_removed': 0,
            'excel_files': 0,
            'pdf_files': 0,
            'errors': []
        }
        
        self.logger.info("DataAggregator initialized")
        self.logger.debug(f"Using input directory: {self.input_dir}")
        self.logger.debug(f"Using output directory: {self.output_dir}")
    
    def _standardize_dataframe(self, df, source_file):
        """
        Standardize DataFrame columns and data types.
        
        Args:
            df (DataFrame): DataFrame to standardize
            source_file (str): Source file path for reference
            
        Returns:
            DataFrame: Standardized DataFrame
        """
        try:
            # Expected columns in the target schema
            expected_columns = [
                'date', 'city', 'market', 'major_category', 'sub_category', 
                'product', 'presentation', 'units', 'daily_min', 'daily_max', 'source', 'source_filepath'
            ]
            
            # Add missing columns if needed
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None
                    self.logger.warning(f"Added missing column '{col}' to data from {os.path.basename(source_file)}")
            
            # Add source filepath
            df['source_filepath'] = source_file
            
            # Standardize date format
            if 'date' in df.columns:
                try:
                    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                except Exception as e:
                    self.logger.warning(f"Error converting date format in {os.path.basename(source_file)}: {str(e)}")
            
            # Ensure numeric columns are float
            numeric_columns = ['daily_min', 'daily_max']
            for col in numeric_columns:
                if col in df.columns:
                    try:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    except Exception as e:
                        self.logger.warning(f"Error converting {col} to numeric in {os.path.basename(source_file)}: {str(e)}")
            
            # Fill missing values with appropriate defaults
            df['sub_category'] = df['sub_category'].fillna('')
            df['presentation'] = df['presentation'].fillna('')
            df['units'] = df['units'].fillna('1 kilogramo')
            
            # Ensure all string columns are actually strings
            string_columns = [
                'city', 'market', 'major_category', 'sub_category', 
                'product', 'presentation', 'units', 'source', 'source_filepath'
            ]
            for col in string_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
            
            return df
            
        except Exception as e:
            error_msg = f"Error standardizing DataFrame from {source_file}: {str(e)}"
            self.logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return df
    
    def _remove_duplicates(self, df):
        """
        Remove duplicate entries from DataFrame.
        
        Args:
            df (DataFrame): DataFrame to deduplicate
            
        Returns:
            DataFrame: Deduplicated DataFrame
        """
        try:
            # Count initial rows
            initial_rows = len(df)
            
            # Define columns to consider for deduplication
            dedup_columns = [
                'date', 'city', 'market', 'major_category', 'sub_category', 
                'product', 'presentation', 'units', 'source'
                # Note: We intentionally exclude 'source_filepath' from deduplication
                # to avoid keeping duplicate data that comes from different files
            ]
            
            # Remove duplicates based on these columns
            df = df.drop_duplicates(subset=dedup_columns, keep='first')
            
            # Count duplicates removed
            duplicates_removed = initial_rows - len(df)
            self.stats['duplicates_removed'] += duplicates_removed
            
            if duplicates_removed > 0:
                self.logger.info(f"Removed {duplicates_removed} duplicate rows")
            
            return df
            
        except Exception as e:
            error_msg = f"Error removing duplicates: {str(e)}"
            self.logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return df
    
    def aggregate_data(self, filter_date_start=None, filter_date_end=None, output_file=None, update_registries=True):
        """
        Aggregate processed CSV files from Excel and PDF registries into a consolidated dataset.
        
        Args:
            filter_date_start (str): Filter data starting from this date (YYYY-MM-DD)
            filter_date_end (str): Filter data up to this date (YYYY-MM-DD)
            output_file (str): Name of output file (default: consolidated_YYYYMM.csv)
            update_registries (bool): Whether to update registry status
            
        Returns:
            DataFrame: Consolidated data
        """
        self.logger.info(f"Aggregating data from registries")
        if filter_date_start or filter_date_end:
            self.logger.info(f"Filtering data between {filter_date_start or 'beginning'} and {filter_date_end or 'end'}")
        
        # Reset statistics
        self.stats = {
            'files_processed': 0,
            'rows_processed': 0,
            'rows_output': 0,
            'duplicates_removed': 0,
            'excel_files': 0,
            'pdf_files': 0,
            'errors': []
        }
        
        # Start timer
        start_time = time.time()
        
        # Create consolidation ID for this run
        consolidation_id = str(uuid.uuid4())
        
        # Get processed Excel files from registry
        excel_df = self.excel_registry.get_processed_excels(include_errors=False)
        
        # Get processed PDF files from registry
        pdf_df = self.pdf_registry.get_processed_pdfs(include_errors=False)
        
        # Filter by date if specified
        month_key = None
        if filter_date_start or filter_date_end:
            # Convert filter dates to pandas datetime
            start_date = pd.to_datetime(filter_date_start) if filter_date_start else None
            end_date = pd.to_datetime(filter_date_end) if filter_date_end else None
            
            # Determine month key for output filename
            if start_date and start_date.year == (end_date.year if end_date else start_date.year):
                if end_date and start_date.month == end_date.month:
                    month_key = f"{start_date.year}-{start_date.month:02d}"
                else:
                    month_key = f"{start_date.year}"
            
            # Filter Excel files by parsed_date
            if not excel_df.empty:
                if start_date:
                    # Check both file_date and parsed_date, keep rows where either is within range
                    date_filter = (excel_df['file_date'] >= start_date) | \
                                (~pd.isna(excel_df['parsed_date']) & (excel_df['parsed_date'] >= start_date))
                    excel_df = excel_df[date_filter]
                
                if end_date:
                    # Check both file_date and parsed_date, keep rows where either is within range
                    date_filter = (excel_df['file_date'] <= end_date) | \
                                (~pd.isna(excel_df['parsed_date']) & (excel_df['parsed_date'] <= end_date))
                    excel_df = excel_df[date_filter]

            # Similarly for PDF files
            if not pdf_df.empty:
                if start_date:
                    # Check both file_date and parsed_date, keep rows where either is within range
                    date_filter = (pdf_df['file_date'] >= start_date) | \
                                (~pd.isna(pdf_df['parsed_date']) & (pdf_df['parsed_date'] >= start_date))
                    pdf_df = pdf_df[date_filter]
                
                if end_date:
                    # Check both file_date and parsed_date, keep rows where either is within range
                    date_filter = (pdf_df['file_date'] <= end_date) | \
                                (~pd.isna(pdf_df['parsed_date']) & (pdf_df['parsed_date'] <= end_date))
                    pdf_df = pdf_df[date_filter]
        
        # Fall back to current month if no date range specified
        if not month_key:
            now = datetime.now()
            month_key = f"{now.year}-{now.month:02d}"
        
        # Generate output filename if not provided
        if output_file is None:
            output_file = f"consolidated_{month_key}.csv"
        
        self.logger.info(f"Found {len(excel_df)} Excel files and {len(pdf_df)} PDF files to consolidate")
        
        # Initialize lists to collect DataFrames, IDs and paths
        dataframes = []
        excel_ids = []
        pdf_ids = []
        excel_paths = []
        pdf_paths = []
        
        # Process Excel files
        for _, row in excel_df.iterrows():
            csv_path = row['processed_csv_path']
            excel_id = row['excel_id']
            
            if not csv_path or not os.path.exists(csv_path):
                self.logger.warning(f"CSV file not found: {csv_path}")
                continue
            
            try:
                # Read CSV file
                df = pd.read_csv(csv_path)
                
                # Add source identifier
                df['source'] = 'excel'
                
                # Standardize DataFrame
                df = self._standardize_dataframe(df, csv_path)
                
                # Add to collection
                dataframes.append(df)
                excel_ids.append(excel_id)
                excel_paths.append(csv_path)
                
                # Update statistics
                self.stats['files_processed'] += 1
                self.stats['excel_files'] += 1
                self.stats['rows_processed'] += len(df)
                
                self.logger.debug(f"Added Excel file: {os.path.basename(csv_path)} with {len(df)} rows")
                
            except Exception as e:
                error_msg = f"Error processing Excel CSV {csv_path}: {str(e)}"
                self.logger.error(error_msg)
                self.stats['errors'].append(error_msg)
        
        # Process PDF files
        for _, row in pdf_df.iterrows():
            csv_path = row['processed_csv_path']
            pdf_id = row['pdf_id']
            
            if not csv_path or not os.path.exists(csv_path):
                self.logger.warning(f"CSV file not found: {csv_path}")
                continue
            
            try:
                # Read CSV file
                df = pd.read_csv(csv_path)
                
                # Add source identifier
                df['source'] = 'pdf'
                
                # Standardize DataFrame
                df = self._standardize_dataframe(df, csv_path)
                
                # Add to collection
                dataframes.append(df)
                pdf_ids.append(pdf_id)
                pdf_paths.append(csv_path)
                
                # Update statistics
                self.stats['files_processed'] += 1
                self.stats['pdf_files'] += 1
                self.stats['rows_processed'] += len(df)
                
                self.logger.debug(f"Added PDF file: {os.path.basename(csv_path)} with {len(df)} rows")
                
            except Exception as e:
                error_msg = f"Error processing PDF CSV {csv_path}: {str(e)}"
                self.logger.error(error_msg)
                self.stats['errors'].append(error_msg)
        
        # Check if we have any data to consolidate
        if not dataframes:
            self.logger.warning("No data to consolidate")
            # Register consolidation with error
            self.consolidation_registry.register_consolidation(
                pipeline_run_id=PIPELINE_RUN_ID or "",
                main_csv_path="",
                month_key=month_key,
                filter_date_start=filter_date_start,
                filter_date_end=filter_date_end,
                error="No data to consolidate"
            )
            return pd.DataFrame()
        
        # Combine all DataFrames
        self.logger.info(f"Combining {len(dataframes)} DataFrames")
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        # Remove duplicates
        self.logger.info(f"Removing duplicates from {len(combined_df)} rows")
        combined_df = self._remove_duplicates(combined_df)
        
        # Update statistics
        self.stats['rows_output'] = len(combined_df)
        
        # Generate output path
        output_path = os.path.join(self.output_dir, output_file)
        
        # Save to CSV
        combined_df.to_csv(output_path, index=False)
        self.logger.info(f"Saved consolidated data to {output_path}")
        
        # Calculate duration
        duration_seconds = time.time() - start_time
        
        # Register consolidation in registry
        consolidation_id = self.consolidation_registry.register_consolidation(
            pipeline_run_id=PIPELINE_RUN_ID or "",
            main_csv_path=output_path,
            month_key=month_key,
            excel_ids=excel_ids,
            pdf_ids=pdf_ids,
            filter_date_start=filter_date_start,
            filter_date_end=filter_date_end,
            num_excel_files=len(excel_ids),
            num_pdf_files=len(pdf_ids),
            total_rows=len(combined_df),
            duration_seconds=duration_seconds,
            duplicates_removed=self.stats['duplicates_removed']
        )
        
        # Mark files as consolidated in their respective registries
        if update_registries:
            self.logger.info("Updating registry status for consolidated files")
            
            # Update Excel files
            for excel_id in excel_ids:
                self.excel_registry.mark_as_consolidated(excel_id, True)
            
            # Update PDF files
            for pdf_id in pdf_ids:
                self.pdf_registry.mark_as_consolidated(pdf_id, True)
        
        # Export registry to CSV
        self.consolidation_registry.export_to_csv()
        
        # Log summary statistics
        self.logger.info(f"Consolidation summary: {self.stats['files_processed']} files processed "
                         f"({self.stats['excel_files']} Excel, {self.stats['pdf_files']} PDF), "
                         f"{self.stats['rows_processed']} rows processed, "
                         f"{self.stats['rows_output']} rows output, "
                         f"{self.stats['duplicates_removed']} duplicates removed")
        
        return combined_df