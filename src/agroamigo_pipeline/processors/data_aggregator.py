import os
import pandas as pd
import numpy as np
from datetime import datetime
import re
from pathlib import Path
import logging
import json

from agroamigo_pipeline.config.settings import PROCESSED_DIR, CONSOLIDATED_DIR
from agroamigo_pipeline.utils.logger import setup_logging, log_function_call, log_dataframe_info
from agroamigo_pipeline.utils.path_manager import ensure_directory
from agroamigo_pipeline.utils.file_registry import FileRegistry

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
        ensure_directory(self.input_dir)
        ensure_directory(self.output_dir)
        
        # Initialize file registry
        self.registry = FileRegistry()
        
        # Initialize statistics
        self.stats = {
            'files_processed': 0,
            'rows_processed': 0,
            'rows_output': 0,
            'duplicates_removed': 0,
            'files_by_source': {},
            'errors': []
        }
        
        self.logger.info("DataAggregator initialized")
        self.logger.debug(f"Using input directory: {self.input_dir}")
        self.logger.debug(f"Using output directory: {self.output_dir}")
    
    def _validate_schema(self, df, expected_columns):
        """
        Validate that DataFrame has the expected schema.
        
        Args:
            df (DataFrame): DataFrame to validate
            expected_columns (list): List of expected column names
            
        Returns:
            bool: True if schema is valid, False otherwise
        """
        if set(expected_columns).issubset(set(df.columns)):
            return True
        
        missing_columns = set(expected_columns) - set(df.columns)
        self.logger.warning(f"Missing columns in DataFrame: {missing_columns}")
        return False
    
    def _read_csv_file(self, file_path):
        """
        Read a CSV file into a DataFrame with error handling.
        
        Args:
            file_path (str): Path to the CSV file
            
        Returns:
            DataFrame: The loaded DataFrame or None if error
        """
        try:
            df = pd.read_csv(file_path)
            
            # Add source filename column for reference
            df['source_filename'] = os.path.basename(file_path)
            
            return df
        except Exception as e:
            error_msg = f"Error reading CSV file {file_path}: {str(e)}"
            self.logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return None
    
    def _extract_date_from_filename(self, filename):
        """
        Extract date from filename with format yyyymmdd-restoffilename.
        
        Args:
            filename (str): Filename to extract date from
            
        Returns:
            datetime or None: Extracted date or None if no date found
        """
        # Look for the pattern yyyymmdd at the beginning of the filename
        match = re.match(r'^(\d{4})(\d{2})(\d{2})[-_]?', filename)
        
        if match:
            year, month, day = match.groups()
            try:
                return datetime(int(year), int(month), int(day))
            except ValueError as e:
                self.logger.warning(f"Invalid date in filename {filename}: {str(e)}")
                return None
        else:
            self.logger.debug(f"No date pattern found in filename: {filename}")
            return None
    
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
                'product', 'presentation', 'units', 'daily_min', 'daily_max', 'source'
            ]
            
            # Check if DataFrame has the required columns
            if not self._validate_schema(df, expected_columns):
                # If missing columns, try to map or add default values
                for col in expected_columns:
                    if col not in df.columns:
                        df[col] = None
                
                self.logger.warning(f"Added missing columns to DataFrame from {os.path.basename(source_file)}")
            
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
                'product', 'presentation', 'units', 'source', 'source_filename'
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
    
    def _resolve_conflicting_data(self, df):
        """
        Resolve conflicting data in the DataFrame.
        
        Args:
            df (DataFrame): DataFrame to resolve conflicts in
            
        Returns:
            DataFrame: DataFrame with conflicts resolved
        """
        try:
            # For conflicting price data, we can use the approach:
            # - When multiple sources report the same product/date/market, average the prices
            
            # Group by all non-price columns and calculate mean for price columns
            groupby_columns = [
                'date', 'city', 'market', 'major_category', 'sub_category', 
                'product', 'presentation', 'units', 'source'
            ]
            
            df = df.groupby(groupby_columns, as_index=False).agg({
                'daily_min': 'mean',
                'daily_max': 'mean',
                'source_filename': lambda x: ', '.join(sorted(set(x)))
            })
            
            return df
            
        except Exception as e:
            error_msg = f"Error resolving conflicting data: {str(e)}"
            self.logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            return df
    
    def aggregate_data(self, filter_date_start=None, filter_date_end=None, output_file=None):
        """
        Aggregate all processed CSV files into a consolidated dataset.
        
        Args:
            filter_date_start (str): Filter data starting from this date (YYYY-MM-DD)
            filter_date_end (str): Filter data up to this date (YYYY-MM-DD)
            output_file (str): Name of output file (default: consolidated_{timestamp}.csv)
            
        Returns:
            DataFrame: Consolidated data
        """
        log_function_call(self.logger, "aggregate_data", 
                         kwargs={"filter_date_start": filter_date_start,
                                 "filter_date_end": filter_date_end,
                                 "output_file": output_file})
        
        self.logger.info(f"Aggregating data from {self.input_dir}")
        if filter_date_start or filter_date_end:
            self.logger.info(f"Filtering data between {filter_date_start or 'beginning'} and {filter_date_end or 'end'}")
        
        # Reset statistics
        self.stats = {
            'files_processed': 0,
            'rows_processed': 0,
            'rows_output': 0,
            'duplicates_removed': 0,
            'files_by_source': {},
            'files_by_date': {},
            'errors': []
        }
        
        # Get all CSV files in the input directory
        csv_files = list(Path(self.input_dir).glob('**/*.csv'))
        self.logger.info(f"Found {len(csv_files)} CSV files in input directory")
        
        # Convert filter dates to datetime objects if provided
        filter_start_dt = None
        filter_end_dt = None
        if filter_date_start:
            try:
                filter_start_dt = datetime.strptime(filter_date_start, '%Y-%m-%d')
                self.logger.debug(f"Converted filter_date_start to datetime: {filter_start_dt}")
            except ValueError as e:
                self.logger.error(f"Invalid filter_date_start format: {str(e)}")
        
        if filter_date_end:
            try:
                filter_end_dt = datetime.strptime(filter_date_end, '%Y-%m-%d')
                self.logger.debug(f"Converted filter_date_end to datetime: {filter_end_dt}")
            except ValueError as e:
                self.logger.error(f"Invalid filter_date_end format: {str(e)}")
        
        # Filter CSV files by date in filename
        filtered_csv_files = []
        skipped_files = []
        
        for file_path in csv_files:
            filename = os.path.basename(str(file_path))
            file_date = self._extract_date_from_filename(filename)
            
            if file_date:
                # Track files by date
                date_str = file_date.strftime('%Y-%m-%d')
                if date_str not in self.stats['files_by_date']:
                    self.stats['files_by_date'][date_str] = 0
                self.stats['files_by_date'][date_str] += 1
                
                # Apply date filtering if specified
                if filter_start_dt and file_date < filter_start_dt:
                    self.logger.debug(f"Skipping file {filename} - date {file_date} is before filter start {filter_start_dt}")
                    skipped_files.append((filename, "before filter start"))
                    continue
                
                if filter_end_dt and file_date > filter_end_dt:
                    self.logger.debug(f"Skipping file {filename} - date {file_date} is after filter end {filter_end_dt}")
                    skipped_files.append((filename, "after filter end"))
                    continue
                
                # File passed date filtering, add to list
                filtered_csv_files.append(file_path)
                
            else:
                self.logger.warning(f"No date found in filename: {filename}")
                if not filter_start_dt and not filter_end_dt:
                    # If no date filters applied, include file anyway
                    filtered_csv_files.append(file_path)
                    self.logger.debug(f"Including file without date: {filename}")
                else:
                    # If date filters applied, skip files without dates
                    skipped_files.append((filename, "no date in filename"))
                    self.logger.debug(f"Skipping file without date: {filename}")
        
        # Log filtering results
        self.logger.info(f"After date filtering: {len(filtered_csv_files)} files included, {len(skipped_files)} files skipped")
        
        # Read and process each filtered CSV file
        dataframes = []
        
        for file_path in filtered_csv_files:
            file_str = str(file_path)
            filename = os.path.basename(file_str)
            
            self.logger.debug(f"Processing file: {filename}")
            
            # Read CSV file
            df = self._read_csv_file(file_str)
            
            if df is None or df.empty:
                self.logger.warning(f"Empty or invalid CSV file: {filename}")
                continue
            
            # Standardize DataFrame
            df = self._standardize_dataframe(df, file_str)
            
            # Skip empty DataFrames
            if df.empty:
                self.logger.warning(f"No data after standardization in file: {filename}")
                continue
            
            # Track statistics
            self.stats['files_processed'] += 1
            self.stats['rows_processed'] += len(df)
            
            # Track files by source
            source = df['source'].iloc[0] if 'source' in df.columns and not df.empty else 'unknown'
            if source not in self.stats['files_by_source']:
                self.stats['files_by_source'][source] = 0
            self.stats['files_by_source'][source] += 1
            
            dataframes.append(df)
            
            self.logger.debug(f"Processed file {filename}: {len(df)} rows")
        
        # If no valid data was found, return empty DataFrame
        if not dataframes:
            self.logger.warning("No valid data found in input files")
            return pd.DataFrame()
        
        # Combine all DataFrames
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        # Remove duplicates
        combined_df = self._remove_duplicates(combined_df)
        
        # Resolve conflicting data
        combined_df = self._resolve_conflicting_data(combined_df)
        
        # Update statistics
        self.stats['rows_output'] = len(combined_df)
        
        # Generate output filename
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            output_file = f"consolidated_{timestamp}.csv"
        
        output_path = os.path.join(self.output_dir, output_file)
        
        # Save to CSV
        combined_df.to_csv(output_path, index=False)
        self.logger.info(f"Saved consolidated data to {output_path}")
        
        # Generate statistics file
        stats_path = os.path.join(self.output_dir, f"stats_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2)
        
        self.logger.info(f"Saved aggregation statistics to {stats_path}")
        
        # Log summary statistics
        self.logger.info(f"Aggregation summary: {self.stats['files_processed']} files processed, "
                         f"{self.stats['rows_processed']} rows processed, "
                         f"{self.stats['rows_output']} rows output, "
                         f"{self.stats['duplicates_removed']} duplicates removed")
        
        return combined_df