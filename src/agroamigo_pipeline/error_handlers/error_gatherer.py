"""
Enhanced error gatherer that collects errors from all pipeline components
and consolidates them into summary reports.
"""

import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
import logging

from agroamigo_pipeline.config.settings import (
    ERROR_LOG_DIR, SCRAPER_LOG_DIR, EXTRACTOR_LOG_DIR, 
    EXCEL_LOG_DIR, PDF_LOG_DIR, MIN_UNIQUE_PRODUCTS,
    MAX_UNIQUE_PRODUCTS, MIN_MAJOR_CATEGORIES, MAX_MAJOR_CATEGORIES,
    REGISTRY_DIR, REGISTRY_PARQUET
)
from agroamigo_pipeline.utils.logger import setup_logging, log_function_call, log_dataframe_info
from agroamigo_pipeline.utils.path_manager import ensure_directory

class ErrorGatherer:
    """Collect and consolidate error logs from all pipeline components."""
    
    def __init__(self, output_dir=None, logger=None):
        """
        Initialize the error gatherer.
        
        Args:
            output_dir (str): Directory to save consolidated error files
            logger (Logger): Logger instance
        """
        # Set up logger if not provided
        if logger is None:
            logging_setup = setup_logging()
            self.logger = logging_setup['error_handler']
        else:
            self.logger = logger
            
        # Use provided path or default from settings
        self.output_dir = output_dir or ERROR_LOG_DIR
        ensure_directory(self.output_dir)
        
        # Create error summary directory within registry
        self.registry_dir = Path(REGISTRY_DIR)
        self.error_summary_dir = self.registry_dir / 'error_summaries'
        ensure_directory(self.error_summary_dir)
        
        # Define registry paths to scan - use the unified registry
        self.registry_files = {
            'unified': Path(REGISTRY_PARQUET),
            'scraper': self.registry_dir / 'scraper_registry.parquet',
            'extractor': self.registry_dir / 'zip_registry.parquet'
        }
        
        # Initialize statistics
        self.stats = {
            'total_errors': 0,
            'critical_errors': 0,
            'warnings': 0,
            'components': {}
        }
        
        self.logger.info("ErrorGatherer initialized")
        self.logger.debug(f"Using output directory: {self.output_dir}")
        self.logger.debug(f"Error summary directory: {self.error_summary_dir}")
        
    def _collect_registry_errors(self, component, registry_path):
        """
        Collect errors from a component's registry.
        
        Args:
            component (str): Component name
            registry_path (Path): Path to the registry parquet file
            
        Returns:
            list: List of error entries
            dict: Component statistics
        """
        self.logger.info(f"Collecting errors from registry: {component}")
        
        errors = []
        component_stats = {
            'total_files': 0,
            'error_files': 0,
            'warning_files': 0,
            'error_types': {}
        }
        
        try:
            # Check if the registry exists
            if not os.path.exists(registry_path):
                self.logger.warning(f"Registry file does not exist: {registry_path}")
                return errors, component_stats
            
            # Read the parquet registry
            try:
                df = pd.read_parquet(registry_path)
                log_dataframe_info(self.logger, df, f"{component} registry")
                
                # For unified registry, filter by component type if needed
                if component == 'unified':
                    # Check if a component type column exists
                    if 'component_type' in df.columns:
                        # We can create separate stats for each component type
                        component_types = df['component_type'].unique()
                        for comp_type in component_types:
                            self.logger.info(f"Processing component type: {comp_type}")
                            type_df = df[df['component_type'] == comp_type]
                            type_errors, type_stats = self._process_error_records(type_df, comp_type)
                            errors.extend(type_errors)
                            self.stats['components'][comp_type] = type_stats
                    else:
                        # Process all records without filtering
                        type_errors, type_stats = self._process_error_records(df, 'unified')
                        errors.extend(type_errors)
                        self.stats['components']['unified'] = type_stats
                else:
                    # For component-specific registries
                    comp_errors, comp_stats = self._process_error_records(df, component)
                    errors.extend(comp_errors)
                    component_stats = comp_stats
                
            except Exception as e:
                self.logger.error(f"Error reading registry file {registry_path}: {str(e)}")
                return errors, component_stats
            
            self.logger.info(f"Collected {len(errors)} errors from registry {component}")
            
            return errors, component_stats
            
        except Exception as e:
            self.logger.error(f"Error collecting errors from {component} registry: {str(e)}")
            self.logger.debug(f"Error details: {e}", exc_info=True)
            return errors, component_stats
    
    def _process_error_records(self, df, component_type):
        """
        Process error records from a DataFrame.
        
        Args:
            df (DataFrame): DataFrame containing error records
            component_type (str): Component type for step name
            
        Returns:
            list: List of error entries
            dict: Component statistics
        """
        errors = []
        component_stats = {
            'total_files': len(df),
            'error_files': 0,
            'warning_files': 0,
            'error_types': {}
        }
        
        # Check if the registry has the expected error columns
        if 'error_occurred' not in df.columns:
            self.logger.warning(f"Registry for {component_type} does not have 'error_occurred' column")
            return errors, component_stats
        
        # Filter for error records
        error_df = df[df['error_occurred'] == True]
        component_stats['error_files'] = len(error_df)
        
        # Determine step name based on component type
        step_name = component_type
        if component_type == 'extractor':
            step_name = 'zip extraction'
        elif component_type == 'pdf':
            step_name = 'pdf processing'
        elif component_type == 'excel':
            step_name = 'excel processing'
        elif component_type == 'scraper':
            step_name = 'scraping'
        
        # Process each error record
        for _, row in error_df.iterrows():
            # Get filename
            filename = row.get('filename', '')
            
            # Get error message
            error_message = row.get('error_message', '')
            if not error_message and 'errors_warnings' in row:
                error_message = row.get('errors_warnings', '')
                
            # Get path
            file_path = row.get('file_path', '')
            if not file_path and 'source_path' in row:
                file_path = row.get('source_path', '')
            
            # Get date from file if possible
            file_date = ""
            if 'date_from_filename' in row and row['date_from_filename']:
                file_date = row['date_from_filename']
            elif 'file_date' in row:
                file_date = row['file_date']
            elif filename:
                # Try to extract date from filename (format: YYYYMMDD)
                import re
                date_match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
                if date_match:
                    file_date = ''.join(date_match.groups())
            
            # Determine error type
            error_type = "generic_error"
            if 'status' in row:
                error_type = row['status'] if row['status'] != 'error' else 'generic_error'
            elif 'error_type' in row:
                error_type = row['error_type']
            
            # Update error type counts
            component_stats['error_types'][error_type] = component_stats['error_types'].get(error_type, 0) + 1
            
            # Create error entry
            error_entry = {
                'filename': filename,
                'date_documented': file_date,
                'step_of_error': step_name,
                'error_message': error_message,
                'filepath': file_path,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            errors.append(error_entry)
        
        return errors, component_stats
    
    def collect_all_errors(self, output_file=None):
        """
        Collect errors from all components and generate consolidated error report.
        
        Args:
            output_file (str): Name of output file (default: consolidated_errors_{timestamp}.csv)
            
        Returns:
            tuple: (DataFrame of errors, path to summary file)
        """
        log_function_call(self.logger, "collect_all_errors", kwargs={"output_file": output_file})
        
        self.logger.info("Collecting errors from all components")
        
        # Reset statistics
        self.stats = {
            'total_errors': 0,
            'components': {}
        }
        
        all_errors = []
        
        # Collect errors from each component registry
        for component, registry_path in self.registry_files.items():
            errors, component_stats = self._collect_registry_errors(component, registry_path)
            all_errors.extend(errors)
            
            # Only add this component's stats if it wasn't processed as part of unified
            if component != 'unified' and component not in self.stats['components']:
                self.stats['components'][component] = component_stats
                self.stats['total_errors'] += component_stats['error_files']
        
        # Generate timestamp for filenames
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        
        # Create DataFrame from all errors
        if all_errors:
            error_df = pd.DataFrame(all_errors)
            log_dataframe_info(self.logger, error_df, "consolidated_errors")
            
            # Generate output filename
            if output_file is None:
                output_file = f"consolidated_errors_{timestamp}.csv"
            
            # Ensure the directory exists
            os.makedirs(self.error_summary_dir, exist_ok=True)
            
            # Save to CSV
            output_path = os.path.join(self.error_summary_dir, output_file)
            error_df.to_csv(output_path, index=False)
            self.logger.info(f"Saved consolidated errors to {output_path}")
            
            # Generate summary statistics text file
            summary_path = os.path.join(self.error_summary_dir, f"error_summary_{timestamp}.txt")
            self._write_summary_text_file(summary_path)
            self.logger.info(f"Saved error summary to {summary_path}")
            
            # Log summary statistics
            self.logger.info(f"Error summary: {self.stats['total_errors']} total errors")
            
            for component, stats in self.stats['components'].items():
                self.logger.info(f"{component}: {stats['error_files']} errors from {stats['total_files']} files")
            
            return error_df, summary_path
        else:
            self.logger.info("No errors found across all components")
            
            # Still write empty summary file
            summary_path = os.path.join(self.error_summary_dir, f"error_summary_{timestamp}.txt")
            self._write_summary_text_file(summary_path)
            
            return pd.DataFrame(), summary_path
    
    def _write_summary_text_file(self, path):
        """
        Write error summary statistics to a text file.
        
        Args:
            path (str): Path to write the summary file
        """
        try:
            with open(path, 'w') as f:
                f.write("AGROAMIGO PIPELINE ERROR SUMMARY\n")
                f.write("===============================\n\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("OVERALL STATISTICS\n")
                f.write("-----------------\n")
                f.write(f"Total errors: {self.stats['total_errors']}\n\n")
                
                # Write component-specific statistics
                f.write("COMPONENT STATISTICS\n")
                f.write("-------------------\n")
                
                for component, stats in sorted(self.stats['components'].items()):
                    f.write(f"\n{component.upper()}:\n")
                    f.write(f"  Total files processed: {stats['total_files']}\n")
                    f.write(f"  Files with errors: {stats['error_files']}\n")
                    
                    if stats['total_files'] > 0:
                        error_rate = (stats['error_files'] / stats['total_files']) * 100
                        f.write(f"  Error rate: {error_rate:.2f}%\n")
                    
                    # Write error types if available
                    if stats['error_types']:
                        f.write("  Error types:\n")
                        for error_type, count in sorted(stats['error_types'].items(), key=lambda x: x[1], reverse=True):
                            f.write(f"    - {error_type}: {count}\n")
                
                f.write("\n\nEnd of report\n")
        except Exception as e:
            self.logger.error(f"Error writing summary file: {str(e)}")
            self.logger.debug(f"Error details: {e}", exc_info=True)
            
    def consolidate_error_files(self, min_products=None, max_products=None, 
                              min_categories=None, max_categories=None,
                              output_file=None):
        """
        Consolidate error files from all components and apply filtering rules.
        
        Args:
            min_products (int): Minimum threshold for unique products
            max_products (int): Maximum threshold for unique products
            min_categories (int): Minimum threshold for unique categories
            max_categories (int): Maximum threshold for unique categories
            output_file (str): Name of output file (default: consolidated_errors_{timestamp}.csv)
            
        Returns:
            DataFrame: Consolidated error data
        """
        log_function_call(self.logger, "consolidate_error_files", 
                         kwargs={"min_products": min_products,
                                 "max_products": max_products,
                                 "min_categories": min_categories,
                                 "max_categories": max_categories,
                                 "output_file": output_file})
        
        # Use thresholds from settings if not provided
        min_products = min_products or MIN_UNIQUE_PRODUCTS
        max_products = max_products or MAX_UNIQUE_PRODUCTS
        min_categories = min_categories or MIN_MAJOR_CATEGORIES
        max_categories = max_categories or MAX_MAJOR_CATEGORIES
        
        self.logger.info(f"Consolidating error files with thresholds: "
                       f"products={min_products}-{max_products}, "
                       f"categories={min_categories}-{max_categories}")
        
        # Delegate to collect_all_errors for the actual work
        return self.collect_all_errors(output_file=output_file)[0]
    
    def _collect_component_errors(self, component, log_dir):
        """
        Collect errors from a specific component's log files.
        
        Args:
            component (str): Component name
            log_dir (str): Directory containing component logs
            
        Returns:
            list: List of error entries
            dict: Component statistics
        """
        self.logger.info(f"Collecting errors from log files for component: {component}")
        
        errors = []
        component_stats = {
            'total_files': 0,
            'error_files': 0,
            'warning_files': 0,
            'error_types': {}
        }
        
        try:
            # Check if the directory exists
            if not os.path.exists(log_dir):
                self.logger.warning(f"Log directory does not exist: {log_dir}")
                return errors, component_stats
            
            # Get all error log files
            error_logs = []
            
            # Different components might have different log file patterns
            if component == 'excel':
                # For Excel processor, look for CSV logs
                error_log_pattern = os.path.join(log_dir, "*.csv")
                error_logs.extend(list(Path(log_dir).glob("*.csv")))
                
            elif component == 'pdf':
                # For PDF processor, look for CSV logs
                error_log_pattern = os.path.join(log_dir, "*.csv")
                error_logs.extend(list(Path(log_dir).glob("*.csv")))
                
            else:
                # For other components, look for JSON logs
                error_log_pattern = os.path.join(log_dir, "*.json")
                error_logs.extend(list(Path(log_dir).glob("*.json")))
            
            self.logger.debug(f"Found {len(error_logs)} error logs matching {error_log_pattern}")
            
            # Process each error log
            for log_file in error_logs:
                try:
                    if str(log_file).endswith('.csv'):
                        # Read CSV error log
                        df = pd.read_csv(log_file)
                        log_function_call(self.logger, "pd.read_csv", args=(log_file,))
                        log_dataframe_info(self.logger, df, f"{component} error log")
                        
                        # Process each row
                        for _, row in df.iterrows():
                            component_stats['total_files'] += 1
                            
                            # Check for errors
                            has_error = False
                            
                            if 'error_occurred' in row and row['error_occurred']:
                                has_error = True
                                error_msg = row.get('errors_warnings', 'Unknown error')
                                
                                # Count error types
                                error_type = "generic_error"
                                if 'unique_products' in row and row['unique_products'] < MIN_UNIQUE_PRODUCTS:
                                    error_type = "too_few_products"
                                elif 'unique_major_categories' in row and row['unique_major_categories'] < MIN_MAJOR_CATEGORIES:
                                    error_type = "too_few_categories"
                                
                                component_stats['error_types'][error_type] = component_stats['error_types'].get(error_type, 0) + 1
                                
                            elif ('unique_products' in row and 
                                  (row['unique_products'] < MIN_UNIQUE_PRODUCTS or 
                                   row['unique_products'] > MAX_UNIQUE_PRODUCTS)):
                                has_error = True
                                error_type = "product_count_outside_range"
                                error_message = f"Product count ({row['unique_products']}) outside acceptable range"
                                component_stats['error_types'][error_type] = component_stats['error_types'].get(error_type, 0) + 1
                                
                            elif ('unique_major_categories' in row and 
                                  (row['unique_major_categories'] < MIN_MAJOR_CATEGORIES or 
                                   row['unique_major_categories'] > MAX_MAJOR_CATEGORIES)):
                                has_error = True
                                error_type = "category_count_outside_range"
                                error_message = f"Category count ({row['unique_major_categories']}) outside acceptable range"
                                component_stats['error_types'][error_type] = component_stats['error_types'].get(error_type, 0) + 1
                            
                            # Only add to errors if there's actually an error
                            if has_error:
                                component_stats['error_files'] += 1
                                
                                # Determine step name based on component
                                step_name = component
                                if component == 'excel':
                                    step_name = 'excel processing'
                                elif component == 'pdf':
                                    step_name = 'pdf processing'
                                
                                # Create error entry
                                error_entry = {
                                    'filename': row.get('filename', os.path.basename(str(log_file))),
                                    'date_documented': '',  # Extract date if possible
                                    'step_of_error': step_name,
                                    'error_message': error_message if 'error_message' in locals() else 'Unknown error',
                                    'filepath': row.get('file_path', ''),
                                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                }
                                
                                errors.append(error_entry)
                    
                    elif str(log_file).endswith('.json'):
                        # Read JSON error log
                        with open(log_file, 'r', encoding='utf-8') as f:
                            log_data = json.load(f)
                            
                        # Process the JSON data
                        if isinstance(log_data, list):
                            # Handle list of errors
                            for entry in log_data:
                                component_stats['total_files'] += 1
                                
                                if entry.get('errors') or entry.get('fatal'):
                                    component_stats['error_files'] += 1
                                    error_type = "generic_error"
                                    
                                    if entry.get('fatal'):
                                        error_type = "fatal_error"
                                    
                                    component_stats['error_types'][error_type] = component_stats['error_types'].get(error_type, 0) + 1
                                    
                                    # Determine step name based on component
                                    step_name = component
                                    if component == 'extractor':
                                        step_name = 'zip extraction'
                                    elif component == 'scraper':
                                        step_name = 'scraping'
                                    
                                    errors.append({
                                        'filename': entry.get('filename', os.path.basename(str(log_file))),
                                        'date_documented': '',  # Extract date if possible
                                        'step_of_error': step_name,
                                        'error_message': str(entry.get('errors', 'Unknown error')),
                                        'filepath': entry.get('fullpath', ''),
                                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    })
                                    
                                elif entry.get('warnings'):
                                    component_stats['warning_files'] += 1
                        
                        elif isinstance(log_data, dict):
                            # Handle single error entry
                            component_stats['total_files'] += 1
                            
                            if log_data.get('errors') or log_data.get('fatal'):
                                component_stats['error_files'] += 1
                                error_type = "generic_error"
                                
                                if log_data.get('fatal'):
                                    error_type = "fatal_error"
                                
                                component_stats['error_types'][error_type] = component_stats['error_types'].get(error_type, 0) + 1
                                
                                # Determine step name based on component
                                step_name = component
                                if component == 'extractor':
                                    step_name = 'zip extraction'
                                elif component == 'scraper':
                                    step_name = 'scraping'
                                
                                errors.append({
                                    'filename': log_data.get('filename', os.path.basename(str(log_file))),
                                    'date_documented': '',  # Extract date if possible
                                    'step_of_error': step_name,
                                    'error_message': str(log_data.get('errors', 'Unknown error')),
                                    'filepath': log_data.get('fullpath', ''),
                                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                })
                                
                            elif log_data.get('warnings'):
                                component_stats['warning_files'] += 1
                
                except Exception as e:
                    self.logger.error(f"Error processing log file {log_file}: {str(e)}")
                    self.logger.debug(f"Error details: {e}", exc_info=True)
            
            self.logger.info(f"Collected {len(errors)} errors from {component_stats['total_files']} files for component {component}")
            
            return errors, component_stats
            
        except Exception as e:
            self.logger.error(f"Error collecting errors for component {component}: {str(e)}")
            self.logger.debug(f"Error details: {e}", exc_info=True)
            return errors, component_stats