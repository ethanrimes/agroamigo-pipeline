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
    REGISTRY_DIR, REGISTRY_PARQUET, PIPELINE_RUN_ID
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
            'webscraper': self.registry_dir / 'webscraper_registry.parquet',
            'extractor': self.registry_dir / 'extractor_registry.parquet',
            'pdf': self.registry_dir / 'pdf_registry.parquet',
            'excel': self.registry_dir / 'excel_registry.parquet',
            'consolidation': self.registry_dir / 'consolidation_registry.parquet'
        }
        
        # Define foreign key relationships for integrity checks
        self.foreign_key_relationships = {
            'pdf': {
                'key_column': 'pdf_id',
                'related_registry': 'extractor',
                'related_key': 'pdf_id'
            },
            'excel': {
                'key_column': 'excel_id',
                'related_registry': 'extractor',
                'related_key': 'excel_id'
            },
            'consolidation': {
                'key_column': 'consolidated_id',
                'related_registries': ['pdf', 'excel'],
                'related_keys': ['pdf_id', 'excel_id']
            }
        }
        
        # Initialize statistics
        self.stats = {
            'total_errors': 0,
            'critical_errors': 0,
            'warnings': 0,
            'integrity_errors': 0,
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
            'integrity_errors': 0,
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
                
                # Process error records
                comp_errors, comp_stats = self._process_error_records(df, component)
                errors.extend(comp_errors)
                component_stats = comp_stats
                
                # Check for foreign key integrity errors
                if component in self.foreign_key_relationships:
                    integrity_errors, integrity_stats = self._check_foreign_key_integrity(df, component)
                    errors.extend(integrity_errors)
                    component_stats['integrity_errors'] = integrity_stats['integrity_errors']
                    component_stats['error_types']['integrity_error'] = integrity_stats['integrity_errors']
                    component_stats['error_files'] += integrity_stats['integrity_errors']
                
            except Exception as e:
                self.logger.error(f"Error reading registry file {registry_path}: {str(e)}")
                return errors, component_stats
            
            self.logger.info(f"Collected {len(errors)} errors from registry {component}")
            
            return errors, component_stats
            
        except Exception as e:
            self.logger.error(f"Error collecting errors from {component} registry: {str(e)}")
            self.logger.debug(f"Error details: {e}", exc_info=True)
            return errors, component_stats
    
    def _check_foreign_key_integrity(self, df, component):
        """
        Check for foreign key integrity issues in the registry.
        
        Args:
            df (DataFrame): Registry DataFrame
            component (str): Component name
            
        Returns:
            tuple: (List of error entries, dict with integrity statistics)
        """
        self.logger.info(f"Checking foreign key integrity for {component}")
        
        integrity_errors = []
        stats = {
            'integrity_errors': 0
        }
        
        try:
            # Get relationship details
            relationship = self.foreign_key_relationships[component]
            key_column = relationship['key_column']
            
            # For PDF and Excel components, check relationship with extractor
            if 'related_registry' in relationship:
                related_registry = relationship['related_registry']
                related_key = relationship['related_key']
                
                # Read related registry
                related_registry_path = self.registry_files[related_registry]
                if not os.path.exists(related_registry_path):
                    self.logger.warning(f"Related registry file does not exist: {related_registry_path}")
                    return integrity_errors, stats
                
                related_df = pd.read_parquet(related_registry_path)
                
                # Get set of keys from related registry
                related_keys = set()
                if related_key in related_df.columns:
                    related_keys = set(related_df[related_key].dropna().unique())
                
                # Check current registry for orphaned records
                if key_column in df.columns:
                    # Find records that should have related entries but don't
                    df_keys = set(df[key_column].dropna().unique())
                    orphaned_keys = df_keys - related_keys
                    
                    if orphaned_keys:
                        self.logger.warning(f"Found {len(orphaned_keys)} orphaned records in {component} registry")
                        
                        # Create error entries for each orphaned record
                        for orphaned_key in orphaned_keys:
                            orphaned_records = df[df[key_column] == orphaned_key]
                            
                            for _, row in orphaned_records.iterrows():
                                # Get filename and path if available
                                filename = row.get('filename', str(orphaned_key))
                                if isinstance(filename, Path):
                                    filename = str(filename)
                                
                                file_path = row.get('file_path', '')
                                if isinstance(file_path, Path):
                                    file_path = str(file_path)
                                
                                # Determine proper step name
                                step_name = component
                                if component == 'pdf':
                                    step_name = 'pdf processing'
                                elif component == 'excel':
                                    step_name = 'excel processing'
                                
                                error_entry = {
                                    'component': component,
                                    'filename': filename,
                                    'file_date': row.get('file_date', ''),
                                    'step_of_error': step_name,
                                    'error_message': f"Integrity error: {key_column} '{orphaned_key}' has no matching record in {related_registry} registry",
                                    'file_path': file_path,
                                    'error_type': 'integrity_error',
                                    'pipeline_run_id': PIPELINE_RUN_ID or '',
                                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                }
                                
                                integrity_errors.append(error_entry)
                    
                    stats['integrity_errors'] = len(integrity_errors)
            
            # For consolidation component, check against both PDF and Excel
            elif 'related_registries' in relationship:
                # Not implementing consolidated integrity checks here as it's more complex
                # Could be expanded in the future
                pass
            
            self.logger.info(f"Found {stats['integrity_errors']} integrity errors in {component} registry")
            return integrity_errors, stats
            
        except Exception as e:
            self.logger.error(f"Error checking foreign key integrity for {component}: {str(e)}")
            self.logger.debug(f"Error details: {e}", exc_info=True)
            return integrity_errors, stats
    
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
                'component': component_type,
                'filename': filename,
                'file_date': file_date,
                'step_of_error': step_name,
                'error_message': error_message,
                'file_path': file_path,
                'error_type': error_type,
                'pipeline_run_id': PIPELINE_RUN_ID or '',
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
            'critical_errors': 0,
            'integrity_errors': 0,
            'components': {}
        }
        
        all_errors = []
        
        # Collect errors from each component registry
        for component, registry_path in self.registry_files.items():
            errors, component_stats = self._collect_registry_errors(component, registry_path)
            all_errors.extend(errors)
            
            # Add this component's stats
            self.stats['components'][component] = component_stats
            self.stats['total_errors'] += component_stats['error_files']
            self.stats['integrity_errors'] += component_stats.get('integrity_errors', 0)
        
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
            
            # Save a copy to the main registry directory
            main_output_path = os.path.join(self.registry_dir, "error_summary.csv")
            error_df.to_csv(main_output_path, index=False)
            
            self.logger.info(f"Saved consolidated errors to {output_path}")
            
            # Generate summary statistics text file
            summary_path = os.path.join(self.error_summary_dir, f"error_summary_{timestamp}.txt")
            self._write_summary_text_file(summary_path)
            
            # Save a copy to the main registry directory
            main_summary_path = os.path.join(self.registry_dir, "error_statistics.txt")
            self._write_summary_text_file(main_summary_path)
            
            self.logger.info(f"Saved error summary to {summary_path}")
            
            # Log summary statistics
            self.logger.info(f"Error summary: {self.stats['total_errors']} total errors")
            self.logger.info(f"Integrity errors: {self.stats['integrity_errors']}")
            
            for component, stats in self.stats['components'].items():
                self.logger.info(f"{component}: {stats['error_files']} errors from {stats['total_files']} files")
            
            return error_df, summary_path
        else:
            self.logger.info("No errors found across all components")
            
            # Still write empty summary file
            summary_path = os.path.join(self.error_summary_dir, f"error_summary_{timestamp}.txt")
            self._write_summary_text_file(summary_path)
            
            # Create empty error DataFrame
            empty_df = pd.DataFrame(columns=[
                'component', 'filename', 'file_date', 'step_of_error', 
                'error_message', 'file_path', 'error_type', 'pipeline_run_id', 'timestamp'
            ])
            
            # Save empty DataFrame to CSV
            empty_output_path = os.path.join(self.error_summary_dir, f"error_summary_{timestamp}.csv")
            empty_df.to_csv(empty_output_path, index=False)
            
            # Save a copy to the main registry directory
            main_empty_path = os.path.join(self.registry_dir, "error_summary.csv")
            empty_df.to_csv(main_empty_path, index=False)
            
            self.logger.info(f"Saved empty error summary to {empty_output_path}")
            
            return empty_df, summary_path
    
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
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Pipeline Run ID: {PIPELINE_RUN_ID or 'Not specified'}\n\n")
                
                f.write("OVERALL STATISTICS\n")
                f.write("-----------------\n")
                f.write(f"Total errors: {self.stats['total_errors']}\n")
                f.write(f"Integrity errors: {self.stats['integrity_errors']}\n\n")
                
                # Write component-specific statistics
                f.write("COMPONENT STATISTICS\n")
                f.write("-------------------\n")
                
                for component, stats in sorted(self.stats['components'].items()):
                    f.write(f"\n{component.upper()}:\n")
                    f.write(f"  Total files processed: {stats['total_files']}\n")
                    f.write(f"  Files with errors: {stats['error_files']}\n")
                    if 'integrity_errors' in stats:
                        f.write(f"  Files with integrity errors: {stats['integrity_errors']}\n")
                    
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
    
    def get_errors_from_last_run(self, pipeline_run_id=None):
        """
        Get errors from the last pipeline run.
        
        Args:
            pipeline_run_id (str): Pipeline run ID to filter by
            
        Returns:
            DataFrame: Errors from the last run
        """
        log_function_call(self.logger, "get_errors_from_last_run", kwargs={"pipeline_run_id": pipeline_run_id})
        
        # Use current pipeline run ID if not provided
        pipeline_run_id = pipeline_run_id or PIPELINE_RUN_ID
        
        self.logger.info(f"Getting errors from run ID: {pipeline_run_id}")
        
        # First get all errors
        all_errors_df, _ = self.collect_all_errors()
        
        if all_errors_df.empty:
            self.logger.info("No errors found")
            return all_errors_df
            
        # Filter by pipeline run ID if available
        if pipeline_run_id and 'pipeline_run_id' in all_errors_df.columns:
            filtered_df = all_errors_df[all_errors_df['pipeline_run_id'] == pipeline_run_id]
            self.logger.info(f"Found {len(filtered_df)} errors for run ID {pipeline_run_id}")
            return filtered_df
        else:
            self.logger.info(f"No pipeline run ID specified or column doesn't exist, returning all {len(all_errors_df)} errors")
            return all_errors_df