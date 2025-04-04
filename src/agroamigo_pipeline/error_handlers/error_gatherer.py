import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

from agroamigo_pipeline.config.settings import (
    ERROR_LOG_DIR, SCRAPER_LOG_DIR, EXTRACTOR_LOG_DIR, 
    EXCEL_LOG_DIR, PDF_LOG_DIR, MIN_UNIQUE_PRODUCTS,
    MAX_UNIQUE_PRODUCTS, MIN_MAJOR_CATEGORIES, MAX_MAJOR_CATEGORIES
)
from agroamigo_pipeline.utils.logger import setup_logging, log_function_call, log_dataframe_info
from agroamigo_pipeline.utils.path_manager import ensure_directory
from agroamigo_pipeline.utils.file_registry import FileRegistry

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
        
        # Initialize file registry
        self.registry = FileRegistry()
        
        # Define log directories to scan
        self.log_dirs = {
            'scraper': SCRAPER_LOG_DIR,
            'extractor': EXTRACTOR_LOG_DIR,
            'excel': EXCEL_LOG_DIR,
            'pdf': PDF_LOG_DIR
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
        
    def _collect_component_errors(self, component, log_dir):
        """
        Collect errors from a specific component.
        
        Args:
            component (str): Component name
            log_dir (str): Directory containing component logs
            
        Returns:
            list: List of error entries
        """
        self.logger.info(f"Collecting errors for component: {component}")
        
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
                                error_message = row.get('errors_warnings', 'Unknown error')
                                
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
                                
                                # Create error entry
                                error_entry = {
                                    'component': component,
                                    'filename': row.get('filename', os.path.basename(str(log_file))),
                                    'file_path': row.get('file_path', ''),
                                    'error_type': error_type if 'error_type' in locals() else 'unknown',
                                    'error_message': error_message if 'error_message' in locals() else 'Unknown error',
                                    'timestamp': datetime.now().isoformat(),
                                    'unique_products': row.get('unique_products', 0),
                                    'unique_categories': row.get('unique_major_categories', 0),
                                    'unique_subcategories': row.get('unique_subcategories', 0),
                                    'is_critical': True  # Consider all errors as critical by default
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
                                    
                                    errors.append({
                                        'component': component,
                                        'filename': entry.get('filename', os.path.basename(str(log_file))),
                                        'file_path': entry.get('fullpath', ''),
                                        'error_type': error_type,
                                        'error_message': str(entry.get('errors', 'Unknown error')),
                                        'timestamp': entry.get('timestamp', datetime.now().isoformat()),
                                        'is_critical': entry.get('fatal', True)
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
                                
                                errors.append({
                                    'component': component,
                                    'filename': log_data.get('filename', os.path.basename(str(log_file))),
                                    'file_path': log_data.get('fullpath', ''),
                                    'error_type': error_type,
                                    'error_message': str(log_data.get('errors', 'Unknown error')),
                                    'timestamp': log_data.get('timestamp', datetime.now().isoformat()),
                                    'is_critical': log_data.get('fatal', True)
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
        
        # Reset statistics
        self.stats = {
            'total_errors': 0,
            'critical_errors': 0,
            'warnings': 0,
            'components': {}
        }
        
        all_errors = []
        
        # Collect errors from each component
        for component, log_dir in self.log_dirs.items():
            errors, component_stats = self._collect_component_errors(component, log_dir)
            all_errors.extend(errors)
            
            # Update statistics
            self.stats['total_errors'] += component_stats['error_files']
            self.stats['warnings'] += component_stats['warning_files']
            self.stats['components'][component] = component_stats
            
            # Count critical errors
            for error in errors:
                if error.get('is_critical', False):
                    self.stats['critical_errors'] += 1
        
        # Create DataFrame from all errors
        if all_errors:
            error_df = pd.DataFrame(all_errors)
            log_dataframe_info(self.logger, error_df, "consolidated_errors")
            
            # Generate output filename
            if output_file is None:
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                output_file = f"consolidated_errors_{timestamp}.csv"
            
            output_path = os.path.join(self.output_dir, output_file)
            
            # Save to CSV
            error_df.to_csv(output_path, index=False)
            self.logger.info(f"Saved consolidated errors to {output_path}")
            
            # Generate summary statistics file
            summary_path = os.path.join(self.output_dir, f"error_summary_{datetime.now().strftime('%Y%m%d%H%M%S')}.json")
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(self.stats, f, indent=2)
            
            self.logger.info(f"Saved error summary to {summary_path}")
            
            # Log summary statistics
            self.logger.info(f"Error summary: {self.stats['total_errors']} total errors, "
                           f"{self.stats['critical_errors']} critical errors, "
                           f"{self.stats['warnings']} warnings")
            
            for component, stats in self.stats['components'].items():
                self.logger.info(f"{component}: {stats['error_files']} errors from {stats['total_files']} files")
            
            return error_df
        else:
            self.logger.info("No errors found across all components")
            return pd.DataFrame()