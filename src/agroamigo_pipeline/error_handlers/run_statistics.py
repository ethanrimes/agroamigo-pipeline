"""
Run statistics module for collecting and reporting pipeline execution statistics.

This module provides functionality to:
1. Parse parquet registry files and save as CSV
2. Generate error summaries for pipeline runs
3. Create run-specific statistics reports
"""

import os
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import json
import shutil

from agroamigo_pipeline.config.settings import REGISTRY_DIR
from agroamigo_pipeline.utils.logger import setup_logging, log_function_call, log_dataframe_info


class RunStatistics:
    """Generate and manage run statistics for pipeline executions."""
    
    def __init__(self, registry_dir=None, logger=None, pipeline_run_id=None, start_timestamp=None):
        """
        Initialize the run statistics module.
        
        Args:
            registry_dir (str): Directory containing registry files
            logger (logging.Logger): Logger instance
            pipeline_run_id (str): ID of the current pipeline run
            start_timestamp (datetime): Timestamp when the pipeline run started
        """
        # Set up logger if not provided
        if logger is None:
            logging_setup = setup_logging()
            self.logger = logging_setup.get('error_handler', logging.getLogger('run_statistics'))
        else:
            self.logger = logger
        
        # Set registry directory
        self.registry_dir = Path(registry_dir) if registry_dir else Path(REGISTRY_DIR)
        
        # Store pipeline run ID
        self.pipeline_run_id = pipeline_run_id
        
        # Store start timestamp or use current time
        self.start_timestamp = start_timestamp or datetime.now()
        self.timestamp_str = self.start_timestamp.strftime('%Y%m%d%H%M%S')
        
        # Create registry subdirectories if they don't exist
        self.parquet_dir = self.registry_dir / 'parquet'
        self.run_statistics_dir = self.registry_dir / 'run_statistics'
        self.error_summaries_dir = self.registry_dir / 'error_summaries'
        
        # Create subdirectories for CSV and text reports
        self.run_csv_dir = self.run_statistics_dir / 'csv'
        self.run_text_dir = self.run_statistics_dir / 'text'
        
        os.makedirs(self.parquet_dir, exist_ok=True)
        os.makedirs(self.run_statistics_dir, exist_ok=True)
        os.makedirs(self.error_summaries_dir, exist_ok=True)
        os.makedirs(self.run_csv_dir, exist_ok=True)
        os.makedirs(self.run_text_dir, exist_ok=True)
        
        # Track registry files and their corresponding CSV names
        self.registry_files = {
            'webscraper': {'parquet': 'webscraper_registry.parquet', 'csv': 'webscraper_summary.csv'},
            'extractor': {'parquet': 'extractor_registry.parquet', 'csv': 'extractor_summary.csv'},
            'pdf': {'parquet': 'pdf_registry.parquet', 'csv': 'pdf_summary.csv'},
            'excel': {'parquet': 'excel_registry.parquet', 'csv': 'excel_summary.csv'},
            'consolidation': {'parquet': 'consolidation_registry.parquet', 'csv': 'consolidation_summary.csv'}
        }
        
        self.logger.info(f"RunStatistics initialized with timestamp: {self.timestamp_str}")
    
    def update_registry_csvs(self):
        """
        Update all registry CSV files from their parquet sources.
        This keeps consistent CSV files at the top level of the registry directory.
        
        Returns:
            dict: Statistics of CSV updates
        """
        log_function_call(self.logger, "update_registry_csvs")
        
        stats = {'updated': [], 'failed': []}
        
        for registry_name, file_info in self.registry_files.items():
            # Use parquet files only from the parquet subdirectory
            parquet_file = self.parquet_dir / file_info['parquet']
            csv_file = self.registry_dir / file_info['csv']
            
            # Check if parquet file exists
            if not parquet_file.exists():
                self.logger.warning(f"Parquet file not found: {parquet_file}")
                stats['failed'].append(registry_name)
                continue
            
            try:
                # Read parquet file
                df = pd.read_parquet(parquet_file)
                
                # Format timestamp columns for CSV readability
                for col in df.columns:
                    if pd.api.types.is_datetime64_dtype(df[col]):
                        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                    # Format list/array columns to be properly displayed in CSV
                    elif not df.empty and isinstance(df[col].iloc[0] if not pd.isna(df[col].iloc[0]) else None, (list, tuple)):
                        df[col] = df[col].apply(lambda x: str(x).replace("'", "").strip('[]()') if not pd.isna(x) else x)
                
                # Save to CSV
                df.to_csv(csv_file, index=False)
                
                stats['updated'].append(registry_name)
                self.logger.info(f"Updated CSV from parquet: {registry_name}")
                
            except Exception as e:
                self.logger.error(f"Failed to update CSV for {registry_name}: {str(e)}")
                stats['failed'].append(registry_name)
        
        return stats
    
    def generate_error_summary(self):
        """
        Generate error summary files consolidating errors from all registries.
        
        Returns:
            tuple: (Path to CSV summary, Path to text summary)
        """
        log_function_call(self.logger, "generate_error_summary")
        
        # Use standard names for run error files
        run_error_csv = f"run_errors_{self.timestamp_str}.csv"
        run_error_summary = f"run_error_summary_{self.timestamp_str}.txt"
        
        # Track all errors and statistics
        all_errors = []
        error_stats = {
            'total_errors': 0,
            'components': {}
        }
        
        # Process each registry file to extract errors
        for registry_name, file_info in self.registry_files.items():
            # Use only parquet files from the parquet subdirectory
            parquet_file = self.parquet_dir / file_info['parquet']
            
            if not parquet_file.exists():
                self.logger.warning(f"Parquet file not found: {parquet_file}")
                continue
            
            try:
                # Read the registry
                df = pd.read_parquet(parquet_file)
                
                # Apply timestamp filtering - only include errors from this run
                if self.start_timestamp:
                    # Try different timestamp columns based on registry type
                    timestamp_column = None
                    for col_name in ['last_processed', 'processed_date', 'date_expanded', 'download_timestamp', 'consolidation_time']:
                        if col_name in df.columns:
                            timestamp_column = col_name
                            break
                    
                    if timestamp_column:
                        df = df[df[timestamp_column] >= self.start_timestamp]
                        self.logger.debug(f"Filtered {registry_name} registry by {timestamp_column} >= {self.start_timestamp}")
                
                component_stats = {
                    'total_files': len(df),
                    'error_files': 0,
                    'error_types': {}
                }
                
                # Check for error columns with different naming conventions
                error_occurred_col = None
                error_message_col = None
                
                # Detect error columns
                for col in df.columns:
                    if 'error_occurred' in col.lower():
                        error_occurred_col = col
                    elif any(err_term in col.lower() for err_term in ['error_message', 'error_warnings', 'errors']):
                        error_message_col = col
                
                if error_occurred_col is None:
                    self.logger.warning(f"No error column found in {registry_name} registry")
                    continue
                
                # Filter for errors
                error_df = df[df[error_occurred_col] == True].copy()
                component_stats['error_files'] = len(error_df)
                
                # Skip if no errors
                if error_df.empty:
                    error_stats['components'][registry_name] = component_stats
                    continue
                
                # Determine step name based on registry
                step_name = registry_name
                if registry_name == 'extractor':
                    step_name = 'zip extraction'
                elif registry_name == 'pdf':
                    step_name = 'pdf processing'
                elif registry_name == 'excel':
                    step_name = 'excel processing'
                elif registry_name == 'webscraper':
                    step_name = 'web scraping'
                elif registry_name == 'consolidation':
                    step_name = 'data consolidation'
                
                # Process each error record
                for _, row in error_df.iterrows():
                    # Get error message
                    error_message = ""
                    if error_message_col and error_message_col in row:
                        error_message = row[error_message_col]
                    
                    # Get filename
                    filename = ""
                    for fname_col in ['filename', 'pdf_filename', 'zip_file_path']:
                        if fname_col in row and row[fname_col]:
                            filename = row[fname_col]
                            if isinstance(filename, Path):
                                filename = str(filename)
                            break
                    
                    # Get file date
                    file_date = ""
                    if 'file_date' in row and row['file_date']:
                        file_date = row['file_date']
                        if isinstance(file_date, pd.Timestamp):
                            file_date = file_date.strftime('%Y-%m-%d')
                    
                    # Get file path
                    file_path = ""
                    for path_col in ['file_path', 'download_path', 'zip_file_path', 'extraction_dir_path']:
                        if path_col in row and row[path_col]:
                            file_path = row[path_col]
                            if isinstance(file_path, Path):
                                file_path = str(file_path)
                            break
                    
                    # Determine error type
                    error_type = "unknown_error"
                    for potential_type_col in ['error_type', 'processing_status']:
                        if potential_type_col in row and row[potential_type_col]:
                            error_type = row[potential_type_col]
                            break
                    
                    # Update error type stats
                    component_stats['error_types'][error_type] = component_stats['error_types'].get(error_type, 0) + 1
                    
                    # Create error entry
                    error_entry = {
                        'component': registry_name,
                        'filename': filename,
                        'file_date': file_date,
                        'step_of_error': step_name,
                        'error_message': error_message,
                        'file_path': file_path,
                        'error_type': error_type,
                        'pipeline_run_id': self.pipeline_run_id,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    
                    all_errors.append(error_entry)
                
                # Update component statistics
                error_stats['components'][registry_name] = component_stats
                error_stats['total_errors'] += component_stats['error_files']
                
            except Exception as e:
                self.logger.error(f"Error processing registry {registry_name}: {str(e)}")
        
        # Generate CSV path
        csv_path = self.run_csv_dir / run_error_csv
        txt_path = self.run_text_dir / run_error_summary
        
        # Generate CSV if there are errors
        if all_errors:
            error_df = pd.DataFrame(all_errors)
            error_df.to_csv(csv_path, index=False)
            self.logger.info(f"Created run error summary CSV: {csv_path}")
        else:
            # Create empty CSV files
            pd.DataFrame(columns=['component', 'filename', 'file_date', 'step_of_error', 
                                 'error_message', 'file_path', 'error_type', 'pipeline_run_id', 'timestamp']).to_csv(csv_path, index=False)
            self.logger.info(f"Created empty run error summary CSV file")
        
        # Generate text summary
        self._write_error_summary_text(txt_path, error_stats)
        
        return csv_path, txt_path
    
    def _write_error_summary_text(self, path, error_stats):
        """
        Write error summary statistics to a text file.
        
        Args:
            path (Path): Path to write the summary file
            error_stats (dict): Error statistics dictionary
        """
        try:
            with open(path, 'w') as f:
                f.write("AGROAMIGO PIPELINE RUN ERROR SUMMARY\n")
                f.write("==================================\n\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Pipeline Run ID: {self.pipeline_run_id or 'Not specified'}\n")
                f.write(f"Run started at: {self.start_timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("OVERALL STATISTICS\n")
                f.write("-----------------\n")
                f.write(f"Total errors: {error_stats['total_errors']}\n\n")
                
                # Write component-specific statistics
                f.write("COMPONENT STATISTICS\n")
                f.write("-------------------\n")
                
                for component, stats in sorted(error_stats['components'].items()):
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
                
            self.logger.info(f"Created error summary text file: {path}")
            
        except Exception as e:
            self.logger.error(f"Error writing error summary text file: {str(e)}")
    
    def generate_run_statistics(self, components=None):
        """
        Generate run-specific statistics based on the components that were run.
        
        Args:
            components (list): List of components that were run in this pipeline execution
            
        Returns:
            tuple: (Path to CSV statistics, Path to text statistics)
        """
        log_function_call(self.logger, "generate_run_statistics", kwargs={"components": components})
        
        # If no components specified, assume all were run
        if not components:
            components = list(self.registry_files.keys())
        
        # Initialize statistics
        run_stats = {
            'pipeline_run_id': self.pipeline_run_id,
            'start_timestamp': self.start_timestamp,
            'components_run': components,
            'end_timestamp': datetime.now(),
            'total_duration_seconds': (datetime.now() - self.start_timestamp).total_seconds(),
            'component_stats': {}
        }
        
        # Process each component
        for component in components:
            mapped_component = self._map_component_name(component)
            
            if mapped_component not in self.registry_files:
                self.logger.warning(f"Unknown component: {component} (mapped to {mapped_component})")
                continue
            
            file_info = self.registry_files.get(mapped_component)
            if not file_info:
                continue
                
            parquet_file = self.parquet_dir / file_info['parquet']
            
            if not parquet_file.exists():
                self.logger.warning(f"Parquet file not found: {parquet_file}")
                continue
            
            try:
                # Read the registry
                df = pd.read_parquet(parquet_file)
                
                # Filter for records processed during this run
                if self.start_timestamp:
                    time_col_found = False
                    for time_col in ['last_processed', 'processed_date', 'date_expanded', 'consolidation_time', 'download_timestamp']:
                        if time_col in df.columns:
                            recent_df = df[df[time_col] >= self.start_timestamp]
                            self.logger.debug(f"Filtered {len(recent_df)}/{len(df)} records using {time_col} column")
                            time_col_found = True
                            break
                    
                    if not time_col_found:
                        # If no timestamp column found, use all records
                        self.logger.warning(f"No timestamp column found in {mapped_component} registry")
                        recent_df = df
                else:
                    # If no start timestamp, use all records
                    recent_df = df
                
                # Get component-specific statistics
                component_stats = self._get_component_statistics(mapped_component, recent_df)
                
                # Store stats
                run_stats['component_stats'][mapped_component] = component_stats
                
            except Exception as e:
                self.logger.error(f"Error generating statistics for {mapped_component}: {str(e)}")
        
        # Create filenames with timestamp
        csv_filename = f"run_statistics_{self.timestamp_str}.csv"
        txt_filename = f"run_report_{self.timestamp_str}.txt"
        
        # Create statistics files in the appropriate subdirectories
        csv_path = self.run_csv_dir / csv_filename
        txt_path = self.run_text_dir / txt_filename
        
        # Create flattened statistics for CSV
        flat_stats = self._flatten_statistics(run_stats)
        flat_df = pd.DataFrame([flat_stats])
        flat_df.to_csv(csv_path, index=False)
        
        # Write detailed text report
        self._write_run_statistics_text(txt_path, run_stats)
        
        self.logger.info(f"Generated run statistics CSV: {csv_path}")
        self.logger.info(f"Generated run statistics text report: {txt_path}")
        
        return csv_path, txt_path
    
    def _map_component_name(self, component):
        """Map command-line component names to registry names."""
        mapping = {
            'scraper': 'webscraper',
            'extractor': 'extractor',
            'excel': 'excel',
            'pdf': 'pdf',
            'aggregator': 'consolidation',
            'error': 'error',  # Not actually a registry
        }
        return mapping.get(component, component)
    
    def _get_component_statistics(self, component, df):
        """
        Get component-specific statistics from a DataFrame.
        
        Args:
            component (str): Component name
            df (pd.DataFrame): DataFrame with records from this run
            
        Returns:
            dict: Component statistics
        """
        stats = {
            'total_records': len(df),
            'success_records': 0,
            'error_records': 0,
            'success_rate': 0,
            'detailed_metrics': {}
        }
        
        # Skip if empty
        if df.empty:
            return stats
        
        # Find error column
        error_column = None
        for col in df.columns:
            if 'error_occurred' in col.lower():
                error_column = col
                break
        
        # Calculate success/error counts
        if error_column:
            stats['error_records'] = df[error_column].sum()
            stats['success_records'] = len(df) - stats['error_records']
            
            if len(df) > 0:
                stats['success_rate'] = (stats['success_records'] / len(df)) * 100
        
        # Component-specific metrics
        if component == 'webscraper':
            if 'download_duration_seconds' in df.columns:
                stats['detailed_metrics']['avg_download_duration'] = df['download_duration_seconds'].mean()
            if 'file_type' in df.columns:
                type_counts = df['file_type'].value_counts().to_dict()
                stats['detailed_metrics']['file_types'] = type_counts
            if 'download_attempt_count' in df.columns:
                stats['detailed_metrics']['avg_attempts'] = df['download_attempt_count'].mean()
                stats['detailed_metrics']['max_attempts'] = df['download_attempt_count'].max()
                
        elif component == 'extractor':
            if 'extraction_duration_seconds' in df.columns:
                stats['detailed_metrics']['avg_extraction_duration'] = df['extraction_duration_seconds'].mean()
            if 'num_contained_files' in df.columns:
                stats['detailed_metrics']['total_extracted_files'] = df['num_contained_files'].sum()
                stats['detailed_metrics']['avg_files_per_zip'] = df['num_contained_files'].mean()
            # Count direct PDF processing
            direct_pdfs = len(df[df['is_valid_zip'] == True])
            if direct_pdfs > 0:
                stats['detailed_metrics']['direct_pdfs_processed'] = direct_pdfs
                
        elif component in ['pdf', 'excel']:
            if 'processing_duration_seconds' in df.columns:
                stats['detailed_metrics']['avg_processing_duration'] = df['processing_duration_seconds'].mean()
            if 'rows_processed' in df.columns:
                stats['detailed_metrics']['total_rows'] = df['rows_processed'].sum()
                stats['detailed_metrics']['avg_rows'] = df['rows_processed'].mean()
            if 'unique_products' in df.columns:
                stats['detailed_metrics']['avg_products'] = df['unique_products'].mean()
            if 'unique_major_categories' in df.columns:
                stats['detailed_metrics']['avg_categories'] = df['unique_major_categories'].mean()
            if 'extraction_method' in df.columns:
                method_counts = df['extraction_method'].value_counts().to_dict()
                stats['detailed_metrics']['extraction_methods'] = method_counts
                
        elif component == 'consolidation':
            if 'consolidation_duration_seconds' in df.columns:
                stats['detailed_metrics']['avg_consolidation_duration'] = df['consolidation_duration_seconds'].mean()
            if 'total_rows' in df.columns:
                stats['detailed_metrics']['total_consolidated_rows'] = df['total_rows'].sum()
            if 'num_excel_files' in df.columns and 'num_pdf_files' in df.columns:
                stats['detailed_metrics']['total_excel_files'] = df['num_excel_files'].sum()
                stats['detailed_metrics']['total_pdf_files'] = df['num_pdf_files'].sum()
            if 'duplicates_removed' in df.columns:
                stats['detailed_metrics']['total_duplicates_removed'] = df['duplicates_removed'].sum()
            if 'data_quality_score' in df.columns:
                stats['detailed_metrics']['avg_quality_score'] = df['data_quality_score'].mean()
                
        return stats
    
    def _flatten_statistics(self, run_stats):
        """
        Flatten the nested statistics structure for CSV output.
        
        Args:
            run_stats (dict): Nested statistics dictionary
            
        Returns:
            dict: Flattened statistics dictionary
        """
        flat_stats = {
            'pipeline_run_id': run_stats.get('pipeline_run_id', ''),
            'start_timestamp': run_stats.get('start_timestamp').strftime('%Y-%m-%d %H:%M:%S') if run_stats.get('start_timestamp') else '',
            'end_timestamp': run_stats.get('end_timestamp').strftime('%Y-%m-%d %H:%M:%S') if run_stats.get('end_timestamp') else '',
            'total_duration_seconds': run_stats.get('total_duration_seconds', 0),
            'components_run': ','.join(run_stats.get('components_run', [])),
        }
        
        # Add component statistics
        for component, stats in run_stats.get('component_stats', {}).items():
            flat_stats[f'{component}_total_records'] = stats.get('total_records', 0)
            flat_stats[f'{component}_success_records'] = stats.get('success_records', 0)
            flat_stats[f'{component}_error_records'] = stats.get('error_records', 0)
            flat_stats[f'{component}_success_rate'] = stats.get('success_rate', 0)
            
            # Add a few key detailed metrics
            detailed = stats.get('detailed_metrics', {})
            if component == 'webscraper':
                flat_stats[f'{component}_avg_download_duration'] = detailed.get('avg_download_duration', 0)
                if 'file_types' in detailed:
                    for ftype, count in detailed['file_types'].items():
                        flat_stats[f'{component}_file_type_{ftype}'] = count
            elif component == 'extractor':
                flat_stats[f'{component}_total_extracted_files'] = detailed.get('total_extracted_files', 0)
                flat_stats[f'{component}_direct_pdfs_processed'] = detailed.get('direct_pdfs_processed', 0)
            elif component in ['pdf', 'excel']:
                flat_stats[f'{component}_total_rows'] = detailed.get('total_rows', 0)
                flat_stats[f'{component}_avg_products'] = detailed.get('avg_products', 0)
                if 'extraction_methods' in detailed:
                    for method, count in detailed['extraction_methods'].items():
                        flat_stats[f'{component}_method_{method}'] = count
            elif component == 'consolidation':
                flat_stats[f'{component}_total_consolidated_rows'] = detailed.get('total_consolidated_rows', 0)
                flat_stats[f'{component}_total_duplicates_removed'] = detailed.get('total_duplicates_removed', 0)
                flat_stats[f'{component}_total_excel_files'] = detailed.get('total_excel_files', 0)
                flat_stats[f'{component}_total_pdf_files'] = detailed.get('total_pdf_files', 0)
        
        return flat_stats
    
    def _write_run_statistics_text(self, path, run_stats):
        """
        Write detailed run statistics to a text file.
        
        Args:
            path (Path): Path to write the report file
            run_stats (dict): Run statistics dictionary
        """
        try:
            with open(path, 'w') as f:
                f.write("AGROAMIGO PIPELINE RUN REPORT\n")
                f.write("============================\n\n")
                f.write(f"Pipeline Run ID: {run_stats.get('pipeline_run_id', 'Not specified')}\n")
                f.write(f"Start Time: {run_stats.get('start_timestamp').strftime('%Y-%m-%d %H:%M:%S') if run_stats.get('start_timestamp') else 'Unknown'}\n")
                f.write(f"End Time: {run_stats.get('end_timestamp').strftime('%Y-%m-%d %H:%M:%S') if run_stats.get('end_timestamp') else 'Unknown'}\n")
                f.write(f"Total Duration: {run_stats.get('total_duration_seconds', 0):.2f} seconds\n")
                f.write(f"Components Run: {', '.join(run_stats.get('components_run', []))}\n\n")
                
                f.write("COMPONENT STATISTICS\n")
                f.write("===================\n")
                
                # Write statistics for each component
                for component, stats in sorted(run_stats.get('component_stats', {}).items()):
                    f.write(f"\n{component.upper()}:\n")
                    f.write(f"  Total Records: {stats.get('total_records', 0)}\n")
                    f.write(f"  Successful Records: {stats.get('success_records', 0)}\n")
                    f.write(f"  Error Records: {stats.get('error_records', 0)}\n")
                    f.write(f"  Success Rate: {stats.get('success_rate', 0):.2f}%\n")
                    
                    # Write detailed metrics
                    detailed = stats.get('detailed_metrics', {})
                    if detailed:
                        f.write("  Detailed Metrics:\n")
                        
                        for metric, value in sorted(detailed.items()):
                            if isinstance(value, dict):
                                f.write(f"    {metric}:\n")
                                for sub_key, sub_value in sorted(value.items()):
                                    f.write(f"      {sub_key}: {sub_value}\n")
                            elif isinstance(value, float):
                                f.write(f"    {metric}: {value:.2f}\n")
                            else:
                                f.write(f"    {metric}: {value}\n")
                
                # Write summary of errors
                if any(stats.get('error_records', 0) > 0 for stats in run_stats.get('component_stats', {}).values()):
                    f.write("\nERROR SUMMARY\n")
                    f.write("=============\n")
                    f.write("* A separate detailed error report is available in the error_summaries directory\n")
                    for component, stats in sorted(run_stats.get('component_stats', {}).items()):
                        if stats.get('error_records', 0) > 0:
                            f.write(f"  {component}: {stats.get('error_records', 0)} errors\n")
                
                f.write("\n\nEnd of report\n")
                
        except Exception as e:
            self.logger.error(f"Error writing run statistics text file: {str(e)}")
    
    def process_end_of_run(self, components=None):
        """
        Process end of pipeline run:
        1. Update registry CSVs
        2. Generate error summary
        3. Generate run statistics
        
        Args:
            components (list): List of components that were run in this pipeline execution
            
        Returns:
            dict: Dictionary with paths to generated files
        """
        log_function_call(self.logger, "process_end_of_run", kwargs={"components": components})
        
        results = {}
        
        # 1. Update registry CSVs
        csv_update_stats = self.update_registry_csvs()
        results['csv_updates'] = csv_update_stats
        
        # 2. Generate error summary
        error_csv_path, error_txt_path = self.generate_error_summary()
        results['error_summary'] = {
            'csv': str(error_csv_path),
            'txt': str(error_txt_path)
        }
        
        # 3. Generate run statistics
        run_csv_path, run_txt_path = self.generate_run_statistics(components)
        results['run_statistics'] = {
            'csv': str(run_csv_path),
            'txt': str(run_txt_path)
        }
        
        # 4. Generate global status files
        self._generate_global_status_files()
        
        self.logger.info("Completed end-of-run processing")
        return results
        
    def _generate_global_status_files(self):
        """
        Generate global status files that provide overall pipeline health metrics.
        These are summary files that show the current state of the pipeline across runs.
        """
        log_function_call(self.logger, "generate_global_status_files")
        
        # Create a status file at the root of registry directory
        status_file = self.registry_dir / "pipeline_status.txt"
        status_csv = self.registry_dir / "pipeline_status.csv"
        
        try:
            # Gather overall statistics
            overall_stats = {
                'last_run_id': self.pipeline_run_id,
                'last_run_time': self.start_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'component_stats': {}
            }
            
            # Get statistics for each component
            for component_name, file_info in self.registry_files.items():
                parquet_file = self.parquet_dir / file_info['parquet']
                
                if not parquet_file.exists():
                    continue
                    
                try:
                    # Read the registry
                    df = pd.read_parquet(parquet_file)
                    
                    # Get basic stats
                    error_column = None
                    for col in df.columns:
                        if 'error_occurred' in col.lower():
                            error_column = col
                            break
                    
                    component_stats = {
                        'total_records': len(df),
                        'error_records': df[error_column].sum() if error_column else 0,
                        'success_rate': 0
                    }
                    
                    if component_stats['total_records'] > 0 and error_column:
                        component_stats['success_rate'] = ((component_stats['total_records'] - component_stats['error_records']) / 
                                                         component_stats['total_records'] * 100)
                    
                    # Get last processed timestamp
                    timestamp_column = None
                    for col_name in ['last_processed', 'processed_date', 'date_expanded', 'download_timestamp', 'consolidation_time']:
                        if col_name in df.columns:
                            timestamp_column = col_name
                            break
                    
                    if timestamp_column and not df.empty:
                        last_timestamp = df[timestamp_column].max()
                        component_stats['last_updated'] = last_timestamp.strftime('%Y-%m-%d %H:%M:%S') if last_timestamp else "Unknown"
                    
                    overall_stats['component_stats'][component_name] = component_stats
                    
                except Exception as e:
                    self.logger.error(f"Error getting stats for {component_name}: {str(e)}")
            
            # Write text status file
            with open(status_file, 'w') as f:
                f.write("AGROAMIGO PIPELINE STATUS\n")
                f.write("=========================\n\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Last Pipeline Run ID: {overall_stats['last_run_id'] or 'Unknown'}\n")
                f.write(f"Last Pipeline Run Time: {overall_stats['last_run_time']}\n\n")
                
                f.write("COMPONENT STATUS\n")
                f.write("===============\n\n")
                
                for component, stats in sorted(overall_stats['component_stats'].items()):
                    f.write(f"{component.upper()}:\n")
                    f.write(f"  Total Records: {stats['total_records']}\n")
                    f.write(f"  Success Rate: {stats['success_rate']:.2f}%\n")
                    if 'last_updated' in stats:
                        f.write(f"  Last Updated: {stats['last_updated']}\n")
                    f.write("\n")
                
                # Add overall health assessment
                health_status = "GOOD"
                if any(stats['success_rate'] < 90 for stats in overall_stats['component_stats'].values()):
                    health_status = "FAIR"
                if any(stats['success_rate'] < 70 for stats in overall_stats['component_stats'].values()):
                    health_status = "POOR"
                    
                f.write("OVERALL HEALTH: " + health_status + "\n")
            
            # Create CSV version
            flat_status = {
                'last_run_id': overall_stats['last_run_id'],
                'last_run_time': overall_stats['last_run_time'],
                'overall_health': health_status
            }
            
            # Add component stats to flattened structure
            for component, stats in overall_stats['component_stats'].items():
                for key, value in stats.items():
                    flat_status[f"{component}_{key}"] = value
            
            # Save to CSV
            pd.DataFrame([flat_status]).to_csv(status_csv, index=False)
            
            self.logger.info(f"Generated global status files: {status_file}, {status_csv}")
            
        except Exception as e:
            self.logger.error(f"Error generating global status files: {str(e)}")