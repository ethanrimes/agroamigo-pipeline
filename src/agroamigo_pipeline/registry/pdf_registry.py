"""
Registry for tracking PDF processing.
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import logging
import uuid
from threading import Lock


from agroamigo_pipeline.registry.base_registry import BaseRegistry

class PDFProcessorRegistry(BaseRegistry):
    """Registry to track PDF file processing."""
    
    def __init__(self, logger=None):
        """Initialize the PDF processor registry."""

        self._lock = Lock()  # Add a lock for thread safety

        # Define schema for PDF processor registry
        schema = pa.schema([
            ('pdf_id', pa.string()),  # Primary key
            ('extraction_id', pa.string()),  # Foreign key to ZipExtractorRegistry
            ('file_date', pa.timestamp('ns')),
            ('pdf_filename', pa.string()),
            ('date_expanded', pa.timestamp('ns')),
            ('file_path', pa.string()),
            ('processing_status', pa.string()),  # "not_processed", "processing", "processed", "error"
            ('processed_date', pa.timestamp('ns')),
            ('parsed_date', pa.timestamp('ns')),  # Date extracted from PDF content
            ('processing_duration_seconds', pa.float64()),
            ('unique_products', pa.int32()),
            ('unique_major_categories', pa.int32()),
            ('unique_subcategories', pa.int32()),
            ('rows_processed', pa.int32()),
            ('error_occurred', pa.bool_()),
            ('error_warnings', pa.string()),
            ('extraction_method', pa.string()),  # "text_extraction", "gpt_vision", etc.
            ('processed_csv_path', pa.string()),
            ('unique_cities_markets', pa.int32()),
            ('pipeline_run_id', pa.string()),  # ID of the pipeline run
            ('consolidated', pa.bool_())  # Whether this file has been consolidated
        ])
        
        # Initialize base registry
        super().__init__("pdf", schema, logger)
        self.logger.debug("PDFProcessorRegistry initialized")
    
    def get_pdf_by_id(self, pdf_id):
        """
        Get a PDF record by its ID.
        
        Args:
            pdf_id (str): PDF ID
            
        Returns:
            dict: PDF record or None if not found
        """
        with self._lock:
            df = self.read()
            matches = df[df['pdf_id'] == pdf_id]
            
            if matches.empty:
                return None
            
            # Convert to dictionary
            return matches.iloc[0].to_dict()
    
    def get_pdfs_by_extraction_id(self, extraction_id):
        """
        Get PDF records by extraction ID.
        
        Args:
            extraction_id (str): Extraction ID from ZipExtractorRegistry
            
        Returns:
            pd.DataFrame: Matching PDF records
        """
        with self._lock:
            df = self.read()
            return df[df['extraction_id'] == extraction_id]
    
    def get_pdfs_by_path(self, file_path):
        """
        Get PDF records by file path.
        
        Args:
            file_path (str): Path to the PDF file
            
        Returns:
            pd.DataFrame: Matching PDF records
        """
        with self._lock:
            df = self.read()
            return df[df['file_path'] == file_path]
    
    def register_pdf(self, extraction_id, file_path, pdf_filename, file_date=None, date_expanded=None):
        """
        Register a PDF file for processing.
        
        Args:
            extraction_id (str): Extraction ID from ZipExtractorRegistry
            file_path (str): Path to the PDF file
            pdf_filename (str): Name of the PDF file
            file_date (datetime, optional): Date associated with the file
            date_expanded (datetime, optional): When the PDF was extracted
            
        Returns:
            str: PDF ID
        """
        with self._lock:
            df = self.read()
            
            current_time = datetime.now()
            
            # Check if file has already been registered
            existing_pdfs = df[df['file_path'] == file_path]
            
            if not existing_pdfs.empty:
                # Get the existing pdf_id
                pdf_id = existing_pdfs.iloc[0]['pdf_id']
                
                # Update status if not already processed
                if existing_pdfs.iloc[0]['processing_status'] == 'not_processed':
                    # Remove old record(s)
                    df = df[df['file_path'] != file_path]
                else:
                    # Already processed or processing, don't update
                    return pdf_id
            else:
                # Generate a new pdf_id
                pdf_id = str(uuid.uuid4())
            
            # Create a new record
            new_record = {
                'pdf_id': pdf_id,
                'extraction_id': extraction_id,
                'file_date': file_date or current_time,
                'pdf_filename': pdf_filename,
                'date_expanded': date_expanded or current_time,
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
                'extraction_method': '',
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
            self.logger.debug(f"Registered PDF for processing: {pdf_filename}")
            
            return pdf_id
    
    def update_processing_status(self, pdf_id, status, error=None):
        """
        Update the processing status of a PDF.
        
        Args:
            pdf_id (str): PDF ID
            status (str): New status ("processing", "processed", "error")
            error (str, optional): Error message if status is "error"
            
        Returns:
            bool: True if successful, False if PDF not found
        """
        with self._lock:
            df = self.read()
            
            # Find the PDF
            pdf_idx = df.index[df['pdf_id'] == pdf_id].tolist()
            
            if not pdf_idx:
                self.logger.warning(f"PDF with ID {pdf_id} not found in registry")
                return False
            
            # Update status
            df.at[pdf_idx[0], 'processing_status'] = status
            
            # Update error fields if applicable
            if status == 'error' and error:
                df.at[pdf_idx[0], 'error_occurred'] = True
                df.at[pdf_idx[0], 'error_warnings'] = error
            
            # Update processed_date if status is "processed" or "error"
            if status in ['processed', 'error']:
                df.at[pdf_idx[0], 'processed_date'] = datetime.now()
            
            # Write updated DataFrame
            self.write(df)
            self.logger.debug(f"Updated PDF {pdf_id} status to {status}")
            
            return True
    
    def update_processing_results(self, pdf_id, results):
        """
        Update the processing results of a PDF.
        
        Args:
            pdf_id (str): PDF ID
            results (dict): Processing results containing statistics and output path
            
        Returns:
            bool: True if successful, False if PDF not found
        """
        with self._lock:
            df = self.read()
            
            # Find the PDF
            pdf_idx = df.index[df['pdf_id'] == pdf_id].tolist()
            
            if not pdf_idx:
                self.logger.warning(f"PDF with ID {pdf_id} not found in registry")
                return False
            
            # Get current record for the PDF
            idx = pdf_idx[0]
            
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
            self.logger.debug(f"Updated PDF {pdf_id} with processing results")
            
            return True
    
    def mark_as_consolidated(self, pdf_id, consolidated=True):
        """
        Mark a PDF as consolidated.
        
        Args:
            pdf_id (str): PDF ID
            consolidated (bool): Whether the PDF has been consolidated
            
        Returns:
            bool: True if successful, False if PDF not found
        """
        with self._lock:
            df = self.read()
            
            # Find the PDF
            pdf_idx = df.index[df['pdf_id'] == pdf_id].tolist()
            
            if not pdf_idx:
                self.logger.warning(f"PDF with ID {pdf_id} not found in registry")
                return False
            
            # Update consolidated field
            df.at[pdf_idx[0], 'consolidated'] = consolidated
            
            # Write updated DataFrame
            self.write(df)
            self.logger.debug(f"Marked PDF {pdf_id} as {'consolidated' if consolidated else 'not consolidated'}")
            
            return True
    
    def get_pending_pdfs(self):
        """
        Get PDFs that need to be processed.
        
        Returns:
            pd.DataFrame: Records of PDFs that need processing
        """
        with self._lock:
            df = self.read()
            return df[df['processing_status'] == 'not_processed']
    
    def get_processed_pdfs(self, include_errors=False):
        """
        Get processed PDFs.
        
        Args:
            include_errors (bool): Whether to include PDFs with errors
            
        Returns:
            pd.DataFrame: Records of processed PDFs
        """
        with self._lock:
            df = self.read()
            
            if include_errors:
                return df[df['processing_status'].isin(['processed', 'error'])]
            else:
                return df[df['processing_status'] == 'processed']
    
    def get_unconsolidated_pdfs(self):
        """
        Get processed PDFs that have not been consolidated.
        
        Returns:
            pd.DataFrame: Records of unconsolidated PDFs
        """
        with self._lock:
            df = self.read()
            return df[(df['processing_status'] == 'processed') & ~df['consolidated']]
    
    def get_statistics(self):
        """
        Get processing statistics.
        
        Returns:
            dict: Dictionary of statistics
        """
        with self._lock:
            df = self.read()
            
            if df.empty:
                return {
                    'total_pdfs': 0,
                    'processed_pdfs': 0,
                    'error_pdfs': 0,
                    'pending_pdfs': 0,
                    'success_rate': 0,
                    'total_rows_processed': 0,
                    'avg_rows_per_pdf': 0,
                    'consolidated_pdfs': 0
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
                'total_pdfs': total,
                'processed_pdfs': processed,
                'error_pdfs': errors,
                'pending_pdfs': pending,
                'success_rate': (processed / total * 100) if total > 0 else 0,
                'total_rows_processed': total_rows,
                'avg_rows_per_pdf': (total_rows / processed) if processed > 0 else 0,
                'consolidated_pdfs': consolidated
            }
    
    def get_pdfs_by_date_range(self, start_date=None, end_date=None, with_errors=False):
        """
        Get PDFs within a date range.
        
        Args:
            start_date (datetime, optional): Start date (inclusive)
            end_date (datetime, optional): End date (inclusive)
            with_errors (bool): If True, include PDFs with errors
            
        Returns:
            pd.DataFrame: PDFs within the date range
        """
        with self._lock:
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