"""
Excel file reader module for reading and preparing Excel files for processing.
"""
import pandas as pd
import logging
from datetime import datetime
from agroamigo_pipeline.utils.exceptions import ExcelProcessingError
from agroamigo_pipeline.processors.excel_components.date_extractor import extract_date_from_raw_excel

class ExcelFileReader:
    """Handles opening and preparing Excel files for processing."""
    
    def __init__(self, logger=None):
        """
        Initialize the Excel file reader.
        
        Args:
            logger (Logger): Logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self.logger.info("ExcelFileReader initialized")
        self.extracted_date = None
    
    def read_and_prepare(self, file_path):
        """
        Read an Excel file and prepare it for processing.
        
        Args:
            file_path (str): Path to the Excel file
            
        Returns:
            DataFrame: Prepared DataFrame
            
        Raises:
            ExcelProcessingError: If the file cannot be read
        """
        self.logger.info(f"Reading and preparing Excel file: {file_path}")
        
        # First, try to extract the date from the raw Excel file
        self.extracted_date = extract_date_from_raw_excel(file_path)
        if self.extracted_date:
            self.logger.info(f"Successfully extracted date from raw Excel: {self.extracted_date}")
        
        # Read Excel file, testing different skiprows values
        successful_read = False
        df = None
        errors = []
        
        for skiprows in [0, 1, 2, 3, 4, 5]:
            try:
                df = pd.read_excel(file_path, skiprows=skiprows)
                
                # Check if DataFrame seems valid
                if len(df.columns) > 1 and len(df) > 0:
                    successful_read = True
                    break
            except Exception as e:
                errors.append(f"Error reading with skiprows={skiprows}: {str(e)}")
        
        if not successful_read:
            raise ExcelProcessingError(f"Failed to read Excel file: {'; '.join(errors)}")
        
        # Store the original unmodified DataFrame for later reference if needed
        original_df = df.copy()
        
        # Clean up column names
        df.columns = [str(col).strip() for col in df.columns]
        
        # Check for "Precio" text to identify the correct location of the table
        precio_row = None
        for i, row in df.iterrows():
            row_values = [str(val).lower() if not pd.isna(val) else '' for val in row]
            row_text = ' '.join(row_values)
            
            if "precio" in row_text and "$" in row_text:
                precio_row = i
                break
        
        # If "Precio" row found, re-read the Excel file with appropriate skiprows
        if precio_row is not None and precio_row > 0:
            self.logger.info(f"Found 'Precio $/Kg' row at index {precio_row}, re-reading file")
            df = pd.read_excel(file_path, skiprows=precio_row+1)
            df.columns = [str(col).strip() for col in df.columns]
        
        # Filter out metadata and footnote rows
        filtered_df = df.copy()
        rows_to_drop = []
        
        for i, row in df.iterrows():
            first_cell = str(row.iloc[0]) if not pd.isna(row.iloc[0]) else ""
            first_cell_lower = first_cell.lower()
            
            # Check for the patterns that should be removed
            if any([
                "*variedad predominante en el mercado" in first_cell_lower,
                "var%" in first_cell_lower,
                "n.d." in first_cell_lower and "disponible" in first_cell_lower
            ]):
                rows_to_drop.append(i)
        
        if rows_to_drop:
            self.logger.info(f"Dropping {len(rows_to_drop)} metadata/footnote rows")
            filtered_df = df.drop(rows_to_drop)
        
        # Add a metadata attribute to store the extracted date
        filtered_df.attrs['extracted_date'] = self.extracted_date
        
        self.logger.info(f"Successfully prepared DataFrame with {len(filtered_df)} rows and {len(filtered_df.columns)} columns")
        return filtered_df
    
    def get_extracted_date(self):
        """
        Get the date extracted from the raw Excel file.
        
        Returns:
            datetime or None: Extracted date
        """
        return self.extracted_date