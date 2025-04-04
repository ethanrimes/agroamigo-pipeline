"""
Excel file reader module for reading and preparing Excel files for processing.
"""
import pandas as pd
import logging
from agroamigo_pipeline.utils.exceptions import ExcelProcessingError

class ExcelFileReader:
    """Handles opening and preparing Excel files for processing."""
    
    def __init__(self, logger=None):
        """
        Initialize the Excel file reader.
        
        Args:
            logger (Logger): Logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
    
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
        
        # Clean up column names
        df.columns = [str(col).strip() for col in df.columns]
        
        # Check for "Precio" text to identify the correct location of the table
        precio_row = None
        for i, row in df.iterrows():
            if isinstance(row[0], str) and "precio" in row[0].lower():
                precio_row = i
                break
        
        # If "Precio" row found, re-read the Excel file with appropriate skiprows
        if precio_row is not None and precio_row > 0:
            df = pd.read_excel(file_path, skiprows=precio_row+1)
            df.columns = [str(col).strip() for col in df.columns]
        
        return df 