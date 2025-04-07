"""
Metadata extraction utilities for PDF files.

This module provides functions for extracting metadata from PDF files,
such as date, city, and market information.
"""

import os
import re
import logging
from datetime import datetime

from agroamigo_pipeline.utils.utils import (
    normalize_string, extract_date_from_spanish_text, format_date, 
    extract_geography_info
)
from agroamigo_pipeline.utils.exceptions import ValidationError
class MetadataExtractor:
    """Extracts metadata from PDF files containing agricultural price reports."""
    
    def __init__(self, logger=None):
        """
        Initialize the metadata extractor.
        
        Args:
            logger (Logger): Logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
    
    def extract_metadata_from_pdf(self, file_path, text):
        """
        Extract metadata from PDF text for informes por ciudades format based on positioning.
        
        Args:
            file_path (str): Path to the PDF file
            text (str): Extracted text
            
        Returns:
            dict: Metadata dictionary
        """
        metadata = {
            'date': None,
            'city': None,
            'market': None,
            'source': 'informes por ciudades'  # Default source for PDFs
        }
        
        try:
            # Look for the city/market line - it's between specific headers
            header_pattern = r'PRECIOS DE VENTA MAYORISTA\s*\n(.*?)\s*\n\s*PRODUCTOS PRIMERA CALIDAD'
            header_match = re.search(header_pattern, text, re.IGNORECASE | re.DOTALL)
            
            if header_match:
                city_market_line = header_match.group(1).strip()
                self.logger.info(f"Extracted header line: '{city_market_line}'")
                
                # Process city and market based on comma presence
                if ',' in city_market_line:
                    parts = city_market_line.split(',')
                    # First part is always city
                    metadata['city'] = parts[0].strip()
                    
                    # Last part is market (handle multiple commas like in "Bogotá, D.C., Corabastos")
                    metadata['market'] = parts[-1].strip()
                    self.logger.info(f"Split by comma - City: '{metadata['city']}', Market: '{metadata['market']}'")
                else:
                    # If no comma, all is city
                    metadata['city'] = city_market_line
                    metadata['market'] = ""
                    self.logger.info(f"No comma found - City: '{metadata['city']}', No market specified")
            
            # Extract date from the pattern that follows PRODUCTOS PRIMERA CALIDAD
            date_pattern = r'PRODUCTOS PRIMERA CALIDAD.*?\n(.*?)(?:\n|$)'
            date_match = re.search(date_pattern, text, re.IGNORECASE | re.DOTALL)
            
            if date_match:
                date_line = date_match.group(1).strip()
                date_obj = extract_date_from_spanish_text(date_line)
                if date_obj:
                    metadata['date'] = format_date(date_obj)
                    self.logger.info(f"Extracted date: '{metadata['date']}' from line: '{date_line}'")
            
            # If date not found by pattern, try to extract from any part of the text
            if not metadata['date']:
                date_obj = extract_date_from_spanish_text(text)
                if date_obj:
                    metadata['date'] = format_date(date_obj)
                    self.logger.info(f"Extracted date from general text: '{metadata['date']}'")
            
            # If metadata is missing, try to extract from filename
            filename = os.path.basename(file_path)
            if not metadata['date']:
                # Look for date patterns in filename (yyyymmdd)
                date_match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
                if date_match:
                    year, month, day = date_match.groups()
                    metadata['date'] = f"{year}-{month}-{day}"
                    self.logger.info(f"Extracted date from filename: '{metadata['date']}'")
            
            # Validate city and market
            if metadata['city'] is None or metadata['city'] == "":
                self.logger.error("No city found in document header")
                raise ValidationError("No city found in document header")
            
            # Don't accept 'Principal' as market
            if metadata['market'] and metadata['market'].lower() == 'principal':
                self.logger.warning("Found 'Principal' as market name, removing it")
                metadata['market'] = ""
            
            # Validate if market is just a number
            if metadata['market'] and metadata['market'].isdigit():
                self.logger.warning(f"Market appears to be just a number: '{metadata['market']}', setting as unknown")
                metadata['market'] = "Unknown"
            
            # Default values if still missing
            if not metadata['date']:
                metadata['date'] = datetime.now().strftime('%Y-%m-%d')
                self.logger.warning(f"No date found, using current date: {metadata['date']}")
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error extracting metadata: {str(e)}")
            
            # Default values
            metadata['date'] = datetime.now().strftime('%Y-%m-%d')
            metadata['city'] = 'Unknown'
            metadata['market'] = 'Unknown'
            
            return metadata