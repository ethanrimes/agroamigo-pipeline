"""
Metadata extraction utilities for PDF files.

This module provides functions for extracting metadata from PDF files,
such as date, city, and market information.
"""

import os
import re
import logging
from datetime import datetime
import pdfplumber  # Import pdfplumber for page extraction

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
    
    def extract_metadata_from_pdf(self, file_path, text=None):
        """
        Extract metadata from PDF text for informes por ciudades format based on positioning.
        
        Args:
            file_path (str): Path to the PDF file
            text (str, optional): Previously extracted text. If None, will extract text from first page only.
            
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
            # If text is not provided or the document appears to be multi-page,
            # extract text only from the first page
            if text is None or len(text.split('\n')) > 50:  # Heuristic to detect multi-page documents
                self.logger.debug(f"Extracting text from first page only for metadata")
                with pdfplumber.open(file_path) as pdf:
                    if len(pdf.pages) > 0:
                        first_page = pdf.pages[0]
                        first_page_text = first_page.extract_text()
                        if first_page_text:
                            self.logger.debug(f"Successfully extracted first page text for metadata")
                            text_for_metadata = first_page_text
                        else:
                            self.logger.warning(f"Could not extract text from first page, using provided text")
                            text_for_metadata = text
                    else:
                        self.logger.warning(f"PDF appears to have no pages, using provided text")
                        text_for_metadata = text
            else:
                self.logger.debug(f"Using provided text for metadata extraction")
                text_for_metadata = text
            
            # Look for the city/market line - it's between specific headers
            header_pattern = r'PRECIOS DE VENTA MAYORISTA\s*\n(.*?)\s*\n\s*PRODUCTOS PRIMERA CALIDAD'
            header_match = re.search(header_pattern, text_for_metadata, re.IGNORECASE | re.DOTALL)
            
            if header_match:
                city_market_line = header_match.group(1).strip()
                self.logger.debug(f"Extracted header line: '{city_market_line}'")
                
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
                    self.logger.debug(f"No comma found - City: '{metadata['city']}', No market specified")
            
            # Extract date from the pattern that follows PRODUCTOS PRIMERA CALIDAD
            date_pattern = r'PRODUCTOS PRIMERA CALIDAD.*?\n(.*?)(?:\n|$)'
            date_match = re.search(date_pattern, text_for_metadata, re.IGNORECASE | re.DOTALL)
            
            if date_match:
                date_line = date_match.group(1).strip()
                date_obj = extract_date_from_spanish_text(date_line)
                if date_obj:
                    metadata['date'] = format_date(date_obj)
                    self.logger.debug(f"Extracted date: '{metadata['date']}' from line: '{date_line}'")
            
            # If date not found by pattern, try to extract from any part of the first page text
            if not metadata['date']:
                date_obj = extract_date_from_spanish_text(text_for_metadata)
                if date_obj:
                    metadata['date'] = format_date(date_obj)
                    self.logger.debug(f"Extracted date from general text: '{metadata['date']}'")
            
            # If metadata is missing, try to extract from filename
            filename = os.path.basename(file_path)
            if not metadata['date']:
                # Look for date patterns in filename (yyyymmdd)
                date_match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
                if date_match:
                    year, month, day = date_match.groups()
                    metadata['date'] = f"{year}-{month}-{day}"
                    self.logger.debug(f"Extracted date from filename: '{metadata['date']}'")
            
            # Validate city and market
            if metadata['city'] is None or metadata['city'] == "":
                # If still not found, try looking in the entire text if we were only using first page
                if text and text_for_metadata != text:
                    self.logger.debug("Attempting to find city in full document text")
                    alt_header_match = re.search(header_pattern, text, re.IGNORECASE | re.DOTALL)
                    if alt_header_match:
                        city_market_line = alt_header_match.group(1).strip()
                        if ',' in city_market_line:
                            parts = city_market_line.split(',')
                            metadata['city'] = parts[0].strip()
                            metadata['market'] = parts[-1].strip()
                        else:
                            metadata['city'] = city_market_line
                
                # If still nothing found
                if metadata['city'] is None or metadata['city'] == "":
                    self.logger.error("No city found in document header")
                    metadata['city'] = 'Unknown'
            
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
            
            # Ensure market has a value
            if not metadata['market']:
                metadata['market'] = 'Unknown'
                self.logger.warning(f"No market found, using 'Unknown'")
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error extracting metadata: {str(e)}")
            
            # Default values
            metadata['date'] = 'Unknown'
            metadata['city'] = 'Unknown'
            metadata['market'] = 'Unknown'
            
            return metadata