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
        Extract metadata from PDF text for informes por ciudades format.
        
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
            # Extract city and market from standardized header format
            # Look for pattern: "PRECIOS DE VENTA MAYORISTA\nBogotá, D.C., Corabastos"
            city_market_pattern = r'PRECIOS DE VENTA MAYORISTA\s*\n(.*?)(?:\n|$)'
            city_market_match = re.search(city_market_pattern, text, re.IGNORECASE)
            
            if city_market_match:
                city_market_line = city_market_match.group(1).strip()
                # Split by comma to separate city and market
                if ',' in city_market_line:
                    parts = city_market_line.split(',')
                    metadata['city'] = parts[0].strip()
                    # Market might be after multiple commas
                    if len(parts) > 1:
                        metadata['market'] = parts[-1].strip()
                else:
                    metadata['city'] = city_market_line
            
            # Extract date from lines after city/market
            # Look for pattern: "PRODUCTOS PRIMERA CALIDAD - Boletín Gratuito\n31 de Marzo de 2021"
            date_pattern = r'PRODUCTOS PRIMERA CALIDAD.*\s*\n(.*?)(?:\n|$)'
            date_match = re.search(date_pattern, text, re.IGNORECASE)
            
            if date_match:
                date_line = date_match.group(1).strip()
                date_obj = extract_date_from_spanish_text(date_line)
                if date_obj:
                    metadata['date'] = format_date(date_obj)
            
            # If date not found, try to extract from any part of the text
            if not metadata['date']:
                date_obj = extract_date_from_spanish_text(text)
                if date_obj:
                    metadata['date'] = format_date(date_obj)
            
            # If metadata is missing, try to extract from filename
            filename = os.path.basename(file_path)
            if not metadata['date']:
                # Look for date patterns in filename (yyyymmdd)
                date_match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
                if date_match:
                    year, month, day = date_match.groups()
                    metadata['date'] = f"{year}-{month}-{day}"
            
            if not metadata['city'] or not metadata['market']:
                city, market = extract_geography_info(filename)
                if city and not metadata['city']:
                    metadata['city'] = city
                if market and not metadata['market']:
                    metadata['market'] = market
            
            # Default values if still missing
            if not metadata['date']:
                metadata['date'] = datetime.now().strftime('%Y-%m-%d')
            if not metadata['city']:
                metadata['city'] = 'Unknown'
            if not metadata['market']:
                metadata['market'] = 'Unknown'
            
            return metadata
            
        except Exception as e:
            self.logger.warning(f"Error extracting metadata: {str(e)}")
            
            # Default values
            metadata['date'] = datetime.now().strftime('%Y-%m-%d')
            metadata['city'] = 'Unknown'
            metadata['market'] = 'Unknown'
            
            return metadata
