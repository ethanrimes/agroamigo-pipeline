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
        self.logger = logger or logging.getLogger("pdf_processor")
    
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
            # Only extract text from first page if no text is provided
            if text is None:
                self.logger.debug(f"{file_path}: No text provided, extracting text from first page only for metadata")
                with pdfplumber.open(file_path) as pdf:
                    if len(pdf.pages) > 0:
                        first_page = pdf.pages[0]
                        first_page_text = first_page.extract_text()
                        if first_page_text:
                            self.logger.debug(f"{file_path}: Successfully extracted first page text for metadata")
                            text_for_metadata = first_page_text
                        else:
                            self.logger.warning(f"{file_path}: Could not extract text from first page")
                            text_for_metadata = ""
                    else:
                        self.logger.warning(f"{file_path}: PDF has no pages")
                        text_for_metadata = ""
            else:
                # Use the provided text parameter directly
                self.logger.debug(f"{file_path}: Using provided text for metadata extraction ({len(text)} characters)")
                text_for_metadata = text
            
            # Get the city, market and date using the new methodology
            city_line, date_line = self._extract_relevant_lines(text_for_metadata, file_path)
            
            # Process city and market
            if city_line:
                self.logger.debug(f"{file_path}: Extracted city/market line: '{city_line}'")
                
                # Process city and market based on comma presence
                if ',' in city_line:
                    parts = city_line.split(',')
                    # First part is always city
                    metadata['city'] = parts[0].strip()
                    
                    # Last part is market (handle multiple commas like in "Bogotá, D.C., Corabastos")
                    metadata['market'] = parts[-1].strip()
                    self.logger.debug(f"{file_path}: Split by comma - City: '{metadata['city']}', Market: '{metadata['market']}'")
                else:
                    # If no comma, all is city, market is null
                    metadata['city'] = city_line.strip()
                    metadata['market'] = None
                    self.logger.debug(f"{file_path}: No comma found - City: '{metadata['city']}', No market specified")
            else:
                self.logger.warning(f"{file_path}: No city/market line found")
            
            # Process date
            if date_line:
                date_obj = extract_date_from_spanish_text(date_line)
                if date_obj:
                    metadata['date'] = format_date(date_obj)
                    self.logger.debug(f"{file_path}: Extracted date: '{metadata['date']}' from line: '{date_line}'")
                else:
                    self.logger.warning(f"{file_path}: Could not extract date from line: '{date_line}'")
            else:
                self.logger.warning(f"{file_path}: No date line found")
            
            # Don't accept 'Principal' as market
            if metadata['market'] and metadata['market'].lower() == 'principal':
                self.logger.warning(f"{file_path}: Found 'Principal' as market name")
                metadata['market'] = None
            
            # Validate if market is just a number
            if metadata['market'] and metadata['market'].isdigit():
                self.logger.warning(f"{file_path}: Market appears to be just a number: '{metadata['market']}'")
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"{file_path}: Error extracting metadata: {str(e)}")
            return metadata
    
    def _extract_relevant_lines(self, text, file_path):
        """
        Extract text from PDF and retrieve specific lines based on pattern matching.
        
        Args:
            text (str): Text extracted from the PDF
            file_path (str): Path to the PDF file for logging
            
        Returns:
            tuple: The city/market line and date line
        """
        try:
            # Split into lines for processing
            lines = text.split("\n")
            
            # Find the target pattern
            target_pattern = "PRECIOS DE VENTA MAYORISTA"
            target_line_index = None
            
            for i, line in enumerate(lines):
                if target_pattern in line:
                    target_line_index = i
                    break
            
            if target_line_index is None:
                self.logger.warning(f"{file_path}: Pattern '{target_pattern}' not found in the PDF.")
                return None, None
            
            # Get target line and next three lines
            relevant_lines = lines[target_line_index:target_line_index+4]
            
            # Remove the PRECIOS DE VENTA MAYORISTA line
            filtered_lines = relevant_lines[1:]
            
            # Remove any line matching PRODUCTOS PRIMERA CALIDAD
            filtered_lines = [line for line in filtered_lines 
                             if "PRODUCTOS PRIMERA CALIDAD" not in line.upper() and 
                             "PRODUCTOS DE PRIMERA CALIDAD" not in line.upper() and 
                             "PRECIOS DE VENTA MAYORISTA" not in line.upper() and
                             "PRECIOS VENTA" not in line.upper()]
            
            # Return the first two remaining lines
            if len(filtered_lines) >= 2:
                return filtered_lines[0], filtered_lines[1]
            elif len(filtered_lines) == 1:
                return filtered_lines[0], None
            else:
                return None, None
                
        except Exception as e:
            self.logger.error(f"{file_path}: Error extracting relevant lines: {str(e)}")
            return None, None