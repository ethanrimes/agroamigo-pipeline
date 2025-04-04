"""
Text extraction utilities for PDF files.

This module provides functions for extracting text and tables from PDFs,
as well as assessing the quality of extracted text.
"""

import os
import re
import pandas as pd
# import PyPDF2
import pdfplumber
import time
import logging

class TextExtractor:
    """Extracts and processes text content from PDF files."""
    
    def __init__(self, logger=None, timing_stats=None):
        """
        Initialize the text extractor.
        
        Args:
            logger (Logger): Logger instance
            timing_stats (dict): Dictionary to store timing statistics
        """
        self.logger = logger or logging.getLogger(__name__)
        self.timing_stats = timing_stats or {
            'pdf_text_extraction': []
        }
    
    def extract_text_with_pypdf(self, file_path):
        """
        Extract text from PDF using PyPDF2.
        
        Args:
            file_path (str): Path to the PDF file
            
        Returns:
            str: Extracted text
        """
        start_time = time.time()
        filename = os.path.basename(file_path)
        self.logger.info(f"        -----> Extracting text with PyPDF2: {filename}")
        
        text = ""
        try:
            with open(file_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                num_pages = len(reader.pages)
                self.logger.info(f"        PDF has {num_pages} pages")
                
                for page_num in range(num_pages):
                    self.logger.info(f"        Extracting text from page {page_num+1}/{num_pages}")
                    page = reader.pages[page_num]
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n\n"
                        self.logger.info(f"        Extracted {len(page_text)} characters from page {page_num+1}")
                    else:
                        self.logger.info(f"        No text extracted from page {page_num+1}")
            
            duration = time.time() - start_time
            self.timing_stats['pdf_text_extraction'].append({
                'method': 'PyPDF2',
                'pdf': os.path.basename(file_path),
                'duration': duration
            })
            self.logger.info(f"        PyPDF2 extraction completed in {duration:.2f} seconds, extracted {len(text)} total characters")
            self.logger.info(f"        <----- Completed PyPDF2 extraction for: {filename}")
            
            return text
        except Exception as e:
            self.logger.warning(f"        Error extracting text with PyPDF2: {str(e)}")
            self.logger.info(f"        <----- Failed PyPDF2 extraction for: {filename}")
            return ""
    
    def extract_text_with_pdfplumber(self, file_path):
        """
        Extract text from PDF using pdfplumber.
        
        Args:
            file_path (str): Path to the PDF file
            
        Returns:
            str: Extracted text
        """
        start_time = time.time()
        filename = os.path.basename(file_path)
        self.logger.debug(f"        -----> Extracting text with pdfplumber: {filename}")
        
        text = ""
        try:
            with pdfplumber.open(file_path) as pdf:
                num_pages = len(pdf.pages)
                self.logger.debug(f"        PDF has {num_pages} pages")
                
                for page_num in range(num_pages):
                    self.logger.debug(f"        Extracting text from page {page_num+1}/{num_pages}")
                    page = pdf.pages[page_num]
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n\n"
                        self.logger.debug(f"        Extracted {len(page_text)} characters from page {page_num+1}")
                    else:
                        self.logger.debug(f"        No text extracted from page {page_num+1}")
            
            duration = time.time() - start_time
            self.timing_stats['pdf_text_extraction'].append({
                'method': 'pdfplumber',
                'pdf': os.path.basename(file_path),
                'duration': duration
            })
            self.logger.debug(f"        pdfplumber extraction completed in {duration:.2f} seconds, extracted {len(text)} total characters")
            self.logger.debug(f"        <----- Completed pdfplumber extraction for: {filename}")
            
            return text
        except Exception as e:
            self.logger.warning(f"        Error extracting text with pdfplumber: {str(e)}")
            self.logger.debug(f"        <----- Failed pdfplumber extraction for: {filename}")
            return ""
    
    def extract_table_with_pdfplumber(self, file_path):
        """
        Extract tables from PDF using pdfplumber.
        
        Args:
            file_path (str): Path to the PDF file
            
        Returns:
            list: List of extracted tables as DataFrames
        """
        start_time = time.time()
        self.logger.debug(f"Extracting tables with pdfplumber: {file_path}")
        
        tables = []
        try:
            with pdfplumber.open(file_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    self.logger.debug(f"Extracting tables from page {i+1}")
                    extracted_tables = page.extract_tables()
                    if extracted_tables:
                        for j, table in enumerate(extracted_tables):
                            if table and len(table) > 1:  # Skip empty tables
                                # Try to determine headers
                                headers = table[0]
                                
                                # If headers contain None values, try to find a better header row
                                if None in headers:
                                    for row_idx in range(1, min(5, len(table))):
                                        if None not in table[row_idx]:
                                            headers = table[row_idx]
                                            table = table[row_idx+1:]
                                            break
                                
                                # Clean up headers
                                headers = [str(h).strip() if h is not None else f"Column_{i}" for i, h in enumerate(headers)]
                                
                                # Create DataFrame
                                if table:
                                    df = pd.DataFrame(table, columns=headers)
                                    tables.append(df)
                                    self.logger.debug(f"Extracted table {j+1} with shape {df.shape}")
            
            duration = time.time() - start_time
            self.timing_stats['pdf_text_extraction'].append({
                'method': 'pdfplumber_tables',
                'pdf': os.path.basename(file_path),
                'duration': duration
            })
            self.logger.debug(f"Table extraction completed in {duration:.2f} seconds")
            
            return tables
            
        except Exception as e:
            self.logger.warning(f"Error extracting tables with pdfplumber: {str(e)}")
            return []
    
    def is_valid_utf8(self, text):
        """
        Check if the text is valid UTF-8.
        
        Args:
            text (str): Text to check
            
        Returns:
            bool: True if text is valid UTF-8, False otherwise
        """
        if not text:
            return False
            
        try:
            # Try to encode the text as UTF-8 and then decode it back
            encoded = text.encode('utf-8')
            decoded = encoded.decode('utf-8')
            # If the original and re-decoded text match, it's valid UTF-8
            return text == decoded
        except UnicodeError:
            # If we get an error, the text isn't valid UTF-8
            self.logger.warning("        Unicode error encountered during UTF-8 validation")
            return False
        except Exception as e:
            # Catch any other errors and log them
            self.logger.warning(f"        Error checking UTF-8 validity: {str(e)}")
            return False
    
    def assess_text_quality(self, text):
        """
        Check if the extracted text is valid and usable.
        
        Args:
            text (str): Extracted text
            
        Returns:
            bool: True if text is valid and usable, False otherwise
        """
        try:
            # Check if text is None or empty
            if not text:
                self.logger.info("        Text is empty or None")
                return False
            
            # Check if the text is valid UTF-8
            if not self.is_valid_utf8(text):
                self.logger.info("        Text is not valid UTF-8")
                return False
            
            # Check if there's enough text to be useful (at least some characters)
            if len(text.strip()) < 100:
                self.logger.info("        Text is too short to be useful")
                return False
                
            return True
            
        except Exception as e:
            # If any error occurs during assessment, log it and return False
            self.logger.warning(f"        Error assessing text quality: {str(e)}")
            return False
