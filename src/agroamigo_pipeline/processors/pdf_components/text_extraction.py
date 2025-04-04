"""
Text extraction utilities for PDF files.

This module provides functions for extracting text and tables from PDFs,
as well as assessing the quality of extracted text.
"""

import os
import re
import pandas as pd
import PyPDF2
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
        self.logger.debug(f"Extracting text with PyPDF2: {file_path}")
        
        text = ""
        try:
            with open(file_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                num_pages = len(reader.pages)
                
                for page_num in range(num_pages):
                    page = reader.pages[page_num]
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n\n"
            
            duration = time.time() - start_time
            self.timing_stats['pdf_text_extraction'].append({
                'method': 'PyPDF2',
                'pdf': os.path.basename(file_path),
                'duration': duration
            })
            self.logger.debug(f"PyPDF2 extraction completed in {duration:.2f} seconds")
            
            return text
        except Exception as e:
            self.logger.warning(f"Error extracting text with PyPDF2: {str(e)}")
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
        self.logger.debug(f"Extracting text with pdfplumber: {file_path}")
        
        text = ""
        try:
            with pdfplumber.open(file_path) as pdf:
                num_pages = len(pdf.pages)
                
                for page_num in range(num_pages):
                    page = pdf.pages[page_num]
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n\n"
            
            duration = time.time() - start_time
            self.timing_stats['pdf_text_extraction'].append({
                'method': 'pdfplumber',
                'pdf': os.path.basename(file_path),
                'duration': duration
            })
            self.logger.debug(f"pdfplumber extraction completed in {duration:.2f} seconds")
            
            return text
        except Exception as e:
            self.logger.warning(f"Error extracting text with pdfplumber: {str(e)}")
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
    
    def assess_text_quality(self, text):
        """
        Assess the quality of extracted text for informes por ciudades format.
        
        Args:
            text (str): Extracted text
            
        Returns:
            float: Quality score between 0 and 1
        """
        if not text:
            return 0.0
        
        # Check for expected patterns in agricultural price reports
        patterns = [
            r'precio',
            r'mercado',
            r'producto',
            r'presentaci[oó]n',
            r'unidades',
            r'ronda',
            r'm[ií]nimo',
            r'm[aá]ximo',
            r'frutas|verduras|hortalizas|leguminosas'
        ]
        
        matches = 0
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                matches += 1
        
        # Calculate score based on pattern matches
        pattern_score = matches / len(patterns)
        
        # Check if text contains structured data (tabular format)
        has_structure = bool(re.search(r'\n[\s]*[\w\d]+[\s]+[\d.,]+[\s]+[\d.,]+', text))
        structure_score = 0.5 if has_structure else 0.0
        
        # Check for the standard report header
        has_header = bool(re.search(r'PRECIOS DE VENTA MAYORISTA', text, re.IGNORECASE))
        header_score = 0.2 if has_header else 0.0
        
        # Combine scores
        return min(1.0, pattern_score * 0.5 + structure_score + header_score)
