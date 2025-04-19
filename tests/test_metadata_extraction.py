#!/usr/bin/env python3
"""
PDF Text Extraction Script

This script extracts text from a PDF file supplied as a command-line argument.
It searches for specific text patterns and outputs the first two relevant lines.
"""

import sys
import re
import pdfplumber
import argparse

def extract_relevant_text(pdf_path):
    """
    Extract text from PDF and retrieve specific lines based on pattern matching.
    
    Args:
        pdf_path (str): Path to the PDF file
        
    Returns:
        tuple: The first two relevant lines after filtering
    """
    try:
        # Open PDF and extract all text
        with pdfplumber.open(pdf_path) as pdf:
            all_text = ""
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    all_text += page_text + "\n"
            
            # Split into lines for processing
            lines = all_text.split("\n")
            
            # Find the target pattern
            target_pattern = "PRECIOS DE VENTA MAYORISTA"
            target_line_index = None
            
            for i, line in enumerate(lines):
                if target_pattern in line:
                    target_line_index = i
                    break
            
            if target_line_index is None:
                print(f"Pattern '{target_pattern}' not found in the PDF.")
                return None, None
            
            # Get target line and next three lines
            relevant_lines = lines[target_line_index:target_line_index+4]
            
            # Remove the PRECIOS DE VENTA MAYORISTA line
            filtered_lines = relevant_lines[1:]
            
            # Remove any line matching PRODUCTOS PRIMERA CALIDAD
            filtered_lines = [line for line in filtered_lines 
                             if "PRODUCTOS PRIMERA CALIDAD" not in line.upper()]
            
            # Return the first two remaining lines
            if len(filtered_lines) >= 2:
                return filtered_lines[0], filtered_lines[1]
            elif len(filtered_lines) == 1:
                return filtered_lines[0], None
            else:
                return None, None
                
    except Exception as e:
        print(f"Error processing PDF: {str(e)}")
        return None, None

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract specific text from PDF file.')
    parser.add_argument('pdf_path', help='Path to the PDF file')
    
    args = parser.parse_args()
    
    # Extract the relevant text
    first_line, second_line = extract_relevant_text(args.pdf_path)
    
    # Output results
    if first_line:
        print("First line:", first_line)
    else:
        print("First line not found or filtered out.")
        
    if second_line:
        print("Second line:", second_line)
    else:
        print("Second line not found or filtered out.")

if __name__ == "__main__":
    main()