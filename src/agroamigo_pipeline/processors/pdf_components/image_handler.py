"""
Image handling utilities for PDF files.

This module provides functions for converting PDFs to images and encoding images to base64.
"""

import os
import time
import logging
import tempfile
import base64
from pdf2image import convert_from_path

class ImageHandler:
    """Handles PDF to image conversion and image encoding."""
    
    def __init__(self, logger=None, timing_stats=None):
        """
        Initialize the image handler.
        
        Args:
            logger (Logger): Logger instance
            timing_stats (dict): Dictionary to store timing statistics
        """
        self.logger = logger or logging.getLogger(__name__)
        self.timing_stats = timing_stats or {
            'pdf_to_images': [],
            'image_encoding': []
        }
    
    def convert_pdf_to_images(self, pdf_path):
        """
        Convert PDF to images and return paths to temporary image files.
        
        Args:
            pdf_path (str): Path to the PDF file
            
        Returns:
            list: List of paths to temporary image files
        """
        start_time = time.time()
        self.logger.info(f"Converting PDF to images: {pdf_path}")
        
        # Create a temporary directory for the images
        temp_dir = tempfile.mkdtemp()
        self.logger.debug(f"Created temporary directory: {temp_dir}")
        
        try:
            # Convert PDF pages to images
            self.logger.debug("Starting PDF to image conversion")
            images = convert_from_path(pdf_path)
            self.logger.info(f"Converted {len(images)} pages to images")
            
            image_paths = []
            # Save each page as a temporary image
            for i, image in enumerate(images):
                image_path = os.path.join(temp_dir, f'page_{i}.jpg')
                image.save(image_path, 'JPEG')
                image_paths.append(image_path)
                self.logger.debug(f"Saved page {i+1} to: {image_path}")
            
            duration = time.time() - start_time
            self.timing_stats['pdf_to_images'].append({
                'pdf': os.path.basename(pdf_path),
                'pages': len(images),
                'duration': duration
            })
            self.logger.info(f"PDF conversion completed in {duration:.2f} seconds")
            
            return image_paths
            
        except Exception as e:
            self.logger.error(f"Failed to convert PDF to images: {str(e)}")
            raise
    
    def encode_image(self, image_path):
        """
        Encode image to base64 string.
        
        Args:
            image_path (str): Path to the image file
            
        Returns:
            str: Base64 encoded image
        """
        start_time = time.time()
        self.logger.debug(f"Encoding image: {image_path}")
        
        try:
            with open(image_path, "rb") as image_file:
                encoded = base64.b64encode(image_file.read()).decode("utf-8")
                
                duration = time.time() - start_time
                self.timing_stats['image_encoding'].append({
                    'image': os.path.basename(image_path),
                    'duration': duration
                })
                self.logger.debug(f"Image encoded in {duration:.2f} seconds")
                
                return encoded
        except Exception as e:
            self.logger.error(f"Failed to encode image {image_path}: {str(e)}")
            raise
