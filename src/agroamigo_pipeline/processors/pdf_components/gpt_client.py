"""
GPT-4o mini client for image-based text extraction.

This module provides functions for querying the GPT-4o mini API with images to extract text content.
"""

import os
import time
import logging
import requests

from agroamigo_pipeline.config.settings import GPT4O_API_KEY

class GPTClient:
    """Client for interacting with GPT-4o mini API."""
    
    def __init__(self, logger=None, timing_stats=None):
        """
        Initialize the GPT client.
        
        Args:
            logger (Logger): Logger instance
            timing_stats (dict): Dictionary to store timing statistics
        """
        self.logger = logger or logging.getLogger(__name__)
        self.timing_stats = timing_stats or {
            'gpt4_query': []
        }
        self.api_key = GPT4O_API_KEY
    
    def query_gpt4o_with_image(self, image_path, base64_image=None):
        """
        Query GPT-4o mini with an image to extract text data.
        
        Args:
            image_path (str): Path to the image file
            base64_image (str): Pre-encoded base64 image data (optional)
            
        Returns:
            str: Extracted text from the image
        """
        if not self.api_key:
            self.logger.error("No GPT-4o API key provided")
            return None
        
        start_time = time.time()
        self.logger.info(f"Querying GPT-4o mini with image: {image_path}")
        
        try:
            # Use provided base64 image or assume it needs to be encoded
            if base64_image is None:
                self.logger.warning("No base64_image provided, image encoding should be done separately")
                return None
            
            # Prepare prompt for GPT-4o mini
            prompt = """
            Extract the table from the image and convert it into a CSV file. The columns should be:
            Producto; Presentacion; Unidades; Minimo; Maximo; Minimo; Maximo.
            Ensure each row corresponds to a product's data with the provided column names, and if any data is missing, mark it as None or leave it blank.
            If a row has only one column of value (such as in the leftmost column), output that value followed by precisely 6 semicolons to fill the remaining columns (e.g., value;;;;;;).
            Do not include any other text or characters, only the CSV data as plain text, with values separated by semicolons, so it can be directly parsed.
            """
            
            # Prepare headers and payload
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            payload = {
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": "You are a specialized data extraction assistant that processes Colombian agricultural market reports."},
                    {"role": "user", "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}}
                    ]}
                ],
                "temperature": 0.2,
                "max_tokens": 4000
            }
            
            # Make API request
            self.logger.debug("Sending request to GPT-4o mini API")
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload
            )
            
            if response.status_code != 200:
                self.logger.error(f"GPT-4o mini API error: {response.status_code} - {response.text}")
                return None
            
            # Extract and process response
            content = response.json()['choices'][0]['message']['content']
            
            duration = time.time() - start_time
            self.timing_stats['gpt4_query'].append({
                'image': os.path.basename(image_path),
                'duration': duration
            })
            self.logger.info(f"GPT-4o mini query completed in {duration:.2f} seconds")
            
            return content
            
        except Exception as e:
            self.logger.error(f"Failed to query GPT-4o mini: {str(e)}")
            return None
