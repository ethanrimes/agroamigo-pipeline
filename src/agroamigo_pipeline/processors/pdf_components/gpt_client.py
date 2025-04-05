import os
import time
import logging
import requests
import json
import traceback

from agroamigo_pipeline.config.settings import GPT4O_API_KEY

class GPTClient:
    """Client for interacting with GPT-4o mini API."""
    
    def __init__(self, logger=None, timing_stats=None, save_responses=True):
        """
        Initialize the GPT client.
        
        Args:
            logger (Logger): Logger instance
            timing_stats (dict): Dictionary to store timing statistics
            save_responses (bool): Whether to save responses to a permanent directory
        """
        self.logger = logger or logging.getLogger(__name__)
        self.timing_stats = timing_stats or {
            'gpt4_query': []
        }
        self.api_key = GPT4O_API_KEY
        self.save_responses = save_responses
        
        # Create permanent directory for saved responses if requested
        if self.save_responses:
            self.responses_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
                                              "logs", "permanent_gpt_responses")
            os.makedirs(self.responses_dir, exist_ok=True)
            self.logger.info(f"Will save GPT-4o responses to permanent directory: {self.responses_dir}")
    
    def query_gpt4o_with_image(self, image_path, base64_image=None, prompt_type="table"):
        """
        Query GPT-4o mini with an image to extract text data.
        
        Args:
            image_path (str): Path to the image file
            base64_image (str): Pre-encoded base64 image data (optional)
            prompt_type (str): Type of prompt to use - "table" or "header"
            
        Returns:
            str: Extracted text from the image
        """
        if not self.api_key:
            self.logger.error("No GPT-4o API key provided")
            return None
        
        start_time = time.time()
        self.logger.info(f"Querying GPT-4o mini with image: {os.path.basename(image_path)}, prompt type: {prompt_type}")
        
        try:
            # Use provided base64 image or assume it needs to be encoded
            if base64_image is None:
                self.logger.warning("No base64_image provided, image encoding should be done separately")
                return None
            
            # Select prompt based on type
            if prompt_type == "header":
                prompt = """
                Extract only the header section from this image. The header includes the title, location, and date—usually found at the top of the document. Return the extracted text as plain text, preserving line breaks. Do not include any table data or body content.
                """
            else:  # Default to table prompt
                prompt = """
                Extract the table from the image and convert it into CSV format using the following column structure:

                Producto;Presentacion;Unidades;Minimo;Maximo;Minimo;Maximo

                Instructions:
                - Each row should represent a product.
                - Use semicolons (`;`) to separate all fields.
                - If any field is missing or unreadable, leave it blank or use "None".
                - If a row only contains a category label (like "Frutas" or "Carnes"), output it as: value;;;;;; (i.e., followed by six semicolons).
                - Ensure all rows are included in the output, including those that only contain a category label, even if they are two in a row.
                - Do not include any extra text, explanations, or characters — output only the raw CSV data as plain text.

                Example format:

                Carnes;;;;;;
                Carne de cerdo;Kilogramo;1 Kilogramo;20.000;24.000;0;0
                Carne de cerdo, costilla;Kilogramo;1 Kilogramo;20.000;24.000;0;0
                Frutas;;;;;;
                Cítricos;;;;;;
                Limón tahití;Canastilla;22 Kilogramo;40.000;45.000;0;0
                Mandarina arrayana;Canastilla;22 Kilogramo;50.000;60.000;0;0
                ...
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
                "max_tokens": 10000
            }
            
            # Make API request
            self.logger.info(f"Sending request to GPT-4o mini API for {os.path.basename(image_path)}")
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=60  # Add timeout to prevent hanging
            )
            
            if response.status_code != 200:
                self.logger.error(f"GPT-4o mini API error: {response.status_code} - {response.text}")
                return None
            
            # Extract and process response
            response_json = response.json()
            content = response_json['choices'][0]['message']['content']
            
            # Log the raw response
            self.logger.info(f"Raw GPT-4o mini response received for {os.path.basename(image_path)} - {len(content)} characters")
            self.logger.debug(f"GPT-4o mini raw response (first 200 chars): {content[:200]}...")
            
            # Save raw response to files
            self._save_response(image_path, content, prompt_type)
            
            duration = time.time() - start_time
            self.timing_stats['gpt4_query'].append({
                'image': os.path.basename(image_path),
                'prompt_type': prompt_type,
                'duration': duration
            })
            self.logger.info(f"GPT-4o mini query for {os.path.basename(image_path)} completed in {duration:.2f} seconds")
            
            return content
            
        except Exception as e:
            self.logger.error(f"Failed to query GPT-4o mini for {os.path.basename(image_path)}: {str(e)}")
            self.logger.error(f"Error details: {traceback.format_exc()}")
            return None
    
    def _save_response(self, image_path, content, prompt_type):
        """Helper method to save responses to log files"""
        image_basename = os.path.basename(image_path)
        
        # Save to log directory
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "gpt_responses")
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, f"gpt_response_{prompt_type}_{image_basename}_{int(time.time())}.txt")
        try:
            with open(log_file, "w", encoding="utf-8") as f:
                f.write(f"Image: {image_path}\n")
                f.write(f"Prompt type: {prompt_type}\n")
                f.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("Raw GPT-4o mini response:\n")
                f.write(content)
            self.logger.info(f"Saved raw GPT-4o mini response to: {log_file}")
        except Exception as e:
            self.logger.warning(f"Failed to save raw GPT-4o mini response to file: {str(e)}")
        
        # Save to permanent directory if requested
        if self.save_responses:
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            permanent_log_file = os.path.join(self.responses_dir, f"permanent_response_{prompt_type}_{image_basename}_{timestamp}.txt")
            try:
                with open(permanent_log_file, "w", encoding="utf-8") as f:
                    f.write(f"Image: {image_path}\n")
                    f.write(f"Prompt type: {prompt_type}\n")
                    f.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Response length: {len(content)} characters\n")
                    f.write("Raw GPT-4o mini response:\n")
                    f.write(content)
                self.logger.info(f"Saved permanent GPT response to: {permanent_log_file}")
            except Exception as e:
                self.logger.error(f"Failed to save permanent GPT response: {str(e)}")