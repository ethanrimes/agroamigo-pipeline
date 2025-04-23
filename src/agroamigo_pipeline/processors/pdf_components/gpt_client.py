import os
import time
import logging
import requests
import json
import traceback

from agroamigo_pipeline.config.settings import GPT4O_API_KEY

class GPTClient:
    """Client for interacting with GPT-4.1 nano API."""
    
    def __init__(self, logger=None, timing_stats=None, save_responses=True):
        """
        Initialize the GPT client.
        
        Args:
            logger (Logger): Logger instance
            timing_stats (dict): Dictionary to store timing statistics
            save_responses (bool): Whether to save responses to a permanent directory
        """
        self.logger = logger or logging.getLogger("pdf_processor")
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
    
    def query_gpt4o_with_image(self, image_path, base64_image=None, prompt_type="table", source_pdf_path=None):
        """
        Query GPT-4.1 nano with an image to extract text data.
        
        Args:
            image_path (str): Path to the image file
            base64_image (str): Pre-encoded base64 image data (optional)
            prompt_type (str): Type of prompt to use - "table" or "header"
            source_pdf_path (str): Path to the source PDF file (for logging purposes)
            
        Returns:
            str: Extracted text from the image
        """
        if not self.api_key:
            self.logger.error("No GPT-4o API key provided")
            return None
        
        start_time = time.time()
        pdf_filename = os.path.basename(source_pdf_path) if source_pdf_path else "unknown_pdf"
        self.logger.info(f"Querying GPT-4.1 nano with image: {os.path.basename(image_path)} from PDF: {pdf_filename}, prompt type: {prompt_type} | file_path: {image_path}")
        
        try:
            # Use provided base64 image or assume it needs to be encoded
            if base64_image is None:
                self.logger.warning("No base64_image provided, image encoding should be done separately | file_path: {image_path}")
                return None
            
            # Select prompt based on type
            if prompt_type == "header":
                prompt = """
                Extract EXACTLY what you see in the header section of this image, preserving the original formatting and line breaks.

                The header contains these key elements:
                1. The title "PRECIOS DE VENTA MAYORISTA"
                2. The location line (city and market)
                3. "PRODUCTOS PRIMERA CALIDAD - Boletín Gratuito"
                4. The date

                DO NOT interpret, modify, or add any information. Extract the text exactly as it appears, maintaining all punctuation and spacing.

                For example, if you see:
                PRECIOS DE VENTA MAYORISTA
                Bogotá, D.C., Corabastos
                PRODUCTOS PRIMERA CALIDAD - Boletín Gratuito
                07 de Septiembre de 2024

                or

                PRECIOS DE VENTA MAYORISTA
                PRODUCTOS DE PRIMERA CALIDAD - Boletín Gratuito
                Pasto, El Poterillo
                16 Diciembre de 2024

                That is precisely what you should return.
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
                
                
                Producto;Presentacion;Unidades;Minimo;Maximo;Minimo;Maximo
                Carnes;;;;;;
                Carne de cerdo;;;;;;
                Carne de cerdo;Kilogramo;1 Kilogramo;20.000;24.000;0;0
                Carne de cerdo, costilla;Kilogramo;1 Kilogramo;20.000;24.000;0;0
                Frutas;;;;;;
                Cítricos;;;;;;
                Limón tahití;Canastilla;22 Kilogramo;40.000;45.000;0;0
                Mandarina arrayana;Canastilla;22 Kilogramo;50.000;60.000;0;0
                Yuca criolla;Bolsa;40 Kilogramo;35.000;38.000;0;0
                """

            # Prepare headers and payload
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            payload = {
                "model": "gpt-4.1-nano",
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
            self.logger.debug(f"Sending request to GPT-4.1 nano API for {os.path.basename(image_path)} from PDF: {pdf_filename} | file_path: {image_path}")
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=60  # Add timeout to prevent hanging
            )
            
            if response.status_code != 200:
                self.logger.error(f"GPT-4.1 nano API error: {response.status_code} - {response.text} | file_path: {image_path}")
                return None
            
            # Extract response
            response_json = response.json()
            raw_content = response_json['choices'][0]['message']['content']
            
            # Store the original raw content for debugging purposes
            original_raw_content = raw_content
            
            # Clean the content immediately with regex processing
            import re
            
            # First store the raw content for logging/debugging
            self.logger.debug(f"Raw GPT-4.1 nano response received for {os.path.basename(image_path)} from PDF: {pdf_filename} - {len(raw_content)} characters | file_path: {image_path}")
            self.logger.debug(f"GPT-4.1 nano raw response (first 200 chars): {raw_content[:200]}... | file_path: {image_path}")
            
            # Process the content - remove all instances of markdown formatting
            content = raw_content
            
            if prompt_type == "table":
                # Remove all instances of triple backticks and plaintext/text tags
                content = re.sub(r'```(?:plaintext|text|csv)?', '', content)
                content = re.sub(r'```', '', content)
                
                # Also clean up any standalone plaintext or text tags
                content = re.sub(r'^(?:plaintext|text|csv)$', '', content, flags=re.MULTILINE)
                
                # Log the cleaning results
                self.logger.info(f"Cleaned GPT-4o response from {len(raw_content)} to {len(content)} characters | file_path: {image_path}")
                if raw_content != content:
                    self.logger.info(f"Markdown formatting removed from GPT-4o response | file_path: {image_path}")
            
            # Save both raw and cleaned response to files
            self._save_response(image_path, original_raw_content, prompt_type + "_raw", source_pdf_path)
            self._save_response(image_path, content, prompt_type, source_pdf_path)
            
            duration = time.time() - start_time
            self.timing_stats['gpt4_query'].append({
                'image': os.path.basename(image_path),
                'pdf': pdf_filename,
                'prompt_type': prompt_type,
                'duration': duration
            })
            self.logger.debug(f"GPT-4.1 nano query for {os.path.basename(image_path)} from PDF: {pdf_filename} completed in {duration:.2f} seconds | file_path: {image_path}")
            
            return content
            
        except Exception as e:
            self.logger.error(f"Failed to query GPT-4.1 nano for {os.path.basename(image_path)} from PDF: {pdf_filename}: {str(e)} | file_path: {image_path}")
            self.logger.error(f"Error details: {traceback.format_exc()} | file_path: {image_path}")
            return None
    
    def _save_response(self, image_path, content, prompt_type, source_pdf_path=None):
        """
        Helper method to save responses to log files
        
        Args:
            image_path (str): Path to the image
            content (str): Content to save (raw or cleaned)
            prompt_type (str): Type of prompt used ("table", "header", "table_raw", etc.)
            source_pdf_path (str): Path to the source PDF file
        """
        image_basename = os.path.basename(image_path)
        pdf_filename = os.path.basename(source_pdf_path) if source_pdf_path else "unknown_pdf"
        
        # Save to log directory
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs", "gpt_responses")
        os.makedirs(log_dir, exist_ok=True)
        
        # Determine if this is raw or cleaned content based on prompt_type
        content_type = "raw" if "_raw" in prompt_type else "cleaned"
        prompt_base = prompt_type.replace("_raw", "")
        
        # Save to permanent directory if requested
        if self.save_responses:
            log_file = os.path.join(log_dir, f"gpt_response_{prompt_type}_{pdf_filename}_{image_basename}_{int(time.time())}.txt")
            try:
                with open(log_file, "w", encoding="utf-8") as f:
                    f.write(f"Source PDF: {source_pdf_path}\n")
                    f.write(f"Image: {image_path}\n")
                    f.write(f"Prompt type: {prompt_base}\n")
                    f.write(f"Content type: {content_type}\n")
                    f.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Content length: {len(content)} characters\n")
                    f.write(f"GPT-4.1 nano response ({content_type}):\n")
                    f.write(content)
                self.logger.debug(f"Saved {content_type} GPT-4.1 nano response to: {log_file} | file_path: {image_path}")
            except Exception as e:
                self.logger.warning(f"Failed to save {content_type} GPT-4.1 nano response to file: {str(e)} | file_path: {image_path}")