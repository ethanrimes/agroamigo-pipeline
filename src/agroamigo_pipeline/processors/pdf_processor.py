"""
PDF processing module for Colombian Agricultural Price Data Pipeline.

This module provides backward compatibility with the refactored modular implementation.
It implements the same interface as the original PDFProcessor but delegates to the 
new thread-safe PDFProcessorOrchestrator class.
"""

from agroamigo_pipeline.processors.pdf_components import PDFProcessorOrchestrator

class PDFProcessor:
    """
    Adapter class for backward compatibility with the original pdf_processor.py.
    This delegates all functionality to the new PDFProcessorOrchestrator class.
    """
    
    def __init__(self, logger=None):
        """
        Initialize the PDF processor adapter.
        
        Args:
            logger (Logger): Logger instance
        """
        self.orchestrator = PDFProcessorOrchestrator(logger=logger)
        self.logger = self.orchestrator.logger
        self.registry = self.orchestrator.registry
        self.stats = self.orchestrator.stats
        self.timing_stats = self.orchestrator.timing_stats
    
    def _extract_text_with_pdfplumber(self, file_path):
        """Delegate to TextExtractor's extract_text_with_pdfplumber method."""
        return self.orchestrator.text_extractor.extract_text_with_pdfplumber(file_path)
    
    def _extract_table_with_pdfplumber(self, file_path):
        """Delegate to TextExtractor's extract_table_with_pdfplumber method."""
        return self.orchestrator.text_extractor.extract_table_with_pdfplumber(file_path)
    
    def _assess_text_quality(self, text):
        """Delegate to TextExtractor's assess_text_quality method."""
        return self.orchestrator.text_extractor.assess_text_quality(text)
    
    def _convert_pdf_to_images(self, pdf_path):
        """Delegate to ImageHandler's convert_pdf_to_images method."""
        return self.orchestrator.image_handler.convert_pdf_to_images(pdf_path)
    
    def _encode_image(self, image_path):
        """Delegate to ImageHandler's encode_image method."""
        return self.orchestrator.image_handler.encode_image(image_path)
    
    def _query_gpt4o_with_image(self, image_path):
        """Delegate to GPTClient's query_gpt4o_with_image method."""
        # First encode the image
        base64_image = self.orchestrator.image_handler.encode_image(image_path)
        return self.orchestrator.gpt_client.query_gpt4o_with_image(image_path, base64_image)
    
    def _extract_metadata_from_pdf(self, file_path, text):
        """Delegate to MetadataExtractor's extract_metadata_from_pdf method."""
        return self.orchestrator.metadata_extractor.extract_metadata_from_pdf(file_path, text)
    
    def _process_product_data(self, text):
        """Delegate to ProductParser's process_product_data method."""
        return self.orchestrator.product_parser.process_product_data(text)
    
    def _process_csv_from_gpt4o(self, csv_text, metadata):
        """Delegate to GPTCSVParser's process_csv_from_gpt4o method."""
        return self.orchestrator.gpt_csv_parser.process_csv_from_gpt4o(csv_text, metadata)
    
    def _process_extracted_text(self, file_path, text):
        """Delegate to ProductParser's process_extracted_text method."""
        # First extract metadata
        metadata = self.orchestrator.metadata_extractor.extract_metadata_from_pdf(file_path, text)
        return self.orchestrator.product_parser.process_extracted_text(file_path, text, metadata)
    
    def _process_pdf_with_gpt4o(self, file_path):
        """Delegate to the orchestrator's process_pdf_with_gpt4o method."""
        # Use the new implementation that returns both the DataFrame and GPT call count
        result, _ = self.orchestrator.process_pdf_with_gpt4o(
            file_path, 
            self.logger, 
            self.orchestrator.timing_stats
        )
        return result
    
    def process_pdf_file(self, file_path, output_dir=None, force_reprocess=False):
        """
        Process a single PDF file and save results to CSV.
        Simply pass to the orchestrator's process_single_pdf_file method.
        """
        # Check registry first
        registry_info = {
            'process': True,
            'stats': None
        }
        if not force_reprocess and self.registry.file_processed(file_path):
            file_info = self.registry.get_file_info(file_path)
            registry_info = {
                'process': False,
                'stats': file_info['processing_history'][-1]['stats'] if file_info else None
            }
        
        # Process the file
        result = self.orchestrator.process_single_pdf_file(
            file_path, 
            output_dir, 
            force_reprocess,
            registry_info
        )
        
        # Update registry if needed
        if result['registry_update']:
            self.registry.register_file(
                result['registry_update']['file_path'],
                output_path=result['registry_update']['output_path'],
                status=result['registry_update']['status'],
                error=result['registry_update']['error'],
                stats=result['registry_update']['stats']
            )
        
        # Return the stats
        return result['stats']
    
    def process_pdf_files(self, pdf_files=None, output_dir=None, start_date=None, end_date=None, max_workers=None, force_reprocess=False):
        """Delegate to the orchestrator's process_pdf_files method."""
        return self.orchestrator.process_pdf_files(
            pdf_files=pdf_files, 
            output_dir=output_dir, 
            start_date=start_date, 
            end_date=end_date, 
            max_workers=max_workers, 
            force_reprocess=force_reprocess
        )