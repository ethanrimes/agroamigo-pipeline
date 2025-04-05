#!/usr/bin/env python3
"""
Tests for the ImageHandler class in the PDF processor.
"""
import os
import shutil
import pytest
import tempfile
from pathlib import Path

from src.agroamigo_pipeline.processors.pdf_components.image_handler import ImageHandler

class TestImageHandler:
    """Tests for the ImageHandler class"""
    
    @pytest.fixture
    def setup_test_dirs(self):
        """Setup test directories and cleanup after test"""
        # Define test directories based on the new structure
        test_input_dir = Path('tests/test_data/image_handler/inputs')
        test_output_dir = Path('tests/test_data/image_handler/outputs')
        
        # Ensure output directory exists
        test_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Get path to test PDF
        test_pdf_path = test_input_dir / 'tunja_complejo_20250221.pdf'
        
        # If the PDF doesn't exist in the input directory, try to find it in other test directories
        if not test_pdf_path.exists():
            # Check if we need to copy from another location
            possible_locations = [
                Path('tests/test_data/pdf_processor/batch_processing_test/tunja_complejo_20250221.pdf'),
                Path('tests/test_data/pdf_processor/gpt4o_vision_test/tunja_complejo_20250221.pdf'),
                Path('tests/test_data/pdf_processor/text_extraction_test/tunja_complejo_20250221.pdf'),
                Path('tests/test_data/pdf_processor/category_extraction_test/tunja_complejo_20250221.pdf'),
                Path('tests/test_data/pdf_processor/metadata_extraction_test/tunja_complejo_20250221.pdf')
            ]
            
            for location in possible_locations:
                if location.exists():
                    print(f"Found PDF at {location}, copying to {test_input_dir}")
                    # Make sure input directory exists
                    test_input_dir.mkdir(parents=True, exist_ok=True)
                    # Copy the PDF
                    shutil.copy(location, test_pdf_path)
                    break
        
        assert test_pdf_path.exists(), f"Test PDF not found: {test_pdf_path}. Please copy a test PDF to this location."
        
        yield {
            'test_output_dir': test_output_dir,
            'test_pdf_path': test_pdf_path
        }
        
        # Clean up temporary files if needed, but keep the output files for inspection
    
    def test_convert_pdf_to_images(self, setup_test_dirs):
        """Test converting PDF to images"""
        # Get the test paths from fixture
        test_dirs = setup_test_dirs
        test_pdf_path = test_dirs['test_pdf_path']
        test_output_dir = test_dirs['test_output_dir']
        
        # Initialize the ImageHandler
        image_handler = ImageHandler()
        
        # Convert PDF to images
        image_paths = image_handler.convert_pdf_to_images(str(test_pdf_path))
        
        # Verify we got some image paths back
        assert len(image_paths) > 0, "No images were generated from the PDF"
        
        # Copy the generated images to our test output directory for inspection
        for i, img_path in enumerate(image_paths):
            filename = f"test_img_{i}.jpg"
            dest_path = test_output_dir / filename
            shutil.copy(img_path, dest_path)
            
            # Verify the copied image exists
            assert dest_path.exists(), f"Failed to copy image to {dest_path}"
        
        # Check if images are readable
        for img_path in image_paths:
            assert os.path.exists(img_path), f"Image file doesn't exist: {img_path}"
            assert os.path.getsize(img_path) > 0, f"Image file is empty: {img_path}"
    
    def test_encode_image(self, setup_test_dirs):
        """Test encoding an image to base64"""
        # Get the test paths from fixture
        test_dirs = setup_test_dirs
        test_pdf_path = test_dirs['test_pdf_path']
        test_output_dir = test_dirs['test_output_dir']
        
        # Initialize the ImageHandler
        image_handler = ImageHandler()
        
        # First convert PDF to images
        image_paths = image_handler.convert_pdf_to_images(str(test_pdf_path))
        assert len(image_paths) > 0, "No images were generated from the PDF"
        
        # Take the first image and encode it
        test_image_path = image_paths[0]
        encoded_image = image_handler.encode_image(test_image_path)
        
        # Verify the encoded image is not empty and is a base64 string
        assert encoded_image, "Encoded image is empty"
        assert isinstance(encoded_image, str), "Encoded image is not a string"
        assert len(encoded_image) > 0, "Encoded image string is empty"
        
        # Save the encoded image to a text file for inspection
        encoded_path = test_output_dir / "encoded_image.txt"
        with open(encoded_path, 'w') as f:
            f.write(encoded_image[:100] + "...")  # Just write the beginning for inspection
        
        assert encoded_path.exists(), f"Failed to save encoded image data to {encoded_path}"
    
    def test_timing_stats(self, setup_test_dirs):
        """Test that timing statistics are correctly recorded"""
        # Get the test paths from fixture
        test_dirs = setup_test_dirs
        test_pdf_path = test_dirs['test_pdf_path']
        
        # Initialize the ImageHandler with a fresh timing_stats dictionary
        timing_stats = {
            'pdf_to_images': [],
            'image_encoding': []
        }
        image_handler = ImageHandler(timing_stats=timing_stats)
        
        # Convert PDF to images
        image_paths = image_handler.convert_pdf_to_images(str(test_pdf_path))
        
        # Encode one of the images
        if image_paths:
            image_handler.encode_image(image_paths[0])
        
        # Verify timing statistics were recorded
        assert len(timing_stats['pdf_to_images']) > 0, "No PDF conversion timing stats recorded"
        assert timing_stats['pdf_to_images'][0]['pdf'] == os.path.basename(test_pdf_path)
        assert timing_stats['pdf_to_images'][0]['duration'] > 0
        
        if image_paths:
            assert len(timing_stats['image_encoding']) > 0, "No image encoding timing stats recorded"
            assert timing_stats['image_encoding'][0]['image'] == os.path.basename(image_paths[0])
            assert timing_stats['image_encoding'][0]['duration'] > 0

if __name__ == "__main__":
    pytest.main(["-xvs", __file__])