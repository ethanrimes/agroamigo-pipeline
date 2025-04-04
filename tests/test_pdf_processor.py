"""
Test suite for the PDF processor component of the Colombian Agricultural Price Data Pipeline.

This test suite verifies the functionality of the PDF processor using different test
scenarios with known PDF inputs and expected outputs.
"""

import os
import pytest
import pandas as pd
from pathlib import Path
import shutil
from unittest.mock import patch, MagicMock

from agroamigo_pipeline.processors.pdf_processor import PDFProcessor
from agroamigo_pipeline.utils.exceptions import PDFProcessingError

# Constants
TEST_DATA_DIR = Path(__file__).parent / "test_data" / "pdf_processor"
OUTPUT_DIR = TEST_DATA_DIR / "outputs"


def setup_module():
    """Setup for the test module."""
    # Ensure output directory exists and is empty
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# def teardown_module():
#     """Teardown for the test module."""
#     # Clean up the output directory after tests
#     if OUTPUT_DIR.exists():
#         shutil.rmtree(OUTPUT_DIR)


class TestPDFProcessor:
    """Test suite for the PDFProcessor class."""

    @pytest.fixture
    def pdf_processor(self):
        """Fixture for creating a PDFProcessor instance."""
        return PDFProcessor()

    def test_single_text_extraction_pdf(self, pdf_processor):
        """
        Test processing a single PDF file with good text extraction quality.
        
        This test uses a known PDF that can be processed with direct text extraction.
        """
        # Test data paths
        test_case_dir = TEST_DATA_DIR / "text_extraction_test"
        test_pdf = test_case_dir / "tunja_complejo_20250221.pdf"
        expected_csv = test_case_dir / "expected_text_tunja.txt"

        
        # Process the PDF
        stats = pdf_processor.process_pdf_file(
            file_path=str(test_pdf),
            output_dir=str(OUTPUT_DIR),
            force_reprocess=True
        )
        
        # Get output file path from registry
        output_file = None
        for root, _, files in os.walk(OUTPUT_DIR):
            for file in files:
                if file.endswith(".csv"):
                    output_file = os.path.join(root, file)
                    break
        
        # Verify output file was created
        assert output_file is not None, "No output CSV file was created"
        
        # Load actual and expected data
        actual_df = pd.read_csv(output_file)
        expected_df = pd.read_csv(expected_csv)
        
        # Verify data matches
        assert not actual_df.empty, "Output DataFrame is empty"
        assert len(actual_df) == len(expected_df), f"Row count mismatch: {len(actual_df)} vs {len(expected_df)}"
        
        # Check key columns
        expected_columns = ['date', 'city', 'market', 'major_category', 'product', 
                           'presentation', 'units', 'daily_min', 'daily_max']
        for col in expected_columns:
            assert col in actual_df.columns, f"Missing column: {col}"
        
        # Verify metadata
        assert actual_df['date'].iloc[0] == expected_df['date'].iloc[0], "Date mismatch"
        assert actual_df['city'].iloc[0] == expected_df['city'].iloc[0], "City mismatch"
        assert actual_df['market'].iloc[0] == expected_df['market'].iloc[0], "Market mismatch"
        
        # Verify statistics
        assert stats['unique_products'] == expected_df['product'].nunique(), "Product count mismatch"
        assert stats['unique_major_categories'] == expected_df['major_category'].nunique(), "Category count mismatch"
        assert stats['extraction_method'] == "text_extraction", "Wrong extraction method used"

    def test_single_gpt4o_vision_pdf(self, pdf_processor):
        """
        Test processing a single PDF file with poor text extraction quality 
        that requires GPT-4o vision API.
        
        This test uses a known PDF that needs to be processed with GPT-4o vision
        due to poor text extraction quality.
        """
        # Test data paths
        test_case_dir = TEST_DATA_DIR / "gpt4o_vision_test"
        test_pdf = test_case_dir / "tunja_complejo_20250221.pdf"
        expected_csv = test_case_dir / "expected_tunja_complejo_20250221.csv"
        mock_response_file = test_case_dir / "gpt4o_mock_tunja_complejo_20250221.csv"

        
        # Mock GPT-4o vision API response
        with patch('agroamigo_pipeline.processors.pdf.gpt_client.GPTClient.query_gpt4o_with_image') as mock_gpt:
            # Load mock CSV response from file
            mock_response_file = test_case_dir / "gpt4o_mock_response.csv"
            with open(mock_response_file, 'r') as f:
                mock_response = f.read()
            
            # Configure mock
            mock_gpt.return_value = mock_response
            
            # Process the PDF
            stats = pdf_processor.process_pdf_file(
                file_path=str(test_pdf),
                output_dir=str(OUTPUT_DIR),
                force_reprocess=True
            )
            
            # Get output file path
            output_file = None
            for root, _, files in os.walk(OUTPUT_DIR):
                for file in files:
                    if file.endswith(".csv"):
                        output_file = os.path.join(root, file)
                        break
            
            # Verify output file was created
            assert output_file is not None, "No output CSV file was created"
            
            # Load actual and expected data
            actual_df = pd.read_csv(output_file)
            expected_df = pd.read_csv(expected_csv)
            
            # Verify data matches
            assert not actual_df.empty, "Output DataFrame is empty"
            assert len(actual_df) == len(expected_df), f"Row count mismatch: {len(actual_df)} vs {len(expected_df)}"
            
            # Verify extraction method
            assert stats['extraction_method'] == "gpt4o_vision", "GPT-4o vision extraction method not used"
            
            # Verify mock was called the expected number of times
            assert mock_gpt.call_count > 0, "GPT-4o vision API was not called"

    def test_batch_processing(self, pdf_processor):
        """
        Test processing multiple PDF files in batch mode.
        
        This test uses a mix of PDFs that require different processing methods.
        """
        # Test data paths
        test_case_dir = TEST_DATA_DIR / "batch_processing_test"
        pdf_files = list(test_case_dir.glob("*.pdf"))
        expected_summary = test_case_dir / "expected_summary.json"
        
        # Ensure we have PDFs to test
        assert len(pdf_files) > 0, "No test PDF files found in batch test directory"
        
        # Mock GPT-4o vision API response
        with patch('agroamigo_pipeline.processors.pdf.gpt_client.GPTClient.query_gpt4o_with_image') as mock_gpt:
            # Configure mock to return CSV data based on the file name
            def mock_response(image_path, *args, **kwargs):
                # Extract PDF name from the image path
                image_name = Path(image_path).name
                pdf_name = image_name.split('_page_')[0]
                
                # Find corresponding mock response
                mock_response_file = test_case_dir / f"gpt4o_mock_{pdf_name}.csv"
                if mock_response_file.exists():
                    with open(mock_response_file, 'r') as f:
                        return f.read()
                return ""
            
            mock_gpt.side_effect = mock_response
            
            # Process the PDFs
            stats = pdf_processor.process_pdf_files(
                pdf_files=[str(pdf) for pdf in pdf_files],
                output_dir=str(OUTPUT_DIR),
                force_reprocess=True
            )
            
            # Verify all files were processed
            assert stats['total_files'] == len(pdf_files), "Not all files were processed"
            assert stats['processed_files'] + stats['failed_files'] + stats['skipped_files'] == len(pdf_files), "File count mismatch"
            
            # Verify output files were created (should match the number of successfully processed files)
            output_files = list(Path(OUTPUT_DIR).glob("*.csv"))
            assert len(output_files) == stats['processed_files'], "Output file count doesn't match processed file count"
            
            # Check each output file against expected output
            for output_file in output_files:
                base_name = output_file.stem
                expected_file = test_case_dir / f"expected_{base_name}.csv"
                
                # Skip if no expected file is available
                if not expected_file.exists():
                    continue
                
                actual_df = pd.read_csv(output_file)
                expected_df = pd.read_csv(expected_file)
                
                assert not actual_df.empty, f"Output DataFrame is empty for {base_name}"
                assert len(actual_df) == len(expected_df), f"Row count mismatch for {base_name}: {len(actual_df)} vs {len(expected_df)}"

    def test_error_handling(self, pdf_processor):
        """
        Test error handling when processing corrupted or invalid PDFs.
        """
        # Test data paths
        test_case_dir = TEST_DATA_DIR / "error_handling_test"
        invalid_pdf = test_case_dir / "corrupted_page_1.pdf"
        
        # Ensure test file exists
        assert invalid_pdf.exists(), "Test file for error handling not found"
        
        # Process the invalid PDF
        stats = pdf_processor.process_pdf_file(
            file_path=str(invalid_pdf),
            output_dir=str(OUTPUT_DIR),
            force_reprocess=True
        )
        
        # Verify error was detected
        assert stats['error_occurred'], "Error was not detected for invalid PDF"
        assert stats['errors_warnings'] != "", "No error message recorded"

    def test_metadata_extraction(self, pdf_processor):
        """
        Test extraction of metadata from PDFs.
        """
        # Test data paths
        test_case_dir = TEST_DATA_DIR / "metadata_extraction_test"
        test_pdf = test_case_dir / "tunja_complejo_20250221.pdf"
        expected_metadata = test_case_dir / "expected_metadata.json"

        from agroamigo_pipeline.utils.logger import setup_logging
        logging_setup = setup_logging()
        pdf_processor.logger = logging_setup['pdf_processor']
        pdf_processor.orchestrator.logger = logging_setup['pdf_processor']
        
        # Add this line to test logging is working
        pdf_processor.logger.info("TESTING: Logging is initialized and working")
        
        # Ensure test file exists
        assert test_pdf.exists(), "Test file for metadata extraction not found"
        
        # Process the PDF
        stats = pdf_processor.process_pdf_file(
            file_path=str(test_pdf),
            output_dir=str(OUTPUT_DIR),
            force_reprocess=True
        )
        
        # Get output file path
        output_file = None
        for root, _, files in os.walk(OUTPUT_DIR):
            for file in files:
                if file.endswith(".csv"):
                    output_file = os.path.join(root, file)
                    break
        
        # Verify output file was created
        assert output_file is not None, "No output CSV file was created"
        
        # Load actual data
        actual_df = pd.read_csv(output_file)
        
        # Verify metadata
        assert 'date' in actual_df.columns, "Date column missing"
        assert 'city' in actual_df.columns, "City column missing"
        assert 'market' in actual_df.columns, "Market column missing"
        
        # Check the first row contains the expected metadata
        first_row = actual_df.iloc[0]
        assert first_row['date'] is not None and first_row['date'] != "", "Date not extracted"
        assert first_row['city'] is not None and first_row['city'] != "", "City not extracted"
        assert first_row['market'] is not None and first_row['market'] != "", "Market not extracted"

    def test_product_category_extraction(self, pdf_processor):
        """
        Test extraction of product categories and hierarchies from PDFs.
        """
        # Test data paths
        test_case_dir = TEST_DATA_DIR / "category_extraction_test"
        test_pdf = test_case_dir / "tunja_complejo_20250221.pdf"
        expected_categories = test_case_dir / "expected_categories.csv"


        
        # Ensure test file exists
        assert test_pdf.exists(), "Test file for category extraction not found"
        
        # Process the PDF
        pdf_processor.process_pdf_file(
            file_path=str(test_pdf),
            output_dir=str(OUTPUT_DIR),
            force_reprocess=True
        )
        
        # Get output file path
        output_file = None
        for root, _, files in os.walk(OUTPUT_DIR):
            for file in files:
                if file.endswith(".csv"):
                    output_file = os.path.join(root, file)
                    break
        
        # Verify output file was created
        assert output_file is not None, "No output CSV file was created"
        
        # Load actual and expected data
        actual_df = pd.read_csv(output_file)
        expected_df = pd.read_csv(expected_categories)
        
        # Verify category columns exist
        assert 'major_category' in actual_df.columns, "Major category column missing"
        assert 'sub_category' in actual_df.columns, "Sub-category column missing"
        
        # Compare unique categories
        actual_major_cats = set(actual_df['major_category'].dropna().unique())
        expected_major_cats = set(expected_df['major_category'].dropna().unique())
        
        assert actual_major_cats == expected_major_cats, "Major category mismatch"
        
        # Check products are assigned to correct categories
        for _, expected_row in expected_df.iterrows():
            product = expected_row['product']
            expected_major_cat = expected_row['major_category']
            
            actual_products = actual_df[actual_df['product'] == product]
            if not actual_products.empty:
                assert actual_products['major_category'].iloc[0] == expected_major_cat, \
                    f"Product {product} assigned to wrong major category"


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])