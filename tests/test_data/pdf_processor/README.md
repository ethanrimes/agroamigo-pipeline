# PDF Processor Test Data

This directory contains test data for the PDF processor component of the Colombian Agricultural Price Data Pipeline.

## Directory Structure

- `outputs/`: Directory for test outputs (created during tests)
- `text_extraction_test/`: Test data for the text extraction functionality
- `gpt4o_vision_test/`: Test data for the GPT-4o vision API functionality
- `batch_processing_test/`: Test data for batch processing functionality
- `error_handling_test/`: Test data for error handling
- `metadata_extraction_test/`: Test data for metadata extraction
- `category_extraction_test/`: Test data for category hierarchy extraction

## PDF Test Files

The test PDFs are market price reports from different cities in Colombia:

1. `yopal_casanare_20250221.pdf`: Price report from Yopal, Casanare (February 21, 2025)
2. `villavicencio_cav_20250221.pdf`: Price report from Villavicencio, CAV (February 21, 2025)
3. `tunja_complejo_20250221.pdf`: Price report from Tunja (February 21, 2025)

## Mock GPT-4o Responses

For testing the GPT-4o vision API functionality, mock response files are provided:

- `gpt4o_mock_yopal_casanare_20250221.csv`
- `gpt4o_mock_villavicencio_cav_20250221.csv`
- `gpt4o_mock_tunja_complejo_20250221.csv`

## Expected Outputs

Each test directory contains expected output files:

- `expected_output.csv`: Expected processed data
- `expected_text_*.txt`: Expected extracted text (first 15 lines)
- `expected_metadata.json`: Expected metadata
- `expected_categories.csv`: Expected category hierarchy
- `expected_summary.json`: Expected batch processing summary

## Using These Test Files

These test files are used by the unit tests in `test_pdf_processor.py` to verify the functionality of the PDF processor component. The tests compare the actual processing results with the expected outputs to ensure the processor is working correctly.