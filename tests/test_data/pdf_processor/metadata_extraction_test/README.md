# Metadata Extraction Test

This test case verifies the PDF processor's ability to extract metadata from agricultural price reports.

## Test Data

- `test_metadata.pdf`: A PDF with known metadata including date, city, and market.

## Expected Output

- `expected_metadata.json`: JSON file containing the expected metadata values.

## Test Verification

The test verifies:
1. Date extraction from multiple formats (Spanish date formats)
2. City extraction from standardized header
3. Market identification
4. Source identification
5. Fallback extraction from filename if metadata is not in the content

This test focuses specifically on the metadata extraction component's ability to identify key report information from different formats and positions within the document.