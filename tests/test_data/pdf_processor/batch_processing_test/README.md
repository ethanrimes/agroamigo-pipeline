# Batch Processing Test

This test case verifies the PDF processor's ability to handle multiple PDF files in a batch, using different processing methods as needed.

## Test Data

- `sample1.pdf`: PDF with good text extraction quality
- `sample2.pdf`: PDF with poor text extraction quality, requiring GPT-4o vision
- `sample3.pdf`: PDF with mixed text quality, borderline case

## Mock Data

- `gpt4o_mock_sample1.csv`: Mock GPT-4o response for sample1.pdf
- `gpt4o_mock_sample2.csv`: Mock GPT-4o response for sample2.pdf
- `gpt4o_mock_sample3.csv`: Mock GPT-4o response for sample3.pdf

## Expected Output

- `expected_sample1.csv`: Expected output from processing sample1.pdf
- `expected_sample2.csv`: Expected output from processing sample2.pdf
- `expected_sample3.csv`: Expected output from processing sample3.pdf
- `expected_summary.json`: Expected summary statistics from batch processing

## Test Verification

The test verifies:
1. All files are processed correctly
2. Each file uses the appropriate extraction method based on text quality
3. Parallel processing works correctly
4. Error handling for any problematic files
5. Summary statistics match expected values
6. Output files match expected structure and content

This test checks the PDF processor's ability to efficiently process multiple files using multithreading while adapting to each file's characteristics.