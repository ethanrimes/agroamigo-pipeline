# GPT-4o Vision Test

This test case verifies the PDF processor's ability to use the GPT-4o vision API for PDFs with poor text extraction quality.

## Test Data

- `pereira_mercasa_20210331.pdf`: Sample price report from Mercasa market in Pereira, dated March 31, 2021. This PDF has lower quality text that cannot be reliably extracted using traditional methods.

## Mock Data

- `gpt4o_mock_response.csv`: Mock CSV response from the GPT-4o vision API. This simulates the API's response when analyzing images of the PDF pages.

## Expected Output

- `expected_output.csv`: Contains the expected processed data with correctly extracted:
  - Metadata (date, city, market)
  - Product categories (major_category, sub_category)
  - Product details (product, presentation, units)
  - Price information (daily_min, daily_max)

## Test Verification

The test verifies:
1. Text quality assessment (should determine text quality is poor)
2. Correct extraction method chosen (should be "gpt4o_vision")
3. Proper conversion of PDF to images for GPT-4o processing
4. Accurate parsing of the CSV text returned by GPT-4o
5. Correct association of product data with categories
6. Complete metadata extraction

This test uses a mock for the GPT-4o vision API to avoid actual API calls during testing.