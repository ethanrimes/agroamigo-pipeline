# Text Extraction Test

This test case verifies the PDF processor's ability to directly extract text from PDFs with good text quality.

## Test Data

- `bogota_corabastos_20230720.pdf`: Sample price report from Corabastos market in Bogotá, dated July 20, 2023. This PDF has high-quality text that can be extracted reliably using PyPDF2 or pdfplumber.

## Expected Output

- `expected_output.csv`: Contains the expected processed data with correctly extracted:
  - Metadata (date, city, market)
  - Product categories (major_category, sub_category)
  - Product details (product, presentation, units)
  - Price information (daily_min, daily_max)

## Test Verification

The test verifies:
1. Text extraction quality assessment
2. Correct extraction method chosen (should be "text_extraction")
3. Proper parsing of product data and categorization
4. Accurate price extraction
5. Complete metadata extraction

This test should not require GPT-4o vision API since the PDF has good text quality.