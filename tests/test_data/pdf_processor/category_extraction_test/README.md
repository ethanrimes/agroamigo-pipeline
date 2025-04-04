# Category Extraction Test

This test case verifies the PDF processor's ability to extract and maintain product category hierarchies from agricultural price reports.

## Test Data

- `test_categories.pdf`: A PDF with a known category structure including major categories and subcategories.

## Expected Output

- `expected_categories.csv`: CSV file containing the expected category hierarchy and product assignments.

## Test Verification

The test verifies:
1. Major category identification (Frutas, Verduras, Tuberculos, etc.)
2. Subcategory identification (Citricos, Otras frutas, etc.)
3. Correct category-product relationships
4. Stack-based category tracking across page boundaries
5. Handling of special category formats (all caps, "Otros X", etc.)

This test focuses specifically on the product parser component's ability to maintain the hierarchical structure of product categories as they appear in the document, ensuring products are correctly associated with their categories.