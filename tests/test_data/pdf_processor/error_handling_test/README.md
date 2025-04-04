# Error Handling Test

This test case verifies the PDF processor's ability to handle invalid or corrupted PDF files gracefully.

## Test Data

- `invalid_or_corrupted.pdf`: A deliberately invalid or corrupted PDF file that should trigger error handling.

## Test Verification

The test verifies:
1. The processor detects the invalid file
2. Appropriate error flags are set in the statistics
3. Meaningful error messages are captured
4. The processor doesn't crash with exceptions
5. The registry correctly records the file as failed
6. Summary statistics are updated properly

This test ensures that the PDF processor can handle problematic files without breaking the entire pipeline and provides useful error information for debugging.