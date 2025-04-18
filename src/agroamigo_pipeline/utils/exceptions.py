"""
Custom exceptions for the Colombian Agricultural Price Data Pipeline.
"""

class PipelineError(Exception):
    """Base class for all pipeline-specific errors."""
    pass

class ScraperError(PipelineError):
    """Error while scraping data from sources."""
    pass

class ExtractorError(PipelineError):
    """Error while extracting files."""
    pass

class FileProcessingError(PipelineError):
    """Error while processing a file."""
    pass

class ExcelProcessingError(FileProcessingError):
    """Error while processing an Excel file."""
    pass

class PDFProcessingError(FileProcessingError):
    """Error while processing a PDF file."""
    pass

class DataAggregationError(PipelineError):
    """Error while aggregating data."""
    pass

class APIError(PipelineError):
    """Error while interacting with external APIs."""
    pass

class ValidationError(PipelineError):
    """Error while validating data."""
    
    def __init__(self, message, validation_errors=None):
        super().__init__(message)
        self.validation_errors = validation_errors or []

# New validation-specific exceptions
class NoMajorCategoryError(ValidationError):
    """Exception raised when major category is missing."""
    def __init__(self, message="Major category is missing"):
        self.message = message
        super().__init__(self.message)

class InvalidDateError(ValidationError):
    """Exception raised when date is invalid."""
    def __init__(self, message="Date is invalid or not a datetime object"):
        self.message = message
        super().__init__(self.message)

class NonNumericPriceError(ValidationError):
    """Exception raised when prices are not numeric."""
    def __init__(self, message="Prices are not numeric"):
        self.message = message
        super().__init__(self.message)

class NoDataError(ValidationError):
    """Exception raised when no data is found."""
    def __init__(self, message="No data found in file"):
        self.message = message
        super().__init__(self.message)

class InvalidMajorCategoryError(ValidationError):
    """Exception raised when a major category doesn't match the allowed list."""
    def __init__(self, message="Major category is not in the allowed list"):
        self.message = message
        super().__init__(self.message)

class EmptyProductDataError(PDFProcessingError):
    """Exception raised when no product data is extracted from a PDF."""
    def __init__(self, message="No product data extracted from PDF or zero unique products"):
        self.message = message
        super().__init__(self.message)