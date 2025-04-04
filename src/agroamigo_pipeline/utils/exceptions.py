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