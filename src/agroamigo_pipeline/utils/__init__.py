# Utility modules
"""
This package includes utility modules for the
Colombian Agricultural Price Data Pipeline.
"""

from .logger import setup_logging, log_function_call, log_dataframe_info, log_dict_info
from .path_manager import ensure_directory, get_files_by_extension, generate_output_path
from .utils import normalize_string, extract_date_from_spanish_text, format_date, extract_geography_info
from .exceptions import (
    PipelineError, ScraperError, ExtractorError, FileProcessingError,
    ExcelProcessingError, PDFProcessingError, DataAggregationError, APIError
)

__all__ = [
    'FileRegistry',
    'setup_logging',
    'log_function_call',
    'log_dataframe_info',
    'log_dict_info',
    'ensure_directory',
    'get_files_by_extension',
    'generate_output_path',
    'normalize_string',
    'extract_date_from_spanish_text',
    'format_date',
    'extract_geography_info',
    'PipelineError',
    'ScraperError',
    'ExtractorError',
    'FileProcessingError',
    'ExcelProcessingError',
    'PDFProcessingError',
    'DataAggregationError',
    'APIError'
]