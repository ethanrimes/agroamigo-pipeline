"""
Registry module for the AgroAmigo pipeline.

This module provides registry classes for tracking files through the processing pipeline.
"""

from agroamigo_pipeline.registry.base_registry import BaseRegistry
from agroamigo_pipeline.registry.webscraper_registry import WebScraperRegistry
from agroamigo_pipeline.registry.extractor_registry import ZipExtractorRegistry
from agroamigo_pipeline.registry.pdf_registry import PDFProcessorRegistry
from agroamigo_pipeline.registry.excel_registry import ExcelProcessorRegistry
from agroamigo_pipeline.registry.consolidation_registry import ConsolidationRegistry

__all__ = [
    'BaseRegistry',
    'WebScraperRegistry',
    'ZipExtractorRegistry',
    'PDFProcessorRegistry',
    'ExcelProcessorRegistry',
    'ConsolidationRegistry'
]