"""
Pytest configuration file for the Colombian Agricultural Price Data Pipeline.
"""

import pytest
from agroamigo_pipeline.utils.logger import setup_logging

# Initialize loggers at pytest collection time
@pytest.fixture(scope="session", autouse=True)
def initialize_logging():
    """Set up logging for all tests."""
    loggers = setup_logging()
    return loggers