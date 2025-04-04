import os
import dotenv
from pathlib import Path

# Load environment variables from .env file
dotenv.load_dotenv()

# Base project directory (automatically detected based on module location)
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()

# Data directories
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
SOURCE_DIR = os.path.join(DATA_DIR, 'source')  # Downloaded files from web scraper
PDF_DIR = os.path.join(DATA_DIR, 'pdf')  # Extracted PDF files
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')  # Individual CSV files
CONSOLIDATED_DIR = os.path.join(DATA_DIR, 'consolidated')  # Final aggregated data
TEMP_DIR = os.path.join(DATA_DIR, 'temp')  # Temporary processing files

# Log directories
LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')
SCRAPER_LOG_DIR = os.path.join(LOG_DIR, 'scraper')
EXTRACTOR_LOG_DIR = os.path.join(LOG_DIR, 'file_extractor')
EXCEL_LOG_DIR = os.path.join(LOG_DIR, 'excel_processor')
PDF_LOG_DIR = os.path.join(LOG_DIR, 'pdf_processor')
ERROR_LOG_DIR = os.path.join(LOG_DIR, 'error_handler')
AGGREGATOR_LOG_DIR = os.path.join(LOG_DIR, 'data_aggregator')

# File tracking database
PROCESSING_REGISTRY = os.path.join(LOG_DIR, 'processing_registry.json')

# Processing parameters
MAX_WORKERS = os.cpu_count()  # Use all available cores for PDF processing
PDF_TEXT_QUALITY_THRESHOLD = 0.7
MIN_UNIQUE_PRODUCTS = 5
MAX_UNIQUE_PRODUCTS = 500
MIN_MAJOR_CATEGORIES = 1
MAX_MAJOR_CATEGORIES = 20

# API configuration
GPT4O_API_KEY = os.getenv('GPT4O_API_KEY')

# Logging configuration
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Ensure all directories exist
def ensure_directories():
    """Create all required directories if they don't exist."""
    directories = [
        SOURCE_DIR, PDF_DIR, PROCESSED_DIR, CONSOLIDATED_DIR, TEMP_DIR,
        SCRAPER_LOG_DIR, EXTRACTOR_LOG_DIR, EXCEL_LOG_DIR, PDF_LOG_DIR, 
        ERROR_LOG_DIR, AGGREGATOR_LOG_DIR
    ]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

# Initialize directories when module is imported
ensure_directories()