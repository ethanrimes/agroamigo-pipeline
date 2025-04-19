# AgroAmigo Pipeline

AgroAmigo Pipeline is a robust data processing system for Colombian agricultural price data (SIPSA - Sistema de Información de Precios y Abastecimiento del Sector Agropecuario). The pipeline extracts, transforms, and consolidates daily price information from various formats into a standardized dataset for analysis.

## Pipeline Stages

The pipeline consists of five primary components that work in sequence:

1. **Web Scraper**: Downloads daily price data files from the DANE (Colombian National Administrative Department of Statistics) website. It identifies and retrieves ZIP archives, Excel files, and PDFs containing price information.

2. **ZIP Extractor**: Processes ZIP archives to extract contained PDFs and Excel files. PDFs are moved to a dedicated PDF directory, while Excel files are placed in the source directory for further processing.

3. **Excel Processor**: Parses Excel files containing structured price data. Extracts date, market, product, and price information, then transforms it into a standardized CSV format.

4. **PDF Processor**: Extracts price data from PDF reports using multiple approaches:
   - Direct text extraction with PyPDF2/pdfplumber
   - Table extraction for structured data
   - Fallback to GPT-4.1 nano for difficult cases when text extraction fails

5. **Data Aggregator**: Consolidates all processed CSV files into a unified dataset, handling deduplication and ensuring a consistent schema.

6. **Error Handler**: Collects and reports errors from all pipeline components, generating comprehensive error summaries.

## Registry Mechanism

The pipeline employs a sophisticated registry system to track files through each processing stage. Each component maintains its own registry that records file metadata, processing status, and results.

### Registry Structure

The registry system consists of five key registries:

1. **WebScraper Registry**: Tracks downloaded files, their source URLs, download dates, and file types.
2. **Extractor Registry**: Maintains records of extracted files from ZIP archives, referencing the source download.
3. **PDF Registry**: Keeps track of PDF files, their processing status, and extraction results.
4. **Excel Registry**: Monitors Excel files, their processing status, and transformation outcomes.
5. **Consolidation Registry**: Records data aggregation operations, referencing source PDFs and Excel files.

### Registry Implementation

- Each registry is implemented as a Parquet file for efficient storage and querying, with CSV exports for human readability.
- Registries maintain foreign key relationships (e.g., PDF Registry references Extractor Registry, which references WebScraper Registry).
- Each record includes:
  - Unique identifiers for cross-registry references
  - File metadata (name, path, date)
  - Processing status (not_processed, processing, processed, error)
  - Error information when applicable
  - Processing statistics (duration, number of records processed, etc.)
  - Timestamps for processing events

This registry system ensures:
- Files are not reprocessed unnecessarily
- Processing errors can be tracked and addressed
- The entire data lineage is maintained for auditability
- Runtime statistics can be collected for monitoring performance

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/agroamigo_pipeline.git
cd agroamigo_pipeline

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the package in development mode
pip install -e .
```

## Command-line Usage

The pipeline can be run using the `main.py` script with various arguments:

```bash
# Run the full pipeline
python main.py

# Run with date filtering
python main.py --start-date 2023-01-01 --end-date 2023-12-31

# Run specific components only
python main.py --components scraper extractor excel pdf aggregator

# Control parallel processing
python main.py --workers 4

# Force reprocessing of files (even if they've been processed before)
python main.py --force-reprocess

# Set log level
python main.py --log-level DEBUG



python main.py --log-level DEBUG --workers 8 --components excel pdf --start-date 2022-01-12 --end-date 2022-01-12 --force-reprocess

python main.py --log-level DEBUG --workers 8 --components extractor excel pdf aggregator --force-reprocess --start-date 2022-01-01 --end-date 2022-01-07 

```

### Command-line Arguments

- `--start-date`: Start date for filtering data (YYYY-MM-DD)
- `--end-date`: End date for filtering data (YYYY-MM-DD)
- `--components`: Components to run (choices: scraper, extractor, excel, pdf, aggregator, error)
- `--workers`: Number of worker processes for parallel processing (default is CPU count)
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--force-reprocess`: Force reprocessing of files even if they've been processed before

## Configuration

Key paths can be configured in `agroamigo_pipeline/config/settings.py`:

```python
# Base directories
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
PARENT_ROOT = PROJECT_ROOT.parent

# Data directories
DATA_DIR = '/path/to/data'                         # Root data directory
SOURCE_DIR = os.path.join(DATA_DIR, 'Source')      # Downloaded files from web scraper
PDF_DIR = os.path.join(DATA_DIR, 'PDF_Files/PDFs') # Extracted PDF files
PROCESSED_DIR = os.path.join(DATA_DIR, 'IndividualCSVs') # Individual CSV files
CONSOLIDATED_DIR = os.path.join(DATA_DIR, 'Consolidated') # Final aggregated data
TEMP_DIR = os.path.join(DATA_DIR, 'PDF_Files/temp') # Temporary processing files

# Log directories
LOG_DIR = os.path.join(PARENT_ROOT, 'logs')

# Registry directory
REGISTRY_DIR = os.path.join(PARENT_ROOT, 'registry')
```

Alternatively, you can use environment variables by creating a `.env` file in the project root.

## Testing

**Note**: The test suite is currently under development and not fully ready for use. Some tests may fail due to missing test data or incomplete mock implementations. Use the test files as reference for understanding component behavior rather than for validation at this stage.

## Output Files

### Registry Files

The pipeline generates several registry files to track processing:

1. **CSV Summaries** (in `registry/` directory):
   - `webscraper_summary.csv`: Log of all downloaded files
   - `extractor_summary.csv`: Records of ZIP extraction operations
   - `pdf_summary.csv`: Status of all PDF processing operations
   - `excel_summary.csv`: Status of all Excel processing operations
   - `consolidation_summary.csv`: Records of data aggregation operations

2. **Parquet Registry Files** (in `registry/parquet/` directory):
   - Full registry data in compressed format for each component

### Error Logs and Statistics

1. **Error Summaries** (in `registry/error_summaries/` directory):
   - `error_summary.txt`: Human-readable error report
   - `errors.csv`: Detailed error records

2. **Run Statistics** (in `registry/run_statistics/` directory):
   - **CSV Files** (in `csv/` subdirectory):
     - `run_statistics_TIMESTAMP.csv`: Key metrics for each pipeline run
     - `run_errors_TIMESTAMP.csv`: Errors from each pipeline run
   
   - **Text Reports** (in `text/` subdirectory):
     - `run_report_TIMESTAMP.txt`: Detailed human-readable run report
     - `run_error_summary_TIMESTAMP.txt`: Detailed error report for each run

3. **Pipeline Status Files** (in `registry/` directory):
   - `pipeline_status.txt`: Current pipeline health status
   - `pipeline_status.csv`: Machine-readable pipeline health metrics

## Data Output Files

The pipeline produces the following data files:

1. **Processed CSV Files** (in `PROCESSED_DIR`):
   - Individual CSV files for each processed Excel or PDF file
   - Follow a standardized schema with columns for date, city, market, product, price, etc.

2. **Consolidated Dataset** (in `CONSOLIDATED_DIR`):
   - `consolidated_YYYY-MM.csv`: Monthly aggregated data files
   - Contains all data from individual CSVs with duplicates removed

## Project Structure

```
agroamigo_pipeline/
├── README.md                   # Project documentation
├── logs                        # Log directories
│   ├── data_aggregator
│   ├── error_handler
│   ├── excel_processor
│   ├── file_extractor
│   ├── pdf_processor
│   └── scraper
├── main.py                     # Main entry point script
├── pyproject.toml              # Project configuration
├── registry                    # Registry files
│   ├── consolidation_summary.csv
│   ├── error_summaries
│   ├── excel_summary.csv
│   ├── extractor_summary.csv
│   ├── parquet                 # Parquet registry files
│   ├── pdf_summary.csv
│   ├── pipeline_status.csv
│   ├── pipeline_status.txt
│   ├── run_statistics          # Run statistics files
│   └── webscraper_summary.csv
├── setup.py                    # Package setup file
├── src                         # Source code
│   └── agroamigo_pipeline
│       ├── __init__.py
│       ├── config
│       │   ├── __init__.py
│       │   └── settings.py     # Configuration settings
│       ├── error_handlers      # Error handling modules
│       │   ├── __init__.py
│       │   ├── error_gatherer.py
│       │   └── run_statistics.py
│       ├── processors          # Data processing modules
│       │   ├── __init__.py
│       │   ├── data_aggregator.py
│       │   ├── excel_components
│       │   ├── excel_processor.py
│       │   ├── pdf_components
│       │   ├── pdf_processor.py
│       │   └── zip_extractor.py
│       ├── registry            # Registry modules
│       │   ├── __init__.py
│       │   ├── base_registry.py
│       │   ├── consolidation_registry.py
│       │   ├── excel_registry.py
│       │   ├── extractor_registry.py
│       │   ├── pdf_registry.py
│       │   └── webscraper_registry.py
│       ├── scraping            # Web scraping modules
│       │   ├── __init__.py
│       │   └── web_scraper.py
│       └── utils               # Utility modules
│           ├── __init__.py
│           ├── date_utils.py
│           ├── exceptions.py
│           ├── logger.py
│           ├── path_manager.py
│           └── utils.py
└── tests                       # Test suite
    ├── README.md
    ├── __init__.py
    ├── conftest.py
    ├── run_all_tests.py
    ├── test_aggregator.py
    ├── test_data               # Test data directory
    ├── test_error_handler.py
    ├── test_excel_processor.py
    ├── test_extractor.py
    ├── test_image_handler.py
    ├── test_pdf_processor.py
    └── test_scraper.py
```

## Main Functionality

AgroAmigo Pipeline is designed to solve the challenge of collecting, standardizing, and consolidating Colombian agricultural price data from multiple sources and formats. Key features include:

1. **Data Collection**: Automatically downloads the latest price data from official sources.

2. **Multi-format Processing**: Handles Excel files and PDF reports, extracting structured data regardless of format.

3. **Fallback Mechanisms**: When standard extraction methods fail, uses AI-powered extraction with GPT-4.1 nano as a fallback.

4. **Processing Tracking**: Tracks every file through the entire pipeline, from download to final consolidation.

5. **Error Handling**: Comprehensive error tracking and reporting for identifying and fixing issues.

6. **Data Consolidation**: Combines all processed data into a unified dataset with a consistent schema.

7. **Statistical Reporting**: Generates detailed reports on pipeline performance and data quality.

This pipeline enables reliable, automated collection and processing of SIPSA daily price data, creating a consistent dataset for agricultural market analysis.