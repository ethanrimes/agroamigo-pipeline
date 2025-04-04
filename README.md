# Colombian Agricultural Price Data Pipeline

A robust data collection and processing pipeline for Colombian agricultural price data.

## Overview

This project processes Colombian agricultural price data from multiple sources (Excel files and PDF reports), extracting, normalizing, and consolidating the data into a consistent schema for analysis. The pipeline handles various formats and structures of input data, providing a unified dataset for agricultural price analysis.

## Features

- **File Extraction**: Extracts files from ZIP archives and organizes by type
- **Excel Processing**: Processes Excel files containing agricultural price data from various formats
- **PDF Processing**: Extracts and processes PDF reports with multiple fallback mechanisms
  - Direct text extraction with PyPDF2/pdfplumber
  - Table extraction for structured data
  - Fallback to GPT-4o mini for difficult cases
- **Data Aggregation**: Consolidates processed data into a unified dataset
- **Error Handling**: Comprehensive error tracking and reporting
- **Logging**: Detailed logging and process monitoring
- **File Registry**: Maintains a registry of processed files to avoid duplicate processing

## Data Source Details

The pipeline processes two main types of agricultural price reports:

1. **Excel Annexes** (anexo): These are Excel files typically containing structured price data across multiple markets, usually published by the Colombian government. Example: `20210202Barranquilla Granabastos 222022.xlsx`

2. **PDF Reports** (DANE Bulletins): These are official price reports from DANE (National Administrative Department of Statistics), containing detailed price information for agricultural products. Example: The Corabastos daily price bulletin.

## Installation

### Prerequisites

- Python 3.7 or higher
- Pip package manager

### Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/colombian-agri-price-pipeline.git
cd colombian-agri-price-pipeline
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file with your API key (optional, for advanced PDF processing):
```
GPT4O_API_KEY=your-api-key-here
```

## Usage

### Command Line Interface

To run the complete pipeline:
```bash
python main.py
```

To run specific components:
```bash
python main.py --components extractor excel pdf
```

To process data within a date range:
```bash
python main.py --start-date 2023-01-01 --end-date 2023-12-31
```

To force reprocessing of files even if already processed:
```bash
python main.py --force-reprocess
```

### Command-line Arguments

- `--start-date`: Start date for filtering data (YYYY-MM-DD)
- `--end-date`: End date for filtering data (YYYY-MM-DD)
- `--components`: Components to run (choices: extractor, excel, pdf, error, aggregator)
- `--workers`: Number of worker processes for PDF processing
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `--force-reprocess`: Force reprocessing of files even if already processed

### As a Module

You can also use the pipeline programmatically:

```python
from datetime import datetime
from agroamigo_pipeline.processors.zip_extractor import ZipExtractor
from agroamigo_pipeline.processors.excel_processor import ExcelProcessor
from agroamigo_pipeline.processors.pdf_processor import PDFProcessor
from agroamigo_pipeline.processors.data_aggregator import DataAggregator

# Extract files from ZIP archives
extractor = ZipExtractor()
extractor.process_zip_files()

# Process Excel files
excel_processor = ExcelProcessor()
excel_processor.process_excel_files()

# Process PDF files
pdf_processor = PDFProcessor()
pdf_processor.process_pdf_files()

# Aggregate data
aggregator = DataAggregator()
result_df = aggregator.aggregate_data(
    filter_date_start='2023-01-01',
    filter_date_end='2023-12-31'
)

# Do something with the aggregated data
result_df.to_csv('aggregated_prices.csv', index=False)
```

## Output Schema

All data is normalized to this CSV schema:

| Column | Description |
|--------|-------------|
| date | Date of the price data (YYYY-MM-DD) |
| city | City where the market is located |
| market | Market name |
| major_category | Major product category (e.g., Frutas, Verduras) |
| sub_category | Sub-category (e.g., Cítricos) |
| product | Product name |
| presentation | How the product is presented/packaged |
| units | Unit of measurement |
| daily_min | Minimum price |
| daily_max | Maximum price |
| source | Data source (anexo or dane) |
| source_filename | Original filename for reference |

## Project Structure

```
agroamigo_pipeline/
├── README.md                   # Project documentation
├── requirements.txt            # Project dependencies
├── main.py                     # Main orchestration script
├── setup.py                    # Package setup file
├── .env                        # Environment variables (GPT-4o API key)
├── tests/                      # Test suite
│   ├── __init__.py
│   ├── test_extractor.py       # Tests for file extractor
│   ├── test_excel_processor.py # Tests for Excel processor
│   ├── test_pdf_processor.py   # Tests for PDF processor
│   ├── test_error_handler.py   # Tests for error handling
│   ├── test_aggregator.py      # Tests for data aggregation
│   └── run_all_tests.py        # Script to run all tests
├── src/                        # Source code
│   ├── __init__.py
│   ├── config/                 # Configuration files
│   │   ├── __init__.py
│   │   └── settings.py         # Centralized path & parameter settings
│   ├── processors/             # Data processors
│   │   ├── __init__.py
│   │   ├── zip_extractor.py
│   │   ├── excel_processor.py
│   │   ├── pdf_processor.py
│   │   └── data_aggregator.py
│   ├── error_handlers/         # Error handling
│   │   ├── __init__.py
│   │   └── error_gatherer.py
│   └── utils/                  # Utility functions
│       ├── __init__.py
│       ├── logger.py           # Centralized logging configuration
│       ├── utils.py            # Shared utilities
│       ├── path_manager.py     # Path management utilities
│       ├── file_registry.py    # File processing registry
│       └── exceptions.py       # Custom exceptions
├── logs/                       # Log files
└── data/                       # Data directories
    ├── source/                 # Downloaded source files
    ├── pdf/                    # Extracted PDF files
    ├── processed/              # Individually processed CSV files
    ├── consolidated/           # Aggregated CSV files
    └── temp/                   # Temporary processing files
```

## Testing

Run all tests:
```bash
python tests/run_all_tests.py
```

Run specific component tests:
```bash
python tests/run_all_tests.py --components excel pdf
```

## Future Enhancements

- Web scraper module to automatically download updates
- API for accessing processed data
- Web interface for data visualization
- Integration with cloud services for scheduled processing
- Support for additional data sources and formats

## Troubleshooting

### Common Issues

- **PDF Extraction Failures**: Some PDF formats may not extract correctly. Check the PDF quality and consider using the `--force-reprocess` flag.
- **Excel Format Errors**: Excel files with unusual structure might fail to process. Check the error logs for details.
- **API Key Issues**: For advanced PDF processing, ensure your GPT-4o API key is valid and correctly set in the `.env` file.

### Logs

Check the log files in the `logs/` directory for detailed information about any failures or issues. Each component has its own log file with specific details.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- DANE (National Administrative Department of Statistics, Colombia) for providing the original data
- Colombian agricultural markets for their price transparency