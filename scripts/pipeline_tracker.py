#!/usr/bin/env python3
"""
Enhanced Pipeline Tracker for AgroAmigo data pipeline.
Creates a comprehensive CSV showing the flow of data across different processing stages.
Specifically tracks file relationships through consolidation.
"""

import os
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
import logging
import json
import argparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('enhanced_pipeline_tracker')

# Directory paths
SCRIPT_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = SCRIPT_DIR.parent
PARQUET_DIR = PROJECT_ROOT / 'registry' / 'parquet'
OUTPUT_DIR = PROJECT_ROOT / 'registry' / 'pipeline_summary'
OUTPUT_FILE = OUTPUT_DIR / 'pipeline_tracking.csv'
SUMMARY_FILE = OUTPUT_DIR / 'pipeline_summary.txt'

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Track file processing through the AgroAmigo data pipeline.')
    parser.add_argument('--output', type=str, help='Output file path (default: registry/pipeline_summary/pipeline_tracking.csv)')
    parser.add_argument('--details', action='store_true', help='Include additional detailed columns in output')
    return parser.parse_args()

def load_registry(registry_name):
    """Load a registry parquet file into a pandas DataFrame."""
    file_path = PARQUET_DIR / f"{registry_name}_registry.parquet"
    
    if not file_path.exists():
        logger.error(f"Registry file not found: {file_path}")
        return pd.DataFrame()
    
    try:
        logger.info(f"Loading registry: {registry_name}")
        return pq.read_table(file_path).to_pandas()
    except Exception as e:
        logger.error(f"Error loading {registry_name} registry: {str(e)}")
        return pd.DataFrame()

def safe_list_convert(list_value):
    """Safely convert parquet list values to Python lists."""
    if isinstance(list_value, list):
        return list_value
    elif isinstance(list_value, str):
        try:
            # Try to evaluate the string as a literal Python list
            value = eval(list_value)
            if isinstance(value, list):
                return value
        except:
            pass
    
    # If we got here, return an empty list
    return []

def build_enhanced_pipeline_structure(include_details=False):
    """
    Build a comprehensive nested dictionary structure mapping the flow of files through the pipeline.
    Specifically focuses on tracking consolidation relationships.
    
    Args:
        include_details (bool): Whether to include additional detailed columns in output
        
    Returns:
        DataFrame: Flattened DataFrame for output
    """
    # Load all registries
    webscraper_df = load_registry('webscraper')
    extractor_df = load_registry('extractor')
    excel_df = load_registry('excel')
    pdf_df = load_registry('pdf')
    consolidation_df = load_registry('consolidation')
    
    # Check if any registries are empty
    if webscraper_df.empty:
        logger.error("Webscraper registry is empty. Cannot proceed.")
        return pd.DataFrame(), {}
    
    # Create lookup dictionaries for faster reference
    webscraper_lookup = {row['download_id']: row for _, row in webscraper_df.iterrows()}
    extractor_lookup = {row['extraction_id']: row for _, row in extractor_df.iterrows()}
    excel_lookup = {row['excel_id']: row for _, row in excel_df.iterrows()}
    pdf_lookup = {row['pdf_id']: row for _, row in pdf_df.iterrows()}
    
    # Create download_id to extraction_id mapping
    download_to_extraction = {}
    for _, row in extractor_df.iterrows():
        download_id = row['download_id']
        extraction_id = row['extraction_id']
        if download_id not in download_to_extraction:
            download_to_extraction[download_id] = []
        download_to_extraction[download_id].append(extraction_id)
    
    # Create extraction_id to pdf_id mapping
    extraction_to_pdf = {}
    for _, row in pdf_df.iterrows():
        extraction_id = row['extraction_id']
        pdf_id = row['pdf_id']
        if extraction_id not in extraction_to_pdf:
            extraction_to_pdf[extraction_id] = []
        extraction_to_pdf[extraction_id].append(pdf_id)
    
    # Create download_id to excel_id mapping
    download_to_excel = {}
    for _, row in excel_df.iterrows():
        download_id = row['download_id']
        excel_id = row['excel_id']
        if download_id not in download_to_excel:
            download_to_excel[download_id] = []
        download_to_excel[download_id].append(excel_id)
    
    # Create sets of consolidated excel and pdf IDs
    consolidated_excel_ids = set()
    consolidated_pdf_ids = set()
    consolidation_lookup = {}
    
    if not consolidation_df.empty:
        for _, row in consolidation_df.iterrows():
            consolidation_id = row['consolidation_id']
            consolidated_id = row['consolidated_id']
            month_key = row['month_key']
            consolidation_error = row['error_occurred']
            
            # Extract excel_ids and pdf_ids from the consolidation
            excel_ids = safe_list_convert(row['excel_ids'])
            pdf_ids = safe_list_convert(row['pdf_ids'])
            
            # Add to sets of consolidated IDs
            consolidated_excel_ids.update(excel_ids)
            consolidated_pdf_ids.update(pdf_ids)
            
            # Create lookup entry
            consolidation_lookup[consolidation_id] = {
                'consolidated_id': consolidated_id,
                'month_key': month_key,
                'error_occurred': consolidation_error,
                'excel_ids': excel_ids,
                'pdf_ids': pdf_ids
            }
    
    # Process file data to build comprehensive tracking
    flattened_data = []
    
    # Add Excel files
    for excel_id, excel_row in excel_lookup.items():
        download_id = excel_row['download_id']
        
        # Check if truly consolidated (in the consolidated_excel_ids set)
        consolidated = excel_id in consolidated_excel_ids
        
        # Find the consolidation info if consolidated
        consolidation_info = None
        consolidated_id = None
        month_key = None
        consolidation_error = None
        
        if consolidated:
            for cons_id, cons_info in consolidation_lookup.items():
                if excel_id in cons_info['excel_ids']:
                    consolidation_info = cons_info
                    consolidated_id = cons_info['consolidated_id']
                    month_key = cons_info['month_key']
                    consolidation_error = cons_info['error_occurred']
                    break
        
        if download_id in webscraper_lookup:
            webscraper_row = webscraper_lookup[download_id]
            
            # Create basic row data
            row = {
                'File date': excel_row.get('file_date', None),
                'Web scraping name': webscraper_row.get('filename', ''),
                'Web scraping error': webscraper_row.get('error_occurred', False),
                'Zip extractor name': None,
                'Zip extractor error': None,
                'Excel extractor name': excel_row.get('filename', ''),
                'Excel extractor error': excel_row.get('error_occurred', False),
                'PDF extractor name': None,
                'PDF extractor error': None,
                'Consolidation name': month_key,
                'Consolidation error': consolidation_error,
                'Consolidated': consolidated,
                'Consolidated ID': consolidated_id
            }
            
            # Add additional details if requested
            if include_details:
                row.update({
                    'Download ID': download_id,
                    'Excel ID': excel_id,
                    'Processed CSV path': excel_row.get('processed_csv_path', ''),
                    'Processing status': excel_row.get('processing_status', ''),
                    'Rows processed': excel_row.get('rows_processed', 0)
                })
            
            flattened_data.append(row)
    
    # Add PDF files
    for pdf_id, pdf_row in pdf_lookup.items():
        extraction_id = pdf_row['extraction_id']
        
        # Check if truly consolidated (in the consolidated_pdf_ids set)
        consolidated = pdf_id in consolidated_pdf_ids
        
        # Find the consolidation info if consolidated
        consolidation_info = None
        consolidated_id = None
        month_key = None
        consolidation_error = None
        
        if consolidated:
            for cons_id, cons_info in consolidation_lookup.items():
                if pdf_id in cons_info['pdf_ids']:
                    consolidation_info = cons_info
                    consolidated_id = cons_info['consolidated_id']
                    month_key = cons_info['month_key']
                    consolidation_error = cons_info['error_occurred']
                    break
        
        if extraction_id in extractor_lookup:
            extractor_row = extractor_lookup[extraction_id]
            download_id = extractor_row['download_id']
            
            if download_id in webscraper_lookup:
                webscraper_row = webscraper_lookup[download_id]
                
                # Create basic row data for PDF files
                row = {
                    'File date': pdf_row.get('file_date', None),
                    'Web scraping name': webscraper_row.get('filename', ''),
                    'Web scraping error': webscraper_row.get('error_occurred', False),
                    'Zip extractor name': os.path.basename(extractor_row.get('zip_file_path', '')),
                    'Zip extractor error': extractor_row.get('error_occurred', False),
                    'Excel extractor name': None,
                    'Excel extractor error': None,
                    'PDF extractor name': pdf_row.get('pdf_filename', ''),
                    'PDF extractor error': pdf_row.get('error_occurred', False),
                    'Consolidation name': month_key,
                    'Consolidation error': consolidation_error,
                    'Consolidated': consolidated,
                    'Consolidated ID': consolidated_id
                }
                
                # Add additional details if requested
                if include_details:
                    row.update({
                        'Download ID': download_id,
                        'Extraction ID': extraction_id,
                        'PDF ID': pdf_id,
                        'Processed CSV path': pdf_row.get('processed_csv_path', ''),
                        'Processing status': pdf_row.get('processing_status', ''),
                        'Rows processed': pdf_row.get('rows_processed', 0)
                    })
                
                flattened_data.append(row)
    
    # Add downloads that don't have Excel or PDF processing
    for download_id, webscraper_row in webscraper_lookup.items():
        # Skip if this download already has Excel files
        if download_id in download_to_excel:
            continue
        
        # Skip if this download already has extractions with PDFs
        has_pdfs = False
        if download_id in download_to_extraction:
            for extraction_id in download_to_extraction[download_id]:
                if extraction_id in extraction_to_pdf:
                    has_pdfs = True
                    break
        
        if has_pdfs:
            continue
        
        # Create basic row data for downloads without further processing
        row = {
            'File date': webscraper_row.get('file_date', None),
            'Web scraping name': webscraper_row.get('filename', ''),
            'Web scraping error': webscraper_row.get('error_occurred', False),
            'Zip extractor name': None,
            'Zip extractor error': None,
            'Excel extractor name': None,
            'Excel extractor error': None,
            'PDF extractor name': None,
            'PDF extractor error': None,
            'Consolidation name': None,
            'Consolidation error': None,
            'Consolidated': False,
            'Consolidated ID': None
        }
        
        # Add additional details if requested
        if include_details:
            row.update({
                'Download ID': download_id,
                'Download path': webscraper_row.get('download_path', '')
            })
        
        flattened_data.append(row)
    
    # Convert to DataFrame
    flattened_df = pd.DataFrame(flattened_data)
    
    # Handle datetime formatting
    if 'File date' in flattened_df.columns and not flattened_df.empty:
        flattened_df['File date'] = pd.to_datetime(flattened_df['File date']).dt.strftime('%Y-%m-%d')
    
    # Calculate statistics for summary
    stats = {}
    
    # Web scraper statistics
    stats['webscraper'] = {
        'total': len(webscraper_df),
        'successful': (~webscraper_df['error_occurred']).sum(),
        'failed': webscraper_df['error_occurred'].sum()
    }
    
    # Zip extractor statistics
    if not extractor_df.empty:
        stats['extractor'] = {
            'total_zips': len(extractor_df),
            'successful_zips': (~extractor_df['error_occurred']).sum(),
            'failed_zips': extractor_df['error_occurred'].sum(),
            'extracted_pdfs': sum(len(extraction_to_pdf.get(eid, [])) for eid in extractor_df['extraction_id'])
        }
    else:
        stats['extractor'] = {
            'total_zips': 0,
            'successful_zips': 0,
            'failed_zips': 0,
            'extracted_pdfs': 0
        }
    
    # Excel processor statistics
    stats['excel'] = {
        'total': len(excel_df),
        'successful': len(excel_df[excel_df['processing_status'] == 'processed']),
        'failed': len(excel_df[excel_df['processing_status'] == 'error'])
    }
    
    # PDF processor statistics
    stats['pdf'] = {
        'total': len(pdf_df),
        'successful': len(pdf_df[pdf_df['processing_status'] == 'processed']),
        'failed': len(pdf_df[pdf_df['processing_status'] == 'error'])
    }
    
    # Consolidation statistics
    if not consolidation_df.empty:
        # Calculate matched Excel and PDF IDs
        matched_excels = len(consolidated_excel_ids)
        matched_pdfs = len(consolidated_pdf_ids)
        
        stats['consolidation'] = {
            'total': len(consolidation_df),
            'successful': (~consolidation_df['error_occurred']).sum(),
            'failed': consolidation_df['error_occurred'].sum(),
            'total_excels': excel_df['excel_id'].nunique(),
            'total_pdfs': pdf_df['pdf_id'].nunique(),
            'matched_excels': matched_excels,
            'matched_pdfs': matched_pdfs
        }
    else:
        stats['consolidation'] = {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'total_excels': 0,
            'total_pdfs': 0,
            'matched_excels': 0,
            'matched_pdfs': 0
        }
    
    # Calculate discrepancy metrics
    stats['discrepancies'] = {
        'webscraper_minus_excels_zips': stats['webscraper']['successful'] - (stats['excel']['total'] + stats['extractor']['total_zips']),
        'extracted_pdfs_minus_total_pdfs': stats['extractor']['extracted_pdfs'] - stats['pdf']['total']
    }
    
    return flattened_df, stats

def generate_summary_text(stats):
    """Generate a formatted text summary of pipeline statistics."""
    summary = []
    
    # Add timestamp
    summary.append(f"AgroAmigo Pipeline Summary")
    summary.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    summary.append("-" * 50)
    
    # Web scraper section
    summary.append("\nWeb Scraper")
    summary.append("-" * 20)
    summary.append(f"Successful: {stats['webscraper']['successful']}")
    summary.append(f"Failed: {stats['webscraper']['failed']}")
    summary.append(f"Total: {stats['webscraper']['total']}")
    
    # Zip extractor section
    summary.append("\nZip Extractor")
    summary.append("-" * 20)
    summary.append(f"Successful zips: {stats['extractor']['successful_zips']}")
    summary.append(f"Failed zips: {stats['extractor']['failed_zips']}")
    summary.append(f"Total zips: {stats['extractor']['total_zips']}")
    summary.append(f"Extracted PDFs: {stats['extractor']['extracted_pdfs']}")
    
    # Excel processor section
    summary.append("\nExcel Processor")
    summary.append("-" * 20)
    summary.append(f"Successful: {stats['excel']['successful']}")
    summary.append(f"Failed: {stats['excel']['failed']}")
    summary.append(f"Total: {stats['excel']['total']}")
    
    # PDF processor section
    summary.append("\nPDF Processor")
    summary.append("-" * 20)
    summary.append(f"Successful: {stats['pdf']['successful']}")
    summary.append(f"Failed: {stats['pdf']['failed']}")
    summary.append(f"Total: {stats['pdf']['total']}")
    
    # Discrepancies section
    summary.append("\nDiscrepancies")
    summary.append("-" * 20)
    summary.append(f"Successful webscraper - (total excels + total zips): {stats['discrepancies']['webscraper_minus_excels_zips']}")
    summary.append(f"Extracted PDFs - total PDFs: {stats['discrepancies']['extracted_pdfs_minus_total_pdfs']}")
    
    # Consolidator section
    summary.append("\nConsolidator/Aggregator")
    summary.append("-" * 20)
    summary.append(f"Total PDFs: {stats['consolidation']['total_pdfs']}")
    summary.append(f"Total Excels: {stats['consolidation']['total_excels']}")
    summary.append(f"Matched PDFs (in consolidation): {stats['consolidation']['matched_pdfs']}")
    summary.append(f"Matched Excels (in consolidation): {stats['consolidation']['matched_excels']}")
    summary.append(f"Successful consolidations: {stats['consolidation']['successful']}")
    summary.append(f"Failed consolidations: {stats['consolidation']['failed']}")
    summary.append(f"Total consolidations: {stats['consolidation']['total']}")
    
    return "\n".join(summary)

def main():
    """Main function to run the script."""
    # Parse arguments
    args = parse_arguments()
    
    # Set output file if provided
    output_file = args.output if args.output else OUTPUT_FILE
    
    logger.info("Starting enhanced pipeline tracking analysis")
    
    # Build the pipeline structure and get statistics
    flattened_df, stats = build_enhanced_pipeline_structure(include_details=args.details)
    
    if flattened_df.empty:
        logger.error("Failed to build pipeline structure. No data to output.")
        return
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Create the pipeline_summary directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Write the flattened data to CSV
    try:
        flattened_df.to_csv(output_file, index=False)
        logger.info(f"Enhanced pipeline tracking report saved to {output_file}")
        
        # Generate and save summary text
        summary_text = generate_summary_text(stats)
        with open(SUMMARY_FILE, 'w') as f:
            f.write(summary_text)
        logger.info(f"Pipeline summary saved to {SUMMARY_FILE}")
        
        # Print the summary to console as well
        print("\n" + summary_text)
        
    except Exception as e:
        logger.error(f"Error writing output: {str(e)}")

if __name__ == "__main__":
    main()