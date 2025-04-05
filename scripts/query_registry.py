"""
Utility script to query and analyze the Parquet registry.
"""

import os
import sys
import argparse
import logging
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Add the project root to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from agroamigo_pipeline.utils.parquet_registry import ParquetRegistry
from agroamigo_pipeline.config.settings import REGISTRY_PARQUET, REGISTRY_CSV
from agroamigo_pipeline.utils.logger import setup_logging

def main():
    """Query and analyze the Parquet registry."""
    parser = argparse.ArgumentParser(description='Query and analyze the Parquet registry')
    parser.add_argument('--parquet-path', type=str, default=REGISTRY_PARQUET,
                        help=f'Path to Parquet registry file (default: {REGISTRY_PARQUET})')
    parser.add_argument('--export-csv', type=str, default=None,
                        help='Export query results to CSV file')
    parser.add_argument('--stats', action='store_true',
                        help='Display summary statistics')
    parser.add_argument('--plot', action='store_true',
                        help='Generate plots of registry data')
    parser.add_argument('--status', type=str, choices=['processed', 'error', 'all'], default='all',
                        help='Filter by processing status')
    parser.add_argument('--days', type=int, default=None,
                        help='Filter to last N days')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of results')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose output')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging_setup = setup_logging(log_level)
    logger = logging_setup['scraper']
    
    if not os.path.exists(args.parquet_path):
        logger.error(f"Parquet registry file not found: {args.parquet_path}")
        return 1
    
    try:
        # Load registry data
        logger.info(f"Loading registry data from: {args.parquet_path}")
        df = pd.read_parquet(args.parquet_path)
        
        if df.empty:
            logger.warning("Registry is empty!")
            return 0
        
        # Apply filters
        if args.status != 'all':
            df = df[df['status'] == args.status]
            logger.info(f"Filtered to status: {args.status}")
        
        if args.days is not None:
            cutoff_date = datetime.now() - timedelta(days=args.days)
            df = df[pd.to_datetime(df['timestamp']) >= cutoff_date]
            logger.info(f"Filtered to last {args.days} days (since {cutoff_date.strftime('%Y-%m-%d')})")
        
        # Apply limit
        if args.limit is not None:
            df = df.head(args.limit)
            logger.info(f"Limited to {args.limit} records")
        
        # Display summary stats
        if args.stats:
            logger.info("\n--- Registry Summary Statistics ---")
            logger.info(f"Total records: {len(df)}")
            
            status_counts = df['status'].value_counts()
            logger.info("\nStatus counts:")
            for status, count in status_counts.items():
                logger.info(f"  {status}: {count}")
            
            error_count = df['error_occurred'].sum()
            logger.info(f"\nFiles with errors: {error_count} ({error_count/len(df)*100:.1f}%)")
            
            extraction_counts = df['extraction_method'].value_counts()
            logger.info("\nExtraction method counts:")
            for method, count in extraction_counts.items():
                if not pd.isna(method) and method:  # Check for non-empty method
                    logger.info(f"  {method}: {count}")
            
            gpt_count = df['used_gpt_extraction'].sum()
            logger.info(f"\nFiles using GPT extraction: {gpt_count} ({gpt_count/len(df)*100:.1f}%)")
            
            total_products = df['unique_products'].sum()
            avg_products = df['unique_products'].mean()
            logger.info(f"\nTotal unique products: {total_products}")
            logger.info(f"Average unique products per file: {avg_products:.1f}")
            
            total_rows = df['rows_processed'].sum()
            avg_rows = df['rows_processed'].mean()
            logger.info(f"\nTotal rows processed: {total_rows}")
            logger.info(f"Average rows per file: {avg_rows:.1f}")
            
            avg_time = df['processing_time_seconds'].mean()
            max_time = df['processing_time_seconds'].max()
            logger.info(f"\nAverage processing time: {avg_time:.2f} seconds")
            logger.info(f"Maximum processing time: {max_time:.2f} seconds")
            
            # Date range
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                min_date = df['timestamp'].min()
                max_date = df['timestamp'].max()
                date_range = (max_date - min_date).days
                logger.info(f"\nDate range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')} ({date_range} days)")
        
        # Generate plots
        if args.plot:
            logger.info("Generating plots...")
            
            # Create a directory for plots
            plots_dir = os.path.join(os.path.dirname(args.parquet_path), 'plots')
            os.makedirs(plots_dir, exist_ok=True)
            
            # Convert timestamp to datetime if it's not already
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Plot 1: Status distribution
            plt.figure(figsize=(10, 6))
            status_counts = df['status'].value_counts()
            status_counts.plot(kind='bar')
            plt.title('Processing Status Distribution')
            plt.xlabel('Status')
            plt.ylabel('Count')
            plt.tight_layout()
            status_plot_path = os.path.join(plots_dir, 'status_distribution.png')
            plt.savefig(status_plot_path)
            logger.info(f"Saved status distribution plot to: {status_plot_path}")
            
            # Plot 2: Files processed over time
            if 'timestamp' in df.columns:
                plt.figure(figsize=(12, 6))
                df['date'] = df['timestamp'].dt.date
                files_per_day = df.groupby('date').size()
                files_per_day.plot(kind='line')
                plt.title('Files Processed Over Time')
                plt.xlabel('Date')
                plt.ylabel('Number of Files')
                plt.tight_layout()
                time_plot_path = os.path.join(plots_dir, 'files_over_time.png')
                plt.savefig(time_plot_path)
                logger.info(f"Saved files over time plot to: {time_plot_path}")
            
            # Plot 3: Processing time distribution
            plt.figure(figsize=(10, 6))
            plt.hist(df['processing_time_seconds'], bins=20)
            plt.title('Processing Time Distribution')
            plt.xlabel('Processing Time (seconds)')
            plt.ylabel('Frequency')
            plt.tight_layout()
            time_dist_plot_path = os.path.join(plots_dir, 'processing_time_distribution.png')
            plt.savefig(time_dist_plot_path)
            logger.info(f"Saved processing time distribution plot to: {time_dist_plot_path}")
            
            # Plot 4: Products per file distribution
            plt.figure(figsize=(10, 6))
            plt.hist(df['unique_products'], bins=20)
            plt.title('Unique Products per File Distribution')
            plt.xlabel('Number of Unique Products')
            plt.ylabel('Frequency')
            plt.tight_layout()
            products_plot_path = os.path.join(plots_dir, 'products_per_file.png')
            plt.savefig(products_plot_path)
            logger.info(f"Saved products per file plot to: {products_plot_path}")
        
        # Export to CSV if requested
        if args.export_csv:
            csv_path = args.export_csv
            df.to_csv(csv_path, index=False)
            logger.info(f"Exported query results to CSV: {csv_path}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error querying registry: {str(e)}")
        if args.verbose:
            import traceback
            logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())