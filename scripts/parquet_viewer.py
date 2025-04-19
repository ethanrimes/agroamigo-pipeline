#!/usr/bin/env python3
"""
Script to view the first few entries of a specified parquet registry file.

Usage:
  python parquet_viewer.py <registry_name> [--rows N] [--pretty]

Arguments:
  registry_name      Name of the registry (webscraper, extractor, excel, pdf, consolidation)
  --rows N           Number of rows to display (default: 5)
  --pretty           Use pretty formatting for display

Example:
  python parquet_viewer.py excel --rows 10 --pretty
"""

import os
import sys
import argparse
from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd
from tabulate import tabulate
import json

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='View entries from a parquet registry file.')
    parser.add_argument('registry_name', type=str, help='Name of the registry (webscraper, extractor, excel, pdf, consolidation)')
    parser.add_argument('--rows', type=int, default=5, help='Number of rows to display (default: 5)')
    parser.add_argument('--pretty', action='store_true', help='Use pretty formatting for display')
    return parser.parse_args()

def get_registry_path(registry_name):
    """Get the path to the specified registry file."""
    # Determine the project root directory
    script_dir = Path(__file__).parent.absolute()
    project_root = script_dir.parent
    
    # Path to the parquet file
    parquet_dir = project_root / 'registry' / 'parquet'
    parquet_path = parquet_dir / f"{registry_name}_registry.parquet"
    
    return parquet_path

def format_list_columns(df):
    """Format list columns for better display."""
    for col in df.columns:
        if isinstance(df[col].iloc[0], list):
            df[col] = df[col].apply(lambda x: ', '.join(str(item) for item in x) if isinstance(x, list) else str(x))
    return df

def format_timestamps(df):
    """Format timestamp columns for better display."""
    for col in df.columns:
        if pd.api.types.is_datetime64_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def main():
    """Main function to run the script."""
    args = parse_arguments()
    
    # Validate registry name
    valid_registries = ['webscraper', 'extractor', 'excel', 'pdf', 'consolidation']
    if args.registry_name not in valid_registries:
        print(f"Error: Invalid registry name. Choose from: {', '.join(valid_registries)}")
        sys.exit(1)
    
    registry_path = get_registry_path(args.registry_name)
    
    # Check if registry file exists
    if not registry_path.exists():
        print(f"Error: Registry file not found at {registry_path}")
        sys.exit(1)
    
    try:
        # Read the parquet file
        print(f"Reading {args.registry_name} registry from {registry_path}...")
        
        # First read the schema
        parquet_file = pq.ParquetFile(registry_path)
        schema = parquet_file.schema
        
        # Get total number of rows
        total_rows = parquet_file.metadata.num_rows
        
        # Get number of columns
        num_columns = len(schema.names)
        
        # Read the data
        table = pq.read_table(registry_path)
        df = table.to_pandas()
        
        # Format list columns and timestamps for better display
        df = format_list_columns(df)
        df = format_timestamps(df)
        
        # Display registry info
        print(f"\nRegistry: {args.registry_name}")
        print(f"Total rows: {total_rows}")
        print(f"Total columns: {num_columns}")
        print(f"Column names: {', '.join(schema.names)}")
        
        # Display the first N rows
        if not df.empty:
            rows_to_display = min(args.rows, len(df))
            print(f"\nDisplaying first {rows_to_display} rows:\n")
            
            if args.pretty:
                # Use tabulate for pretty display
                print(tabulate(df.head(rows_to_display), headers='keys', tablefmt='pretty', showindex=False))
            else:
                # Use standard DataFrame display
                pd.set_option('display.max_columns', None)
                pd.set_option('display.width', None)
                print(df.head(rows_to_display))
            
            # Display first row as JSON for detailed inspection
            print("\nFirst row as JSON:")
            json_row = json.dumps(df.iloc[0].to_dict(), indent=2, default=str)
            print(json_row)
        else:
            print("\nNo data in registry.")
            
    except Exception as e:
        print(f"Error reading parquet file: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()