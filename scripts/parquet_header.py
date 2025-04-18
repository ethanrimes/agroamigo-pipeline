#!/usr/bin/env python3
"""
Parquet Header Extractor

This script extracts and displays the header information and schema of a parquet file.
Usage: python parquet_header.py <path_to_parquet_file>
"""

import sys
import argparse
import pyarrow.parquet as pq
import pandas as pd
import json

def extract_parquet_info(file_path):
    """
    Extract and display header information and schema from a parquet file.
    
    Args:
        file_path (str): Path to the parquet file
    """
    try:
        # Open the parquet file
        parquet_file = pq.ParquetFile(file_path)
        
        # Extract metadata
        metadata = parquet_file.metadata
        
        # Print basic file information
        print("\n=== PARQUET FILE INFORMATION ===")
        print(f"File path: {file_path}")
        print(f"Number of row groups: {metadata.num_row_groups}")
        print(f"Number of rows: {metadata.num_rows}")
        print(f"Number of columns: {metadata.num_columns}")
        print(f"Created by: {metadata.created_by if metadata.created_by else 'Unknown'}")
        
        # Print schema information
        print("\n=== SCHEMA INFORMATION ===")
        schema = parquet_file.schema
        print(schema)
        
        # Get detailed column information
        print("\n=== DETAILED COLUMN INFORMATION ===")
        for i in range(metadata.num_columns):
            col_path = metadata.column(i).path_in_schema
            col_type = schema.get_field_index(col_path)
            field = schema.field(col_type)
            print(f"Column: {col_path}")
            print(f"  Type: {field.type}")
            print(f"  Nullable: {field.nullable}")
            
        # Read a small sample of data
        print("\n=== DATA SAMPLE (First 5 rows) ===")
        df_sample = pd.read_parquet(file_path, engine='pyarrow').head(5)
        print(df_sample)
        
        # Print row group information
        print("\n=== ROW GROUP INFORMATION ===")
        for i in range(metadata.num_row_groups):
            row_group = metadata.row_group(i)
            print(f"Row Group {i}:")
            print(f"  Number of rows: {row_group.num_rows}")
            print(f"  Total byte size: {row_group.total_byte_size}")
            
        # Get parquet file statistics if available
        print("\n=== COLUMN STATISTICS ===")
        for i in range(metadata.num_columns):
            col = metadata.column(i)
            col_path = col.path_in_schema
            print(f"Column: {col_path}")
            
            stats_available = False
            for j in range(metadata.num_row_groups):
                row_group = metadata.row_group(j)
                for k in range(row_group.num_columns):
                    column_chunk = row_group.column(k)
                    if column_chunk.path_in_schema == col_path and column_chunk.is_stats_set:
                        stats = column_chunk.statistics
                        stats_available = True
                        print(f"  Min value: {stats.min}")
                        print(f"  Max value: {stats.max}")
                        print(f"  Null count: {stats.null_count}")
                        print(f"  Distinct count: {stats.distinct_count if stats.has_distinct_count else 'Not available'}")
                        break
            
            if not stats_available:
                print("  No statistics available")
                
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract header information and schema from a parquet file')
    parser.add_argument('file_path', help='Path to the parquet file')
    parser.add_argument('--json', action='store_true', help='Output in JSON format')
    
    args = parser.parse_args()
    
    if args.json:
        try:
            # Open the parquet file
            parquet_file = pq.ParquetFile(args.file_path)
            
            # Extract metadata
            metadata = parquet_file.metadata
            
            # Create JSON output
            output = {
                "file_info": {
                    "path": args.file_path,
                    "num_row_groups": metadata.num_row_groups,
                    "num_rows": metadata.num_rows,
                    "num_columns": metadata.num_columns,
                    "created_by": metadata.created_by if metadata.created_by else "Unknown"
                },
                "schema": {
                    "fields": []
                },
                "row_groups": []
            }
            
            # Add schema information
            schema = parquet_file.schema
            for i in range(metadata.num_columns):
                col_path = metadata.column(i).path_in_schema
                col_type = schema.get_field_index(col_path)
                field = schema.field(col_type)
                output["schema"]["fields"].append({
                    "name": col_path,
                    "type": str(field.type),
                    "nullable": field.nullable
                })
            
            # Add row group information
            for i in range(metadata.num_row_groups):
                row_group = metadata.row_group(i)
                rg_info = {
                    "id": i,
                    "num_rows": row_group.num_rows,
                    "total_byte_size": row_group.total_byte_size,
                    "columns": []
                }
                
                for j in range(row_group.num_columns):
                    column_chunk = row_group.column(j)
                    col_info = {
                        "name": column_chunk.path_in_schema,
                        "file_offset": column_chunk.file_offset,
                        "compressed_size": column_chunk.compressed_size,
                        "uncompressed_size": column_chunk.uncompressed_size
                    }
                    
                    if column_chunk.is_stats_set:
                        stats = column_chunk.statistics
                        col_info["statistics"] = {
                            "min": str(stats.min) if stats.min is not None else None,
                            "max": str(stats.max) if stats.max is not None else None,
                            "null_count": stats.null_count,
                            "distinct_count": stats.distinct_count if stats.has_distinct_count else None
                        }
                    
                    rg_info["columns"].append(col_info)
                
                output["row_groups"].append(rg_info)
            
            # Print JSON output
            print(json.dumps(output, indent=2))
            
        except Exception as e:
            error_output = {"error": str(e)}
            print(json.dumps(error_output))
            sys.exit(1)
    else:
        extract_parquet_info(args.file_path)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: Please provide the path to a parquet file")
        print("Usage: python parquet_header.py <path_to_parquet_file>")
        sys.exit(1)
    
    main()