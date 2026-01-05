#!/usr/bin/env python3
"""Script to inspect Parquet file structure and sample data."""

import argparse
import pyarrow.parquet as pq
import pandas as pd
import sys


def inspect_parquet(file_path, num_rows=10):
    """Inspect a Parquet file and print schema, columns, and sample rows."""
    try:
        # Read metadata only (no data)
        pf = pq.ParquetFile(file_path)
        
        print("=" * 80)
        print(f"Parquet File: {file_path}")
        print("=" * 80)
        print(f"\nFile Metadata:")
        print(f"  Total rows: {pf.metadata.num_rows:,}")
        print(f"  Total columns: {len(pf.schema)}")
        print(f"  Row groups: {pf.num_row_groups}")
        
        # Get column names
        all_cols = [f.name for f in pf.schema]
        data_cols = [c for c in all_cols if not c.startswith('__index')]
        index_cols = [c for c in all_cols if c.startswith('__index')]
        
        print(f"\nColumns ({len(data_cols)} data columns):")
        # Get column types dynamically from the Parquet file schema
        # This works with any Parquet file structure by reading the actual schema
        column_types = {}
        
        try:
            # Method 1: Try to get Arrow schema from ParquetFile
            if hasattr(pf, 'schema_arrow'):
                arrow_schema = pf.schema_arrow
                for col in data_cols:
                    try:
                        field = arrow_schema.field(col)
                        column_types[col] = str(field.type)
                    except (KeyError, AttributeError):
                        pass
            
            # Method 2: If Method 1 didn't work, read a small sample to get schema
            if not column_types:
                try:
                    # Read just the first row group to get schema information
                    table_sample = pf.read_row_groups([0], columns=data_cols[:1] if data_cols else [])
                    arrow_schema = table_sample.schema
                    for col in data_cols:
                        try:
                            field = arrow_schema.field(col)
                            column_types[col] = str(field.type)
                        except (KeyError, AttributeError):
                            pass
                except Exception:
                    pass
            
            # Method 3: If still no types, try reading full table (for small files)
            if not column_types and pf.metadata.num_rows < 10000:
                try:
                    table = pq.read_table(file_path, columns=data_cols)
                    arrow_schema = table.schema
                    for col in data_cols:
                        try:
                            field = arrow_schema.field(col)
                            column_types[col] = str(field.type)
                        except (KeyError, AttributeError):
                            pass
                except Exception:
                    pass
            
            # Display columns with types if available, otherwise just names
            for i, col in enumerate(data_cols, 1):
                col_type = column_types.get(col, None)
                if col_type:
                    print(f"  {i:2d}. {col:30s} ({col_type})")
                else:
                    print(f"  {i:2d}. {col:30s}")
                    
        except Exception as e:
            # Fallback: just show column names without types
            # Silently continue - type information is optional
            for i, col in enumerate(data_cols, 1):
                print(f"  {i:2d}. {col:30s}")
        
        if index_cols:
            print(f"\nIndex columns (excluded from display): {len(index_cols)}")
            print(f"  {index_cols[:5]}{'...' if len(index_cols) > 5 else ''}")
        
        # Read sample rows
        print(f"\n{'=' * 80}")
        print(f"Sample Data (first {num_rows} rows):")
        print(f"{'=' * 80}")
        
        # Read table
        table = pq.read_table(file_path)
        df = table.to_pandas()
        
        # Remove index columns
        if index_cols:
            df = df.drop(columns=index_cols, errors='ignore')
        
        # Display sample rows
        sample_df = df.head(num_rows)
        
        # Print column headers
        print("\nRow |", " | ".join([f"{col:20s}" for col in data_cols]))
        print("-" * (len(data_cols) * 24 + 10))
        
        # Print each row
        for idx, (_, row) in enumerate(sample_df.iterrows(), 1):
            row_values = []
            for col in data_cols:
                val = row[col]
                val_str = str(val)
                
                # Truncate long values
                if len(val_str) > 20:
                    val_str = val_str[:17] + "..."
                
                # Handle None/NaN
                if pd.isna(val):
                    val_str = "<NA>"
                
                row_values.append(f"{val_str:20s}")
            
            print(f"{idx:3d} |", " | ".join(row_values))
        
        print(f"\n{'=' * 80}")
        print(f"Total rows in file: {len(df):,}")
        print(f"Rows displayed: {len(sample_df):,}")
        print(f"{'=' * 80}")
        
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Inspect Parquet file - displays schema, columns, and sample rows',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '-f', '--file',
        required=True,
        help='Path to the Parquet file to inspect'
    )
    parser.add_argument(
        '-n', '--num-rows',
        type=int,
        default=10,
        help='Number of sample rows to display (default: 10)'
    )
    
    args = parser.parse_args()
    inspect_parquet(args.file, args.num_rows)


if __name__ == '__main__':
    main()
