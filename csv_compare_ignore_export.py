#!/usr/bin/env python3
"""
CSV Automatic Comparison Tool
High-performance CSV comparison based on primary keys, generates HTML reports showing differences
"""

import pandas as pd
import numpy as np
import hashlib
import json
import random
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CSVComparator:
    """High-performance CSV comparison tool"""
    
    def __init__(self, primary_keys: List[str], sample_rate: float = 0.1, 
                 exclude_fields: List[str] = None):
        """
        Initialize comparator
        
        Args:
            primary_keys: List of primary key column names (supports composite keys)
            sample_rate: Smart sampling rate (between 0-1)
            exclude_fields: List of fields to exclude from comparison
        """
        self.primary_keys = primary_keys
        self.sample_rate = sample_rate
        self.exclude_fields = exclude_fields or []
        self.comparison_results = {}
        self.identical_data = []  # Store identical rows data
        
    def load_csv_chunked(self, file_path: str, chunk_size: int = 10000) -> pd.DataFrame:
        """
        Load large CSV files in chunks
        
        Args:
            file_path: CSV file path
            chunk_size: Chunk size
            
        Returns:
            Merged DataFrame
        """
        logger.info(f"Starting to load CSV file: {file_path}")
        chunks = []
        
        try:
            for chunk in pd.read_csv(file_path, chunksize=chunk_size):
                chunks.append(chunk)
                
            df = pd.concat(chunks, ignore_index=True)
            logger.info(f"Successfully loaded {len(df)} rows of data")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load CSV file: {e}")
            raise
    
    def create_composite_key(self, df: pd.DataFrame) -> pd.Series:
        """
        Create composite primary key
        
        Args:
            df: DataFrame
            
        Returns:
            Composite key Series
        """
        if len(self.primary_keys) == 1:
            return df[self.primary_keys[0]].astype(str)
        else:
            # Combine multiple primary keys, ensure all values are converted to strings
            key_parts = []
            for key in self.primary_keys:
                key_parts.append(df[key].astype(str))
            return pd.Series(['|'.join(parts) for parts in zip(*key_parts)], index=df.index)
    
    def intelligent_sampling(self, df1: pd.DataFrame, df2: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Intelligent sampling strategy
        
        Args:
            df1, df2: DataFrames to compare
            
        Returns:
            Sampled DataFrames
        """
        # Create primary keys
        key1 = self.create_composite_key(df1)
        key2 = self.create_composite_key(df2)
        
        logger.info(f"Sample primary keys from file1: {key1.head(3).tolist()}")
        logger.info(f"Sample primary keys from file2: {key2.head(3).tolist()}")
        
        # Find common keys
        common_keys = set(key1) & set(key2)
        
        logger.info(f"Found {len(common_keys)} common keys")
        
        if len(common_keys) == 0:
            logger.warning("No common keys found, using random sampling")
            sample_size = int(min(len(df1), len(df2)) * self.sample_rate)
            return df1.sample(n=sample_size), df2.sample(n=sample_size)
        
        # If sampling rate is 1.0 or few common keys, return all data with common keys
        if self.sample_rate >= 1.0 or len(common_keys) <= 100:
            sampled_keys = list(common_keys)
        else:
            # Sample common keys
            sample_size = int(len(common_keys) * self.sample_rate)
            sampled_keys = random.sample(list(common_keys), sample_size)
        
        # Filter data
        df1_sampled = df1[key1.isin(sampled_keys)]
        df2_sampled = df2[key2.isin(sampled_keys)]
        
        logger.info(f"Smart sampling completed: {len(sampled_keys)} keys, {len(df1_sampled)} + {len(df2_sampled)} rows")
        return df1_sampled, df2_sampled
    
    def parallel_compare_chunks(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                               chunk_size: int = 1000) -> Dict[str, Any]:
        """
        Compare data chunks in parallel
        
        Args:
            df1, df2: DataFrames to compare
            chunk_size: Chunk size
            
        Returns:
            Comparison results dictionary
        """
        # Create primary key index
        key1 = self.create_composite_key(df1)
        key2 = self.create_composite_key(df2)
        
        df1_indexed = df1.set_index(key1)
        df2_indexed = df2.set_index(key2)
        
        # Find common keys
        common_keys = list(set(df1_indexed.index) & set(df2_indexed.index))
        
        results = {
            'total_keys': len(set(df1_indexed.index) | set(df2_indexed.index)),
            'common_keys': len(common_keys),
            'only_in_df1': len(set(df1_indexed.index) - set(df2_indexed.index)),
            'only_in_df2': len(set(df2_indexed.index) - set(df1_indexed.index)),
            'differences': [],
            'identical_rows': 0
        }
        
        # Reset identical_data for new comparison
        self.identical_data = []
        
        # Process common keys in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            
            for i in range(0, len(common_keys), chunk_size):
                chunk_keys = common_keys[i:i + chunk_size]
                future = executor.submit(
                    self._compare_chunk, 
                    df1_indexed, df2_indexed, chunk_keys
                )
                futures.append(future)
            
            # Collect results
            for future in as_completed(futures):
                chunk_result = future.result()
                results['differences'].extend(chunk_result['differences'])
                results['identical_rows'] += chunk_result['identical_rows']
                self.identical_data.extend(chunk_result['identical_data'])
        
        logger.info(f"Comparison completed: {results['common_keys']} common keys")
        return results
    
    def _compare_chunk(self, df1: pd.DataFrame, df2: pd.DataFrame, 
                      keys: List[str]) -> Dict[str, Any]:
        """
        Compare data chunk
        
        Args:
            df1, df2: Indexed DataFrames
            keys: List of primary keys to compare
            
        Returns:
            Chunk comparison results
        """
        differences = []
        identical_rows = 0
        identical_data = []
        
        for key in keys:
            try:
                row1 = df1.loc[key]
                row2 = df2.loc[key]
                
                # Handle Series and DataFrame cases
                if isinstance(row1, pd.DataFrame):
                    row1 = row1.iloc[0]
                if isinstance(row2, pd.DataFrame):
                    row2 = row2.iloc[0]
                
                # Compare each field (excluding specified fields)
                diff_fields = []
                for col in df1.columns:
                    # Skip excluded fields
                    if col in self.exclude_fields:
                        continue
                        
                    if col in df2.columns:
                        val1 = row1[col] if not pd.isna(row1[col]) else ""
                        val2 = row2[col] if not pd.isna(row2[col]) else ""
                        
                        if str(val1) != str(val2):
                            diff_fields.append({
                                'field': col,
                                'value1': val1,
                                'value2': val2
                            })
                
                if diff_fields:
                    differences.append({
                        'key': key,
                        'differences': diff_fields
                    })
                else:
                    identical_rows += 1
                    # Store identical row data (excluding excluded fields)
                    row_data = {}
                    for col in df1.columns:
                        if col not in self.exclude_fields:
                            row_data[col] = row1[col] if not pd.isna(row1[col]) else ""
                    row_data['primary_key'] = key
                    identical_data.append(row_data)
                    
            except KeyError:
                logger.warning(f"Primary key {key} does not exist in one of the DataFrames")
                continue
            except Exception as e:
                logger.error(f"Error processing primary key {key}: {e}")
                continue
        
        return {
            'differences': differences,
            'identical_rows': identical_rows,
            'identical_data': identical_data
        }
    
    def compare_csvs(self, file1: str, file2: str, 
                    output_html: str = "comparison_report.html") -> Dict[str, Any]:
        """
        Compare two CSV files
        
        Args:
            file1, file2: CSV file paths
            output_html: Output HTML report path
            
        Returns:
            Comparison results
        """
        logger.info("Starting CSV comparison process")
        
        # Load data
        df1 = self.load_csv_chunked(file1)
        df2 = self.load_csv_chunked(file2)
        
        # Validate primary keys exist
        for key in self.primary_keys:
            if key not in df1.columns:
                raise ValueError(f"Primary key '{key}' does not exist in file1")
            if key not in df2.columns:
                raise ValueError(f"Primary key '{key}' does not exist in file2")
        
        # Log excluded fields
        if self.exclude_fields:
            logger.info(f"Excluded fields from comparison: {', '.join(self.exclude_fields)}")
        
        # Smart sampling
        df1_sample, df2_sample = self.intelligent_sampling(df1, df2)
        
        # Execute comparison
        results = self.parallel_compare_chunks(df1_sample, df2_sample)
        
        # Generate HTML report
        self.generate_html_report(results, file1, file2, output_html)
        
        # Store results
        self.comparison_results = results
        
        return results
    
    def generate_html_report(self, results: Dict[str, Any], 
                           file1: str, file2: str, output_path: str):
        """
        Generate HTML comparison report
        
        Args:
            results: Comparison results
            file1, file2: Source file paths
            output_path: HTML output path
        """
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>CSV Comparison Report</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .summary {{ margin: 20px 0; }}
        .stats {{ display: flex; justify-content: space-around; margin: 20px 0; }}
        .stat-box {{ background-color: #e8f4f8; padding: 15px; border-radius: 5px; text-align: center; }}
        .differences {{ margin: 20px 0; }}
        .diff-row {{ border: 1px solid #ddd; margin: 10px 0; padding: 10px; border-radius: 5px; }}
        .diff-row:nth-child(even) {{ background-color: #f9f9f9; }}
        .field-diff {{ margin: 5px 0; padding: 5px; background-color: #fff3cd; border-radius: 3px; }}
        .key {{ font-weight: bold; color: #0066cc; }}
        .field-name {{ font-weight: bold; }}
        .value1 {{ color: #d32f2f; }}
        .value2 {{ color: #388e3c; }}
        .no-differences {{ color: #666; font-style: italic; }}
        .excluded-fields {{ background-color: #f8f9fa; padding: 10px; border-radius: 5px; margin: 10px 0; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>CSV File Comparison Report</h1>
        <p><strong>File 1:</strong> {file1}</p>
        <p><strong>File 2:</strong> {file2}</p>
        <p><strong>Comparison Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Primary Keys:</strong> {', '.join(self.primary_keys)}</p>"""
        
        if self.exclude_fields:
            html_content += f"""
        <div class="excluded-fields">
            <strong>Excluded Fields from Comparison:</strong> {', '.join(self.exclude_fields)}
        </div>"""
        
        html_content += f"""
    </div>

    <div class="summary">
        <h2>Comparison Summary</h2>
        <div class="stats">
            <div class="stat-box">
                <h3>{results['total_keys']}</h3>
                <p>Total Primary Keys</p>
            </div>
            <div class="stat-box">
                <h3>{results['common_keys']}</h3>
                <p>Common Primary Keys</p>
            </div>
            <div class="stat-box">
                <h3>{results['only_in_df1']}</h3>
                <p>Only in File 1</p>
            </div>
            <div class="stat-box">
                <h3>{results['only_in_df2']}</h3>
                <p>Only in File 2</p>
            </div>
            <div class="stat-box">
                <h3>{results['identical_rows']}</h3>
                <p>Identical Rows</p>
            </div>
            <div class="stat-box">
                <h3>{len(results['differences'])}</h3>
                <p>Rows with Differences</p>
            </div>
        </div>
    </div>

    <div class="differences">
        <h2>Detailed Differences (First 100 entries)</h2>
        """
        
        if not results['differences']:
            html_content += '<p class="no-differences">No data differences found</p>'
        else:
            for i, diff in enumerate(results['differences'][:100]):
                html_content += f"""
                <div class="diff-row">
                    <p class="key">Primary Key: {diff['key']}</p>
                """
                
                for field_diff in diff['differences']:
                    html_content += f"""
                    <div class="field-diff">
                        <span class="field-name">{field_diff['field']}:</span>
                        <span class="value1">File 1: {field_diff['value1']}</span> → 
                        <span class="value2">File 2: {field_diff['value2']}</span>
                    </div>
                    """
                
                html_content += "</div>"
        
        html_content += """
    </div>
    
    <div class="summary">
        <h2>Recommended Data Transformations</h2>
        <p>Based on comparison results, consider focusing on transformation rules for the following fields:</p>
        <ul>
        """
        
        # Analyze field difference patterns (excluding excluded fields)
        field_diff_count = {}
        for diff in results['differences']:
            for field_diff in diff['differences']:
                field_name = field_diff['field']
                if field_name not in self.exclude_fields:
                    field_diff_count[field_name] = field_diff_count.get(field_name, 0) + 1
        
        for field, count in sorted(field_diff_count.items(), key=lambda x: x[1], reverse=True):
            html_content += f"<li>{field}: {count} differences</li>"
        
        html_content += """
        </ul>
    </div>
</body>
</html>
        """
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML report generated: {output_path}")
    
    def export_differences_csv(self, output_path: str):
        """
        Export difference data to CSV
        
        Args:
            output_path: Output CSV path
        """
        if not self.comparison_results or not self.comparison_results['differences']:
            logger.warning("No difference data to export")
            return
        
        diff_data = []
        for diff in self.comparison_results['differences']:
            for field_diff in diff['differences']:
                diff_data.append({
                    'primary_key': diff['key'],
                    'field': field_diff['field'],
                    'value_file1': field_diff['value1'],
                    'value_file2': field_diff['value2']
                })
        
        df_diff = pd.DataFrame(diff_data)
        df_diff.to_csv(output_path, index=False)
        logger.info(f"Difference data exported: {output_path}")
    
    def export_identical_rows_csv(self, output_path: str):
        """
        Export identical rows data to CSV (excluding excluded fields)
        
        Args:
            output_path: Output CSV path
        """
        if not self.identical_data:
            logger.warning("No identical data to export")
            return
        
        df_identical = pd.DataFrame(self.identical_data)
        df_identical.to_csv(output_path, index=False)
        logger.info(f"Identical rows data exported: {output_path} (total: {len(self.identical_data)} rows)")
    
    def export_identical_rows_separate_files(self, file1_output: str, file2_output: str, 
                                           original_df1: pd.DataFrame, original_df2: pd.DataFrame):
        """
        Export identical rows to separate files for file1 and file2
        
        Args:
            file1_output: Output path for file1 identical rows
            file2_output: Output path for file2 identical rows
            original_df1: Original DataFrame from file1
            original_df2: Original DataFrame from file2
        """
        if not self.identical_data:
            logger.warning("No identical data to export")
            return
        
        # Extract primary keys of identical rows
        identical_keys = [row['primary_key'] for row in self.identical_data]
        
        # Create composite keys for original dataframes
        key1 = self.create_composite_key(original_df1)
        key2 = self.create_composite_key(original_df2)
        
        # Filter original dataframes for identical rows
        df1_identical = original_df1[key1.isin(identical_keys)]
        df2_identical = original_df2[key2.isin(identical_keys)]
        
        # Remove excluded fields if specified
        if self.exclude_fields:
            df1_identical = df1_identical.drop(columns=[col for col in self.exclude_fields if col in df1_identical.columns])
            df2_identical = df2_identical.drop(columns=[col for col in self.exclude_fields if col in df2_identical.columns])
        
        # Export to separate files
        df1_identical.to_csv(file1_output, index=False)
        df2_identical.to_csv(file2_output, index=False)
        
        logger.info(f"File1 identical rows exported: {file1_output} (total: {len(df1_identical)} rows)")
        logger.info(f"File2 identical rows exported: {file2_output} (total: {len(df2_identical)} rows)")


def main():
    """Command line main function"""
    parser = argparse.ArgumentParser(description='CSV Automatic Comparison Tool')
    parser.add_argument('file1', help='First CSV file path')
    parser.add_argument('file2', help='Second CSV file path')
    parser.add_argument('--keys', '-k', nargs='+', required=True, help='Primary key column names (supports multiple)')
    parser.add_argument('--exclude', '-e', nargs='+', default=[], dest='exclude_fields', 
                       help='List of fields to exclude from comparison')
    parser.add_argument('--sample-rate', '-s', type=float, default=0.1, help='Sampling rate (0-1)')
    parser.add_argument('--output', '-o', default='comparison_report.html', help='Output HTML report path')
    parser.add_argument('--export-csv', help='Export differences data CSV path')
    parser.add_argument('--export-identical', help='Export identical rows CSV path')
    parser.add_argument('--export-identical-separate', nargs=2, metavar=('FILE1_OUTPUT', 'FILE2_OUTPUT'),
                       help='Export identical rows to separate files for file1 and file2')
    
    args = parser.parse_args()
    
    # Create comparator
    comparator = CSVComparator(
        primary_keys=args.keys,
        sample_rate=args.sample_rate,
        exclude_fields=args.exclude_fields
    )
    
    try:
        # Load original dataframes for separate export if needed
        original_df1 = None
        original_df2 = None
        if args.export_identical_separate:
            original_df1 = comparator.load_csv_chunked(args.file1)
            original_df2 = comparator.load_csv_chunked(args.file2)
        
        # Execute comparison
        results = comparator.compare_csvs(args.file1, args.file2, args.output)
        
        # Export differences CSV (if specified)
        if args.export_csv:
            comparator.export_differences_csv(args.export_csv)
        
        # Export identical rows CSV (if specified)
        if args.export_identical:
            comparator.export_identical_rows_csv(args.export_identical)
        
        # Export identical rows to separate files (if specified)
        if args.export_identical_separate:
            comparator.export_identical_rows_separate_files(
                args.export_identical_separate[0], 
                args.export_identical_separate[1],
                original_df1, 
                original_df2
            )
        
        # Print summary
        print(f"\n=== Comparison Complete ===")
        print(f"Total Primary Keys: {results['total_keys']}")
        print(f"Common Primary Keys: {results['common_keys']}")
        print(f"Identical Rows: {results['identical_rows']}")
        print(f"Rows with Differences: {len(results['differences'])}")
        print(f"HTML Report: {args.output}")
        
    except Exception as e:
        logger.error(f"Error during comparison process: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
