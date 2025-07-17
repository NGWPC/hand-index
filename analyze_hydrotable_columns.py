#!/usr/bin/env python3

import pandas as pd
import numpy as np
from collections import defaultdict
import sys

def analyze_hydrotable_columns(csv_path):
    """
    Analyze hydrotable CSV to identify columns that should be list types
    when aggregated by HydroID.
    """
    
    print(f"Reading {csv_path}...")
    df = pd.read_csv(csv_path)
    
    print(f"Total rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"Unique HydroIDs: {df['HydroID'].nunique()}")
    
    # Group by HydroID and analyze each column
    grouped = df.groupby('HydroID')
    
    results = {}
    
    for column in df.columns:
        if column == 'HydroID':
            continue
            
        # For each HydroID, count unique values in this column
        unique_counts = []
        non_null_counts = []
        
        for hydro_id, group in grouped:
            # Remove NaN values for counting
            non_null_values = group[column].dropna()
            unique_values = non_null_values.unique()
            
            unique_counts.append(len(unique_values))
            non_null_counts.append(len(non_null_values))
        
        # Statistics
        max_unique = max(unique_counts) if unique_counts else 0
        avg_unique = np.mean(unique_counts) if unique_counts else 0
        max_non_null = max(non_null_counts) if non_null_counts else 0
        avg_non_null = np.mean(non_null_counts) if non_null_counts else 0
        
        # Count how many HydroIDs have multiple unique values
        multiple_values_count = sum(1 for x in unique_counts if x > 1)
        total_hydroids = len(unique_counts)
        
        results[column] = {
            'max_unique_per_hydroid': max_unique,
            'avg_unique_per_hydroid': avg_unique,
            'max_rows_per_hydroid': max_non_null,
            'avg_rows_per_hydroid': avg_non_null,
            'hydroids_with_multiple_values': multiple_values_count,
            'total_hydroids': total_hydroids,
            'pct_hydroids_with_multiple_values': (multiple_values_count / total_hydroids * 100) if total_hydroids > 0 else 0
        }
    
    return results, df

def print_analysis_results(results):
    """Print analysis results in a readable format."""
    
    print("\n" + "="*80)
    print("COLUMN ANALYSIS RESULTS")
    print("="*80)
    
    # Sort by percentage of HydroIDs with multiple values (descending)
    sorted_results = sorted(results.items(), 
                          key=lambda x: x[1]['pct_hydroids_with_multiple_values'], 
                          reverse=True)
    
    print(f"{'Column':<30} {'MaxUnique':<10} {'AvgUnique':<10} {'MultiValHydroIDs':<15} {'%WithMulti':<10}")
    print("-" * 80)
    
    for column, stats in sorted_results:
        print(f"{column:<30} {stats['max_unique_per_hydroid']:<10} "
              f"{stats['avg_unique_per_hydroid']:<10.2f} "
              f"{stats['hydroids_with_multiple_values']:<15} "
              f"{stats['pct_hydroids_with_multiple_values']:<10.1f}%")

def identify_list_candidates(results, threshold_percent=50):
    """
    Identify columns that should be list types based on analysis.
    
    Args:
        results: Analysis results from analyze_hydrotable_columns
        threshold_percent: Minimum percentage of HydroIDs with multiple values
    """
    
    candidates = []
    
    for column, stats in results.items():
        if (stats['pct_hydroids_with_multiple_values'] >= threshold_percent and 
            stats['max_unique_per_hydroid'] > 1):
            candidates.append({
                'column': column,
                'max_unique': stats['max_unique_per_hydroid'],
                'avg_unique': stats['avg_unique_per_hydroid'],
                'pct_with_multiple': stats['pct_hydroids_with_multiple_values']
            })
    
    return candidates

def show_examples(df, column, num_examples=3):
    """Show examples of HydroIDs with multiple values for a column."""
    
    print(f"\nExamples for column '{column}':")
    print("-" * 40)
    
    grouped = df.groupby('HydroID')
    examples_shown = 0
    
    for hydro_id, group in grouped:
        if examples_shown >= num_examples:
            break
            
        unique_values = group[column].dropna().unique()
        if len(unique_values) > 1:
            print(f"HydroID {hydro_id}: {len(unique_values)} unique values")
            print(f"  Values: {list(unique_values)}")
            examples_shown += 1

if __name__ == "__main__":
    csv_path = "example-hydrotable.csv"
    
    # Run analysis
    results, df = analyze_hydrotable_columns(csv_path)
    
    # Print results
    print_analysis_results(results)
    
    # Identify candidates for list types
    print("\n" + "="*80)
    print("COLUMNS THAT SHOULD BE LIST TYPES")
    print("="*80)
    
    candidates = identify_list_candidates(results, threshold_percent=10)  # Lower threshold for discovery
    
    if candidates:
        print(f"Found {len(candidates)} columns that should be list types:")
        print()
        
        for candidate in candidates:
            print(f"Column: {candidate['column']}")
            print(f"  Max unique values per HydroID: {candidate['max_unique']}")
            print(f"  Average unique values per HydroID: {candidate['avg_unique']:.2f}")
            print(f"  % of HydroIDs with multiple values: {candidate['pct_with_multiple']:.1f}%")
            
            # Show examples
            show_examples(df, candidate['column'], num_examples=2)
            print()
    else:
        print("No columns found that should be list types based on the analysis.")
    
    print("\n" + "="*80)
    print("CURRENT SCHEMA LIST COLUMNS")
    print("="*80)
    
    # Check which columns are already list types in the schema
    schema_list_columns = [
        'stage', 'discharge_cms', 'default_discharge_cms', 
        'subdiv_discharge_cms', 'precalb_discharge_cms'
    ]
    
    print("Columns already defined as DECIMAL[] in schema:")
    for col in schema_list_columns:
        if col in results:
            stats = results[col]
            print(f"  {col}: {stats['pct_hydroids_with_multiple_values']:.1f}% HydroIDs have multiple values")