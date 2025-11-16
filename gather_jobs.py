#!/usr/bin/env python3
"""
Script to gather all jobs.csv files from subdirectories and merge them into a single jobs.csv at the root.
"""

import pandas as pd
from pathlib import Path


def gather_jobs():
    """Find all jobs.csv files and merge them into a single file at the root."""
    root_dir = Path(__file__).parent
    output_file = root_dir / "jobs.csv"
    
    # Find all jobs.csv files in subdirectories (excluding the root)
    jobs_files = []
    for jobs_file in root_dir.rglob("jobs.csv"):
        # Skip the output file if it already exists
        if jobs_file == output_file:
            continue
        # Only include files in subdirectories
        if jobs_file.parent != root_dir:
            jobs_files.append(jobs_file)
    
    if not jobs_files:
        print("No jobs.csv files found in subdirectories.")
        return
    
    print(f"Found {len(jobs_files)} jobs.csv files:")
    for f in jobs_files:
        print(f"  - {f.relative_to(root_dir)}")
    
    # Read and concatenate all CSV files
    dataframes = []
    for jobs_file in jobs_files:
        try:
            df = pd.read_csv(jobs_file)
            print(f"  Loaded {len(df)} rows from {jobs_file.relative_to(root_dir)}")
            dataframes.append(df)
        except Exception as e:
            print(f"  Error reading {jobs_file.relative_to(root_dir)}: {e}")
            continue
    
    if not dataframes:
        print("No data to merge.")
        return
    
    # Concatenate all dataframes
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    # Remove duplicates based on all columns (or just url if that's the unique identifier)
    # Using url as the unique identifier since it should be unique per job
    initial_count = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['url'], keep='first')
    duplicates_removed = initial_count - len(combined_df)
    
    if duplicates_removed > 0:
        print(f"Removed {duplicates_removed} duplicate entries.")
    
    # Write to output file
    combined_df.to_csv(output_file, index=False)
    print(f"\nSuccessfully created {output_file} with {len(combined_df)} unique jobs.")


if __name__ == "__main__":
    gather_jobs()

