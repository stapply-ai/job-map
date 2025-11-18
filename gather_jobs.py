#!/usr/bin/env python3
"""
Script to gather all jobs.csv files from subdirectories and merge them into a single jobs.csv at the root.
Also gathers all jobs_diff_*.csv files from subdirectories and merges them into a single jobs_diff file at the root.
"""

from datetime import datetime
import pandas as pd
from pathlib import Path

from export_utils import FIELDNAMES


def gather_jobs():
    """Find all jobs.csv files and merge them into a single file at the root. Also gather all diff files."""
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
    
    # Remove duplicates based on url (the unique identifier)
    initial_count = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['url'], keep='first')
    duplicates_removed = initial_count - len(combined_df)
    
    if duplicates_removed > 0:
        print(f"Removed {duplicates_removed} duplicate entries.")
    
    # Write to output file
    combined_df.to_csv(output_file, index=False)
    print(f"\nSuccessfully created {output_file} with {len(combined_df)} unique jobs.")
    
    # Find all jobs_diff_*.csv files in subdirectories
    diff_files = []
    for diff_file in root_dir.rglob("jobs_diff_*.csv"):
        # Only include files in subdirectories (not root)
        if diff_file.parent != root_dir:
            diff_files.append(diff_file)
    
    if not diff_files:
        print("\nNo jobs_diff_*.csv files found in subdirectories.")
        return
    
    print(f"\nFound {len(diff_files)} jobs_diff_*.csv files:")
    for f in diff_files:
        print(f"  - {f.relative_to(root_dir)}")
    
    # Read and concatenate all diff CSV files
    diff_dataframes = []
    for diff_file in diff_files:
        try:
            df = pd.read_csv(diff_file)
            print(f"  Loaded {len(df)} rows from {diff_file.relative_to(root_dir)}")
            diff_dataframes.append(df)
        except Exception as e:
            print(f"  Error reading {diff_file.relative_to(root_dir)}: {e}")
            continue
    
    if not diff_dataframes:
        print("No diff data to merge.")
        return
    
    # Concatenate all diff dataframes
    combined_diff_df = pd.concat(diff_dataframes, ignore_index=True)
    
    # Ensure status field exists
    if 'status' not in combined_diff_df.columns:
        print("  Warning: status field not found in diff files, adding default 'new' status.")
        combined_diff_df['status'] = 'new'
    
    # Remove duplicates based on url (keeping the first occurrence)
    # This handles cases where the same job appears in multiple platform diff files
    initial_diff_count = len(combined_diff_df)
    combined_diff_df = combined_diff_df.drop_duplicates(subset=['url'], keep='first')
    diff_duplicates_removed = initial_diff_count - len(combined_diff_df)
    
    if diff_duplicates_removed > 0:
        print(f"Removed {diff_duplicates_removed} duplicate entries from diff files.")
    
    # Create output diff file with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    diff_filename = f"{output_file.stem}_diff_{timestamp}{output_file.suffix}"
    diff_output_file = output_file.with_name(diff_filename)
    
    # Ensure all expected fields exist
    diff_fieldnames = FIELDNAMES + ["status"]
    for field in diff_fieldnames:
        if field not in combined_diff_df.columns:
            combined_diff_df[field] = ""
    
    # Write diff file
    combined_diff_df.to_csv(diff_output_file, index=False)
    print(f"\nSuccessfully created {diff_output_file.name} with {len(combined_diff_df)} diff entries.")


if __name__ == "__main__":
    gather_jobs()

