#!/usr/bin/env python3
"""
Remove case-insensitive duplicates from companies.csv files.
Converts all URLs to lowercase and keeps the first occurrence of each unique company.
"""

import csv
import os
from pathlib import Path


def remove_duplicates_from_csv(csv_path):
    """Remove case-insensitive duplicates from a CSV file."""
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        return

    print(f"\nProcessing: {csv_path}")

    # Read the CSV file
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        print(f"  Empty file, skipping.")
        return

    # Separate header and data
    header = rows[0] if rows else []
    data_rows = rows[1:] if len(rows) > 1 else []

    original_count = len(data_rows)

    # Track seen values (case-insensitive) and keep unique rows
    # Convert all URLs to lowercase
    seen = set()
    unique_rows = []

    for row in data_rows:
        if row:  # Skip empty rows
            # Convert URL to lowercase
            url = row[0].strip().lower() if row[0] else ''

            if url and url not in seen:
                seen.add(url)
                # Store the lowercase version in the row
                row[0] = url
                unique_rows.append(row)

    # Write back to the file
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(unique_rows)

    removed_count = original_count - len(unique_rows)
    print(f"  Original rows: {original_count}")
    print(f"  Unique rows: {len(unique_rows)}")
    print(f"  Duplicates removed: {removed_count}")


def main():
    """Find and process all companies.csv files."""
    # Get the current directory
    base_path = Path('.')

    # Find all companies.csv files
    companies_files = list(base_path.glob('**/*companies.csv'))

    if not companies_files:
        print("No companies.csv files found.")
        return

    print(f"Found {len(companies_files)} companies.csv file(s)")

    # Process each file
    for csv_file in companies_files:
        remove_duplicates_from_csv(str(csv_file))

    print("\n✓ Done!")


if __name__ == "__main__":
    main()
