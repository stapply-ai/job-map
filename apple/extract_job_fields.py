#!/usr/bin/env python3
"""
Extract specific job fields as requested by the user.

This script demonstrates extracting exactly what was requested:
- URL
- Title
- Location
- Description

Output formats supported: JSON, CSV, or console display
"""

from api_client import AppleJobsAPI
import json
import csv
import sys
from typing import List, Dict


def extract_job_fields(job) -> Dict[str, str]:
    """
    Extract the requested fields from a job object.

    Args:
        job: Job object from the API

    Returns:
        Dictionary with url, title, location, and description
    """
    return {
        "url": job.url,
        "title": job.postingTitle,
        "location": job.locations[0].name if job.locations else "N/A",
        "description": job.jobSummary
    }


def save_to_json(jobs_data: List[Dict], filename: str = "jobs.json"):
    """Save job data to JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(jobs_data, f, indent=2, ensure_ascii=False)
    print(f"✓ Saved {len(jobs_data)} jobs to {filename}")


def save_to_csv(jobs_data: List[Dict], filename: str = "jobs.csv"):
    """Save job data to CSV file."""
    if not jobs_data:
        print("No data to save")
        return

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['url', 'title', 'location', 'description'])
        writer.writeheader()
        writer.writerows(jobs_data)
    print(f"✓ Saved {len(jobs_data)} jobs to {filename}")


def display_jobs(jobs_data: List[Dict], max_display: int = 10):
    """Display jobs in a readable format."""
    for i, job in enumerate(jobs_data[:max_display], 1):
        print(f"\n{'='*100}")
        print(f"Job #{i}")
        print(f"{'='*100}")
        print(f"Title:       {job['title']}")
        print(f"Location:    {job['location']}")
        print(f"URL:         {job['url']}")
        print(f"Description: {job['description'][:200]}...")

    if len(jobs_data) > max_display:
        print(f"\n... and {len(jobs_data) - max_display} more jobs")


def main():
    """Main function to extract and export job data."""

    # Configuration
    LOCALE = "en-us"
    MAX_PAGES = 5  # Adjust as needed

    print("="*100)
    print("Apple Jobs Field Extractor")
    print("="*100)
    print(f"\nConfiguration:")
    print(f"  - Locale: {LOCALE}")
    print(f"  - Max Pages: {MAX_PAGES}")
    print()

    # Initialize client
    print("Initializing API client...")
    client = AppleJobsAPI(locale=LOCALE)
    print("✓ Client initialized\n")

    # Get total job count
    total_jobs = client.get_total_jobs()
    print(f"Total jobs available: {total_jobs}\n")

    # Fetch jobs
    print(f"Fetching jobs (up to {MAX_PAGES} pages)...")
    jobs = client.search_all_jobs(max_pages=MAX_PAGES)
    print(f"✓ Retrieved {len(jobs)} jobs\n")

    if not jobs:
        print("No jobs found!")
        return

    # Extract requested fields
    print("Extracting fields: URL, Title, Location, Description...")
    jobs_data = [extract_job_fields(job) for job in jobs]
    print(f"✓ Extracted data from {len(jobs_data)} jobs\n")

    # Display sample
    print("Sample of jobs:")
    display_jobs(jobs_data, max_display=3)

    # Export options
    print(f"\n{'='*100}")
    print("Export Options")
    print(f"{'='*100}\n")

    # Save to JSON
    json_filename = f"apple_jobs_{len(jobs_data)}_jobs.json"
    save_to_json(jobs_data, json_filename)

    # Save to CSV
    csv_filename = f"apple_jobs_{len(jobs_data)}_jobs.csv"
    save_to_csv(jobs_data, csv_filename)

    print(f"\n{'='*100}")
    print("Summary")
    print(f"{'='*100}")
    print(f"Total jobs retrieved: {len(jobs_data)}")
    print(f"Fields extracted: url, title, location, description")
    print(f"JSON output: {json_filename}")
    print(f"CSV output: {csv_filename}")
    print(f"\n✓ All done!")


def search_with_query(query: str, max_pages: int = 3):
    """
    Search for jobs with a specific query.

    Example:
        python extract_job_fields.py --query "engineer"
    """
    client = AppleJobsAPI(locale="en-us")

    print(f"Searching for: '{query}'")
    total = client.get_total_jobs(query=query)
    print(f"Found {total} matching jobs\n")

    jobs = client.search_all_jobs(query=query, max_pages=max_pages)
    jobs_data = [extract_job_fields(job) for job in jobs]

    # Save results
    filename = f"jobs_{query.replace(' ', '_')}.json"
    save_to_json(jobs_data, filename)

    return jobs_data


if __name__ == "__main__":
    try:
        # Check for command line arguments
        if len(sys.argv) > 1:
            if sys.argv[1] == "--query" and len(sys.argv) > 2:
                query = " ".join(sys.argv[2:])
                search_with_query(query, max_pages=3)
            elif sys.argv[1] == "--help":
                print("Usage:")
                print("  python extract_job_fields.py              # Fetch all jobs")
                print("  python extract_job_fields.py --query TERM # Search for specific jobs")
                print("  python extract_job_fields.py --help       # Show this help")
            else:
                print(f"Unknown argument: {sys.argv[1]}")
                print("Use --help for usage information")
        else:
            # Default: fetch jobs without query
            main()

    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
