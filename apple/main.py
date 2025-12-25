#!/usr/bin/env python3
"""
Apple Jobs Scraper

Scrapes job postings from Apple's careers website and saves them to apple.json.
Follows the same pattern as other scrapers (google, microsoft, etc.) for easy integration.
"""

import json
import os
import sys
import importlib.util
from datetime import datetime
from pathlib import Path

# Import api_client from the apple directory using importlib to avoid conflicts
script_dir = Path(__file__).resolve().parent
api_client_path = script_dir / "api_client.py"
spec = importlib.util.spec_from_file_location("apple_api_client", api_client_path)
apple_api_client = importlib.util.module_from_spec(spec)
sys.modules["apple_api_client"] = apple_api_client
spec.loader.exec_module(apple_api_client)
AppleJobsAPI = apple_api_client.AppleJobsAPI


def scrape_apple_jobs(force: bool = False) -> tuple[str, int, bool]:
    """
    Scrape Apple jobs and store them in apple/apple.json.
    Returns (json_path, num_jobs, was_scraped).

    Args:
        force: If True, force scraping even if data was recently scraped

    Returns:
        Tuple of (json_path_str, num_jobs, was_scraped)
    """
    json_path = str(script_dir / "apple.json")

    # Check if file exists and is fresh (unless force=True)
    # Default freshness: 6 hours (since Apple scraping can be slow)
    max_age_hours = 6.0
    if not force and os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            if isinstance(existing, dict):
                jobs = existing.get("jobs", [])
                last_scraped_str = existing.get("last_scraped")

                # Check freshness based on last_scraped timestamp
                if last_scraped_str and jobs:
                    try:
                        last_scraped = datetime.fromisoformat(last_scraped_str)
                        # Handle timezone-aware datetimes
                        if last_scraped.tzinfo is not None:
                            from datetime import timezone

                            now = datetime.now(timezone.utc)
                        else:
                            now = datetime.now()
                            last_scraped = last_scraped.replace(tzinfo=None)
                        hours_elapsed = (now - last_scraped).total_seconds() / 3600
                        if hours_elapsed < max_age_hours:
                            print(
                                f"Existing Apple data found (scraped {hours_elapsed:.1f} hours ago). Reusing without rescraping."
                            )
                            return json_path, len(jobs), False
                        else:
                            print(
                                f"Existing Apple data is stale ({hours_elapsed:.1f} hours old, max {max_age_hours}h). Rescraping..."
                            )
                    except (ValueError, TypeError):
                        # If timestamp parsing fails, check file modification time as fallback
                        file_mtime = Path(json_path).stat().st_mtime
                        hours_elapsed = (datetime.now().timestamp() - file_mtime) / 3600
                        if hours_elapsed < max_age_hours and jobs:
                            print(
                                f"Existing Apple data found (file modified {hours_elapsed:.1f} hours ago). Reusing without rescraping."
                            )
                            return json_path, len(jobs), False
                elif jobs:
                    # No timestamp but has jobs - check file modification time
                    file_mtime = Path(json_path).stat().st_mtime
                    hours_elapsed = (datetime.now().timestamp() - file_mtime) / 3600
                    if hours_elapsed < max_age_hours:
                        print(
                            f"Existing Apple data found (file modified {hours_elapsed:.1f} hours ago). Reusing without rescraping."
                        )
                        return json_path, len(jobs), False
            else:
                # Old format (list instead of dict)
                if existing:
                    print(
                        "Existing Apple data found (old format). Reusing without rescraping."
                    )
                    return json_path, len(existing), False
        except (OSError, json.JSONDecodeError) as e:
            print(f"Error reading existing Apple data: {e}. Will rescrape.")

    # Initialize client and fetch jobs
    print("Fetching all Apple jobs...")
    client = AppleJobsAPI(locale="en-us")
    all_jobs = client.search_all_jobs()

    if not all_jobs:
        print("No jobs found!")
        return json_path, 0, True

    print(f"Found {len(all_jobs)} total jobs")

    # Convert Job objects to dictionaries
    jobs_data = []
    # Note: Detailed fetching is commented out for performance (takes too long with 6000+ jobs)
    # Uncomment the lines below if you need full_description with minimumQualifications, etc.
    # print(f"Fetching detailed information for {len(all_jobs)} jobs...")

    for i, job in enumerate(all_jobs, 1):
        # Fetch detailed job information (commented out for performance)
        # job = client.get_job_details(job)

        # Handle multiple locations
        locations = [loc.name for loc in job.locations] if job.locations else ["N/A"]

        job_dict = {
            "url": job.url,
            "title": job.postingTitle,
            "locations": locations,
            "location": locations[0]
            if locations
            else "N/A",  # Keep first location for backward compatibility
            "description": job.jobSummary,  # Use jobSummary from search results (faster)
            # "description": job.full_description,  # Use merged description with all fields (slower, requires get_job_details)
            "postingDate": job.postingDate,
            "positionId": job.positionId,
            "id": job.id,
            "reqId": job.reqId,
        }
        jobs_data.append(job_dict)

        if i % 50 == 0:
            print(f"Processed {i}/{len(all_jobs)} jobs...")

    # Wrap in standard format with metadata
    wrapped = {
        "last_scraped": datetime.now().isoformat(),
        "name": "Apple",
        "jobs": jobs_data,
    }

    # Save to file
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(wrapped, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(jobs_data)} jobs to {json_path}")
    return json_path, len(jobs_data), True


if __name__ == "__main__":
    scrape_apple_jobs(force=True)
