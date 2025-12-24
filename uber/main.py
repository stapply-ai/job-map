#!/usr/bin/env python3
"""
Uber Jobs Scraper

Scrapes job postings from Uber's careers website and saves them to uber.json.
Follows the same pattern as other scrapers (google, microsoft, apple, etc.) for easy integration.
"""

import json
import os
import sys
import importlib.util
from datetime import datetime
from pathlib import Path

# Import api_client from the uber directory using importlib to avoid conflicts
script_dir = Path(__file__).resolve().parent
api_client_path = script_dir / "api_client.py"
spec = importlib.util.spec_from_file_location("uber_api_client", api_client_path)
uber_api_client = importlib.util.module_from_spec(spec)
sys.modules["uber_api_client"] = uber_api_client
spec.loader.exec_module(uber_api_client)
UberCareersAPI = uber_api_client.UberCareersAPI


def scrape_uber_jobs(force: bool = False) -> tuple[str, int, bool]:
    """
    Scrape Uber jobs and store them in uber/uber.json.
    Returns (json_path, num_jobs, was_scraped).

    Args:
        force: If True, force scraping even if data was recently scraped

    Returns:
        Tuple of (json_path_str, num_jobs, was_scraped)
    """
    json_path = str(script_dir / "uber.json")

    # Check if file exists and is fresh (unless force=True)
    # Default freshness: 24 hours (since Uber scraping can be slow)
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
                                f"Existing Uber data found (scraped {hours_elapsed:.1f} hours ago). Reusing without rescraping."
                            )
                            return json_path, len(jobs), False
                        else:
                            print(
                                f"Existing Uber data is stale ({hours_elapsed:.1f} hours old, max {max_age_hours}h). Rescraping..."
                            )
                    except (ValueError, TypeError):
                        # If timestamp parsing fails, check file modification time as fallback
                        file_mtime = Path(json_path).stat().st_mtime
                        hours_elapsed = (datetime.now().timestamp() - file_mtime) / 3600
                        if hours_elapsed < max_age_hours and jobs:
                            print(
                                f"Existing Uber data found (file modified {hours_elapsed:.1f} hours ago). Reusing without rescraping."
                            )
                            return json_path, len(jobs), False
                elif jobs:
                    # No timestamp but has jobs - check file modification time
                    file_mtime = Path(json_path).stat().st_mtime
                    hours_elapsed = (datetime.now().timestamp() - file_mtime) / 3600
                    if hours_elapsed < max_age_hours:
                        print(
                            f"Existing Uber data found (file modified {hours_elapsed:.1f} hours ago). Reusing without rescraping."
                        )
                        return json_path, len(jobs), False
            else:
                # Old format (list instead of dict)
                if existing:
                    print(
                        "Existing Uber data found (old format). Reusing without rescraping."
                    )
                    return json_path, len(existing), False
        except (OSError, json.JSONDecodeError) as e:
            print(f"Error reading existing Uber data: {e}. Will rescrape.")

    # Initialize client and fetch jobs
    print("Fetching all Uber jobs...")
    api = UberCareersAPI()
    all_jobs = api.get_all_jobs(page_size=50)

    if not all_jobs:
        print("No jobs found!")
        return json_path, 0, True

    print(f"Found {len(all_jobs)} total jobs")

    # Convert Job objects to dictionaries
    jobs_data = []
    for i, job in enumerate(all_jobs, 1):
        # Handle multiple locations - use all_locations if available, otherwise primary location
        locations = []
        if job.all_locations:
            for loc in job.all_locations:
                loc_parts = []
                if loc.city:
                    loc_parts.append(loc.city)
                if loc.region:
                    loc_parts.append(loc.region)
                if loc.country_name:
                    loc_parts.append(loc.country_name)
                if loc_parts:
                    locations.append(", ".join(loc_parts))

        # Fallback to primary location if no all_locations
        if not locations:
            loc_parts = []
            if job.location.city:
                loc_parts.append(job.location.city)
            if job.location.region:
                loc_parts.append(job.location.region)
            if job.location.country_name:
                loc_parts.append(job.location.country_name)
            if loc_parts:
                locations.append(", ".join(loc_parts))

        # Default to "N/A" if no location found
        if not locations:
            locations = ["N/A"]

        job_dict = {
            "url": job.url,
            "title": job.title,
            "locations": locations,
            "location": locations[0]
            if locations
            else "N/A",  # Keep first location for backward compatibility
            "description": job.description,
            "id": job.id,
            "department": job.department,
            "team": job.team,
            "level": job.level,
            "time_type": job.time_type,
            "creation_date": job.creation_date,
            "updated_date": job.updated_date,
        }
        jobs_data.append(job_dict)

        if i % 50 == 0:
            print(f"Processed {i}/{len(all_jobs)} jobs...")

    # Wrap in standard format with metadata
    wrapped = {
        "last_scraped": datetime.now().isoformat(),
        "name": "Uber",
        "jobs": jobs_data,
    }

    # Save to file
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(wrapped, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(jobs_data)} jobs to {json_path}")
    return json_path, len(jobs_data), True


if __name__ == "__main__":
    scrape_uber_jobs(force=True)
