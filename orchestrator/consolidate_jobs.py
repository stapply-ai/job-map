"""
CSV Consolidation Script

Consolidates all scraped jobs from different ATS platforms into a single simplified CSV.
Output: all_jobs.csv (url, title, location, company)

This keeps the file size small and makes it easy to review all jobs at a glance.
"""

import json
import csv
import glob
from pathlib import Path
from typing import List, Dict, Set
from datetime import datetime


def extract_ashby_jobs(json_file: Path) -> List[Dict[str, str]]:
    """Extract jobs from Ashby JSON file"""
    jobs = []
    company_slug = json_file.stem

    try:
        with open(json_file, "r") as f:
            data = json.load(f)

        for job in data.get("jobs", []):
            jobs.append({
                "url": job.get("jobUrl", job.get("applyUrl", "")),
                "title": job.get("title", ""),
                "location": job.get("location", ""),
                "company": company_slug,
                "platform": "ashby",
            })

    except Exception as e:
        print(f"  ⚠️  Error reading {json_file}: {e}")

    return jobs


def extract_greenhouse_jobs(json_file: Path) -> List[Dict[str, str]]:
    """Extract jobs from Greenhouse JSON file"""
    jobs = []
    company_slug = json_file.stem

    try:
        with open(json_file, "r") as f:
            data = json.load(f)

        for job in data.get("jobs", []):
            # Greenhouse structure
            location = job.get("location", {})
            if isinstance(location, dict):
                location_str = location.get("name", "")
            else:
                location_str = str(location)

            jobs.append({
                "url": job.get("absolute_url", ""),
                "title": job.get("title", ""),
                "location": location_str,
                "company": company_slug,
                "platform": "greenhouse",
            })

    except Exception as e:
        print(f"  ⚠️  Error reading {json_file}: {e}")

    return jobs


def extract_lever_jobs(json_file: Path) -> List[Dict[str, str]]:
    """Extract jobs from Lever JSON file"""
    jobs = []
    company_slug = json_file.stem

    try:
        with open(json_file, "r") as f:
            data = json.load(f)

        # Lever returns a list directly
        job_list = data if isinstance(data, list) else data.get("postings", [])

        for job in job_list:
            # Lever structure
            location = job.get("location", {})
            if isinstance(location, dict):
                location_str = location.get("name", "")
            else:
                location_str = str(location)

            jobs.append({
                "url": job.get("hostedUrl", job.get("applyUrl", "")),
                "title": job.get("text", ""),
                "location": location_str,
                "company": company_slug,
                "platform": "lever",
            })

    except Exception as e:
        print(f"  ⚠️  Error reading {json_file}: {e}")

    return jobs


def extract_workable_jobs(json_file: Path) -> List[Dict[str, str]]:
    """Extract jobs from Workable JSON file"""
    jobs = []
    company_slug = json_file.stem

    try:
        with open(json_file, "r") as f:
            data = json.load(f)

        # Workable structure
        job_list = data if isinstance(data, list) else data.get("jobs", [])

        for job in job_list:
            location = job.get("location", {})
            if isinstance(location, dict):
                location_str = location.get("city", "")
            else:
                location_str = str(location)

            jobs.append({
                "url": job.get("url", ""),
                "title": job.get("title", ""),
                "location": location_str,
                "company": company_slug,
                "platform": "workable",
            })

    except Exception as e:
        print(f"  ⚠️  Error reading {json_file}: {e}")

    return jobs


PLATFORM_EXTRACTORS = {
    "ashby": extract_ashby_jobs,
    "greenhouse": extract_greenhouse_jobs,
    "lever": extract_lever_jobs,
    "workable": extract_workable_jobs,
}


def consolidate_jobs(platforms: List[str], output_file: str = "all_jobs.csv") -> int:
    """
    Consolidate jobs from multiple platforms into a single CSV

    Args:
        platforms: List of platform names to include
        output_file: Output CSV file path

    Returns:
        Total number of jobs consolidated
    """
    print("=" * 80)
    print("JOB CONSOLIDATION")
    print("=" * 80)

    all_jobs = []
    seen_urls: Set[str] = set()  # Track duplicates
    duplicates = 0

    project_root = Path(__file__).parent.parent

    for platform in platforms:
        if platform not in PLATFORM_EXTRACTORS:
            print(f"⚠️  Unknown platform: {platform}")
            continue

        print(f"\n--- Processing {platform.upper()} ---")

        # Find all JSON files for this platform
        pattern = f"{platform}/companies/*.json"
        json_files = list(project_root.glob(pattern))

        if not json_files:
            print(f"  No job files found (looking for: {pattern})")
            continue

        print(f"  Found {len(json_files)} company files")

        platform_jobs = 0
        extractor = PLATFORM_EXTRACTORS[platform]

        for json_file in json_files:
            jobs = extractor(json_file)
            platform_jobs += len(jobs)

            # Filter duplicates and add to all_jobs
            for job in jobs:
                url = job.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_jobs.append(job)
                elif url:
                    duplicates += 1

        print(f"  ✓ Extracted {platform_jobs} jobs from {len(json_files)} companies")

    # Save to CSV
    print(f"\n--- Saving to {output_file} ---")

    if not all_jobs:
        print("⚠️  No jobs to save")
        return 0

    output_path = project_root / output_file

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["url", "title", "location", "company", "platform"]
        )
        writer.writeheader()
        writer.writerows(all_jobs)

    print(f"  ✓ Saved {len(all_jobs)} unique jobs")
    print(f"  ⓘ Skipped {duplicates} duplicate URLs")

    # Print statistics
    print(f"\n--- Statistics ---")
    platform_counts = {}
    for job in all_jobs:
        platform = job["platform"]
        platform_counts[platform] = platform_counts.get(platform, 0) + 1

    for platform, count in sorted(platform_counts.items()):
        print(f"  {platform.capitalize()}: {count} jobs")

    print(f"\n  Total: {len(all_jobs)} jobs")

    return len(all_jobs)


def main():
    """Main entry point for CLI"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Consolidate scraped jobs into a single CSV"
    )
    parser.add_argument(
        "--platforms",
        type=str,
        default="ashby,greenhouse,lever,workable",
        help="Platforms to include (comma-separated, default: all)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="all_jobs.csv",
        help="Output CSV file (default: all_jobs.csv)",
    )

    args = parser.parse_args()

    platforms = [p.strip() for p in args.platforms.split(",")]

    total = consolidate_jobs(platforms, args.output)

    if total > 0:
        print(f"\n✅ Consolidation complete! {total} jobs saved to {args.output}")
    else:
        print("\n⚠️  No jobs found to consolidate")


if __name__ == "__main__":
    main()
