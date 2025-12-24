#!/usr/bin/env python3
"""
Quick Start: Fetch all Uber jobs with title, URL, location, and description.

This is the simplest way to get started with the API.
Run this script to immediately fetch all jobs and save them to a JSON file.
"""

import json
from datetime import datetime
from api_client import UberCareersAPI


def main():
    """Fetch all jobs and save to JSON file."""
    print("🚗 Uber Careers API - Quick Start")
    print("=" * 60)

    # Initialize API client
    api = UberCareersAPI()

    # Fetch all jobs
    print("\n📥 Fetching all jobs from Uber Careers...")
    print("   (This may take 10-15 seconds)")

    all_jobs = api.get_all_jobs(page_size=50)

    print(f"✅ Successfully fetched {len(all_jobs)} jobs!")

    # Extract requested fields: title, url, location, description
    jobs_data = []
    for job in all_jobs:
        jobs_data.append({
            'title': job.title,
            'url': job.url,
            'location': {
                'city': job.location.city,
                'region': job.location.region,
                'country': job.location.country_name
            },
            'description': job.description
        })

    # Save to JSON file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f'uber_jobs_{timestamp}.json'

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(jobs_data, f, indent=2, ensure_ascii=False)

    print(f"\n💾 Saved to: {filename}")

    # Show some statistics
    print("\n📊 Job Statistics:")
    print(f"   Total jobs: {len(jobs_data)}")

    # Count by location
    countries = {}
    for job in jobs_data:
        country = job['location']['country']
        countries[country] = countries.get(country, 0) + 1

    print(f"   Countries: {len(countries)}")
    print(f"   Top 5 locations:")
    for country, count in sorted(countries.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"     - {country}: {count} jobs")

    # Show sample job
    print("\n📄 Sample Job:")
    sample = jobs_data[0]
    print(f"   Title: {sample['title']}")
    print(f"   URL: {sample['url']}")
    loc = sample['location']
    print(f"   Location: {loc['city']}, {loc['region']}, {loc['country']}")
    print(f"   Description: {sample['description'][:150]}...")

    print("\n" + "=" * 60)
    print("✨ Done! Check the JSON file for all job data.")


if __name__ == "__main__":
    main()
