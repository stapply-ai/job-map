"""
Example: Fetch all jobs at Uber with title, URL, location, and description.

This script demonstrates how to fetch all job listings from Uber's careers API
and extract the specific fields requested: title, url, location, and description.
"""

import json
from api_client import UberCareersAPI


def fetch_all_jobs_basic():
    """
    Fetch all jobs and return only the basic fields:
    - title
    - url
    - location
    - description
    """
    print("Initializing Uber Careers API client...")
    api = UberCareersAPI()

    print("Fetching all jobs (this may take a minute)...")
    all_jobs = api.get_all_jobs(page_size=50)

    print(f"\nTotal jobs found: {len(all_jobs)}\n")

    # Extract only the requested fields
    jobs_data = []
    for job in all_jobs:
        job_info = {
            'title': job.title,
            'url': job.url,
            'location': {
                'city': job.location.city,
                'region': job.location.region,
                'country': job.location.country_name
            },
            'description': job.description
        }
        jobs_data.append(job_info)

    return jobs_data


def fetch_all_jobs_detailed():
    """
    Fetch all jobs and return comprehensive information including:
    - title, url, location, description (as requested)
    - additional fields: department, team, level, etc.
    """
    print("Initializing Uber Careers API client...")
    api = UberCareersAPI()

    print("Fetching all jobs (this may take a minute)...")
    all_jobs = api.get_all_jobs(page_size=50)

    print(f"\nTotal jobs found: {len(all_jobs)}\n")

    # Extract comprehensive job information
    jobs_data = []
    for job in all_jobs:
        job_info = {
            # Requested fields
            'title': job.title,
            'url': job.url,
            'location': {
                'city': job.location.city,
                'region': job.location.region,
                'country': job.location.country_name
            },
            'description': job.description,

            # Additional useful fields
            'id': job.id,
            'department': job.department,
            'team': job.team,
            'level': job.level,
            'time_type': job.time_type,
            'creation_date': job.creation_date,
            'updated_date': job.updated_date,
            'all_locations': [
                {
                    'city': loc.city,
                    'region': loc.region,
                    'country': loc.country_name
                }
                for loc in job.all_locations
            ]
        }
        jobs_data.append(job_info)

    return jobs_data


def save_jobs_to_json(jobs_data, filename='uber_jobs.json'):
    """Save jobs data to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(jobs_data, f, indent=2, ensure_ascii=False)
    print(f"Jobs saved to {filename}")


def display_sample_jobs(jobs_data, num_samples=5):
    """Display a sample of jobs."""
    print(f"\nShowing {min(num_samples, len(jobs_data))} sample jobs:\n")
    print("=" * 100)

    for i, job in enumerate(jobs_data[:num_samples], 1):
        print(f"\n{i}. {job['title']}")
        print(f"   URL: {job['url']}")

        location = job['location']
        loc_str = ', '.join(filter(None, [location['city'], location['region'], location['country']]))
        print(f"   Location: {loc_str}")

        # Show first 200 characters of description
        desc_preview = job['description'][:200].replace('\n', ' ')
        print(f"   Description: {desc_preview}...")

        # Show additional fields if available
        if 'department' in job:
            print(f"   Department: {job['department']}")
        if 'team' in job:
            print(f"   Team: {job['team']}")

        print("-" * 100)


def main():
    """Main execution function."""
    print("=" * 100)
    print("Uber Careers API - Fetch All Jobs Example")
    print("=" * 100)

    # Choose which version to run
    print("\nSelect option:")
    print("1. Fetch basic fields only (title, url, location, description)")
    print("2. Fetch comprehensive fields (includes department, team, etc.)")
    print("3. Fetch with specific filters (e.g., Engineering jobs only)")

    choice = input("\nEnter choice (1-3) or press Enter for option 1: ").strip()

    if choice == '2':
        print("\nFetching jobs with comprehensive details...\n")
        jobs_data = fetch_all_jobs_detailed()
    elif choice == '3':
        print("\nFetching Engineering jobs only...\n")
        api = UberCareersAPI()
        jobs = api.get_all_jobs(
            departments=['Engineering'],
            page_size=50
        )
        jobs_data = [
            {
                'title': job.title,
                'url': job.url,
                'location': {
                    'city': job.location.city,
                    'region': job.location.region,
                    'country': job.location.country_name
                },
                'description': job.description,
                'department': job.department,
                'team': job.team
            }
            for job in jobs
        ]
    else:
        print("\nFetching jobs with basic fields...\n")
        jobs_data = fetch_all_jobs_basic()

    # Display sample jobs
    display_sample_jobs(jobs_data)

    # Ask to save to file
    save = input("\nWould you like to save all jobs to a JSON file? (y/n): ").strip().lower()
    if save == 'y':
        filename = input("Enter filename (default: uber_jobs.json): ").strip()
        if not filename:
            filename = 'uber_jobs.json'
        save_jobs_to_json(jobs_data, filename)

    print(f"\n✅ Complete! Fetched {len(jobs_data)} total jobs.")
    print("\nYou can now:")
    print("  - Import the JSON file into your application")
    print("  - Process the job descriptions with NLP")
    print("  - Analyze hiring trends by location/department")
    print("  - Build a custom job board or alert system")


if __name__ == "__main__":
    main()
