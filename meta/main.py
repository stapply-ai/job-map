"""
Meta Jobs Scraper with Intelligent Caching

This scraper fetches job listings from Meta Careers using:
1. Playwright to capture GraphQL responses for job listings
2. Playwright to fetch individual job descriptions (with caching)

Caching System:
- Descriptions are cached by job ID in meta_descriptions_cache.json
- Only NEW or UPDATED jobs need to be fetched
- Massive time savings on subsequent runs (seconds vs hours)

Performance:
- First run: ~1171 jobs, ~40-50 minutes for all descriptions
- Subsequent runs: Only new jobs fetched, typically seconds to minutes
- Cache hits are instant, no network requests needed

Usage:
    python3 main.py                    # Fetch all jobs with descriptions
    python3 main.py --limit 10         # Test with 10 jobs
    python3 main.py --no-descriptions  # Skip descriptions entirely
"""

import json
import time
from playwright.sync_api import sync_playwright
import os
from typing import Optional
from datetime import datetime

JOBS_PAGE_URL = "https://www.metacareers.com/jobs"
DESCRIPTION_CACHE_FILE = "meta_descriptions_cache.json"

# Unwanted patterns to remove from job descriptions
UNWANTED_PATTERNS = [
    # Navigation
    "Skip to main content",
    "Jobs",
    "Teams",
    "Career Programs",
    "Working at Meta",
    "Blog",
    # Apply/Action buttons
    "Apply for this job",
    "Take the first step toward a rewarding career at Meta",
    "Apply now",
    "APPLY NOW",
    # Search/Filter
    "Find your role",
    "Explore jobs that match your skills and experience",
    "Search by technology, team or location to find an opening that's right for you",
    "View jobs",
    "Job Search",
    # Teams/Departments (header navigation)
    "Business Teams",
    "Technology Teams",
    "Program Management",
    "Engineering",
    "Product Management",
    "Data Science",
    "Design",
    "Research",
    # Working at Meta section
    "Accessiblity and Engagement",
    "Benefits",
    "Culture",
    "Hiring Process",
    # Account
    "My account",
    "Career profile",
    "Account settings",
    "Messages",
    # Blog/About
    "Meta Careers Blog",
    "About us",
    "About Meta",
    "Media gallery",
    "Brand resources",
    "For investors",
    # Footer legal
    "Community Standards",
    "Data Policy",
    "Terms",
    "Cookie Policy",
    "Report a bug",
    "Looking for contractor roles?",
    # Cookie consent
    "Accept cookies from Meta Careers on this browser",
    "We use cookies to help personalize and improve content and services",
    "serve relevant ads and provide a safer experience",
    "You can review your cookie controls at any time",
    "Learn more about cookie uses and controls in our Cookie Policy",
    "Learn More",
    "Accept All",
    # Research/Programs (common navigation items)
    "Accelerate Eng Talent",
    "Students and Grads",
    "Rotational Programs",
    # Footer accommodations and legal
    "If you need assistance or an accommodation due to a disability",
    "fill out the Accommodations request form",
    "Notice regarding automated employment decision tools in New York City",
    "If you have any trouble, you can report an issue",
]

# Patterns that indicate the end of job description content
END_PATTERNS = [
    "©2025 Meta",
    "©2024 Meta",
    "©Meta",
    "Notice regarding automated employment decision tools",
]


def clean_job_description(raw_text):
    """Clean job description by removing navigation, footer, and unwanted content"""
    if not raw_text:
        return None

    lines = raw_text.split("\n")
    cleaned_lines = []

    # Track if we've seen actual job content
    job_content_started = False
    title_seen = False

    for line in lines:
        line_stripped = line.strip()

        # Check if we've hit the end markers (copyright, footer, etc.)
        if any(pattern in line_stripped for pattern in END_PATTERNS):
            break

        # Skip empty lines at the beginning
        if not job_content_started and not line_stripped:
            continue

        # Skip unwanted patterns
        if any(
            pattern.lower() in line_stripped.lower() for pattern in UNWANTED_PATTERNS
        ):
            continue

        # Skip very short lines that are likely navigation
        if len(line_stripped) < 3:
            continue

        # Skip lines that look like "+2 more" or similar category indicators
        if line_stripped.startswith("+") and "more" in line_stripped.lower():
            continue

        # Skip lines that are just punctuation or numbers
        if all(c in "+-.,;:/()[]{}" for c in line_stripped.replace(" ", "")):
            continue

        # Skip the duplicate title at the beginning (usually appears twice)
        if not title_seen and not job_content_started:
            # First occurrence of what looks like a title
            if len(line_stripped) > 10 and len(line_stripped) < 200:
                title_seen = True
                continue

        # Skip location lines (e.g., "Sunnyvale, CA +1 location")
        if "+" in line_stripped and "location" in line_stripped.lower():
            continue

        # Skip lines that look like navigation (short, few words)
        if not job_content_started and len(line_stripped.split()) <= 3:
            # Skip single or double word lines at the beginning
            continue

        # Mark that we've started seeing real content
        # Job descriptions typically have sentences with multiple words
        if len(line_stripped) > 50 or (len(line_stripped.split()) > 8):
            job_content_started = True

        cleaned_lines.append(line)

    # Join and clean up extra whitespace
    cleaned_text = "\n".join(cleaned_lines)

    # Remove multiple consecutive newlines
    while "\n\n\n" in cleaned_text:
        cleaned_text = cleaned_text.replace("\n\n\n", "\n\n")

    # Remove leading/trailing whitespace
    cleaned_text = cleaned_text.strip()

    # Return None if cleaned text is too short (likely just navigation)
    if len(cleaned_text) < 100:
        return None

    return cleaned_text


def fetch_job_description_playwright(job_url, browser):
    """Fetch job description from individual job page using Playwright"""
    try:
        page = browser.new_page()
        page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(1.5)  # Wait for content to render

        # Extract job description from the page
        raw_description = page.evaluate("""
            () => {
                // Try to find the main job content first
                const contentSelectors = [
                    '[role="main"]',
                    'main',
                    'article'
                ];

                for (const selector of contentSelectors) {
                    const element = document.querySelector(selector);
                    if (element) {
                        const text = element.innerText || element.textContent;
                        if (text && text.length > 200) {
                            return text.trim();
                        }
                    }
                }

                // Fallback: get body text
                const body = document.body;
                if (body) {
                    return body.innerText || body.textContent;
                }

                return null;
            }
        """)

        page.close()

        # Clean the description to remove navigation and footer content
        cleaned_description = clean_job_description(raw_description)
        return cleaned_description

    except Exception as e:
        print(f"  ✗ Error fetching {job_url}: {e}")
        return None


def scrape_meta_jobs():
    """Scrape Meta jobs using Playwright for browser automation"""
    all_jobs = []
    graphql_data = []

    with sync_playwright() as p:
        print("Launching browser...")
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        )
        page = context.new_page()

        # Capture GraphQL responses
        def handle_response(response):
            if "graphql" in response.url:
                try:
                    json_data = response.json()
                    graphql_data.append(json_data)
                    print(f"Captured GraphQL response from {response.url}")
                except Exception as e:
                    print(f"Error parsing GraphQL response: {e}")

        page.on("response", handle_response)

        print(f"Navigating to {JOBS_PAGE_URL}...")
        response = page.goto(
            JOBS_PAGE_URL, wait_until="domcontentloaded", timeout=60000
        )

        actual_url = page.url
        print(f"Actual URL after navigation: {actual_url}")
        print(f"Response status: {response.status}")

        # Wait for GraphQL requests to complete
        print("Waiting for jobs to load...")
        time.sleep(5)

        print("Extracting jobs from GraphQL responses...")
        all_jobs = []

        # Process GraphQL responses if we captured any
        if graphql_data:
            print(f"\nProcessing {len(graphql_data)} GraphQL responses...")
            with open("meta_graphql_responses.json", "w", encoding="utf-8") as f:
                json.dump(graphql_data, f, indent=2)
            print("Saved GraphQL responses to meta_graphql_responses.json")

            # Try to extract jobs from GraphQL data
            for gql_response in graphql_data:
                if isinstance(gql_response, dict) and "data" in gql_response:
                    # Navigate through possible GraphQL response structures
                    data = gql_response.get("data", {})

                    # Check for job_search_with_featured_jobs structure (the one Meta uses)
                    if "job_search_with_featured_jobs" in data:
                        job_search = data["job_search_with_featured_jobs"]
                        job_results = job_search.get("all_jobs", [])

                        if job_results:
                            print(f"Found {len(job_results)} jobs in GraphQL response!")
                            for job in job_results:
                                job_id = job.get("id")
                                # Construct URL from job ID
                                job_url = (
                                    f"https://www.metacareers.com/jobs/{job_id}/"
                                    if job_id
                                    else None
                                )

                                all_jobs.append(
                                    {
                                        "id": job_id,
                                        "title": job.get("title"),
                                        "locations": job.get("locations", []),
                                        "teams": job.get("teams", []),
                                        "sub_teams": job.get("sub_teams", []),
                                        "url": job_url,
                                    }
                                )
                        continue

                    # Fallback: try other possible paths
                    job_results = (
                        data.get("job_search_results", {}).get("results", [])
                        or data.get("jobSearchResults", {}).get("results", [])
                        or data.get("careers", {}).get("jobs", [])
                        or []
                    )

                    if job_results:
                        print(f"Found {len(job_results)} jobs in GraphQL response!")
                        for job in job_results:
                            all_jobs.append(
                                {
                                    "id": job.get("id"),
                                    "title": job.get("title"),
                                    "location": job.get("location")
                                    or job.get("locations"),
                                    "team": job.get("team") or job.get("teams"),
                                    "url": job.get("posting_url") or job.get("url"),
                                    "updated_time": job.get("updated_time"),
                                }
                            )

        print(f"Total jobs extracted: {len(all_jobs)}")

        browser.close()

    return all_jobs


def load_description_cache():
    """Load cached descriptions from file"""
    if os.path.exists(DESCRIPTION_CACHE_FILE):
        try:
            with open(DESCRIPTION_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"  ⚠ Warning: Could not load cache file: {e}")
            return {}
    return {}


def save_description_cache(cache):
    """Save description cache to file"""
    try:
        with open(DESCRIPTION_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"  ⚠ Warning: Could not save cache file: {e}")


def fetch_descriptions_for_jobs(all_jobs, limit=None):
    """Fetch descriptions for all jobs using Playwright with caching

    Args:
        all_jobs: List of job dictionaries
        limit: Maximum number of jobs to fetch descriptions for (None = all jobs)
    """
    jobs_to_process = all_jobs[:limit] if limit else all_jobs

    # Load cache
    cache = load_description_cache()
    cache_hits = 0
    cache_misses = 0

    print(f"\nFetching descriptions for {len(jobs_to_process)} jobs...")
    if limit and limit < len(all_jobs):
        print(f"  (Limited to first {limit} jobs out of {len(all_jobs)} total)")

    if cache:
        print(f"  Loaded cache with {len(cache)} entries")

    # First pass: use cache where possible
    jobs_needing_fetch = []
    for job in jobs_to_process:
        job_id = job.get("id")
        if job_id and job_id in cache:
            job["description"] = cache[job_id]
            cache_hits += 1
        else:
            jobs_needing_fetch.append(job)

    if cache_hits > 0:
        print(f"  ✓ Using cached descriptions for {cache_hits} jobs")

    # Second pass: fetch missing descriptions
    if jobs_needing_fetch:
        print(f"  Fetching {len(jobs_needing_fetch)} new descriptions...")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)

            for i, job in enumerate(jobs_needing_fetch):
                job_url = job.get("url")
                job_id = job.get("id")

                if job_url:
                    description = fetch_job_description_playwright(job_url, browser)
                    job["description"] = description

                    # Cache the description if we have a job ID
                    if job_id and description:
                        cache[job_id] = description
                        cache_misses += 1

                    # Print progress every 5 jobs for small batches, every 25 for larger
                    interval = 5 if len(jobs_needing_fetch) <= 50 else 25
                    if (i + 1) % interval == 0 or i == 0:
                        desc_count = sum(
                            1
                            for j in jobs_needing_fetch[: i + 1]
                            if j.get("description")
                        )
                        print(
                            f"  Progress: {i + 1}/{len(jobs_needing_fetch)} fetched ({desc_count} with descriptions)"
                        )
                else:
                    job["description"] = None
                    print(f"  ⚠ Job {job.get('title', 'Unknown')} has no URL")

            browser.close()

        # Save updated cache
        if cache_misses > 0:
            save_description_cache(cache)
            print(f"  ✓ Cached {cache_misses} new descriptions")

    # Set description to None for remaining jobs if limited
    if limit:
        for job in all_jobs[limit:]:
            job["description"] = None

    print(
        f"  ✓ Completed fetching descriptions (Cache: {cache_hits} hits, {cache_misses} misses)"
    )
    return all_jobs


def main(fetch_descriptions=True, description_limit=None):
    """
    Main function to scrape Meta jobs

    Args:
        fetch_descriptions: Whether to fetch job descriptions (default: True)
        description_limit: Maximum number of descriptions to fetch (None = all)
    """
    all_jobs = scrape_meta_jobs()

    print(f"\nTotal jobs fetched: {len(all_jobs)}")

    # Fetch descriptions
    if all_jobs and fetch_descriptions:
        all_jobs = fetch_descriptions_for_jobs(all_jobs, limit=description_limit)

    # Wrap in standardized format
    wrapped = {
        "last_scraped": datetime.now().isoformat(),
        "name": "Meta",
        "jobs": all_jobs,
    }

    with open("meta.json", "w", encoding="utf-8") as f:
        json.dump(wrapped, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(all_jobs)} jobs to meta.json")
    return all_jobs


def scrape_meta(
    force: bool = False,
    fetch_descriptions: bool = True,
    description_limit: Optional[int] = None,
):
    """
    Scrape Meta jobs and store them in meta/meta.json.
    Mirrors the (path, count, was_scraped) contract used by other scrapers.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(script_dir, "meta.json")

    if not force and os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            jobs = (
                existing.get("jobs", existing)
                if isinstance(existing, dict)
                else existing
            )
            if isinstance(jobs, list):
                if isinstance(existing, dict):
                    last_scraped_str = existing.get("last_scraped")
                    if last_scraped_str:
                        try:
                            last_scraped = datetime.fromisoformat(last_scraped_str)
                            hours_elapsed = (
                                datetime.now() - last_scraped
                            ).total_seconds() / 3600
                            print(
                                f"Existing Meta data scraped {hours_elapsed:.1f} hours ago. Reusing."
                            )
                        except Exception:
                            print(
                                "Existing Meta data found. Reusing without rescraping."
                            )
                    else:
                        print("Existing Meta data found. Reusing without rescraping.")
                else:
                    print("Existing Meta data found. Reusing without rescraping.")
                return json_path, len(jobs), False
        except (OSError, json.JSONDecodeError):
            pass

    jobs = main(
        fetch_descriptions=fetch_descriptions,
        description_limit=description_limit,
    )

    job_count = len(jobs) if isinstance(jobs, list) else 0
    return json_path, job_count, True


if __name__ == "__main__":
    # Fetch all job descriptions by default
    # Descriptions are cached in meta_descriptions_cache.json to avoid re-fetching
    #
    # Options:
    #   - Fetch all: main(fetch_descriptions=True, description_limit=None)  [default]
    #   - Test with subset: main(fetch_descriptions=True, description_limit=10)
    #   - Skip descriptions: main(fetch_descriptions=False)
    #
    # Cache benefits:
    #   - First run: Fetches ~1171 descriptions (~40-50 minutes)
    #   - Subsequent runs: Only fetches NEW jobs (seconds to minutes)
    #   - To clear cache: delete meta_descriptions_cache.json

    scrape_meta(force=True, fetch_descriptions=True, description_limit=None)
