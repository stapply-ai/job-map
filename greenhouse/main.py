# greenhouse_scraper.py
import asyncio
import argparse
import csv
import json
import os
import random
import time
from datetime import datetime
from urllib.parse import urlparse

import aiohttp

MAX_RETRIES = 3
BASE_RETRY_DELAY = 2  # seconds
MIN_SCRAPE_DELAY = 1  # seconds
MAX_SCRAPE_DELAY = 3  # seconds


def extract_company_slug(url: str) -> str:
    """Extract company slug from Greenhouse job board URL"""
    parsed = urlparse(url)
    # Extract the path and remove leading slash
    path = parsed.path.lstrip("/")
    return path


def load_company_data(file_path: str) -> dict | None:
    """Load company data from JSON file"""
    if not os.path.exists(file_path):
        return None
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def should_scrape_company(
    company_data: dict | None, force: bool = False
) -> tuple[bool, float | None]:
    """
    Determine if we should scrape a company based on last_scraped timestamp.
    Returns (should_scrape, hours_since_last_scrape)
    """
    if force:
        return True, None

    if company_data is None:
        return True, None

    last_scraped_str = company_data.get("last_scraped")
    if not last_scraped_str:
        return True, None

    try:
        last_scraped = datetime.fromisoformat(last_scraped_str)
        hours_elapsed = (datetime.now() - last_scraped).total_seconds() / 3600

        # Scrape if more than 12 hours old
        should_scrape = hours_elapsed >= 12
        return should_scrape, hours_elapsed
    except (ValueError, TypeError):
        return True, None


def save_company_data(file_path: str, api_data: dict, company_name: str = None) -> None:
    """Save company data with last_scraped timestamp and company name"""
    api_data["last_scraped"] = datetime.now().isoformat()
    if company_name:
        api_data["name"] = company_name
    with open(file_path, "w") as f:
        json.dump(api_data, f, indent=2)


async def try_fetch_jobs(session, url):
    """Helper to fetch jobs from a given greenhouse API URL, returning (data, error, status)"""
    try:
        async with session.get(url) as response:
            if response.status == 404:
                return None, f"404 Not Found at {url}", 404
            if response.status != 200:
                return None, f"Error {response.status} at {url}", response.status
            try:
                data = await response.json()
            except aiohttp.client_exceptions.ContentTypeError as e:
                return None, f"Failed to parse JSON: {e}", response.status
            return data, None, response.status
    except (
        aiohttp.client_exceptions.ClientPayloadError,
        aiohttp.ClientError,
        aiohttp.http_exceptions.HttpProcessingError,
    ) as err:
        return None, f"Network exception: {err}", None


async def scrape_greenhouse_jobs(
    company_slug: str, force: bool = False, company_name: str = None
):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    companies_dir = os.path.join(script_dir, "companies")

    if not os.path.exists(companies_dir):
        os.makedirs(companies_dir)

    file_path = os.path.join(companies_dir, f"{company_slug}.json")

    # Check if we should scrape this company
    company_data = load_company_data(file_path)
    should_scrape, hours_elapsed = should_scrape_company(company_data, force)

    if not should_scrape:
        print(
            f"Scraped {company_slug} {hours_elapsed:.1f} hours ago. I will not scrape again."
        )
        # Return existing data info with skipped flag
        num_jobs = len(company_data.get("jobs", []))
        return company_data, num_jobs, False  # False = not scraped (skipped)

    # Log decision to scrape
    if force:
        print(f"Forcing scrape for '{company_slug}' (force=True).")
    elif hours_elapsed is not None:
        print(
            f"Scraped {company_slug} {hours_elapsed:.1f} hours ago. I will scrape again."
        )
    elif company_data is None:
        print(f"Company '{company_slug}' data file does not exist. I will scrape.")
    elif not company_data.get("last_scraped"):
        print(f"Company '{company_slug}' has no last_scraped field. I will scrape.")
    else:
        print(f"Company '{company_slug}' last_scraped field is invalid. I will scrape.")

    urls = [
        f"https://api.greenhouse.io/v1/boards/{company_slug}/jobs?content=true",
        f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs",
    ]

    print(f"Fetching {urls[0]}...")
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        attempt = 1
        while attempt <= MAX_RETRIES:
            # Try first url, fallback to second if fails (404 or >=400)
            data, error, status = await try_fetch_jobs(session, urls[0])
            if data:
                save_company_data(file_path, data, company_name)
                return data, len(data.get("jobs", [])), True  # True = scraped
            # If 404, try next url
            if status == 404 or (status is not None and status >= 400):
                print(f"Primary endpoint failed ({error}), trying alternate endpoint.")
                data2, error2, status2 = await try_fetch_jobs(session, urls[1])
                if data2:
                    save_company_data(file_path, data2, company_name)
                    return data2, len(data2.get("jobs", [])), True
                # if also failed, report which reason to user
                if status2 == 404:
                    print(f"Company '{company_slug}' not found at both endpoints (404)")
                    return None, 0, False
                elif status2 is not None:
                    print(
                        f"Error {status2} for company '{company_slug}' at both endpoints"
                    )
                    return None, 0, False
                elif error2:
                    print(f"Network error for '{company_slug}': {error2}")
                    if attempt == MAX_RETRIES:
                        print(
                            f"Exceeded retries for '{company_slug}' due to network error: {error2}"
                        )
                        return None, 0, False
                else:
                    print(f"Unknown error for '{company_slug}' at both endpoints.")
                    return None, 0, False
            elif error:
                if attempt == MAX_RETRIES:
                    print(
                        f"Exceeded retries for '{company_slug}' due to network error: {error}"
                    )
                    return None, 0, False
                delay = BASE_RETRY_DELAY * attempt + random.uniform(0, 1)
                print(
                    f"Request failed for '{company_slug}' ({error}). Retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)
                attempt += 1
            else:
                # Should never reach here unless bug
                print(f"Unexpected outcome for '{company_slug}'. Skipping.")
                return None, 0, False
            # Try next attempt if network error
            delay = BASE_RETRY_DELAY * attempt + random.uniform(0, 1)
            await asyncio.sleep(delay)
            attempt += 1


async def scrape_all_greenhouse_jobs(force: bool = False):
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "greenhouse_companies.csv")

    count = 0
    successful_companies = 0
    failed_companies = 0
    skipped_companies = 0

    # Build a mapping from slug to company name
    slug_to_name = {}
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            company_url = row["url"]
            company_name = row["name"]
            company_slug = extract_company_slug(company_url)
            slug_to_name[company_slug] = company_name

    companies = list(slug_to_name.keys())
    print(f"Processing {len(companies)} companies...")

    for company_slug in companies:
        company_name = slug_to_name.get(company_slug)

        print(f"\nProcessing company: {company_slug}")
        data, num_jobs, was_scraped = await scrape_greenhouse_jobs(
            company_slug, force, company_name
        )

        if data is not None:
            count += num_jobs
            if was_scraped:
                successful_companies += 1
                print(f"Successfully scraped {num_jobs} jobs from {company_slug}")
                await asyncio.sleep(random.uniform(MIN_SCRAPE_DELAY, MAX_SCRAPE_DELAY))
            else:
                skipped_companies += 1
        else:
            failed_companies += 1
            print(f"Failed to scrape {company_slug}")

    print(
        f"\nDone! Processed {count} total jobs from {successful_companies} companies "
        f"({skipped_companies} skipped, {failed_companies} failed)"
    )
    return script_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Greenhouse job scraper")
    parser.add_argument(
        "company_slug",
        nargs="?",
        help="Company slug to scrape (optional, scrapes all if not provided)",
    )
    parser.add_argument(
        "--force", action="store_true", help="Force re-scrape all companies"
    )
    args = parser.parse_args()

    start_time = time.perf_counter()
    try:
        if args.company_slug:
            asyncio.run(scrape_greenhouse_jobs(args.company_slug))
        else:
            asyncio.run(scrape_all_greenhouse_jobs(args.force))
    finally:
        elapsed = time.perf_counter() - start_time
        print(f"Total runtime: {elapsed:.2f} seconds")
