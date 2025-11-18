# workable_scraper.py
import asyncio
import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta
from urllib.parse import urlparse

import aiohttp

MAX_RETRIES = 3
BASE_RETRY_DELAY = 2  # seconds
MIN_SCRAPE_DELAY = 1  # seconds
MAX_SCRAPE_DELAY = 3  # seconds


def extract_company_slug(url: str) -> str:
    """Extract company slug from Workable job board URL"""
    parsed = urlparse(url)
    # Extract the path and remove leading slash
    path = parsed.path.lstrip("/")
    return path


def load_checkpoint(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def write_checkpoint(
    path: str,
    *,
    last_run: datetime,
    last_company: str | None,
    force: bool,
    total_jobs: int,
    success: bool,
) -> None:
    payload = {
        "last_run": last_run.isoformat(),
        "last_company": last_company,
        "force": force,
        "total_jobs": total_jobs,
        "success": success,
    }
    with open(path, "w") as f:
        json.dump(payload, f, indent=2)


async def scrape_workable_jobs(company_slug: str):
    url = f"https://apply.workable.com/api/v1/widget/accounts/{company_slug}"
    print(f"Fetching {url}...")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    companies_dir = os.path.join(script_dir, "companies")

    if not os.path.exists(companies_dir):
        os.makedirs(companies_dir)

    file_path = os.path.join(companies_dir, f"{company_slug}.json")
    if os.path.exists(file_path):
        print(f"Company '{company_slug}' already scraped")
        return None, 0

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        attempt = 1
        while attempt <= MAX_RETRIES:
            try:
                async with session.get(url) as response:
                    if response.status == 404:
                        print(f"Company '{company_slug}' not found (404)")
                        return None, 0

                    if response.status != 200:
                        print(f"Error {response.status} for company '{company_slug}'")
                        return None, 0

                    try:
                        data = await response.json()
                    except aiohttp.client_exceptions.ContentTypeError as e:
                        print(f"Failed to parse JSON for company '{company_slug}': {e}")
                        return None, 0

                    with open(file_path, "w") as f:
                        json.dump(data, f)

                    return data, len(data.get("jobs", []))
            except (
                aiohttp.client_exceptions.ClientPayloadError,
                aiohttp.ClientError,
                aiohttp.http_exceptions.HttpProcessingError,
            ) as err:
                if attempt == MAX_RETRIES:
                    print(
                        f"Exceeded retries for '{company_slug}' due to network error: {err}"
                    )
                    return None, 0
                delay = BASE_RETRY_DELAY * attempt + random.uniform(0, 1)
                print(
                    f"Request failed for '{company_slug}' ({err}). Retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)
                attempt += 1


async def scrape_all_workable_jobs(force: bool = False):
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "workable_companies.csv")
    checkpoint_path = os.path.join(script_dir, "checkpoint.json")
    checkpoint = load_checkpoint(checkpoint_path)
    last_run = None
    if "last_run" in checkpoint:
        try:
            last_run = datetime.fromisoformat(checkpoint["last_run"])
        except ValueError:
            last_run = None

    resume_company: str | None = None
    recent_run = last_run is not None and last_run > datetime.now() - timedelta(days=1)
    if recent_run and not force:
        if checkpoint.get("success"):
            print(
                "All companies were scraped in the last 24 hours. Use --force to override."
            )
            return script_dir
        resume_company = checkpoint.get("last_company")
        if resume_company:
            print(f"Resuming scrape starting after '{resume_company}'")
        else:
            print("Resuming scrape from the beginning (no last company recorded)")

    count = 0
    successful_companies = 0
    failed_companies = 0
    last_company = None

    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row
        companies = [row for row in reader if row]

    start_index = 0
    if resume_company:
        for idx, row in enumerate(companies):
            if extract_company_slug(row[0]) == resume_company:
                start_index = idx + 1
                break
        else:
            print(
                f"Resume company '{resume_company}' not found in CSV. Starting from beginning."
            )
            start_index = 0

    for row in companies[start_index:]:
        company_url = row[0]
        company_slug = extract_company_slug(company_url)
        last_company = company_slug

        print(f"Processing company: {company_slug}")
        data, num_jobs = await scrape_workable_jobs(company_slug)

        if data is not None:
            count += num_jobs
            successful_companies += 1
            print(f"Successfully scraped {num_jobs} jobs from {company_slug}")
        else:
            failed_companies += 1
            print(f"Failed to scrape {company_slug}")
        write_checkpoint(
            checkpoint_path,
            last_run=datetime.now(),
            last_company=last_company,
            force=force,
            total_jobs=count,
            success=False,
        )
        # Random delay between scrapes
        await asyncio.sleep(random.uniform(MIN_SCRAPE_DELAY, MAX_SCRAPE_DELAY))

    print(
        f"Done! Scraped {count} total jobs from {successful_companies} companies ({failed_companies} failed)"
    )
    write_checkpoint(
        checkpoint_path,
        last_run=datetime.now(),
        last_company=last_company,
        force=force,
        total_jobs=count,
        success=failed_companies == 0,
    )
    return script_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Workable job scraper")
    parser.add_argument(
        "--force", action="store_true", help="Force re-scrape all companies"
    )
    parser.add_argument(
        "company_slug",
        nargs="?",
        help="Company slug to scrape (optional, scrapes all if not provided)",
    )
    args = parser.parse_args()

    if args.company_slug:
        asyncio.run(scrape_workable_jobs(args.company_slug))
    else:
        asyncio.run(scrape_all_workable_jobs(args.force))
