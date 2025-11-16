# ashby_scraper.py
import asyncio
import json
import aiohttp
import csv
import os
import argparse
from urllib.parse import urlparse
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta
# Use this proxy for all HTTP requests
PROXY_URL = "http://core-residential.evomi.com:1000"
PROXY_AUTH = aiohttp.BasicAuth("kalilbouz0", "KpJTWgxfN9tqIe52xIsD")

# Import processing function
import sys

sys.path.append(str(Path(__file__).parent.parent))
from ashby.process_ashby import process_ashby_companies


def extract_company_slug(url: str) -> str:
    """Extract company slug from Ashby job board URL"""
    parsed = urlparse(url)
    # Extract the path and remove leading slash
    path = parsed.path.lstrip("/")
    return path


async def scrape_ashby_jobs(company_slug: str):
    url = f"https://api.ashbyhq.com/posting-api/job-board/{company_slug}?includeCompensation=true"
    print(f"Fetching fresh data from {url}...")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    companies_dir = os.path.join(script_dir, "companies")

    if not os.path.exists(companies_dir):
        os.makedirs(companies_dir)

    file_path = os.path.join(companies_dir, f"{company_slug}.json")

    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        async with session.get(url, proxy=PROXY_URL, proxy_auth=PROXY_AUTH) as response:
        # async with session.get(url) as response:
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

    return file_path, len(data.get("jobs", []))


async def scrape_all_ashby_jobs(force: bool = False):
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "companies.csv")
    last_run_path = os.path.join(script_dir, "last_run.txt")
    last_run = None
    if os.path.exists(last_run_path):
        with open(last_run_path, "r") as f:
            last_run = datetime.strptime(f.read(), "%Y-%m-%d")
    with open(last_run_path, "w") as f:
        f.write(datetime.now().strftime("%Y-%m-%d"))


    if last_run is not None and last_run > datetime.now() - timedelta(days=1) and not force:
        # Check if all companies have been scraped in the last 24 hours
        # Number of companies in the csv should be equal to the number of companies in the companies folder
        with open(csv_path, "r") as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            companies = list(reader)
        if len(companies) == len(os.listdir(os.path.join(script_dir, "companies"))):
            print("All companies have been scraped in the last 24 hours, skipping...")
            return script_dir   
        else:
            print("Not all companies have been scraped in the last 24 hours, scraping...")
            count = 0
            successful_companies = 0
            failed_companies = 0
            for company in companies:
                company_slug = extract_company_slug(company[0])
                if not os.path.exists(os.path.join(script_dir, "companies", f"{company_slug}.json")):
                    print(f"Company {company_slug} has not been scraped in the last 24 hours, scraping...")
                    result, num_jobs = await scrape_ashby_jobs(company_slug)
                    if result is not None:
                        count += num_jobs
                        successful_companies += 1
                    else:
                        failed_companies += 1
                        print(f"Failed to scrape {company_slug}")
            print(f"Done! Scraped {count} total jobs from {successful_companies} companies ({failed_companies} failed)")
            return script_dir

    count = 0
    successful_companies = 0
    failed_companies = 0

    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip header row

        for row in reader:
            if not row:  # Skip empty rows
                continue

            company_url = row[0]
            company_slug = extract_company_slug(company_url)

            print(f"Processing company: {company_slug}")
            result, num_jobs = await scrape_ashby_jobs(company_slug)

            if result is not None:
                count += num_jobs
                successful_companies += 1
                print(f"Successfully scraped {num_jobs} jobs from {company_slug}")
            else:
                failed_companies += 1
                print(f"Failed to scrape {company_slug}")

    print(
        f"Done! Scraped {count} total jobs from {successful_companies} companies ({failed_companies} failed)"
    )
    return script_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape Ashby job boards and optionally process to database"
    )
    parser.add_argument(
        "company_slug",
        nargs="?",
        help="Company slug to scrape (optional, scrapes all if not provided)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-scrape all companies",
    )

    args = parser.parse_args()

    if args.company_slug:
        result, num_jobs = asyncio.run(scrape_ashby_jobs(args.company_slug))
        script_dir = os.path.dirname(os.path.abspath(__file__))
    else:
        script_dir = asyncio.run(scrape_all_ashby_jobs(args.force))
