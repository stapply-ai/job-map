# workable_scraper.py
import asyncio
import json
import aiohttp
import csv
import time
import os
from urllib.parse import urlparse


def extract_company_slug(url: str) -> str:
    """Extract company slug from Workable job board URL"""
    parsed = urlparse(url)
    # Extract the path and remove leading slash
    path = parsed.path.lstrip("/")
    return path


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


async def scrape_all_workable_jobs():
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "workable_companies.csv")
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
            data, num_jobs = await scrape_workable_jobs(company_slug)

            if data is not None:
                count += num_jobs
                successful_companies += 1
                print(f"Successfully scraped {num_jobs} jobs from {company_slug}")
                time.sleep(1)  # Rate limiting
            else:
                failed_companies += 1
                print(f"Failed to scrape {company_slug}")

    print(
        f"Done! Scraped {count} total jobs from {successful_companies} companies ({failed_companies} failed)"
    )


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        asyncio.run(scrape_all_workable_jobs())
    else:
        asyncio.run(scrape_workable_jobs(sys.argv[1]))
