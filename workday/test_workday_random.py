"""Integration test that scrapes a random Workday company from the CSV list."""

from __future__ import annotations

import asyncio
import csv
import json
import random
import unittest
from pathlib import Path
from typing import List

import aiohttp

from workday.main import CompanyRow, WorkdayScraper, HEADERS, extract_company_slug

WORKDAY_DIR = Path(__file__).resolve().parents[1] / "workday"
CSV_PATH = WORKDAY_DIR / "workday_companies.csv"
COMPANIES_DIR = WORKDAY_DIR / "companies"


def choose_random_company(rows: List[dict[str, str]]) -> CompanyRow:
    random_row = random.choice(rows)
    url = (random_row.get("url") or "").strip()
    if not url:
        raise AssertionError("Selected row is missing a URL")
    name = (random_row.get("name") or "Unknown").strip() or "Unknown"
    slug = extract_company_slug(url)
    return CompanyRow(name=name, url=url, slug=slug)


def load_company_rows() -> List[dict[str, str]]:
    with CSV_PATH.open("r", encoding="utf-8") as fh:
        rows = [row for row in csv.DictReader(fh) if row.get("url")]
    if not rows:
        raise AssertionError("workday_companies.csv must include at least one row")
    return rows


async def scrape_single_company(company: CompanyRow) -> tuple[int, bool]:
    # Force scraping even if cache exists to ensure the parser runs end-to-end.
    scraper = WorkdayScraper(force=True)
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        job_count, ran = await scraper.scrape_company(session, company, str(COMPANIES_DIR))
    return job_count, ran


class WorkdayRandomCompanyTest(unittest.TestCase):
    def test_random_company_scrape(self) -> None:
        rows = load_company_rows()
        company = choose_random_company(rows)
        job_count, ran = asyncio.run(scrape_single_company(company))

        self.assertTrue(ran, "Expected scraper to run for randomly selected company")
        output_path = COMPANIES_DIR / f"{company.slug}.json"
        self.assertTrue(output_path.exists(), f"Expected output JSON at {output_path}")

        with output_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)

        self.assertEqual(data.get("slug"), company.slug)
        self.assertEqual(data.get("company"), company.name)
        self.assertEqual(data.get("url"), company.url)
        self.assertEqual(data.get("job_count"), job_count)
        self.assertIn("jobs", data)
        self.assertIn("status", data)


if __name__ == "__main__":
    unittest.main()
