import argparse
import asyncio
import csv
import json
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urljoin, urlparse, urlencode, urlunparse

import aiohttp
from aiohttp import ClientResponseError, ClientSession, ClientTimeout
from bs4 import BeautifulSoup

MAX_RETRIES = 3
BASE_RETRY_DELAY = 2
MIN_SCRAPE_DELAY = 1
MAX_SCRAPE_DELAY = 3
MAX_PAGES = 200
MAX_CONCURRENT_DETAIL_REQUESTS = 8
REQUEST_TIMEOUT = ClientTimeout(total=45)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/143.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}
CACHE_HOURS = 24


@dataclass(slots=True)
class CompanyRow:
    name: str
    url: str
    slug: str


def extract_company_slug(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.replace(".", "_") or "workday"
    path = parsed.path.strip("/").replace("/", "_")
    query = parsed.query.replace("=", "-").replace("&", "-")
    suffix = "_".join(filter(None, [path or None, query or None]))
    return f"{host}_{suffix}" if suffix else host


def load_company_data(file_path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(file_path):
        return None
    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, json.JSONDecodeError):
        return None


def should_scrape(
    company_data: Optional[Dict[str, Any]], force: bool, min_hours: int = CACHE_HOURS
) -> tuple[bool, Optional[float]]:
    if force or not company_data:
        return True, None
    ts = company_data.get("last_request") or company_data.get("last_scraped")
    if not ts:
        return True, None
    try:
        last = datetime.fromisoformat(ts)
        hours = (datetime.now() - last).total_seconds() / 3600
        return hours >= min_hours, hours
    except (ValueError, TypeError):
        return True, None


def save_company_data(file_path: str, payload: Dict[str, Any]) -> None:
    now = datetime.now().isoformat()
    payload["last_scraped"] = now
    payload["last_request"] = now
    with open(file_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def set_query_param(url: str, **params: Any) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update({k: str(v) for k, v in params.items() if v is not None})
    return urlunparse(parsed._replace(query=urlencode(query)))


def clean_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = " ".join(value.split())
    return text or None


def extract_job_id_hint(href: str) -> Optional[str]:
    path = urlparse(href).path.rstrip("/")
    if not path:
        return None
    tail = path.split("/")[-1]
    if "_" in tail:
        tail = tail.split("_")[-1]
    tail = tail.split("?")[0]
    return tail or None


class WorkdayScraper:
    def __init__(self, force: bool = False) -> None:
        self.force = force
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_DETAIL_REQUESTS)

    async def _fetch(self, session: ClientSession, url: str) -> Tuple[Optional[str], Optional[int]]:
        attempt = 1
        while attempt <= MAX_RETRIES:
            try:
                async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
                    status = resp.status
                    if status == 500:
                        print(f"Received 500 for {url}; not retrying")
                        return None, status
                    if status == 404:
                        print(f"Received 404 for {url}")
                        return None, status
                    resp.raise_for_status()
                    return await resp.text(), status
            except (ClientResponseError, aiohttp.ClientError) as err:
                status = getattr(err, "status", None)
                if status == 500:
                    print(f"Error fetching {url} ({err}). Not retrying due to 500 status")
                    return None, status
                if attempt == MAX_RETRIES:
                    print(f"Failed to fetch {url}: {err}")
                    return None, status
                backoff = BASE_RETRY_DELAY * attempt + random.uniform(0, 1)
                print(f"Error fetching {url} ({err}). Retrying in {backoff:.1f}s...")
                await asyncio.sleep(backoff)
                attempt += 1
        return None, None

    def _parse_index_page(self, html: str, base_url: str, page_number: int) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        results: List[Dict[str, Any]] = []
        for anchor in soup.select("a[data-automation-id='jobTitle']"):
            href_value = anchor.get("href")
            if not href_value:
                continue
            job_url = urljoin(base_url, str(href_value))
            if not job_url:
                continue
            card = anchor.find_parent("li") or anchor.find_parent("article")
            location = None
            posted_on = None
            subtitle: List[str] = []
            if card:
                loc_dd = card.select_one("[data-automation-id='locations'] dd")
                if loc_dd:
                    location = clean_text(loc_dd.get_text(strip=True))
                posted_dd = card.select_one("[data-automation-id='postedOn'] dd")
                if posted_dd:
                    posted_on = clean_text(posted_dd.get_text(strip=True))
                subtitle_items = card.select("ul[data-automation-id='subtitle'] li")
                if subtitle_items:
                    subtitle = []
                    for li in subtitle_items:
                        value = clean_text(li.get_text(strip=True))
                        if value:
                            subtitle.append(value)
            results.append(
                {
                    "title": clean_text(anchor.get_text(strip=True)),
                    "job_url": job_url,
                    "job_id_hint": extract_job_id_hint(job_url),
                    "location_summary": location,
                    "posted_on_summary": posted_on,
                    "subtitle": subtitle,
                    "page": page_number,
                }
            )
        return results

    async def _fetch_job_list(
        self, session: ClientSession, base_url: str
    ) -> Tuple[List[Dict[str, Any]], Optional[int]]:
        all_jobs: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        page = 1
        last_status: Optional[int] = None
        while page <= MAX_PAGES:
            page_url = base_url if page == 1 else set_query_param(base_url, page=page)
            html, status = await self._fetch(session, page_url)
            if status is not None:
                last_status = status
            if not html:
                break
            parsed = self._parse_index_page(html, base_url, page)
            new_jobs = [job for job in parsed if job["job_url"] not in seen_urls]
            if not new_jobs:
                break
            for job in new_jobs:
                seen_urls.add(job["job_url"])
            all_jobs.extend(new_jobs)
            print(f"Found {len(new_jobs)} jobs on page {page} ({base_url})")
            page += 1
        return all_jobs, last_status

    def _parse_detail_page(self, html: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")

        def pick_value(automation_id: str) -> Optional[str]:
            block = soup.select_one(f"[data-automation-id='{automation_id}']")
            if not block:
                return None
            dd = block.find("dd")
            if dd:
                return clean_text(dd.get_text(strip=True))
            return clean_text(block.get_text(strip=True))

        description_block = soup.select_one("div[data-automation-id='jobPostingDescription']")
        description_html = description_block.decode_contents().strip() if description_block else None
        description_text = clean_text(description_block.get_text("\n", strip=True)) if description_block else None
        apply_link = soup.select_one("a[data-automation-id='adventureButton']")

        title_el = soup.select_one("h2[data-automation-id='jobPostingHeader']")

        return {
            "job_title": clean_text(title_el.get_text(strip=True)) if title_el else None,
            "remote_type": pick_value("remoteType"),
            "location_full": pick_value("locations"),
            "time_type": pick_value("time"),
            "posted_on": pick_value("postedOn"),
            "time_left_to_apply": pick_value("timeLeftToApply"),
            "job_requisition_id": pick_value("requisitionId"),
            "job_description_html": description_html,
            "job_description_text": description_text,
            "apply_url": apply_link.get("href") if apply_link else None,
        }

    async def _fetch_job_detail(
        self, session: ClientSession, job: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        async with self.semaphore:
            html, status = await self._fetch(session, job["job_url"])
        if not html:
            status_text = status if status is not None else "unknown"
            print(f"Skipping detail for {job['job_url']} (status {status_text})")
            return None
        detail = self._parse_detail_page(html)
        job_id = detail.get("job_requisition_id") or job.get("job_id_hint")
        merged = {
            **job,
            **detail,
            "job_id": job_id,
        }
        return merged

    async def scrape_company(self, session: ClientSession, company: CompanyRow, companies_dir: str) -> tuple[int, bool]:
        file_path = os.path.join(companies_dir, f"{company.slug}.json")
        existing = load_company_data(file_path)
        should_run, hours = should_scrape(existing, self.force)
        if not should_run:
            cached_jobs = existing.get("job_count", 0) if existing else 0
            hours_text = f"{hours:.1f}h" if hours is not None else "recently"
            print(
                f"Skipping {company.slug} (scraped {hours_text} ago, {cached_jobs} jobs cached)"
            )
            return cached_jobs, False

        print(f"Fetching job index for {company.name} ({company.url})")
        jobs, status_code = await self._fetch_job_list(session, company.url)
        normalized_status = status_code if status_code is not None else 0
        if not jobs:
            print(f"No jobs found for {company.name}; caching empty result")
            payload = {
                "company": company.name,
                "url": company.url,
                "slug": company.slug,
                "job_count": 0,
                "jobs": [],
                "status": normalized_status,
            }
            os.makedirs(companies_dir, exist_ok=True)
            save_company_data(file_path, payload)
            return 0, True

        detail_tasks = [self._fetch_job_detail(session, job) for job in jobs]
        details = [result for result in await asyncio.gather(*detail_tasks) if result]
        print(f"Parsed {len(details)} detailed jobs for {company.name}")

        payload = {
            "company": company.name,
            "url": company.url,
            "slug": company.slug,
            "job_count": len(details),
            "jobs": details,
            "status": normalized_status,
        }
        os.makedirs(companies_dir, exist_ok=True)
        save_company_data(file_path, payload)
        return len(details), True


async def scrape_all_workday_jobs(force: bool = False, company_slug: Optional[str] = None) -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "workday_companies.csv")
    companies_dir = os.path.join(script_dir, "companies")

    companies: List[CompanyRow] = []
    with open(csv_path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            url = row.get("url", "").strip()
            name = row.get("name", "").strip() or "Unknown"
            if not url:
                continue
            slug = extract_company_slug(url)
            companies.append(CompanyRow(name=name, url=url, slug=slug))

    if company_slug:
        companies = [c for c in companies if c.slug == company_slug]
        if not companies:
            print(f"No company with slug '{company_slug}' in CSV")
            return

    connector = aiohttp.TCPConnector(ssl=False)
    total_jobs = 0
    scraped = skipped = failed = 0

    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        scraper = WorkdayScraper(force=force)
        for company in companies:
            print(f"\nProcessing {company.name} ({company.slug})")
            try:
                job_count, ran = await scraper.scrape_company(session, company, companies_dir)
                total_jobs += job_count
                if ran:
                    scraped += 1
                else:
                    skipped += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                print(f"Failed to scrape {company.name}: {exc}")
            finally:
                await asyncio.sleep(random.uniform(MIN_SCRAPE_DELAY, MAX_SCRAPE_DELAY))

    print(
        f"\nDone! {scraped} scraped, {skipped} skipped, {failed} failed. "
        f"Captured {total_jobs} total jobs."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Workday job scraper")
    parser.add_argument("company_slug", nargs="?", help="Optional company slug to scrape")
    parser.add_argument("--force", action="store_true", help="Force re-scrape regardless of cache age")
    args = parser.parse_args()

    start = time.perf_counter()
    try:
        asyncio.run(scrape_all_workday_jobs(force=args.force, company_slug=args.company_slug))
    finally:
        elapsed = time.perf_counter() - start
        print(f"Total runtime: {elapsed:.2f}s")
