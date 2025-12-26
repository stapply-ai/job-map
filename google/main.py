import time
from datetime import datetime
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlsplit, urlunsplit
import html2text

BASE_RESULTS = "https://www.google.com/about/careers/applications/jobs/results"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; JobAggregator/1.0)"}

# Parallelization settings
MAX_WORKERS = 10  # Number of parallel job page fetches


def canonicalize(url: str) -> str:
    """
    Strip query params (hl, _gl, etc.) and fragments so URLs look like:
    https://www.google.com/about/careers/applications/jobs/results/<id>-<slug>
    """
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def fetch_results_page(page_number: int) -> str:
    params = {"hl": "en_US"}
    if page_number > 1:
        params["page"] = page_number
    r = requests.get(BASE_RESULTS, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.text


def extract_job_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", attrs={"aria-label": True, "href": True}):
        aria = a["aria-label"]
        if aria.startswith("Learn more about"):
            href = urljoin(
                "https://www.google.com/about/careers/applications/", a["href"]
            )
            links.append(canonicalize(href))
    # dedupe but keep stable order
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def parse_job_page(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    # Try CSS selector first, then fallback to simpler selectors
    title = None
    title_selector = "div.sPeqm h2"
    title_elem = soup.select_one(title_selector)
    if title_elem:
        title = title_elem.get_text(strip=True)
    else:
        # Fallback to h1
        title_elem = soup.find("h1")
        if title_elem:
            title = title_elem.get_text(strip=True)

    # Location using CSS selector with class names
    location = None
    location_selector = "div.op1BBf span.pwO9Dc.vo5qdf span"
    location_elem = soup.select_one(location_selector)
    if location_elem:
        location = location_elem.get_text(strip=True)
    else:
        # Fallback: look for the first "Google | ..."
        text = soup.get_text("\n", strip=True)
        for line in text.splitlines():
            if line.startswith("Google |"):
                location = line.replace("Google |", "").strip()
                break

    # Description from specific divs: KwJkGe, aG5W3, BDNOWe
    description_parts = []
    description_selectors = ["div.KwJkGe", "div.aG5W3", "div.BDNOWe"]

    for selector in description_selectors:
        elem = soup.select_one(selector)
        if elem:
            md = html2text.html2text(str(elem)).strip()
            if md:
                description_parts.append(md)

    description = "\n\n".join(description_parts) if description_parts else None

    data = {
        "url": url,
        "title": title,
        "location": location,
        "description": description,
    }
    return data


def fetch_job_with_retry(link: str, max_retries: int = 3) -> dict | None:
    """Fetch a job page with retry logic."""
    for attempt in range(max_retries):
        try:
            job = parse_job_page(link)
            return job
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"✗ Failed after {max_retries} attempts: {link} - {e}")
                return None
            time.sleep(0.5 * (attempt + 1))  # Exponential backoff
    return None


def scrape(sleep_s=0.5):
    """Scrape Google jobs with parallelized job page fetching."""
    # First, collect all job links from all pages
    all_links = []
    seen = set()
    page_number = 1

    print("Collecting job links from results pages...")
    while True:
        html = fetch_results_page(page_number)
        links = extract_job_links(html)
        print(f"Page {page_number}: {len(links)} links")

        if not links:
            break

        # Add new links
        new_links = 0
        for link in links:
            if link not in seen:
                seen.add(link)
                all_links.append(link)
                new_links += 1

        print(f"  Added {new_links} new links, {len(all_links)} total")
        page_number += 1
        time.sleep(sleep_s)

    print(f"\nTotal unique job links: {len(all_links)}")
    print(f"Fetching job details in parallel (max {MAX_WORKERS} workers)...\n")

    # Fetch all job pages in parallel
    jobs = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_link = {
            executor.submit(fetch_job_with_retry, link): link for link in all_links
        }

        completed = 0
        for future in as_completed(future_to_link):
            link = future_to_link[future]
            completed += 1

            try:
                job = future.result()
                if job:
                    jobs.append(job)
                    print(f"[{completed}/{len(all_links)}] ✓ {job['title']}")
                else:
                    print(f"[{completed}/{len(all_links)}] ✗ Failed to fetch {link}")
            except Exception as e:
                print(f"[{completed}/{len(all_links)}] ✗ Error: {link} - {e}")

    return jobs


def scrape_google_jobs(force: bool = False, sleep_s: float = 0.5):
    """
    Scrape Google jobs and store them in google/google.json.
    Returns (json_path, num_jobs, was_scraped).
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(script_dir, "google.json")

    if not force and os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            if isinstance(existing, dict):
                jobs = existing.get("jobs", [])
                last_scraped_str = existing.get("last_scraped")
            else:
                jobs = existing
                last_scraped_str = None
            
            if last_scraped_str:
                try:
                    last_scraped = datetime.fromisoformat(last_scraped_str)
                    hours_elapsed = (
                        datetime.now() - last_scraped
                    ).total_seconds() / 3600
                    print(
                        f"Existing Google data scraped {hours_elapsed:.1f} hours ago. Reusing."
                    )
                except Exception:
                    print("Existing Google data found. Reusing without rescraping.")
            else:
                print("Existing Google data found. Reusing without rescraping.")
            return json_path, len(jobs), False
        except (OSError, json.JSONDecodeError):
            pass

    data = scrape(sleep_s=sleep_s)
    print("jobs:", len(data))

    wrapped = {
        "last_scraped": datetime.now().isoformat(),
        "name": "Google",
        "jobs": data,
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(wrapped, f, indent=4)

    return json_path, len(data), True


if __name__ == "__main__":
    scrape_google_jobs(force=True)
