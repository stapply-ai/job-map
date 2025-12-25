import json
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

API_URL = "https://www.amazon.jobs/api/jobs/search"
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    # Force plain responses to avoid issues with zstd/other encodings
    "Accept-Encoding": "identity",
    "User-Agent": "Mozilla/5.0",
}

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = SCRIPT_DIR / "amazon.json"

# Retry / robustness settings
MAX_PAGES: Optional[int] = None  # Set to an int to hard-cap pages if needed
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 1.0
BACKOFF_MULTIPLIER = 2.0
MAX_BACKOFF_SECONDS = 30.0

# Parallelization settings
MAX_WORKERS = 10  # Number of parallel page fetches
PAGE_BATCH_SIZE = 50  # Fetch this many pages at a time in parallel

# API limits - many job APIs cap results at 10,000
MAX_OFFSET = 10000  # Don't request beyond this offset


session = requests.Session()


def _sleep_with_jitter(base_delay: float) -> None:
    """Simple jitter so we don't hammer the exact same cadence."""
    jitter = random.uniform(0, base_delay * 0.25)
    time.sleep(base_delay + jitter)


class OutOfBoundsError(Exception):
    """Raised when requesting a page beyond available results."""

    pass


def fetch_page(page: int, size: int = 25) -> Dict[str, Any]:
    """Fetch a single page with retries and exponential backoff."""
    # API uses 'start' (offset) not 'page' number
    # Convert page number to offset: page 0 -> start 0, page 1 -> start 25, etc.
    start_offset = page * size

    payload = {
        "searchType": "JOB_SEARCH",
        "start": start_offset,
        "size": size,
        "filters": [
            {
                "field": "business_category",
                "values": ["aws"],
            }
        ],
    }

    attempt = 0
    delay = INITIAL_BACKOFF_SECONDS

    while True:
        attempt += 1
        try:
            resp = session.post(API_URL, json=payload, headers=HEADERS, timeout=30)

            # 400 errors typically mean we're beyond available results - don't retry
            if resp.status_code == 400:
                raise OutOfBoundsError(
                    f"Page {page} is beyond available results (400 error)"
                )

            # Handle common throttling / transient server errors explicitly
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt > MAX_RETRIES:
                    print(
                        f"Giving up on page {page} after {attempt - 1} retries "
                        f"(status {resp.status_code})."
                    )
                    resp.raise_for_status()
                print(
                    f"Transient error (status {resp.status_code}) on page {page}, "
                    f"attempt {attempt}/{MAX_RETRIES}, backing off {delay:.1f}s..."
                )
                _sleep_with_jitter(min(delay, MAX_BACKOFF_SECONDS))
                delay *= BACKOFF_MULTIPLIER
                continue

            resp.raise_for_status()

            try:
                data = resp.json()
                # Empty response likely means out of bounds
                if not data.get("searchHits"):
                    raise OutOfBoundsError(f"Page {page} returned empty results")
                return data
            except json.JSONDecodeError as e:
                # JSON decode errors on later pages often mean we're out of bounds
                # Only retry a couple times
                if attempt >= 2:
                    raise OutOfBoundsError(
                        f"Page {page} returned invalid JSON (likely out of bounds)"
                    )
                print(
                    f"JSON decode error on page {page} (attempt {attempt}/2): {e}. "
                    f"Backing off {delay:.1f}s..."
                )
                _sleep_with_jitter(min(delay, MAX_BACKOFF_SECONDS))
                delay *= BACKOFF_MULTIPLIER
                continue
        except requests.RequestException as e:
            # Don't retry 400 errors
            if "400 Client Error" in str(e):
                raise OutOfBoundsError(f"Page {page} is beyond available results")

            if attempt > MAX_RETRIES:
                print(
                    f"Network error fetching page {page} after {attempt - 1} retries: {e}"
                )
                raise

            print(
                f"Request error on page {page} (attempt {attempt}/{MAX_RETRIES}): {e}. "
                f"Backing off {delay:.1f}s..."
            )
            _sleep_with_jitter(min(delay, MAX_BACKOFF_SECONDS))
            delay *= BACKOFF_MULTIPLIER


def first(arr):
    """Safely get first element from an array"""
    return arr[0] if isinstance(arr, list) and arr else None


def process_hit(hit: Dict[str, Any]) -> Dict[str, Any]:
    """Extract job data from a search hit."""
    fields = hit.get("fields", {})
    return {
        "title": first(fields.get("title")),
        "location": first(fields.get("location")),
        "description": first(fields.get("description")),
        "shortDescription": first(fields.get("shortDescription")),
        "basicQualifications": first(fields.get("basicQualifications")),
        "preferredQualifications": first(fields.get("preferredQualifications")),
        "createdDate": first(fields.get("createdDate")),
        "updateDate": first(fields.get("updateDate")),
        "urlNextStep": first(fields.get("urlNextStep")),
    }


def collect_all_jobs():
    """Collect all jobs using parallel page fetches for better performance."""
    all_jobs: List[Dict[str, Any]] = []

    # First, fetch page 0 to get total count
    print("Fetching initial page to determine total job count...")
    try:
        first_page_data = fetch_page(0, size=25)
    except Exception as e:
        print(f"Fatal error fetching initial page: {e}")
        return []

    total_jobs = first_page_data.get("found", 0)
    print(f"Total jobs reported by API: {total_jobs}")

    # Process first page
    hits = first_page_data.get("searchHits", [])
    for hit in hits:
        all_jobs.append(process_hit(hit))
    print(f"Fetched page 0 ({len(hits)} jobs)")

    # Calculate total pages needed
    page_size = 25
    total_pages = (total_jobs + page_size - 1) // page_size  # Ceiling division

    # Apply offset limit (many APIs cap at 10,000 results)
    max_pages_from_offset = MAX_OFFSET // page_size
    if total_pages > max_pages_from_offset:
        print(
            f"API reports {total_pages} pages, but limiting to {max_pages_from_offset} due to offset cap"
        )
        total_pages = max_pages_from_offset

    if MAX_PAGES is not None:
        total_pages = min(total_pages, MAX_PAGES + 1)
        print(f"Limiting to MAX_PAGES={MAX_PAGES}")

    # Fetch remaining pages in parallel batches
    page = 1
    while page < total_pages:
        batch_end = min(page + PAGE_BATCH_SIZE, total_pages)
        batch_pages = list(range(page, batch_end))

        print(
            f"\nFetching pages {page}-{batch_end - 1} in parallel ({len(batch_pages)} pages)..."
        )

        # Fetch batch in parallel
        batch_jobs = []
        batch_failures = 0
        out_of_bounds_count = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_page = {
                executor.submit(fetch_page, p, page_size): p for p in batch_pages
            }

            for future in as_completed(future_to_page):
                page_num = future_to_page[future]
                try:
                    data = future.result()
                    hits = data.get("searchHits", [])

                    for hit in hits:
                        batch_jobs.append(process_hit(hit))

                    # Don't print every single page to reduce spam
                    if page_num % 10 == 0 or len(hits) < page_size:
                        print(f"  ✓ Page {page_num} ({len(hits)} jobs)")

                except OutOfBoundsError:
                    out_of_bounds_count += 1
                    # Only print first few out of bounds errors
                    if out_of_bounds_count <= 3:
                        print(f"  ⚠ Page {page_num} out of bounds, stopping early")

                except Exception as e:
                    batch_failures += 1
                    print(f"  ✗ Error fetching page {page_num}: {e}")

        all_jobs.extend(batch_jobs)

        # Calculate success rate
        success_rate = (len(batch_pages) - batch_failures - out_of_bounds_count) / len(
            batch_pages
        )

        print(f"Batch complete: {len(batch_jobs)} jobs fetched, {len(all_jobs)} total")
        print(
            f"  Success rate: {success_rate * 100:.1f}% ({batch_failures} failures, {out_of_bounds_count} out of bounds)"
        )

        # If we hit out of bounds errors or high failure rate, stop early
        if out_of_bounds_count > len(batch_pages) * 0.5:
            print("\n⚠ Over 50% of batch was out of bounds - stopping pagination")
            break

        if batch_failures > len(batch_pages) * 0.7:
            print("\n⚠ Over 70% of batch failed - stopping pagination")
            break

        page = batch_end

    return all_jobs


def scrape_amazon_jobs(force: bool = False) -> tuple[str, int, bool]:
    """
    Scrape Amazon AWS jobs and store them in amazon/amazon.json.
    Returns (json_path, num_jobs, was_scraped).
    """
    if not force and OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
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
                        f"Existing Amazon data scraped {hours_elapsed:.1f} hours ago. Reusing."
                    )
                except Exception:
                    print("Existing Amazon data found. Reusing without rescraping.")
            else:
                print("Existing Amazon data found. Reusing without rescraping.")
            return str(OUTPUT_FILE), len(jobs), False
        except (OSError, json.JSONDecodeError):
            pass

    jobs = collect_all_jobs()

    wrapped = {
        "last_scraped": datetime.now().isoformat(),
        "name": "Amazon",
        "jobs": jobs,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(wrapped, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(jobs)} Amazon jobs to {OUTPUT_FILE}")
    return str(OUTPUT_FILE), len(jobs), True


if __name__ == "__main__":
    path, count, _ = scrape_amazon_jobs(force=True)
    print(f"Scraped {count} Amazon jobs into {path}")
