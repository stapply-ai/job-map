import requests
import json
import time
from datetime import datetime, timezone
from pathlib import Path

# -----------------------
# Config
# -----------------------

COMPANY = "nvidia"
BASE_URL = "https://nvidia.eightfold.ai"
SEARCH_ENDPOINT = f"{BASE_URL}/api/pcsx/search"
DETAILS_ENDPOINT = f"{BASE_URL}/api/pcsx/position_details"

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = SCRIPT_DIR / "nvidia.json"

PAGE_SIZE = 10  # Use API's supported page size
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
RATE_LIMIT_DELAY = 0.5  # seconds between requests
DETAILS_DELAY = 0.5  # seconds between detail requests
WRITE_BATCH_SIZE = 5  # Write output every N pages

HEADERS = {"accept": "application/json, text/plain, */*", "user-agent": "Mozilla/5.0"}

# -----------------------
# Helpers
# -----------------------


def load_output():
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

    return {"last_scraped": None, "company": COMPANY, "count": 0, "jobs": []}


def write_output(data):
    data["last_scraped"] = datetime.now(timezone.utc).isoformat()
    data["count"] = len(data["jobs"])

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def ts_to_date(ts):
    if not ts:
        return None
    return datetime.fromtimestamp(ts, timezone.utc).date().isoformat()


# -----------------------
# Fetching
# -----------------------


def fetch_page(start):
    """Fetch a page of jobs with retry logic"""
    params = {
        "domain": "nvidia.com",
        "query": "",
        "location": "",
        "start": start,
        "sort_by": "timestamp",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start_time = time.time()
            r = requests.get(
                SEARCH_ENDPOINT,
                params=params,
                headers=HEADERS,
                timeout=15,
            )
            elapsed = time.time() - start_time
            r.raise_for_status()

            # Log slow requests
            if elapsed > 5:
                print(f"  ⏱️  Slow request: {elapsed:.1f}s for start={start}")

            return r.json().get("data", {}).get("positions", [])

        except requests.exceptions.Timeout:
            print(f"⏱️  Timeout on attempt {attempt}/{MAX_RETRIES}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY * attempt)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Rate limit
                wait_time = RETRY_DELAY * (2**attempt)
                print(f"⏸️  Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
            elif e.response.status_code >= 500:  # Server error
                print(
                    f"🔴 Server error {e.response.status_code} on attempt {attempt}/{MAX_RETRIES}"
                )
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(RETRY_DELAY * attempt)
            else:
                raise

        except requests.exceptions.RequestException as e:
            print(f"⚠️  Request error on attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_DELAY * attempt)

    return []


def fetch_job_details(position_id):
    """Fetch detailed job information including description"""
    params = {
        "position_id": position_id,
        "domain": "nvidia.com",
        "hl": "en",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(
                DETAILS_ENDPOINT,
                params=params,
                headers=HEADERS,
                timeout=15,
            )
            r.raise_for_status()
            data = r.json().get("data", {})
            return {
                "description": data.get("jobDescription"),
                "standardized_locations": data.get("standardizedLocations", []),
            }

        except requests.exceptions.Timeout:
            print(
                f"⏱️  Timeout fetching details for {position_id} (attempt {attempt}/{MAX_RETRIES})"
            )
            if attempt == MAX_RETRIES:
                return None
            time.sleep(RETRY_DELAY * attempt)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait_time = RETRY_DELAY * (2**attempt)
                print(f"⏸️  Rate limited on details, waiting {wait_time}s...")
                time.sleep(wait_time)
            elif e.response.status_code >= 500:
                print(
                    f"🔴 Server error {e.response.status_code} on details (attempt {attempt}/{MAX_RETRIES})"
                )
                if attempt == MAX_RETRIES:
                    return None
                time.sleep(RETRY_DELAY * attempt)
            else:
                return None

        except requests.exceptions.RequestException as e:
            print(
                f"⚠️  Request error fetching details for {position_id} (attempt {attempt}/{MAX_RETRIES}): {e}"
            )
            if attempt == MAX_RETRIES:
                return None
            time.sleep(RETRY_DELAY * attempt)

    return None


# -----------------------
# Main
# -----------------------


def main():
    print("▶ Starting scrape (caching enabled for performance)")

    # Load existing data for description caching
    existing_data = load_output()
    description_cache = {}

    if existing_data.get("jobs"):
        print(f"📦 Loading {len(existing_data['jobs'])} cached job descriptions...")
        for job in existing_data["jobs"]:
            job_id = job.get("eightfold_id")
            if job_id and job.get("description"):
                description_cache[job_id] = {
                    "description": job["description"],
                    "standardized_locations": job.get("standardized_locations"),
                }
        print(f"✓ Cached {len(description_cache)} job descriptions")

    # Step 1: Fetch all job listings (fast, just metadata)
    print("\n🔍 Step 1: Fetching all job listings (metadata only)...")
    all_positions = []
    start = 0
    page_count = 0
    consecutive_empty = 0
    MAX_CONSECUTIVE_EMPTY = 3

    while True:
        try:
            positions = fetch_page(start)

            if not positions:
                consecutive_empty += 1
                if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                    print("✓ No more job listings")
                    break
                start += PAGE_SIZE
                time.sleep(RATE_LIMIT_DELAY)
                continue

            consecutive_empty = 0
            all_positions.extend(positions)
            page_count += 1

            print(f"  📄 Page {page_count}: {len(positions)} jobs | Total: {len(all_positions)}")

            start += PAGE_SIZE
            time.sleep(RATE_LIMIT_DELAY)

        except KeyboardInterrupt:
            print("\n⏹️  Interrupted by user")
            break
        except Exception as e:
            print(f"❌ Error fetching page: {e}")
            break

    print(f"\n✓ Fetched {len(all_positions)} total job listings")

    # Step 2: Build jobs with cached or new descriptions
    print("\n🔍 Step 2: Fetching descriptions (only for new/changed jobs)...")

    output = {"last_scraped": None, "company": COMPANY, "count": 0, "jobs": []}
    seen_ids = set()
    cached_count = 0
    fetched_count = 0

    for idx, p in enumerate(all_positions, 1):
        job_id = p["id"]

        if job_id in seen_ids:
            continue
        seen_ids.add(job_id)

        try:
            job_data = {
                "eightfold_id": job_id,
                "jr_id": p.get("displayJobId"),
                "title": p.get("name"),
                "locations": p.get("locations", []),
                "department": p.get("department"),
                "work_location_option": p.get("workLocationOption"),
                "posted_at": ts_to_date(p.get("postedTs")),
                "created_at": ts_to_date(p.get("creationTs")),
                "url": BASE_URL + p.get("positionUrl"),
            }

            # Check cache first
            if job_id in description_cache:
                cached_desc = description_cache[job_id]
                job_data["description"] = cached_desc["description"]
                if cached_desc.get("standardized_locations"):
                    job_data["standardized_locations"] = cached_desc["standardized_locations"]
                cached_count += 1

                if idx % 100 == 0:
                    print(f"  Progress: {idx}/{len(all_positions)} | Cached: {cached_count} | Fetched: {fetched_count}")
            else:
                # Fetch description for new job
                details = fetch_job_details(job_id)
                if details:
                    job_data["description"] = details.get("description")
                    if details.get("standardized_locations"):
                        job_data["standardized_locations"] = details["standardized_locations"]
                else:
                    job_data["description"] = None

                fetched_count += 1
                print(f"  [{idx}/{len(all_positions)}] 🆕 Fetched: {p.get('name', 'Unknown')[:60]}")
                time.sleep(DETAILS_DELAY)

            output["jobs"].append(job_data)

        except KeyboardInterrupt:
            print("\n⏹️  Interrupted by user")
            break
        except Exception as e:
            print(f"⚠️  Error processing job {job_id}: {e}")
            continue

    # Save final output
    write_output(output)

    # Final summary
    print("\n📊 Summary:")
    print(f"  Total jobs: {len(output['jobs'])}")
    print(f"  Cached descriptions: {cached_count} (fast)")
    print(f"  New descriptions fetched: {fetched_count} (slow)")
    print(f"  Cache hit rate: {cached_count/(cached_count+fetched_count)*100:.1f}%" if (cached_count+fetched_count) > 0 else "  Cache hit rate: N/A")


def scrape_nvidia_jobs(force: bool = False) -> tuple[str, int, bool]:
    """
    Scrape NVIDIA jobs and store them in nvidia/nvidia.json.
    Returns (json_path, num_jobs, was_scraped).
    """
    if not force and OUTPUT_FILE.exists():
        try:
            existing = load_output()
            last_scraped_str = existing.get("last_scraped")
            jobs = existing.get("jobs", [])

            if last_scraped_str:
                try:
                    last_scraped = datetime.fromisoformat(last_scraped_str)
                    hours_elapsed = (
                        datetime.now(timezone.utc) - last_scraped
                    ).total_seconds() / 3600
                    if hours_elapsed < 12:
                        print(
                            f"Existing NVIDIA data scraped {hours_elapsed:.1f} hours ago. Reusing."
                        )
                        return str(OUTPUT_FILE), len(jobs), False
                except Exception:
                    pass
        except (OSError, json.JSONDecodeError):
            pass

    main()

    try:
        data = load_output()
        jobs = data.get("jobs", [])
        return str(OUTPUT_FILE), len(jobs), True
    except Exception:
        return str(OUTPUT_FILE), 0, True


if __name__ == "__main__":
    main()
