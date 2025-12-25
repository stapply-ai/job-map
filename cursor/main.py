#!/usr/bin/env python3

import json
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from markdownify import markdownify as md

BASE_URL = "https://cursor.com"
CAREERS_URL = f"{BASE_URL}/careers"

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; StapplyMap/1.0)"}

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = SCRIPT_DIR / "cursor.json"


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return r.text


def extract_jobs(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    jobs = []
    seen = set()

    for a in soup.select('a[href^="/careers/"]'):
        href = a.get("href")
        if not href:
            continue

        text = a.get_text(" ", strip=True)

        # only real job cards
        if "Apply" not in text:
            continue

        url = urljoin(BASE_URL, href)
        if url in seen:
            continue
        seen.add(url)

        # Example:
        # "Account Executive GTM · Full-time · SF / NY Apply →"
        cleaned = text.replace("Apply →", "").strip()
        parts = [p.strip() for p in cleaned.split("·")]

        title = parts[0]
        location = parts[-1] if len(parts) >= 3 else None

        jobs.append(
            {
                "url": url,
                "title": title,
                "location": location,
            }
        )

    return jobs


def extract_description_markdown(job_url: str) -> str | None:
    html = fetch_html(job_url)
    soup = BeautifulSoup(html, "html.parser")

    container = soup.select_one(
        "#main section article "
        "div.col-span-full.md\\:col-start-1.md\\:col-end-19."
        "lg\\:col-start-1.lg\\:col-end-17."
        "xl\\:col-start-7.xl\\:col-end-19 > div > div"
    )

    if not container:
        return None

    return md(container.decode_contents(), heading_style="ATX")


def scrape_cursor_jobs(
    force: bool = False, sleep_s: float = 0.4
) -> Tuple[str, int, bool]:
    """
    Scrape Cursor jobs and store them in cursor/cursor.json.
    Returns (json_path, num_jobs, was_scraped).
    """
    if not force and OUTPUT_FILE.exists():
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)

            if isinstance(existing, dict):
                last_scraped_str = existing.get("last_scraped")
                jobs = existing.get("jobs", [])
            else:
                last_scraped_str = None
                jobs = existing

            if last_scraped_str:
                try:
                    last_scraped = datetime.fromisoformat(last_scraped_str)
                    hours_elapsed = (
                        datetime.now() - last_scraped
                    ).total_seconds() / 3600
                    if hours_elapsed < 12:
                        print(
                            f"Existing Cursor data scraped {hours_elapsed:.1f} hours ago. Reusing."
                        )
                        return str(OUTPUT_FILE), len(jobs), False
                except Exception:
                    pass
        except Exception:
            pass

    print("[*] Fetching Cursor careers page")
    careers_html = fetch_html(CAREERS_URL)

    print("[*] Extracting Cursor jobs")
    jobs = extract_jobs(careers_html)
    print(f"[*] Found {len(jobs)} Cursor roles")

    for i, job in enumerate(jobs, 1):
        print(f"    [{i}/{len(jobs)}] {job['title']}")
        try:
            job["description"] = extract_description_markdown(job["url"])
        except Exception as e:
            job["description"] = None
            job["error"] = str(e)

        time.sleep(sleep_s)

    # For compatibility with extract_* helpers in ai.py, wrap as {"jobs": [...]}
    wrapped = {
        "last_scraped": datetime.now().isoformat(),
        "name": "Cursor",
        "jobs": jobs,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(wrapped, f, indent=2, ensure_ascii=False)

    print(f"[✓] Saved {len(jobs)} Cursor jobs to {OUTPUT_FILE}")
    return str(OUTPUT_FILE), len(jobs), True


if __name__ == "__main__":
    scrape_cursor_jobs(force=True)
