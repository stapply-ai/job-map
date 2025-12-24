import time
from datetime import datetime
import json
import os

import requests

API_URL = "https://api.lifeattiktok.com/api/v1/public/supplier/search/job/posts"

HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US",
    "content-type": "application/json",
    "website-path": "tiktok",
    "origin": "https://lifeattiktok.com",
    "referer": "https://lifeattiktok.com/",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
}


def _fetch_page(offset: int, limit: int = 12) -> tuple[list[dict], int]:
    payload = {
        "recruitment_id_list": [],
        "job_category_id_list": [],
        "subject_id_list": [],
        "location_code_list": [],
        "keyword": "",
        "limit": limit,
        "offset": offset,
    }

    r = requests.post(API_URL, headers=HEADERS, json=payload, timeout=15)
    r.raise_for_status()
    data = r.json()

    job_list = data.get("data", {}).get("job_post_list", [])
    total = data.get("data", {}).get("count", 0)
    return job_list, total


def scrape_tiktok_jobs(force: bool = False, limit: int = 12, sleep_s: float = 0.3):
    """
    Scrape TikTok jobs and store them in tiktok/tiktok.json.
    Returns (json_path, num_jobs, was_scraped).
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(script_dir, "tiktok.json")

    if not force and os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            if isinstance(existing, dict):
                last_scraped_str = existing.get("last_scraped")
                jobs_out = existing.get("jobs", [])
            else:
                last_scraped_str = None
                jobs_out = existing

            if last_scraped_str:
                try:
                    last_scraped = datetime.fromisoformat(last_scraped_str)
                    hours_elapsed = (
                        datetime.now() - last_scraped
                    ).total_seconds() / 3600
                    if hours_elapsed < 12:
                        print(
                            f"Existing TikTok data scraped {hours_elapsed:.1f} hours ago. Reusing."
                        )
                        return json_path, len(jobs_out), False
                except Exception:
                    pass
        except (OSError, json.JSONDecodeError):
            pass

    jobs_out: list[dict] = []
    offset = 0

    while True:
        job_list, total = _fetch_page(offset, limit=limit)

        if not job_list:
            break

        for job in job_list:
            job_id = job.get("id")
            job_title = job.get("title")

            if not job_id or not job_title:
                continue

            city = job.get("city_info", {}).get("en_name")
            country = (
                job.get("city_info", {})
                .get("parent", {})
                .get("parent", {})
                .get("en_name")
            )

            location = ", ".join([x for x in [city, country] if x]) or None

            description_parts = []

            if job.get("description"):
                description_parts.append(job["description"].strip())

            if job.get("requirement"):
                description_parts.append(job["requirement"].strip())

            job_post_info = job.get("job_post_info", {})
            if any(
                job_post_info.get(k) for k in ["min_salary", "max_salary", "currency"]
            ):
                salary = f"""
Salary information:
- Min: {job_post_info.get("min_salary")}
- Max: {job_post_info.get("max_salary")}
- Currency: {job_post_info.get("currency")}
                """.strip()
                description_parts.append(salary)

            full_description = "\n\n".join(description_parts) or None

            jobs_out.append(
                {
                    "title": job_title,
                    "url": f"https://lifeattiktok.com/search/{job_id}",
                    "location": location,
                    "description": full_description,
                }
            )

        offset += limit
        print(f"Fetched {len(jobs_out)} / {total}")
        time.sleep(sleep_s)

    wrapped = {
        "last_scraped": datetime.now().isoformat(),
        "name": "TikTok",
        "jobs": jobs_out,
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(wrapped, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Done. Total jobs saved: {len(jobs_out)}")
    return json_path, len(jobs_out), True


if __name__ == "__main__":
    scrape_tiktok_jobs(force=True)
