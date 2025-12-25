"""Export consolidated Workday job data to CSV using shared utilities."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Dict, List

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from export_utils import generate_job_id, write_jobs_csv  # noqa: E402


def _format_location(job: Dict[str, object]) -> str:
    for key in ("location_full", "location_summary"):
        value = job.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    subtitle = job.get("subtitle")
    if isinstance(subtitle, list):
        subtitle_parts = [part.strip() for part in subtitle if isinstance(part, str) and part.strip()]
        if subtitle_parts:
            return "; ".join(subtitle_parts)
    return ""


def _extract_ats_id(job: Dict[str, object]) -> str:
    for key in ("job_id", "job_requisition_id", "job_id_hint"):
        value = job.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def main() -> None:
    workday_dir = Path(__file__).resolve().parent
    companies_dir = workday_dir / "companies"
    jobs_csv_path = workday_dir / "jobs.csv"

    job_rows: List[Dict[str, str]] = []

    if not companies_dir.exists() or not companies_dir.is_dir():
        print(f"Companies directory does not exist: {companies_dir}")
    else:
        for json_file in sorted(companies_dir.glob("*.json")):
            company_slug = json_file.stem
            try:
                with json_file.open("r", encoding="utf-8") as fh:
                    data = json.load(fh)
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON file: {json_file.name}")
                continue

            if not isinstance(data, dict):
                continue

            company_name = (
                data.get("company")
                or data.get("name")
                or company_slug
            )

            jobs = data.get("jobs", [])
            if not isinstance(jobs, list):
                continue

            for job_data in jobs:
                if not isinstance(job_data, dict):
                    continue

                url = (job_data.get("apply_url") or job_data.get("job_url") or "").strip()
                title = (job_data.get("job_title") or job_data.get("title") or "").strip()
                location = _format_location(job_data)
                ats_id = _extract_ats_id(job_data)

                job_rows.append(
                    {
                        "url": url,
                        "title": title,
                        "location": location,
                        "company": str(company_name),
                        "ats_id": ats_id,
                        "id": generate_job_id("workday", url, ats_id),
                    }
                )

    print(f"Processed {len(job_rows)} total jobs")
    diff_path = write_jobs_csv(jobs_csv_path, job_rows)
    if diff_path:
        print(f"Created diff file: {diff_path.name}")


if __name__ == "__main__":
    main()
