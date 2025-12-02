#!/usr/bin/env python3
"""
Backfill a normalized ISO 8601 `posted_at` column into new_ai.csv.

Rules per ATS/source (derived from the underlying JSON data):
- Ashby:      use job["publishedAt"]
- Greenhouse: use job["updated_at"] (fallback to job["first_published"])
- Lever:      use job["createdAt"] (milliseconds since epoch)
- Rippling:   use job["created_on"]
- Workable:   prefer job["published_on"], fallback to job["created_at"]

`posted_at` is stored as an ISO 8601 UTC datetime string, e.g. 2025-03-10T14:32:00Z.
"""

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional


ROOT_DIR = Path(__file__).resolve().parent
NEW_AI_CSV = ROOT_DIR / "new_ai.csv"


def _to_utc_iso(dt: datetime) -> str:
    """Normalize a datetime to UTC and return ISO 8601 string."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    # Use 'Z' suffix for UTC
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_datetime(value: str) -> Optional[str]:
    """Parse an ISO-like string and normalize to UTC ISO, or return None."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        return _to_utc_iso(dt)
    except Exception:
        return None


def _parse_date_to_iso_utc(date_str: str) -> Optional[str]:
    """Parse YYYY-MM-DD to midnight UTC ISO string."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return _to_utc_iso(dt)
    except Exception:
        return None


def build_url_to_posted_at_map() -> Dict[str, str]:
    """
    Build a mapping from job URL to normalized posted_at ISO datetime by scanning
    all ATS JSON data under the project.
    """
    url_to_posted: Dict[str, str] = {}

    def maybe_set(url: Optional[str], iso_ts: Optional[str]):
        if not url or not iso_ts:
            return
        existing = url_to_posted.get(url)
        if not existing:
            url_to_posted[url] = iso_ts
            return
        # Keep the earliest timestamp if there is a conflict
        try:
            if datetime.fromisoformat(iso_ts.replace("Z", "+00:00")) < datetime.fromisoformat(
                existing.replace("Z", "+00:00")
            ):
                url_to_posted[url] = iso_ts
        except Exception:
            # If comparison fails, just keep the existing one
            pass

    # Ashby
    ashby_dir = ROOT_DIR / "ashby" / "companies"
    if ashby_dir.exists():
        for path in ashby_dir.glob("*.json"):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"Warning: failed to read Ashby file {path.name}: {e}", file=sys.stderr)
                continue

            for job in data.get("jobs", []):
                url = job.get("jobUrl") or job.get("applyUrl")
                published = job.get("publishedAt")
                iso_ts = _parse_iso_datetime(published) if published else None
                maybe_set(url, iso_ts)

    # Greenhouse
    gh_dir = ROOT_DIR / "greenhouse" / "companies"
    if gh_dir.exists():
        for path in gh_dir.glob("*.json"):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(
                    f"Warning: failed to read Greenhouse file {path.name}: {e}", file=sys.stderr
                )
                continue

            for job in data.get("jobs", []):
                url = job.get("absolute_url")
                updated = job.get("updated_at") or job.get("first_published")
                iso_ts = _parse_iso_datetime(updated) if updated else None
                maybe_set(url, iso_ts)

    # Lever
    lever_dir = ROOT_DIR / "lever" / "companies"
    if lever_dir.exists():
        for path in lever_dir.glob("*.json"):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"Warning: failed to read Lever file {path.name}: {e}", file=sys.stderr)
                continue

            if isinstance(data, list):
                job_list = data
            else:
                job_list = data.get("postings") or data.get("jobs") or []

            for job in job_list:
                url = job.get("hostedUrl") or job.get("applyUrl")
                created_at = job.get("createdAt")
                iso_ts: Optional[str] = None
                if isinstance(created_at, (int, float)):
                    try:
                        dt = datetime.fromtimestamp(created_at / 1000.0, tz=timezone.utc)
                        iso_ts = _to_utc_iso(dt)
                    except Exception:
                        iso_ts = None
                elif isinstance(created_at, str):
                    iso_ts = _parse_iso_datetime(created_at)
                maybe_set(url, iso_ts)

    # Rippling
    rippling_dir = ROOT_DIR / "rippling" / "companies"
    if rippling_dir.exists():
        for path in rippling_dir.glob("*.json"):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(
                    f"Warning: failed to read Rippling file {path.name}: {e}", file=sys.stderr
                )
                continue

            job_list = data.get("jobs") or data.get("results") or []
            for job in job_list:
                url = job.get("url") or job.get("applyUrl")
                created_on = job.get("created_on")
                iso_ts = _parse_iso_datetime(created_on) if created_on else None
                maybe_set(url, iso_ts)

    # Workable
    workable_dir = ROOT_DIR / "workable" / "companies"
    if workable_dir.exists():
        for path in workable_dir.glob("*.json"):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                print(
                    f"Warning: failed to read Workable file {path.name}: {e}", file=sys.stderr
                )
                continue

            job_list = data if isinstance(data, list) else data.get("jobs") or data.get("results") or []
            for job in job_list:
                url = job.get("url") or job.get("application_url")
                published_on = job.get("published_on")
                created_at = job.get("created_at")

                iso_ts = None
                if published_on:
                    iso_ts = _parse_date_to_iso_utc(published_on)
                if not iso_ts and created_at:
                    iso_ts = _parse_date_to_iso_utc(created_at)

                maybe_set(url, iso_ts)

    print(f"Built posted_at map for {len(url_to_posted)} URLs")
    return url_to_posted


def backfill_new_ai_csv() -> None:
    """Backfill posted_at into new_ai.csv."""
    if not NEW_AI_CSV.exists():
        print(f"❌ {NEW_AI_CSV} does not exist; nothing to backfill")
        return

    url_to_posted = build_url_to_posted_at_map()

    rows = []
    with open(NEW_AI_CSV, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        has_posted_at = "posted_at" in fieldnames
        for row in reader:
            url = row.get("url", "").strip()
            posted_at = url_to_posted.get(url)
            if posted_at:
                row["posted_at"] = posted_at
            else:
                # Leave as-is if already present, otherwise empty
                row.setdefault("posted_at", "")
            rows.append(row)

    if not has_posted_at:
        fieldnames.append("posted_at")

    tmp_path = NEW_AI_CSV.with_suffix(".csv.tmp")
    backup_path = NEW_AI_CSV.with_suffix(".csv.bak")

    with open(tmp_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # Backup original and replace
    NEW_AI_CSV.replace(backup_path)
    tmp_path.replace(NEW_AI_CSV)

    print(
        f"✅ Backfilled posted_at into {NEW_AI_CSV.name}. "
        f"Backup saved as {backup_path.name}."
    )


def main() -> None:
    backfill_new_ai_csv()


if __name__ == "__main__":
    main()


