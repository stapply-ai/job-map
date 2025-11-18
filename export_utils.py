from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from uuid import NAMESPACE_URL, uuid5


FIELDNAMES = ["url", "title", "location", "company", "ats_id", "id"]


def generate_job_id(platform: str, url: str | None, ats_id: str | None) -> str:
    """
    Generate a deterministic UUID for a job using the platform, ats_id, and URL.
    Falls back gracefully when values are missing so the ID stays stable
    between runs.
    """
    platform = platform or "unknown"
    url = url or ""
    ats_id = ats_id or ""
    unique_key = f"{platform}:{ats_id}:{url}"
    return str(uuid5(NAMESPACE_URL, unique_key))


def _build_row_key(row: Dict[str, str]) -> Tuple[str, str]:
    """
    Build a comparable key for a job row. Prefer ats_id, fall back to url so
    we can still diff older CSVs that predate the ats_id column.
    """
    ats_id = (row.get("ats_id") or "").strip()
    url = (row.get("url") or "").strip()
    return ats_id, url


def _rows_equal(row_a: Dict[str, str], row_b: Dict[str, str]) -> bool:
    for field in FIELDNAMES:
        if (row_a.get(field) or "").strip() != (row_b.get(field) or "").strip():
            return False
    return True


def _compute_diff(
    previous_rows: Iterable[Dict[str, str]], new_rows: Iterable[Dict[str, str]]
) -> List[Dict[str, str]]:
    previous_index = {_build_row_key(row): row for row in previous_rows}
    diff_rows: List[Dict[str, str]] = []

    for row in new_rows:
        key = _build_row_key(row)
        previous = previous_index.get(key)
        if previous is None or not _rows_equal(previous, row):
            diff_rows.append(row)

    return diff_rows


def write_jobs_csv(jobs_csv_path: Path, rows: List[Dict[str, str]]) -> Path | None:
    """
    Write the jobs CSV and, when a previous file exists, emit a diff file that
    highlights new or updated rows.

    Returns the diff file path if one was created.
    """
    jobs_csv_path = Path(jobs_csv_path)
    jobs_csv_path.parent.mkdir(parents=True, exist_ok=True)

    diff_path: Path | None = None
    previous_rows: List[Dict[str, str]] = []

    if jobs_csv_path.exists():
        with open(jobs_csv_path, "r", encoding="utf-8", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            previous_rows = list(reader)

        diff_rows = _compute_diff(previous_rows, rows)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        diff_filename = f"{jobs_csv_path.stem}_diff_{timestamp}{jobs_csv_path.suffix}"
        diff_path = jobs_csv_path.with_name(diff_filename)

        with open(diff_path, "w", encoding="utf-8", newline="") as diff_file:
            writer = csv.DictWriter(diff_file, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(diff_rows)

    with open(jobs_csv_path, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    return diff_path


