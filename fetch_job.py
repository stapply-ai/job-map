#!/usr/bin/env python3
"""
Fetch job descriptions from JSON files and save to database.

Reads jobs from ai.csv and new_ai.csv, fetches job descriptions from JSON files
(or re-scrapes if missing), and efficiently syncs with the database.

Usage:
    python fetch_job.py
    python fetch_job.py --dry-run
    python fetch_job.py --ai-csv ai-06-12-2025.csv --new-ai-csv new_ai.csv
"""

import csv
import html
import json
import logging
import os
import re
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, unquote, quote
from datetime import datetime, timezone
from uuid import UUID, uuid4
import asyncio

# Add parent directory to path for imports
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Import database models and utilities
from ashby.process_ashby import (
    CompanyTable,
    get_or_create_company,
    Base,
)
from sqlalchemy import (
    create_engine,
    text,
    Column,
    String,
    Boolean,
    DateTime,
    Float,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.attributes import flag_modified
from dotenv import load_dotenv


# Define MapJobTable for map_jobs table (minimal schema for fetch_job.py)
class MapJobTable(Base):
    __tablename__ = "map_jobs"
    id = Column(PGUUID(as_uuid=True), primary_key=True)
    url = Column(String, nullable=False)
    title = Column(String, nullable=False)
    location = Column(String)
    company = Column(String, nullable=False)
    description = Column(Text)
    employment_type = Column(String)
    ats_type = Column(String)
    company_id = Column(PGUUID(as_uuid=True))
    posted_at = Column(DateTime)
    lat = Column(Float)
    lon = Column(Float)
    salary_min = Column(Float)
    salary_max = Column(Float)
    salary_currency = Column(String)
    salary_period = Column(String)
    remote = Column(Boolean)
    source = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=text("now()"))


def check_job_exists_by_url(session, url: str, ats_type: str, company_name: str):
    """Check if job exists in DB by URL, ats_type, and company_name."""
    existing = (
        session.query(MapJobTable)
        .filter_by(url=url, ats_type=ats_type, company=company_name)
        .first()
    )
    return existing


# Import utilities from ai.py
from ai import (
    ATS_CONFIGS,
    normalize_company_name,
    find_companies_by_name,
    extract_slug_from_url,
    fetch_fresh_data,
    normalize_datetime_to_utc_iso,
    posted_at_from_source,
    get_coordinates,
)

# Import models for parsing JSON
from models.ashby import AshbyApiResponse, AshbyJob
from models.gh import GreenhouseJob
from models.lever import LeverJob
from models.workable import WorkableJob
from pydantic import ValidationError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def parse_posted_at(posted_at_str: Optional[str]) -> Optional[datetime]:
    """Parse posted_at string to datetime object."""
    if not posted_at_str:
        return None
    try:
        # Handle ISO format with Z
        if posted_at_str.endswith("Z"):
            posted_at_str = posted_at_str[:-1] + "+00:00"
        dt = datetime.fromisoformat(posted_at_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception as e:
        logger.debug(f"Error parsing posted_at '{posted_at_str}': {e}")
        return None


def parse_float(value: Optional[str]) -> Optional[float]:
    """Parse string to float, returning None if invalid."""
    if not value or value.strip() == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_bool(value: Optional[str]) -> Optional[bool]:
    """Parse string to boolean."""
    if not value or value.strip() == "":
        return None
    val_lower = value.strip().lower()
    if val_lower in ("true", "1", "yes"):
        return True
    if val_lower in ("false", "0", "no"):
        return False
    return None


def extract_description_from_ashby(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Ashby JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        parsed = AshbyApiResponse(**data)

        for job in parsed.jobs:
            job_url = job.job_url or job.apply_url or ""
            if (job.id == ats_id) or (job_url == url):
                return job.description_plain or job.description_html

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def process_greenhouse_content(content: Optional[str]) -> Optional[str]:
    """
    Process Greenhouse job content: decode HTML entities but keep HTML tags.
    Returns processed HTML description or None if content is empty.
    """
    if not content:
        return None

    # Decode HTML entities (e.g., &lt; -> <, &quot; -> ", &nbsp; -> non-breaking space, etc.)
    decoded = html.unescape(content)

    # Replace non-breaking spaces with regular spaces
    decoded = decoded.replace("\xa0", " ")

    # Clean up extra whitespace but keep HTML structure
    decoded = re.sub(r"\n\s*\n", "\n\n", decoded)
    decoded = decoded.strip()

    return decoded if decoded else None


def extract_description_from_greenhouse(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Greenhouse JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", [])
        if not isinstance(job_list, list):
            return None

        for job_data in job_list:
            try:
                job = GreenhouseJob(**job_data)
            except ValidationError:
                continue

            job_url = job.absolute_url or ""
            job_id_str = str(job.id) if job.id is not None else ""

            if (job_id_str == ats_id) or (job_url == url):
                return process_greenhouse_content(job.content)

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def combine_lever_description(job_data: dict) -> Optional[str]:
    """
    Combine Lever job description from descriptionPlain, additionalPlain, and lists.
    Returns combined description or None if no description found.
    """
    parts = []

    # Add descriptionPlain
    if job_data.get("descriptionPlain"):
        parts.append(job_data["descriptionPlain"].strip())

    # Add lists (RESPONSIBILITIES, QUALIFICATIONS, etc.)
    # Check if lists exists and is a non-empty list
    lists = job_data.get("lists")
    if lists is not None and isinstance(lists, list) and len(lists) > 0:
        for list_item in lists:
            if isinstance(list_item, dict):
                header = list_item.get("text", "").strip()
                content = list_item.get("content", "")
                if content:
                    # Keep raw HTML content as-is to preserve original list formatting/bullets
                    if header:
                        parts.append(f"\n\n{header}\n{content.strip()}")
                    else:
                        parts.append(f"\n\n{content.strip()}")

    # Add additionalPlain (often contains salary info)
    if job_data.get("additionalPlain"):
        parts.append(job_data["additionalPlain"].strip())

    if not parts:
        return None

    return "\n\n".join(parts)


def extract_description_from_lever(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Lever JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = (
            data
            if isinstance(data, list)
            else data.get("postings", []) or data.get("jobs", [])
        )
        if not isinstance(job_list, list):
            return None

        for job_data in job_list:
            try:
                job = LeverJob(**job_data)
            except ValidationError:
                continue

            job_url = job.hostedUrl or job.applyUrl or ""
            job_id_str = job.id or ""

            if (job_id_str == ats_id) or (job_url == url):
                # Use helper function to combine all Lever description fields
                return combine_lever_description(job_data)

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_workable(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Workable JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = (
            data
            if isinstance(data, list)
            else data.get("results", []) or data.get("jobs", [])
        )
        if not isinstance(job_list, list):
            return None

        for job_data in job_list:
            try:
                job = WorkableJob(**job_data)
            except ValidationError:
                continue

            job_url = job.url or job.application_url or ""
            job_id_str = str(job.shortcode or job.code or "")

            if (job_id_str == ats_id) or (job_url == url):
                # Workable jobs might have description in raw data
                return job_data.get("description") or job_data.get("descriptionPlain")

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_rippling(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Rippling JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) or data.get("results", []) or []
        if not isinstance(job_list, list):
            return None

        for job_data in job_list:
            job_url = job_data.get("url") or job_data.get("applyUrl") or ""
            job_id_str = str(job_data.get("id", ""))

            if (job_id_str == ats_id) or (job_url == url):
                # Try different description fields
                description = job_data.get("description")
                if isinstance(description, dict):
                    return description.get("role") or description.get("company")
                return description or job_data.get("descriptionPlain")

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_google(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Google JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Google JSON can be either a dict with "jobs" key or directly an array
        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return None

        # Normalize inputs for comparison
        url = url.strip() if url else ""
        ats_id = ats_id.strip() if ats_id else ""

        for job_data in job_list:
            job_url = (job_data.get("url") or "").strip()

            # For Google, ats_id is the URL, so match by URL
            if job_url == url or job_url == ats_id:
                description = job_data.get("description")
                if description and isinstance(description, str):
                    # Google descriptions are plain text
                    return description.strip()

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_microsoft(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Microsoft JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return None

        # Normalize inputs for comparison
        url = url.strip() if url else ""
        ats_id = ats_id.strip() if ats_id else ""

        for job_data in job_list:
            job_url = (job_data.get("url") or "").strip()
            eightfold_id = job_data.get("eightfold_id")
            # Handle both string and integer eightfold_id
            job_id_str = str(eightfold_id) if eightfold_id is not None else ""

            # Match by URL or eightfold_id
            if (
                job_url == url
                or job_url == ats_id
                or job_id_str == ats_id
                or (eightfold_id is not None and str(eightfold_id) == ats_id)
            ):
                description = job_data.get("description")
                if description and isinstance(description, str) and description.strip():
                    # Microsoft descriptions are in HTML format
                    return description.strip()
                return None

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_nvidia(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from NVIDIA JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return None

        # Normalize inputs for comparison
        url = url.strip() if url else ""
        ats_id = ats_id.strip() if ats_id else ""

        for job_data in job_list:
            job_url = (job_data.get("url") or "").strip()
            eightfold_id = job_data.get("eightfold_id")
            job_id_str = str(eightfold_id) if eightfold_id is not None else ""

            # Match by URL or eightfold_id
            if (
                job_url == url
                or job_url == ats_id
                or job_id_str == ats_id
                or (eightfold_id is not None and str(eightfold_id) == ats_id)
            ):
                description = job_data.get("description")
                if description and isinstance(description, str) and description.strip():
                    # NVIDIA descriptions are in HTML format
                    return description.strip()
                return None

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_amazon(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Amazon JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return None

        # Normalize inputs for comparison
        url = url.strip() if url else ""
        ats_id = ats_id.strip() if ats_id else ""

        for job_data in job_list:
            # Amazon uses urlNextStep as the URL field
            job_url = (job_data.get("urlNextStep") or "").strip()

            # Match by URL
            if job_url == url or job_url == ats_id:
                description = job_data.get("description")
                if description and isinstance(description, str) and description.strip():
                    # Amazon descriptions are plain text with HTML tags
                    return description.strip()
                return None

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_meta(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Meta JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return None

        # Normalize inputs for comparison
        url = url.strip() if url else ""
        ats_id = ats_id.strip() if ats_id else ""

        for job_data in job_list:
            job_url = (job_data.get("url") or "").strip()
            job_id = str(job_data.get("id") or "").strip()

            # Match by URL or id
            if job_url == url or job_url == ats_id or job_id == ats_id:
                description = job_data.get("description")
                if description and isinstance(description, str) and description.strip():
                    # Meta descriptions are HTML/text format
                    return description.strip()
                return None

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_tiktok(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from TikTok JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return None

        # Normalize inputs for comparison
        url = url.strip() if url else ""
        ats_id = ats_id.strip() if ats_id else ""

        for job_data in job_list:
            job_url = (job_data.get("url") or "").strip()

            # Match by URL
            if job_url == url or job_url == ats_id:
                description = job_data.get("description")
                if description and isinstance(description, str) and description.strip():
                    # TikTok descriptions are HTML/text format
                    return description.strip()
                return None

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_tesla(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Tesla JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return None

        # Normalize inputs for comparison
        url = url.strip() if url else ""
        ats_id = ats_id.strip() if ats_id else ""

        for job_data in job_list:
            job_url = (job_data.get("url") or "").strip()

            # Match by URL
            if job_url == url or job_url == ats_id:
                description = job_data.get("description")
                if description and isinstance(description, str) and description.strip():
                    # Tesla descriptions are HTML/text format
                    return description.strip()
                return None

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_cursor(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Cursor JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return None

        # Normalize inputs for comparison
        url = url.strip() if url else ""
        ats_id = ats_id.strip() if ats_id else ""

        for job_data in job_list:
            job_url = (job_data.get("url") or "").strip()

            # Match by URL
            if job_url == url or job_url == ats_id:
                description = job_data.get("description")
                if description and isinstance(description, str) and description.strip():
                    # Cursor descriptions are text format
                    return description.strip()
                return None

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_apple(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Apple JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Apple JSON can be either a dict with "jobs" key or directly an array
        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return None

        # Normalize inputs for comparison
        url = url.strip() if url else ""
        ats_id = ats_id.strip() if ats_id else ""

        for job_data in job_list:
            job_url = (job_data.get("url") or "").strip()
            job_position_id = str(
                job_data.get("positionId") or job_data.get("id") or ""
            ).strip()

            # Match by URL or positionId
            if job_url == url or job_position_id == ats_id:
                description = job_data.get("description")
                if description and isinstance(description, str) and description.strip():
                    return description.strip()
                return None

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_uber(
    json_file: Path, ats_id: str, url: str
) -> Optional[str]:
    """Extract job description from Uber JSON file."""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Uber JSON can be either a dict with "jobs" key or directly an array
        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return None

        # Normalize inputs for comparison
        url = url.strip() if url else ""
        ats_id = ats_id.strip() if ats_id else ""

        for job_data in job_list:
            job_url = (job_data.get("url") or "").strip()
            job_id = str(job_data.get("id") or "").strip()

            # Match by URL or id
            if job_url == url or job_id == ats_id:
                description = job_data.get("description")
                if description and isinstance(description, str) and description.strip():
                    return description.strip()
                return None

    except Exception as e:
        logger.debug(f"Error extracting description from {json_file}: {e}")

    return None


def extract_description_from_json(
    json_file: Path, ats_id: str, ats_type: str, url: str
) -> Optional[str]:
    """Extract job description from JSON file based on ATS type."""
    if ats_type == "ashby":
        return extract_description_from_ashby(json_file, ats_id, url)
    elif ats_type == "greenhouse":
        return extract_description_from_greenhouse(json_file, ats_id, url)
    elif ats_type == "lever":
        return extract_description_from_lever(json_file, ats_id, url)
    elif ats_type == "workable":
        return extract_description_from_workable(json_file, ats_id, url)
    elif ats_type == "rippling":
        return extract_description_from_rippling(json_file, ats_id, url)
    elif ats_type == "google":
        return extract_description_from_google(json_file, ats_id, url)
    elif ats_type == "microsoft":
        return extract_description_from_microsoft(json_file, ats_id, url)
    elif ats_type == "nvidia":
        return extract_description_from_nvidia(json_file, ats_id, url)
    elif ats_type == "amazon":
        return extract_description_from_amazon(json_file, ats_id, url)
    elif ats_type == "meta":
        return extract_description_from_meta(json_file, ats_id, url)
    elif ats_type == "tiktok":
        return extract_description_from_tiktok(json_file, ats_id, url)
    elif ats_type == "tesla":
        return extract_description_from_tesla(json_file, ats_id, url)
    elif ats_type == "cursor":
        return extract_description_from_cursor(json_file, ats_id, url)
    elif ats_type == "apple":
        return extract_description_from_apple(json_file, ats_id, url)
    elif ats_type == "uber":
        return extract_description_from_uber(json_file, ats_id, url)
    else:
        logger.warning(f"Unknown ATS type: {ats_type}")
        return None


def find_company_json_file(company_name: str, ats_type: str) -> Optional[Path]:
    """Find the JSON file for a company given its name and ATS type."""
    # Handle special sources that have a single JSON file instead of companies_dir
    special_source_files = {
        "google": ROOT_DIR / "google" / "google.json",
        "microsoft": ROOT_DIR / "microsoft" / "microsoft.json",
        "nvidia": ROOT_DIR / "nvidia" / "nvidia.json",
        "amazon": ROOT_DIR / "amazon" / "amazon.json",
        "meta": ROOT_DIR / "meta" / "meta.json",
        "tiktok": ROOT_DIR / "tiktok" / "tiktok.json",
        "cursor": ROOT_DIR / "cursor" / "cursor.json",
        "tesla": ROOT_DIR / "tesla" / "tesla.json",
        "apple": ROOT_DIR / "apple" / "apple.json",
        "uber": ROOT_DIR / "uber" / "uber.json",
    }

    if ats_type in special_source_files:
        json_file = special_source_files[ats_type]
        if json_file.exists():
            return json_file
        return None

    if ats_type not in ATS_CONFIGS:
        return None

    config = ATS_CONFIGS[ats_type]

    # For other ATS types, use companies_dir
    companies_dir = config.get("companies_dir")
    if not companies_dir:
        return None

    # Find company by name to get slug
    matches = find_companies_by_name(company_name, ats_type)
    if not matches:
        return None

    # Use the first match
    _, slug, _ = matches[0]

    # Try to find JSON file
    json_file = companies_dir / f"{slug}.json"
    if json_file.exists():
        return json_file

    # Try URL-encoded version
    encoded_slug = quote(slug, safe="")
    json_file = companies_dir / f"{encoded_slug}.json"
    if json_file.exists():
        return json_file

    return None


def fetch_job_description(
    company_name: str, ats_type: str, ats_id: str, url: str, dry_run: bool = False
) -> Optional[str]:
    """Fetch job description from JSON file or re-scrape if needed."""
    # Special sources that don't support re-scraping via fetch_fresh_data
    special_sources = {
        "google",
        "microsoft",
        "nvidia",
        "amazon",
        "meta",
        "tiktok",
        "cursor",
        "tesla",
        "apple",
        "uber",
    }

    # First, try to find existing JSON file
    json_file = find_company_json_file(company_name, ats_type)
    if json_file and json_file.exists():
        description = extract_description_from_json(json_file, ats_id, ats_type, url)
        if description:
            return description
        # For special sources, descriptions may not be in JSON (e.g., Microsoft, NVIDIA)
        # Don't try to re-scrape as they don't support it
        if ats_type in special_sources:
            logger.debug(
                f"Description not found in JSON for {company_name} ({ats_type}) - special source, skipping re-scrape"
            )
            return None

    # JSON file not found or description missing, try to re-scrape
    # Skip re-scraping for special sources
    if not dry_run and ats_type not in special_sources:
        logger.info(
            f"Description not found in JSON, attempting to re-scrape for {company_name} ({ats_type})"
        )
        matches = find_companies_by_name(company_name, ats_type)
        if matches:
            _, slug, _ = matches[0]
            was_scraped = fetch_fresh_data(company_name, ats_type, slug)
            if was_scraped:
                # Try to extract again after scraping
                json_file = find_company_json_file(company_name, ats_type)
                if json_file and json_file.exists():
                    description = extract_description_from_json(
                        json_file, ats_id, ats_type, url
                    )
                    if description:
                        return description

    return None


def read_csv_jobs(csv_path: Path) -> List[Dict]:
    """Read jobs from CSV file."""
    jobs = []
    if not csv_path.exists():
        logger.warning(f"CSV file not found: {csv_path}")
        return jobs

    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                jobs.append(dict(row))
    except Exception as e:
        logger.error(f"Error reading CSV {csv_path}: {e}")
        return []

    logger.info(f"Read {len(jobs)} jobs from {csv_path}")
    return jobs


def delete_removed_jobs(
    session, rm_ai_csv_path: Path, stats: Dict, dry_run: bool = False
):
    """
    Delete jobs from database based on rm_ai.csv file.
    Reads jobs from rm_ai.csv and deletes them from database by (url, ats_type, company).
    """
    if not rm_ai_csv_path.exists():
        logger.info(f"rm_ai.csv not found at {rm_ai_csv_path}, no jobs to delete")
        return

    logger.info(f"Reading removed jobs from {rm_ai_csv_path}")
    removed_jobs = []
    try:
        with open(rm_ai_csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("url", "").strip()
                ats_type = row.get("ats_type", "").strip()
                company = row.get("company", "").strip()
                if url and ats_type and company:
                    removed_jobs.append(row)
    except Exception as e:
        logger.error(f"Error reading rm_ai.csv: {e}")
        return

    if not removed_jobs:
        logger.info("No removed jobs found in rm_ai.csv")
        return

    logger.info(f"Found {len(removed_jobs)} jobs to delete from rm_ai.csv")

    if dry_run:
        logger.info(f"[DRY RUN] Would delete {len(removed_jobs)} jobs")
        for job in removed_jobs[:5]:  # Show first 5 as examples
            logger.info(
                f"  [DRY RUN] Would delete: {job.get('title')} ({job.get('url')})"
            )
        if len(removed_jobs) > 5:
            logger.info(f"  ... and {len(removed_jobs) - 5} more")
        stats["deleted"] = len(removed_jobs)
        return

    # Collect job IDs to delete for bulk deletion
    job_ids_to_delete = []
    jobs_not_found = []

    for job in removed_jobs:
        url = job.get("url", "").strip()
        ats_type = job.get("ats_type", "").strip()
        company = job.get("company", "").strip()

        if not url or not ats_type or not company:
            logger.warning(
                f"Skipping job with missing fields: {job.get('title', 'Unknown')}"
            )
            continue

        try:
            # Find job in database
            existing_job = check_job_exists_by_url(session, url, ats_type, company)
            if existing_job:
                job_ids_to_delete.append(existing_job.id)
                logger.debug(f"Marked for deletion: {existing_job.title} ({url})")
            else:
                jobs_not_found.append((url, ats_type, company))
                logger.debug(
                    f"Job not found in database: {url} ({ats_type}, {company})"
                )
        except Exception as e:
            logger.error(f"Error checking job {url}: {e}")
            continue

    # Perform bulk delete
    if job_ids_to_delete:
        logger.info(f"Deleting {len(job_ids_to_delete)} jobs from database")
        try:
            deleted_count = (
                session.query(MapJobTable)
                .filter(MapJobTable.id.in_(job_ids_to_delete))
                .delete(synchronize_session=False)
            )
            session.commit()
            stats["deleted"] = deleted_count
            logger.info(f"Successfully deleted {deleted_count} jobs from database")
        except Exception as e:
            logger.error(f"Error during bulk delete: {e}", exc_info=True)
            session.rollback()
            stats["errors"] += len(job_ids_to_delete)

    if jobs_not_found:
        logger.warning(
            f"{len(jobs_not_found)} jobs from rm_ai.csv were not found in database (may have been already deleted)"
        )


def convert_csv_job_to_db_job(
    csv_job: Dict, company_id: UUID, description: Optional[str]
) -> Dict:
    """Convert CSV job dict to database job dict."""
    posted_at = parse_posted_at(csv_job.get("posted_at"))
    lat = parse_float(csv_job.get("lat"))
    lon = parse_float(csv_job.get("lon"))
    salary_min = parse_float(csv_job.get("salary_min"))
    salary_max = parse_float(csv_job.get("salary_max"))
    is_remote = parse_bool(csv_job.get("is_remote"))

    db_job = {
        "url": csv_job.get("url", "").strip(),
        "title": csv_job.get("title", "").strip(),
        "location": csv_job.get("location", "").strip() or None,
        "company": csv_job.get("company", "").strip(),
        "description": description,
        "employment_type": csv_job.get("employment_type", "").strip() or None,
        "ats_type": csv_job.get("ats_type", "").strip() or None,
        "company_id": company_id,
        "posted_at": posted_at,
        "lat": lat,
        "lon": lon,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "salary_currency": csv_job.get("salary_currency", "").strip() or None,
        "salary_period": csv_job.get("salary_period", "").strip() or None,
        "remote": is_remote,
        "source": csv_job.get("ats_type", "").strip() or None,
        "is_active": True,
    }

    return db_job


def process_jobs(
    database_url: str,
    ai_csv_path: Optional[Path] = None,
    new_ai_csv_path: Optional[Path] = None,
    rm_ai_csv_path: Optional[Path] = None,
    dry_run: bool = False,
    init: bool = False,
):
    """Process jobs from CSV files and sync with database."""
    # Fix postgres:// to postgresql:// for SQLAlchemy compatibility
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)

    logger.info("Starting job description fetching process")
    logger.info(f"Database URL: {database_url[:20]}...")
    logger.info(f"Dry run: {dry_run}")
    logger.info(f"Init mode: {init}")

    # Initialize database connection
    logger.info("Initializing database connection...")
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    logger.info("Database connection established")

    # Read CSV files based on mode
    all_csv_jobs = []

    if init:
        # Init mode: read from most recent ai-*.csv file
        if ai_csv_path is None:
            from glob import glob

            pattern = str(ROOT_DIR / "ai-*.csv")
            csv_files = [Path(f) for f in glob(pattern) if Path(f).is_file()]
            if csv_files:
                ai_csv_path = max(csv_files, key=lambda f: f.stat().st_mtime)
                logger.info(f"[INIT] Using most recent CSV: {ai_csv_path.name}")
            else:
                ai_csv_path = ROOT_DIR / "map" / "public" / "ai.csv"

        if ai_csv_path.exists():
            all_csv_jobs.extend(read_csv_jobs(ai_csv_path))
            logger.info(
                f"[INIT] Loaded {len(all_csv_jobs)} jobs from {ai_csv_path.name}"
            )
        else:
            logger.error(f"[INIT] CSV file not found: {ai_csv_path}")
    else:
        # Normal mode: read only from new_ai.csv
        if new_ai_csv_path is None:
            new_ai_csv_path = ROOT_DIR / "new_ai.csv"

        if new_ai_csv_path.exists():
            all_csv_jobs.extend(read_csv_jobs(new_ai_csv_path))
            if dry_run:
                logger.info(
                    f"[DRY RUN] Loaded {len(all_csv_jobs)} jobs from {new_ai_csv_path.name}"
                )
            else:
                logger.info(
                    f"Loaded {len(all_csv_jobs)} jobs from {new_ai_csv_path.name}"
                )
        else:
            logger.warning(f"new_ai.csv not found at {new_ai_csv_path}")

    if not all_csv_jobs:
        logger.error("No jobs found in CSV files")
        return

    logger.info(f"Total jobs to process: {len(all_csv_jobs)}")

    # Deduplicate by URL (jobs can appear in both CSVs)
    jobs_by_url: Dict[str, Dict] = {}
    for job in all_csv_jobs:
        url = job.get("url", "").strip()
        if url:
            # Keep the first occurrence
            if url not in jobs_by_url:
                jobs_by_url[url] = job

    unique_jobs = list(jobs_by_url.values())
    logger.info(f"Unique jobs after deduplication: {len(unique_jobs)}")

    # Statistics
    stats = {
        "total": len(unique_jobs),
        "new": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0,
        "deleted": 0,
    }

    # Process jobs in batches by company/ATS type for efficiency
    jobs_by_company_ats: Dict[Tuple[str, str], List[Dict]] = {}
    for job in unique_jobs:
        company = job.get("company", "").strip()
        ats_type = job.get("ats_type", "").strip()
        if company and ats_type:
            key = (company, ats_type)
            if key not in jobs_by_company_ats:
                jobs_by_company_ats[key] = []
            jobs_by_company_ats[key].append(job)

    logger.info(f"Processing {len(jobs_by_company_ats)} company/ATS combinations")

    # Process each company/ATS combination
    for (company_name, ats_type), jobs in jobs_by_company_ats.items():
        logger.info(f"Processing {len(jobs)} jobs for {company_name} ({ats_type})...")

        # Get or create company (trim company name)
        company_name = company_name.strip()
        if not dry_run:
            company_id = get_or_create_company(session, company_name)
        else:
            company_id = None

        # Process each job
        for csv_job in jobs:
            url = csv_job.get("url", "").strip()
            ats_id = csv_job.get("ats_id", "").strip()
            title = csv_job.get("title", "").strip()

            if not url:
                logger.warning(f"Skipping job with no URL: {title}")
                stats["skipped"] += 1
                continue

            try:
                # Check if job exists in database
                existing_job = None
                if not dry_run:
                    existing_job = check_job_exists_by_url(
                        session, url, ats_type, company_name
                    )

                # Fetch description from JSON file
                description = None
                if existing_job and existing_job.description:
                    # Use existing description if available
                    description = existing_job.description
                    logger.debug(f"Using existing description for {url}")
                else:
                    # Fetch description if not in DB
                    logger.info(f"Fetching description for {title} ({url})")
                    description = fetch_job_description(
                        company_name, ats_type, ats_id, url, dry_run
                    )
                    if description:
                        logger.debug(
                            f"Successfully fetched description ({len(description)} chars)"
                        )
                    else:
                        logger.warning(f"Could not fetch description for {url}")

                if dry_run:
                    logger.info(
                        f"[DRY RUN] Would process job: {title} - Description: {'Found' if description else 'Missing'}"
                    )
                    stats["skipped"] += 1
                    continue

                # Convert CSV job to database format
                db_job_dict = convert_csv_job_to_db_job(
                    csv_job, company_id, description
                )

                if existing_job:
                    # Update existing job
                    for key, value in db_job_dict.items():
                        if key != "url":
                            setattr(existing_job, key, value)
                    existing_job.is_active = True
                    stats["updated"] += 1
                    logger.debug(f"Updated job: {title}")
                else:
                    # Create new job
                    # Generate ID from ats_id if available (for Ashby), otherwise generate UUID
                    job_id = None
                    if ats_type == "ashby" and ats_id:
                        try:
                            job_id = UUID(ats_id)
                        except ValueError:
                            pass

                    # If no ID was generated, create a new UUID
                    if job_id is None:
                        job_id = uuid4()

                    new_job = MapJobTable(id=job_id, **db_job_dict)
                    session.add(new_job)
                    stats["new"] += 1
                    logger.debug(f"Created new job: {title}")

            except Exception as e:
                logger.error(f"Error processing job {url}: {e}", exc_info=True)
                stats["errors"] += 1

        # Commit batch for this company
        if not dry_run:
            try:
                # Flush to ensure all changes are sent to database
                session.flush()
                session.commit()
                logger.info(f"Committed batch for {company_name}")
            except Exception as e:
                logger.error(f"Error committing batch for {company_name}: {e}")
                session.rollback()
                stats["errors"] += len(jobs)

    # Handle deleted jobs by reading from rm_ai.csv
    # Skip deletions in init mode (only for incremental updates)
    if not init:
        if not dry_run:
            if rm_ai_csv_path is None:
                rm_ai_csv_path = ROOT_DIR / "rm_ai.csv"
            delete_removed_jobs(session, rm_ai_csv_path, stats, dry_run)
        else:
            if rm_ai_csv_path is None:
                rm_ai_csv_path = ROOT_DIR / "rm_ai.csv"
            if rm_ai_csv_path.exists():
                logger.info(
                    f"[DRY RUN] Skipping deletion of jobs from rm_ai.csv ({rm_ai_csv_path.name})"
                )
    else:
        logger.info("[INIT] Skipping deletion processing (init mode)")

    # Print statistics
    logger.info("=" * 60)
    logger.info("Processing complete!")
    logger.info(f"Total jobs processed: {stats['total']}")
    logger.info(f"New jobs: {stats['new']}")
    logger.info(f"Updated jobs: {stats['updated']}")
    logger.info(f"Skipped jobs: {stats['skipped']}")
    logger.info(f"Deleted jobs: {stats['deleted']}")
    logger.info(f"Errors: {stats['errors']}")
    logger.info("=" * 60)

    session.close()


def main():
    parser = argparse.ArgumentParser(
        description="Fetch job descriptions from JSON files and save to database"
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL"),
        help="PostgreSQL connection string (default: from DATABASE_URL env var)",
    )
    parser.add_argument(
        "--ai-csv",
        type=Path,
        default=None,
        help="Path to ai.csv file (default: most recent ai-*.csv or map/public/ai.csv)",
    )
    parser.add_argument(
        "--new-ai-csv",
        type=Path,
        default=None,
        help="Path to new_ai.csv file (default: ./new_ai.csv)",
    )
    parser.add_argument(
        "--rm-ai-csv",
        type=Path,
        default=None,
        help="Path to rm_ai.csv file (default: ./rm_ai.csv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode - don't modify database",
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="Init mode - read from most recent ai-*.csv file for first-time setup",
    )

    args = parser.parse_args()

    if not args.database_url:
        logger.error("DATABASE_URL not provided via --database-url or .env file")
        sys.exit(1)

    # Convert relative paths to absolute
    ai_csv_path = args.ai_csv
    if ai_csv_path and not ai_csv_path.is_absolute():
        ai_csv_path = ROOT_DIR / ai_csv_path

    new_ai_csv_path = args.new_ai_csv
    if new_ai_csv_path and not new_ai_csv_path.is_absolute():
        new_ai_csv_path = ROOT_DIR / new_ai_csv_path

    rm_ai_csv_path = args.rm_ai_csv
    if rm_ai_csv_path and not rm_ai_csv_path.is_absolute():
        rm_ai_csv_path = ROOT_DIR / rm_ai_csv_path

    process_jobs(
        database_url=args.database_url,
        ai_csv_path=ai_csv_path,
        new_ai_csv_path=new_ai_csv_path,
        rm_ai_csv_path=rm_ai_csv_path,
        dry_run=args.dry_run,
        init=args.init,
    )


if __name__ == "__main__":
    main()
