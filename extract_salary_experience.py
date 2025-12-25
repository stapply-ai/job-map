#!/usr/bin/env python3
"""
Extract salary and experience from job descriptions for jobs missing this data.

Reads the most recent ai-*.csv file and for each job without salary/experience,
extracts this information from the job description using regex patterns.

Usage:
    python extract_salary_experience.py
    python extract_salary_experience.py --csv ai-08-12-2025.csv
"""

import csv
import html
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple
from glob import glob
from datetime import datetime

# Paths
ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR

# Cache for loaded JSON files
_json_cache: Dict[str, Dict] = {}
_company_json_paths: Dict[str, Path] = {}


def find_most_recent_ai_csv() -> Optional[Path]:
    """Find the most recent ai-*.csv file."""
    csv_files = glob(str(DATA_DIR / "ai-*.csv"))
    if not csv_files:
        return None

    # Sort by modification time, most recent first
    csv_files.sort(key=lambda x: Path(x).stat().st_mtime, reverse=True)
    return Path(csv_files[0])


def normalize_company_name(name: str) -> str:
    """Normalize company name for matching (lowercase, strip)."""
    if not name:
        return ""
    return name.lower().strip()


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


def combine_lever_description(job: dict) -> Optional[str]:
    """
    Combine Lever job description from descriptionPlain, additionalPlain, and lists.
    Returns combined description or None if no description found.
    """
    parts = []

    # Add descriptionPlain
    if job.get("descriptionPlain"):
        parts.append(job["descriptionPlain"].strip())

    # Add lists (RESPONSIBILITIES, QUALIFICATIONS, etc.)
    if job.get("lists") and isinstance(job["lists"], list):
        for list_item in job["lists"]:
            if isinstance(list_item, dict):
                header = list_item.get("text", "").strip()
                content = list_item.get("content", "")
                if content:
                    # Strip HTML tags from content
                    content_plain = re.sub(r"<[^>]+>", "", content)
                    content_plain = content_plain.strip()
                    if content_plain:
                        if header:
                            parts.append(f"\n\n{header}\n{content_plain}")
                        else:
                            parts.append(f"\n\n{content_plain}")

    # Add additionalPlain (often contains salary info)
    if job.get("additionalPlain"):
        parts.append(job["additionalPlain"].strip())

    if not parts:
        return None

    return "\n\n".join(parts)


def build_company_json_map():
    """Build a map of company name -> JSON file path (one-time setup)."""
    global _company_json_paths
    if _company_json_paths:
        return

    # Build both exact and normalized mappings
    for companies_dir in DATA_DIR.rglob("companies"):
        if not companies_dir.is_dir():
            continue
        for json_file in companies_dir.glob("*.json"):
            company_name = json_file.stem
            # Store both exact and normalized versions
            _company_json_paths[company_name] = json_file
            _company_json_paths[normalize_company_name(company_name)] = json_file


def get_job_description_fast(
    job_url: str, company: str, title: str, ats_id: str = None, ats_type: str = None
) -> Tuple[Optional[str], float]:
    """
    Try to quickly retrieve job description from JSON files with caching.
    Returns (description if found, None otherwise, time_taken_in_seconds).
    """
    start_time = time.time()

    # Build map if not already built
    build_company_json_map()

    # Try to find JSON file - try exact match first, then normalized
    json_file = _company_json_paths.get(company)
    if not json_file or not json_file.exists():
        normalized_company = normalize_company_name(company)
        json_file = _company_json_paths.get(normalized_company)
        if not json_file or not json_file.exists():
            return None, time.time() - start_time

    # Use normalized company name for cache key
    cache_key = normalize_company_name(company)

    # Load JSON with caching
    if cache_key not in _json_cache:
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                _json_cache[cache_key] = json.load(f)
        except Exception:
            return None, time.time() - start_time

    data = _json_cache[cache_key]
    # Handle both dict (with "jobs" key) and list formats
    if isinstance(data, dict):
        jobs = data.get("jobs", [])
    elif isinstance(data, list):
        jobs = data
    else:
        return None, time.time() - start_time

    if not isinstance(jobs, list):
        return None, time.time() - start_time

    # Search for matching job - try multiple strategies
    for job in jobs:
        # Strategy 1: Match by URL (most reliable)
        job_url_field = (
            job.get("jobUrl")
            or job.get("url")
            or job.get("absolute_url")
            or job.get("hostedUrl")
        )
        if job_url_field and job_url_field == job_url:
            # For Lever jobs, combine descriptionPlain, additionalPlain, and lists
            if ats_type == "lever":
                description = combine_lever_description(job)
                if description:
                    return description, time.time() - start_time
            # For Greenhouse jobs, process content field (decode HTML entities, strip tags)
            elif ats_type == "greenhouse":
                content = job.get("content")
                description = process_greenhouse_content(content)
                if description:
                    return description, time.time() - start_time
            else:
                description = (
                    job.get("descriptionPlain")  # Ashby uses this
                    or job.get("description")
                    or job.get("text")
                    or job.get(
                        "descriptionHtml"
                    )  # Fallback to HTML if plain not available
                )
                if description:
                    # If we got HTML, try to extract plain text (basic)
                    if description.startswith("<") and "descriptionPlain" not in str(
                        job
                    ):
                        # Skip HTML for now, but could add HTML parsing here
                        continue
                    return description.strip(), time.time() - start_time

        # Strategy 2: Match by ID (for Ashby, Lever, and Greenhouse jobs)
        if ats_id and ats_type in ("ashby", "lever", "greenhouse"):
            job_id = job.get("id")
            if job_id and str(job_id) == str(ats_id):
                if ats_type == "lever":
                    description = combine_lever_description(job)
                elif ats_type == "greenhouse":
                    content = job.get("content")
                    description = process_greenhouse_content(content)
                else:
                    description = (
                        job.get("descriptionPlain")
                        or job.get("description")
                        or job.get("text")
                    )
                if description:
                    return description.strip(), time.time() - start_time

        # Strategy 3: Match by title (fallback)
        job_title = job.get("title", "")
        if job_title and job_title.strip().lower() == title.strip().lower():
            if ats_type == "lever":
                description = combine_lever_description(job)
            elif ats_type == "greenhouse":
                content = job.get("content")
                description = process_greenhouse_content(content)
            else:
                description = (
                    job.get("descriptionPlain")
                    or job.get("description")
                    or job.get("text")
                )
            if description:
                return description.strip(), time.time() - start_time

    return None, time.time() - start_time


def extract_salary_from_description(
    description: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract salary information from job description using regex.
    Returns (salary_string, matched_text) if found, (None, None) otherwise.
    Filters out false positives like company revenue (billions, millions in wrong context).
    """
    if not description:
        return None, None

    # Normalize description (remove HTML tags if any, decode HTML entities, lowercase for matching)
    desc_clean = re.sub(r"<[^>]+>", "", description)
    # Decode HTML entities like &mdash; and &ndash; to their unicode equivalents
    desc_clean = html.unescape(desc_clean)

    # Check for false positive indicators in context (company revenue, statistics, etc.)
    # Simplified - only check for obvious false positives like billions/millions in wrong context
    # Note: context is lowercased, so patterns should match lowercase
    false_positive_indicators = [
        r"\b(billion|billions|million|millions)\s+.*?\$",  # Only flag billions, not millions
        r"\b(paid|pay|pays|revenue|revenues|raised|valued|valuation)\s+\d+.*?\$",  # "paid $X" but not "pay range" or "Annual Salary"
        r"\$\s*\d+(?:,\d+)*(?:[km])?\s+in\s+revenue",  # "$500k in revenue", "$500,000 in revenue" (case-insensitive via context_lower)
        r"\$\s*\d+(?:,\d+)*(?:[km])?\s+revenue",  # "$500k revenue", "$500,000 revenue"
        r"\$\s*\d+(?:,\d+)*(?:[km])?\s+arr\b",  # "$500k ARR", "$750K ARR", "$500,000 ARR" (case-insensitive via context_lower)
        r"\$\s*\d+(?:,\d+)*(?:[km])?\s+arr\s+",  # "$500k ARR " (with space after)
    ]

    # Pattern 1: Salary range with currency symbols: "$100k-150k", "$100,000 - $150,000"
    # Handle multi-line cases and various formats
    # IMPORTANT: Range patterns must come BEFORE single value patterns to avoid matching just the first number
    patterns = [
        # Estimated annual base salary: $93,000.00 - 135,000.00 (handles "estimated", "annual", and ranges without second currency)
        r"(?i)(?:estimated\s+)?(?:annual\s+)?(?:base\s+)?salary[:\s]*(?:of\s+)?[\$£€¥]\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:k|K)?\s*(?:[-–—]|&mdash;|&ndash;)\s*[\$£€¥]?\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:k|K)?",
        # Annual Salary: $210,000 - $248,500 or $210,000&mdash;$248,500 (handles multi-line, HTML entities)
        # Also handles European format: €155.000 - €205.000 (dots as thousand separators)
        r"(?i)(?:annual\s+)?salary[:\s]*[\$£€¥]\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:k|K)?\s*(?:[-–—]|&mdash;|&ndash;)\s*[\$£€¥]?\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:k|K)?",
        # salary range: $100k-150k, compensation range: $100k-150k (most specific, highest priority)
        # Handles both comma and dot thousand separators
        r"(?i)(?:salary|compensation|base\s+salary|base\s+compensation)(?:\s+range)?[:\s]+[\$£€¥]\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:k|K)?\s*(?:[-–—to]+|&mdash;|&ndash;)\s*[\$£€¥]?\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:k|K)?",
        # salary: $100k-150k, compensation: $100k-150k
        r"(?i)(?:salary|compensation|base\s+salary|base\s+compensation)[:\s]+[\$£€¥]\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:k|K)?\s*(?:[-–—to]+|&mdash;|&ndash;)\s*[\$£€¥]?\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:k|K)?",
        # $100k-150k, $100K-150K, $130,900 - $177,100, $210,000&mdash;$248,500, €155.000 - €205.000
        # Handles both comma and dot thousand separators
        r"[\$£€¥]\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:k|K)?\s*(?:[-–—]|&mdash;|&ndash;)\s*[\$£€¥]?\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:k|K)?",
        # $100k to $150k, $130,900 to $177,100
        r"[\$£€¥]\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:k|K)?\s+to\s+[\$£€¥]?\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:k|K)?",
        # $100,000 - $150,000 per year
        r"[\$£€¥]\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:[-–—]|&mdash;|&ndash;)\s*[\$£€¥]?\s*(\d{1,3}(?:[.,]\d{3})*(?:\.\d+)?)\s*(?:per|\/)\s*(?:year|annum|annually)",
        # Single salary: $100k, $100,000 (with salary/compensation context) - ONLY if no range found
        r"(?i)(?:salary|compensation|base)\s+[:\s]+[\$£€¥]\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:k|K)?(?!\s*(?:[-–—]|&mdash;|&ndash;|to)\s*[\$£€¥]?\s*\d)",
        # Single salary: $100k, $100,000 (standalone, but check for false positives) - ONLY if no range found
        r"[\$£€¥]\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:k|K)?(?!\s*(?:[-–—]|&mdash;|&ndash;|to)\s*[\$£€¥]?\s*\d)\s*(?:per|\/)?\s*(?:year|annum|annually)?",
    ]

    for pattern in patterns:
        match = re.search(pattern, desc_clean, re.IGNORECASE)
        if match:
            matched_text = match.group(0)
            # Get context around the match (100 chars before and after for false positive detection)
            start = max(0, match.start() - 100)
            end = min(len(desc_clean), match.end() + 100)
            context = desc_clean[start:end].strip()
            context_lower = context.lower()

            # Check for false positive indicators
            is_false_positive = False
            for indicator in false_positive_indicators:
                if re.search(indicator, context_lower):
                    is_false_positive = True
                    break

            if is_false_positive:
                continue

            if len(match.groups()) == 2:
                # Range found
                # Handle both comma and dot thousand separators (European format uses dots)
                # Remove commas (thousand separators) and convert to float (handles decimal points correctly)
                min_val_str = match.group(1).replace(",", "")
                max_val_str = match.group(2).replace(",", "")
                # Convert to float (handles decimal points like ".00" correctly)
                min_float = float(min_val_str)
                max_float = float(max_val_str)
                if "k" in matched_text.lower() and min_float < 1000:
                    min_float *= 1000
                if "k" in matched_text.lower() and max_float < 1000:
                    max_float *= 1000

                # Filter out unrealistic salaries (too high or too low)
                # Typical salary range: $30k - $500k (allowing some flexibility)
                if min_float < 20000 or max_float > 1000000:
                    continue
                if min_float > max_float:
                    continue

                # Extract currency
                currency = (
                    "$"
                    if "$" in matched_text
                    else (
                        "€"
                        if "€" in matched_text
                        else ("£" if "£" in matched_text else "¥")
                    )
                )
                # Format with commas if original had commas (preserve formatting)
                min_str = (
                    match.group(1) if "," in match.group(1) else str(int(min_float))
                )
                max_str = (
                    match.group(2) if "," in match.group(2) else str(int(max_float))
                )
                # Get shorter context for return (50 chars)
                short_start = max(0, match.start() - 50)
                short_end = min(len(desc_clean), match.end() + 50)
                short_context = desc_clean[short_start:short_end].strip()
                return f"{currency}{min_str}-{currency}{max_str}", short_context
            elif len(match.groups()) == 1:
                # Single value found
                # Handle both comma and dot thousand separators (European format uses dots)
                val = match.group(1).replace(",", "").replace(".", "")
                val_float = float(val)
                if "k" in matched_text.lower() and val_float < 1000:
                    val_float *= 1000

                # Filter out unrealistic salaries
                if val_float < 20000 or val_float > 1000000:
                    continue

                currency = (
                    "$"
                    if "$" in matched_text
                    else (
                        "€"
                        if "€" in matched_text
                        else ("£" if "£" in matched_text else "¥")
                    )
                )
                # Get shorter context for return (50 chars)
                short_start = max(0, match.start() - 50)
                short_end = min(len(desc_clean), match.end() + 50)
                short_context = desc_clean[short_start:short_end].strip()
                return f"{currency}{int(val_float)}", short_context

    return None, None


def extract_experience_from_description(
    description: str,
) -> Tuple[Optional[int], Optional[str]]:
    """
    Extract experience requirement (years) from job description using regex.
    Returns (minimum years if range found, or years if single value, matched_context) or (None, None).
    """
    if not description:
        return None, None

    # Normalize description
    desc_clean = re.sub(r"<[^>]+>", "", description)

    # Patterns for experience requirements (ordered from most specific to least specific)
    patterns = [
        # "3+ years of experience with research operations, community engagement" - with "with" clause
        r"(\d+)\+\s+years?\s+of\s+experience\s+with\s+(?:\w+(?:\s+,\s+)?\s*)+",
        # "3+ years of proven experience in payroll system implementation"
        r"(\d+)\+\s+years?\s+of\s+(?:proven\s+)?experience\s+in\s+\w+(?:\s+\w+)*",
        # "Have 4+ years of experience", "Possess 2+ years of research engineering experience"
        r"(?:have|possess|require|requires|required|need|needs)\s+(\d+)\+?\s+years?\s+(?:of\s+)?(?:\w+\s+){0,8}(?:experience|exp)",
        # "3–5 years of social media strategy experience"
        r"(\d+)\s*[-–—to]+\s*(\d+)\+?\s+years?\s+of\s+(?:\w+\s+){0,5}(?:experience|exp)",
        # "2–4 years building full-stack products" - with action verb
        r"(\d+)\s*[-–—to]+\s*(\d+)\+?\s+years?\s+(?:building|developing|designing|managing|working|creating|implementing|maintaining|supporting)\s+\w+(?:\s+\w+)*",
        # "3+ years building" - with action verb
        r"(\d+)\+\s+years?\s+(?:building|developing|designing|managing|working|creating|implementing|maintaining|supporting)\s+\w+(?:\s+\w+)*",
        # "3-5 years", "3 to 5 years", "3–5 years" with experience keyword
        r"(\d+)\s*[-–—to]+\s*(\d+)\+?\s+years?\s+(?:of\s+)?(?:\w+\s+){0,8}(?:experience|exp)",
        # "5+ years", "5+ years of experience", "5+ years of research engineering experience"
        r"(\d+)\+\s+years?\s+(?:of\s+)?(?:\w+\s+){0,8}(?:experience|exp)",
        # "at least 3 years", "minimum 3 years"
        r"(?:at\s+least|minimum|min\.?)\s+(\d+)\s+years?\s+(?:of\s+)?(?:\w+\s+){0,8}(?:experience|exp)",
        # "3-5 years" (without "experience" keyword, but with context words)
        r"(\d+)\s*[-–—to]+\s*(\d+)\+?\s+years?\s+(?:in|with|working|building|developing|designing|managing|shipping)",
        # "5+ years" (without "experience" keyword, but with context words)
        r"(\d+)\+\s+years?\s+(?:in|with|working|building|developing|designing|managing|shipping)",
        # "3 years experience", "5 years of experience" (without +)
        r"(\d+)\s+years?\s+(?:of\s+)?(?:\w+\s+){0,8}(?:experience|exp)",
        # "3-5 years" (simple, without experience keyword)
        r"(\d+)\s*[-–—to]+\s*(\d+)\+?\s+years?",
        # "5+ years" (simple, without experience keyword)
        r"(\d+)\+\s+years?",
    ]

    for pattern in patterns:
        match = re.search(pattern, desc_clean, re.IGNORECASE)
        if match:
            # Get context around the match (50 chars before and after)
            start = max(0, match.start() - 50)
            end = min(len(desc_clean), match.end() + 50)
            context = desc_clean[start:end].strip()

            if len(match.groups()) == 2:
                # Range found, return minimum
                return int(match.group(1)), context
            elif len(match.groups()) == 1:
                # Single value found
                return int(match.group(1)), context

    return None, None


def parse_salary(
    salary_str: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse salary string into min, max, and currency.
    Returns (salary_min, salary_max, salary_currency)
    """
    if not salary_str:
        return None, None, None

    # Remove common prefixes
    salary_str = salary_str.strip()

    # Extract currency
    currency = None
    if salary_str.startswith("$") or "$" in salary_str:
        currency = "USD"
    elif salary_str.startswith("€") or "€" in salary_str:
        currency = "EUR"
    elif salary_str.startswith("£") or "£" in salary_str:
        currency = "GBP"
    elif "USD" in salary_str.upper():
        currency = "USD"
    elif "EUR" in salary_str.upper():
        currency = "EUR"
    elif "GBP" in salary_str.upper():
        currency = "GBP"

    # Default to USD if no currency found
    if not currency:
        currency = "USD"

    # Remove all currency symbols and extract numbers (handle ranges like "100k-150k", "100,000-150,000", "155.000-205.000", etc.)
    # Remove currency symbols first
    salary_str = re.sub(r"[\$£€¥]", "", salary_str).strip()
    # Handle comma thousand separators (remove commas, but keep decimal points)
    # Note: We don't remove dots here because they might be decimal points (e.g., "93000.00")
    # European format with dots as thousand separators is handled differently
    salary_str = salary_str.replace(",", "")

    # Pattern for ranges: "100k-150k" or "100000-150000" or "93000.00-135000.00"
    range_pattern = r"(\d+(?:\.\d+)?)\s*(?:k|K)?\s*[-–—]\s*(\d+(?:\.\d+)?)\s*(?:k|K)?"
    match = re.search(range_pattern, salary_str)
    if match:
        # Convert to float (handles decimal points correctly)
        min_val = float(match.group(1))
        max_val = float(match.group(2))
        # Convert k to thousands
        if "k" in salary_str.lower() and min_val < 1000:
            min_val *= 1000
        if "k" in salary_str.lower() and max_val < 1000:
            max_val *= 1000
        return str(int(min_val)), str(int(max_val)), currency

    # Pattern for single value: "100k" or "100000"
    single_pattern = r"(\d+(?:\.\d+)?)\s*(?:k|K)?"
    match = re.search(single_pattern, salary_str)
    if match:
        val = float(match.group(1))
        # Convert k to thousands
        if "k" in salary_str.lower() and val < 1000:
            val *= 1000
        return str(int(val)), str(int(val)), currency

    return None, None, None


def main():
    """Main extraction loop."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Extract salary and experience from job descriptions"
    )
    parser.add_argument(
        "--csv",
        type=str,
        help="Specific CSV file to process (default: most recent ai-*.csv)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't write changes, just show what would be extracted",
    )
    args = parser.parse_args()

    # Find CSV file
    if args.csv:
        csv_path = Path(args.csv)
        if not csv_path.exists():
            print(f"❌ CSV file not found: {csv_path}")
            sys.exit(1)
    else:
        csv_path = find_most_recent_ai_csv()
        if not csv_path:
            print("❌ No ai-*.csv files found")
            sys.exit(1)

    print(f"📄 Processing: {csv_path.name}")

    # Read CSV
    jobs = []
    fieldnames = None
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        jobs = list(reader)

    if not fieldnames:
        print("❌ CSV file has no headers")
        sys.exit(1)

    # Check if experience_years column exists, add if not
    has_experience_col = "experience_years" in fieldnames
    if not has_experience_col:
        fieldnames = list(fieldnames) + ["experience_years"]
        for job in jobs:
            job["experience_years"] = ""

    # Filter jobs that need extraction
    jobs_to_process = []
    for job in jobs:
        needs_salary = not job.get("salary_min") or not job.get("salary_max")
        needs_experience = not job.get("experience_years")

        if needs_salary or needs_experience:
            jobs_to_process.append((job, needs_salary, needs_experience))

    total_jobs = len(jobs_to_process)
    if total_jobs == 0:
        print("✅ All jobs already have salary and experience data")
        sys.exit(0)

    print(f"📊 Found {total_jobs} jobs needing extraction")
    print(f"   - {sum(1 for _, ns, _ in jobs_to_process if ns)} need salary")
    print(f"   - {sum(1 for _, _, ne in jobs_to_process if ne)} need experience")
    print()

    # Process jobs
    updated_count = 0
    failed_count = 0
    extraction_results = []  # Store all extraction results for review

    for idx, (job, needs_salary, needs_experience) in enumerate(jobs_to_process, 1):
        url = job.get("url", "").strip()
        title = job.get("title", "").strip()
        company = job.get("company", "").strip()

        if not url:
            continue

        print(f"[{idx}/{total_jobs}] {title[:60]}...")
        print(f"    URL: {url}")

        # Get description - pass ats_id and ats_type for better matching
        ats_id = job.get("ats_id", "").strip()
        ats_type = job.get("ats_type", "").strip()
        description, desc_time = get_job_description_fast(
            url, company, title, ats_id, ats_type
        )
        if not description:
            print(f"    ❌ Description not found (took {desc_time:.3f}s)")
            failed_count += 1
            if args.dry_run:
                extraction_results.append(
                    {
                        "url": url,
                        "title": title,
                        "company": company,
                        "needs_salary": needs_salary,
                        "needs_experience": needs_experience,
                        "description_found": False,
                        "error": "Description not found",
                    }
                )
            continue

        print(
            f"    📝 Description retrieved ({len(description)} chars, {desc_time:.3f}s)"
        )

        # Extract salary and experience using regex
        extracted_salary_raw = None
        extracted_salary_context = None
        extracted_experience = None
        extracted_experience_context = None

        if needs_salary:
            extracted_salary_raw, extracted_salary_context = (
                extract_salary_from_description(description)
            )

        if needs_experience:
            extracted_experience, extracted_experience_context = (
                extract_experience_from_description(description)
            )

        # Parse salary
        salary_min = None
        salary_max = None
        salary_currency = None
        salary_summary = None

        if needs_salary and extracted_salary_raw:
            salary_min, salary_max, salary_currency = parse_salary(extracted_salary_raw)
            if salary_min and salary_max:
                if salary_min == salary_max:
                    salary_summary = (
                        f"${int(salary_min) / 1000:.0f}K"
                        if salary_currency == "USD"
                        else f"{salary_currency} {int(salary_min) / 1000:.0f}K"
                    )
                else:
                    salary_summary = (
                        f"${int(salary_min) / 1000:.0f}K - ${int(salary_max) / 1000:.0f}K"
                        if salary_currency == "USD"
                        else f"{salary_currency} {int(salary_min) / 1000:.0f}K - {int(salary_max) / 1000:.0f}K"
                    )
                print(f"    ✅ Salary extracted: {salary_summary}")
                print(f"        Raw: {extracted_salary_raw}")
                print(
                    f"        Parsed: min={salary_min}, max={salary_max}, currency={salary_currency}"
                )
                if extracted_salary_context:
                    print(f"        Context: ...{extracted_salary_context}...")
            else:
                print(f"    ⚠️  Could not parse salary: {extracted_salary_raw}")
        elif needs_salary:
            print("    ⚠️  No salary found in description")

        # Update experience if needed
        if needs_experience and extracted_experience is not None:
            print(f"    ✅ Experience extracted: {extracted_experience} years")
            if extracted_experience_context:
                print(f"        Context: ...{extracted_experience_context}...")
        elif needs_experience:
            print("    ⚠️  No experience requirement found in description")

        # Store extraction result for review
        if args.dry_run:
            # Get description snippet (first 500 chars and last 200 chars)
            desc_snippet = (
                description[:500]
                if len(description) <= 500
                else description[:500] + "\n...\n" + description[-200:]
            )

            extraction_results.append(
                {
                    "url": url,
                    "title": title,
                    "company": company,
                    "needs_salary": needs_salary,
                    "needs_experience": needs_experience,
                    "description_found": True,
                    "description_length": len(description),
                    "description_snippet": desc_snippet,
                    "extracted_salary_raw": extracted_salary_raw,
                    "extracted_salary_context": extracted_salary_context,
                    "extracted_salary_min": salary_min,
                    "extracted_salary_max": salary_max,
                    "extracted_salary_currency": salary_currency,
                    "extracted_salary_summary": salary_summary,
                    "extracted_experience_years": extracted_experience,
                    "extracted_experience_context": extracted_experience_context,
                    "current_salary_min": job.get("salary_min"),
                    "current_salary_max": job.get("salary_max"),
                    "current_experience_years": job.get("experience_years"),
                }
            )

        # Update job data (for non-dry-run)
        if not args.dry_run:
            if needs_salary and salary_min and salary_max:
                job["salary_min"] = salary_min
                job["salary_max"] = salary_max
                if salary_currency:
                    job["salary_currency"] = salary_currency
                job["salary_period"] = "1 YEAR"
                job["salary_summary"] = salary_summary

            if needs_experience and extracted_experience is not None:
                job["experience_years"] = str(extracted_experience)

        updated_count += 1
        print()

    # Write updated CSV
    if not args.dry_run and updated_count > 0:
        # Create backup
        backup_path = csv_path.with_suffix(
            f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        import shutil

        shutil.copy2(csv_path, backup_path)
        print(f"💾 Backup created: {backup_path.name}")

        # Write updated CSV
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(jobs)

        print(f"✅ Updated {csv_path.name}")
        print(f"   - {updated_count} jobs updated")
        print(f"   - {failed_count} jobs failed")
    elif args.dry_run:
        print("🔍 Dry run complete")
        print(f"   - {updated_count} jobs would be updated")
        print(f"   - {failed_count} jobs would fail")

        # Save extraction results to file
        if extraction_results:
            output_file = (
                csv_path.parent
                / f"extraction_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(extraction_results, f, indent=2, ensure_ascii=False)
            print(f"💾 Extraction results saved to: {output_file.name}")
            print(f"   - {len(extraction_results)} extraction results recorded")
    else:
        print("ℹ️  No jobs were updated")


if __name__ == "__main__":
    main()
