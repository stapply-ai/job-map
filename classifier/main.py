#!/usr/bin/env python3
"""
Classifier script to classify jobs as "European tech internship" or "Not a European tech internship"
using Ollama API with custom model euro_intern_classifier.
"""

import csv
import json
import time
import requests
from typing import Dict, List, Optional, Tuple
from pathlib import Path


# Configuration
OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "euro_intern_classifier"
CHECKPOINT_INTERVAL = 10  # Save checkpoint every N jobs
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2  # Base delay for exponential backoff

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent
CHECKPOINT_FILE = SCRIPT_DIR / "checkpoint.json"
OUTPUT_CSV = SCRIPT_DIR / "eu.csv"


def find_all_jobs_csv_files() -> List[Path]:
    """Find all jobs.csv files in subdirectories."""
    jobs_files = []
    for csv_file in DATA_DIR.rglob("jobs.csv"):
        # Skip if it's in the classifier directory itself
        if "classifier" not in str(csv_file):
            jobs_files.append(csv_file)
    return sorted(jobs_files)


def load_checkpoint() -> Dict:
    """Load checkpoint file if it exists."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                checkpoint = json.load(f)
                # Convert processed_job_ids list back to set for faster lookups
                checkpoint["processed_job_ids"] = set(
                    checkpoint.get("processed_job_ids", [])
                )
                return checkpoint
        except Exception as e:
            print(f"⚠️  Error loading checkpoint: {e}. Starting fresh.")
    return {
        "processed_job_ids": set(),
        "last_checkpoint_time": None,
        "total_processed": 0,
    }


def save_checkpoint(checkpoint: Dict):
    """Save checkpoint to file."""
    try:
        # Convert set to list for JSON serialization
        checkpoint_copy = checkpoint.copy()
        checkpoint_copy["processed_job_ids"] = list(checkpoint["processed_job_ids"])
        checkpoint_copy["last_checkpoint_time"] = time.strftime("%Y-%m-%d %H:%M:%S")

        with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
            json.dump(checkpoint_copy, f, indent=2)
    except Exception as e:
        print(f"⚠️  Error saving checkpoint: {e}")


def get_job_description_fast(
    job_url: str, company: str, title: str
) -> Tuple[Optional[str], float]:
    """
    Try to quickly retrieve job description from JSON files.
    Returns (description if found, None otherwise, time_taken_in_seconds).
    """
    start_time = time.time()
    # Try to find matching JSON files in companies directories
    for companies_dir in DATA_DIR.rglob("companies"):
        if not companies_dir.is_dir():
            continue

        # Look for company JSON file
        company_json = companies_dir / f"{company}.json"
        if not company_json.exists():
            continue

        try:
            with open(company_json, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Extract jobs list (structure varies by platform)
            jobs = data.get("jobs", [])
            if not isinstance(jobs, list):
                continue

            # Try to match job by URL first, then by title
            for job in jobs:
                # Check URL match (various field names)
                job_url_field = (
                    job.get("jobUrl")
                    or job.get("url")
                    or job.get("absolute_url")
                    or job.get("hostedUrl")
                )
                if job_url_field == job_url:
                    # Found by URL
                    description = (
                        job.get("descriptionPlain")
                        or job.get("description")
                        or job.get("text")
                    )
                    if description:
                        return description.strip()[:2000]  # Limit length

                # Check title match as fallback
                job_title = job.get("title", "")
                if job_title == title:
                    description = (
                        job.get("descriptionPlain")
                        or job.get("description")
                        or job.get("text")
                    )
                    if description:
                        elapsed = time.time() - start_time
                        return description.strip()[:2000], elapsed  # Limit length

        except Exception:
            # Skip this file if there's an error
            continue

    elapsed = time.time() - start_time
    return None, elapsed


def call_ollama_api(
    title: str, location: str, description: Optional[str] = None
) -> Tuple[Optional[str], float, Optional[str], str]:
    """
    Call Ollama API to classify the job.
    Returns (classification, time_taken_in_seconds, raw_response, prompt).
    Classification: "European tech internship" or "Not a European tech internship" or None on error.
    """
    # Build prompt
    prompt = f"Title: {title}\nLocation: {location}\nDescription: {description or 'Not available'}"

    payload = {"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}

    for attempt in range(MAX_RETRIES):
        try:
            start_time = time.time()
            response = requests.post(
                f"{OLLAMA_URL}/api/generate", json=payload, timeout=60
            )
            response.raise_for_status()

            result = response.json()
            elapsed_time = time.time() - start_time
            raw_response = result.get("response", "").strip()
            classification = raw_response

            # Normalize the response - check "Not a" FIRST to avoid substring matching issues
            if "Not a European tech internship" in classification:
                return (
                    "Not a European tech internship",
                    elapsed_time,
                    raw_response,
                    prompt,
                )
            elif "European tech internship" in classification:
                return "European tech internship", elapsed_time, raw_response, prompt
            else:
                # Try to infer from response
                if (
                    "internship" in classification.lower()
                    and "european" in classification.lower()
                ):
                    return (
                        "European tech internship",
                        elapsed_time,
                        raw_response,
                        prompt,
                    )
                else:
                    return (
                        "Not a European tech internship",
                        elapsed_time,
                        raw_response,
                        prompt,
                    )

        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY_BASE ** (attempt + 1)
                print(
                    f"  ⏳ API error (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                print(f"  ⚠️  API error after {MAX_RETRIES} attempts: {e}")
                return None, 0.0, None, prompt
        except Exception as e:
            print(f"  ⚠️  Unexpected error calling Ollama API: {e}")
            return None, 0.0, None, prompt

    return None, 0.0, None, prompt


def ensure_output_csv_header():
    """Ensure output CSV file has header if it doesn't exist."""
    if not OUTPUT_CSV.exists():
        with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["url", "title", "location", "company", "classification"])


def append_to_output_csv(
    url: str, title: str, location: str, company: str, classification: str
):
    """Append a classified job to the output CSV."""
    with open(OUTPUT_CSV, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([url, title, location, company, classification])


def count_total_jobs(jobs_files: List[Path]) -> int:
    """Count total number of jobs in all CSV files."""
    total = 0
    for csv_file in jobs_files:
        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                total += sum(1 for row in reader if row.get("url", "").strip())
        except Exception:
            continue
    return total


def format_time(seconds: float) -> str:
    """Format seconds into human-readable time."""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def main():
    """Main classification loop."""
    print("🚀 Starting job classification...")

    # Find all jobs.csv files
    jobs_files = find_all_jobs_csv_files()
    if not jobs_files:
        print("❌ No jobs.csv files found!")
        return

    print(f"📁 Found {len(jobs_files)} jobs.csv file(s)")

    # Load checkpoint
    checkpoint = load_checkpoint()
    processed_count = checkpoint["total_processed"]
    processed_ids = checkpoint["processed_job_ids"]

    print(f"📊 Checkpoint loaded: {len(processed_ids)} jobs already processed")

    # Count total jobs and calculate estimate
    print("📊 Counting total jobs...")
    total_jobs = count_total_jobs(jobs_files)
    remaining_jobs = total_jobs - len(processed_ids)
    estimated_seconds = remaining_jobs * 1.0  # 1 second per job on average
    estimated_time = format_time(estimated_seconds)

    print("\n📈 Statistics:")
    print(f"   Total jobs: {total_jobs}")
    print(f"   Already processed: {len(processed_ids)}")
    print(f"   Remaining: {remaining_jobs}")
    print(f"   Estimated time: ~{estimated_time}")
    print()

    # Ensure output CSV has header
    ensure_output_csv_header()

    # Process all jobs
    new_jobs_processed = 0
    skipped_jobs = 0
    failed_jobs = 0

    for csv_file in jobs_files:
        print(f"\n📄 Processing {csv_file.name}...")

        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)

                for row in reader:
                    url = row.get("url", "").strip()
                    title = row.get("title", "").strip()
                    location = row.get("location", "").strip()
                    company = row.get("company", "").strip()

                    if not url:
                        continue

                    # Skip if already processed
                    if url in processed_ids:
                        skipped_jobs += 1
                        continue

                    # Print separator between jobs
                    if new_jobs_processed > 0:
                        print()

                    # Try to get description (fast lookup)
                    description, desc_time = get_job_description_fast(
                        url, company, title
                    )
                    if description:
                        print(
                            f"  📝 Description retrieved in {desc_time:.3f}s ({len(description)} chars)"
                        )
                    else:
                        print(
                            f"  📝 Description lookup took {desc_time:.3f}s (not found)"
                        )

                    # Classify job
                    print(f"  [{new_jobs_processed + 1}] Classifying: {title[:50]}...")
                    classification, ollama_time, raw_response, prompt = call_ollama_api(
                        title, location, description
                    )

                    # Show full prompt
                    print("  📋 Full prompt:")
                    for line in prompt.split("\n"):
                        print(f"     {line}")

                    if classification:
                        print(f"  ⏱️  Ollama response time: {ollama_time:.3f}s")
                        print(f"  💬 Raw response: {raw_response}")
                        print(f"  ✅ Classification: {classification}")

                        # Only save European tech internships to CSV
                        if classification == "European tech internship":
                            append_to_output_csv(
                                url, title, location, company, classification
                            )
                            print("  💾 Saved to eu.csv")
                        else:
                            print("  ⏭️  Skipped (not a European tech internship)")

                        # Always mark as processed to avoid reprocessing
                        processed_ids.add(url)
                        new_jobs_processed += 1
                        processed_count += 1

                        # Save checkpoint periodically
                        if new_jobs_processed % CHECKPOINT_INTERVAL == 0:
                            checkpoint["processed_job_ids"] = processed_ids
                            checkpoint["total_processed"] = processed_count
                            save_checkpoint(checkpoint)
                            print(
                                f"  💾 Checkpoint saved ({processed_count} total processed)"
                            )
                    else:
                        failed_jobs += 1
                        print(f"  ❌ Failed to classify: {title[:50]}")
                        if raw_response:
                            print(f"  💬 Raw response: {raw_response}")

                    # Small delay to avoid overwhelming the API
                    time.sleep(0.1)

        except Exception as e:
            print(f"  ⚠️  Error processing {csv_file.name}: {e}")
            continue

    # Final checkpoint save
    checkpoint["processed_job_ids"] = processed_ids
    checkpoint["total_processed"] = processed_count
    save_checkpoint(checkpoint)

    # Summary
    print("\n✅ Classification complete!")
    print(f"   Total jobs found: {total_jobs}")
    print(f"   New jobs processed: {new_jobs_processed}")
    print(f"   Skipped (already processed): {skipped_jobs}")
    print(f"   Failed: {failed_jobs}")
    print(f"   Total processed (all time): {processed_count}")
    print(f"   Results saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
