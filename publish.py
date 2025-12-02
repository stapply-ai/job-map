#!/usr/bin/env python3
"""
Prepare X threads from generated thread files and split them into
multiple threads with up to 10 NEW listings each.

Usage:
    python publish.py --mode hourly
    python publish.py --mode daily

This script:
- Reads either hourly_thread.txt or daily_thread.txt
- Extracts individual job blocks from the file
- Keeps only (NEW) listings
- Groups them into "threads" of up to 10 jobs
- Writes each thread to its own text file for easy manual publishing
"""

import argparse
from pathlib import Path
from typing import List


ROOT_DIR = Path(__file__).resolve().parent


def extract_job_blocks(lines: List[str]) -> List[str]:
    """
    Extract individual job blocks from a thread file.

    We treat any line starting with "✨ " as the start of a job block and
    capture all following non-empty lines (title, location, link) until
    the next job start or EOF.

    Lines like "[Post N]" and the header line are ignored.
    """
    blocks: List[str] = []
    current: List[str] = []

    for raw in lines:
        line = raw.rstrip("\n")

        if not line:
            # allow empty lines inside a block but don't force-close
            if current:
                current.append("")
            continue

        if line.startswith("=== "):
            # header line
            continue

        if line.startswith("[Post "):
            # explicit post marker - ignore, block content is driven by "✨ "
            continue

        if line.startswith("✨ "):
            # new job block; close previous if any
            if current:
                # trim trailing blank lines
                while current and current[-1] == "":
                    current.pop()
                if current:
                    blocks.append("\n".join(current))
                current = []
            current.append(line)
        else:
            if current:
                current.append(line)

    if current:
        while current and current[-1] == "":
            current.pop()
        if current:
            blocks.append("\n".join(current))

    return blocks


def filter_new_blocks(blocks: List[str]) -> List[str]:
    """Keep only blocks whose first line contains (NEW)."""
    result: List[str] = []
    for block in blocks:
        first_line = block.splitlines()[0] if block else ""
        if "(NEW)" in first_line:
            result.append(block)
    return result


def chunk_into_threads(blocks: List[str], max_jobs_per_thread: int = 10) -> List[str]:
    """Group job blocks into threads of at most max_jobs_per_thread jobs."""
    threads: List[str] = []
    for i in range(0, len(blocks), max_jobs_per_thread):
        chunk = blocks[i : i + max_jobs_per_thread]
        threads.append("\n\n".join(chunk))
    return threads


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare X threads from hourly/daily thread files."
    )
    parser.add_argument(
        "--mode",
        choices=["hourly", "daily"],
        default="hourly",
        help="Which thread file to use (default: hourly).",
    )
    args = parser.parse_args()

    if args.mode == "hourly":
        source_name_candidates = ["hourly_thread.txt"]
        prefix = "hourly"
    else:
        # support both daily_thread.txt and daily_digest.txt if you decide to rename
        source_name_candidates = ["daily_thread.txt", "daily_digest.txt"]
        prefix = "daily"

    source_path = None
    for name in source_name_candidates:
        candidate = ROOT_DIR / name
        if candidate.exists():
            source_path = candidate
            break

    if source_path is None:
        print(
            f"❌ Could not find any of: {', '.join(source_name_candidates)} "
            f"in {ROOT_DIR}"
        )
        return

    with open(source_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    blocks = extract_job_blocks(lines)
    new_blocks = filter_new_blocks(blocks)

    if not new_blocks:
        print(f"ℹ️  No (NEW) listings found in {source_path.name}")
        return

    threads = chunk_into_threads(new_blocks, max_jobs_per_thread=10)

    out_files: List[Path] = []
    for idx, thread_text in enumerate(threads, 1):
        out_path = ROOT_DIR / f"{prefix}_thread_to_publish_{idx}.txt"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"=== {prefix.upper()} THREAD {idx} (NEW listings only) ===\n\n")
            f.write(thread_text)
            f.write("\n")
        out_files.append(out_path)

    print(
        f"✅ Prepared {len(threads)} {prefix} thread file(s) "
        f"with NEW listings only:"
    )
    for p in out_files:
        print(f" - {p.name}")


if __name__ == "__main__":
    main()


