#!/usr/bin/env python3
"""
Post job listings to social media platforms (X/Twitter, etc.)

Features:
- Daily digest: All new jobs from today, posted at 5 PM PST
- Periodic posts: New jobs since last run, posted every hour
- Summary format posts with job count and link
- Duplicate tracking to avoid reposting
"""

import csv
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Set
from datetime import date, datetime, timezone, timedelta
import json
import asyncio
import os
import subprocess
import time
import urllib.error
import urllib.request


from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

from dotenv import load_dotenv
load_dotenv()

try:
    from browser_use import Agent, Browser, BrowserProfile, ChatBrowserUse
except ImportError:
    Agent = None
    Browser = None
    BrowserProfile = None
    ChatBrowserUse = None

# Get root directory
ROOT_DIR = Path(__file__).resolve().parent

# Track posted jobs
POSTED_JOBS_FILE = ROOT_DIR / "posted_jobs.json"


def load_posted_jobs() -> Set[str]:
    """Load set of job URLs that have already been posted."""
    if not POSTED_JOBS_FILE.exists():
        return set()

    try:
        with open(POSTED_JOBS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return set(data.get("posted_urls", []))
    except Exception as e:
        print(f"Error loading posted jobs: {e}", file=sys.stderr)
        return set()


def save_posted_jobs(posted_urls: Set[str]):
    """Save set of posted job URLs."""
    try:
        data = {
            "posted_urls": list(posted_urls),
            "last_updated": datetime.now().isoformat(),
        }
        with open(POSTED_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Error saving posted jobs: {e}", file=sys.stderr)


def get_new_jobs_from_csv(
    csv_path: Path, date_filter: Optional[str] = None
) -> List[Dict]:
    """
    Read new jobs from new_ai.csv.
    If date_filter is provided, only return jobs with that date_added.
    Otherwise, return all jobs that haven't been posted yet.
    """
    if not csv_path.exists():
        return []

    posted_urls = load_posted_jobs()
    new_jobs = []

    try:
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("url", "").strip()
                date_added = row.get("date_added", "").strip()

                # Skip if already posted
                if url in posted_urls:
                    continue

                # Apply date filter if provided
                if date_filter and date_added != date_filter:
                    continue

                new_jobs.append(row)
    except Exception as e:
        print(f"Error reading CSV: {e}", file=sys.stderr)
        return []

    return new_jobs


def format_summary_post(jobs: List[Dict], is_daily: bool = False) -> str:
    """
    Legacy helper: format a short summary post for X/Twitter.
    Currently not used by the main posting flow (which generates
    threaded posts), but kept for manual/debug use.
    """
    if not jobs:
        return ""

    count = len(jobs)
    today_str = date.today().strftime("%B %d, %Y")

    if is_daily:
        post = f"🚀 {count} AI job{'s' if count != 1 else ''} in today's feed ({today_str})!\n\n"
    else:
        post = f"🆕 {count} AI job{'s' if count != 1 else ''} since last update!\n\n"

    # Add top companies (if available)
    companies = {}
    for job in jobs[:10]:  # Top 10 for preview
        company = job.get("company", "Unknown")
        companies[company] = companies.get(company, 0) + 1

    if companies:
        top_companies = sorted(companies.items(), key=lambda x: x[1], reverse=True)[:5]
        company_list = ", ".join([f"{name} ({count})" for name, count in top_companies])
        post += f"Top companies: {company_list}\n\n"

    # Add link placeholder (will need to be replaced with actual link)
    post += "🔗 View all jobs: [LINK_TO_BE_ADDED]"

    return post


def job_label(job: Dict) -> str:
    """
    Determine if a job is (NEW) or (RELISTED) based on posted_at vs when we first
    saw it (date_added). Heuristic:
      - If posted_at date == date_added date -> (NEW)
      - Else if posted_at exists and is earlier than date_added date -> (RELISTED)
      - Fallback: (NEW)
    """
    posted_at = (job.get("posted_at") or "").strip()
    date_added = (job.get("date_added") or "").strip()

    posted_date = None
    added_date = None

    try:
        if posted_at:
            posted_dt = datetime.fromisoformat(posted_at.replace("Z", "+00:00"))
            posted_date = posted_dt.date()
    except Exception:
        posted_date = None

    try:
        if date_added:
            # date_added is "%d-%m-%Y-%H-%M" -> take the date part
            parts = date_added.split("-")
            if len(parts) >= 3:
                day_str = "-".join(parts[:3])
                added_dt = datetime.strptime(day_str, "%d-%m-%Y")
                added_date = added_dt.date()
    except Exception:
        added_date = None

    if posted_date and added_date:
        if posted_date == added_date:
            return "(NEW)"
        if posted_date < added_date:
            return "(RELISTED)"
    return "(NEW)"


def format_job_line(job: Dict) -> str:
    """
    Format a single job as a compact X post block:

    (NEW|RELISTED) title @company
    location
    link

    - Uses emojis.
    - Uses raw company name; can be swapped for an @handle map later.
    """
    label = job_label(job)
    title = (job.get("title") or "").strip() or "Untitled role"
    company = (job.get("company") or "").strip() or "Unknown"
    location = (job.get("location") or "").strip()
    url = (job.get("url") or "").strip()
    posted_at_raw = (job.get("posted_at") or "").strip()

    # Placeholder for future handle mapping:
    # handle = COMPANY_TO_HANDLE.get(company.lower(), company)
    handle = company

    lines = []
    # First line: label + title + @company + emoji
    lines.append(f"✨ {label} {title} @{handle}")

    # Second line: minimal location line (already stored compactly upstream)
    if location:
        lines.append(location)

    # Optional third line (before link): posted_at timestamp for NEW jobs
    if label == "(NEW)" and posted_at_raw:
        # Try to parse and format posted_at in local time; fall back to raw if parsing fails
        try:
            posted_dt = datetime.fromisoformat(posted_at_raw.replace("Z", "+00:00"))
            local_dt = posted_dt.astimezone()  # convert to local timezone
            # Example: Dec 02, 2025 10:34 AM PST
            formatted = local_dt.strftime("%b %d, %Y %I:%M %p %Z")
        except Exception:
            formatted = posted_at_raw
        lines.append(f"📅 posted: {formatted}")

    # Link line
    if url:
        lines.append(url)

    return "\n".join(lines)


MAX_CHARS_PER_POST = 260  # leave buffer under the platform limit


def build_x_thread_for_jobs(jobs: List[Dict]) -> List[str]:
    """
    Build a list of X post strings forming a thread from jobs.

    - Each job is formatted using format_job_line.
    - Jobs are packed into posts such that each post stays below
      MAX_CHARS_PER_POST.
    """
    posts: List[str] = []
    current_blocks: List[str] = []
    current_len = 0

    for job in jobs:
        block = format_job_line(job)
        # +2 for two newlines between blocks when joined
        separator_len = 2 if current_blocks else 0
        block_len = len(block) + separator_len

        if current_blocks and current_len + block_len > MAX_CHARS_PER_POST:
            posts.append("\n\n".join(current_blocks))
            current_blocks = [block]
            current_len = len(block)
        else:
            if separator_len:
                current_blocks.append("")  # blank line between blocks
                current_len += separator_len
            current_blocks.append(block)
            current_len += len(block)

    if current_blocks:
        posts.append("\n\n".join(current_blocks))

    return posts


CDP_ENDPOINT = "http://localhost:9222"


def _is_cdp_available(url: str = f"{CDP_ENDPOINT}/json/version") -> bool:
    """Return True if a Chrome instance is listening on the CDP endpoint."""
    try:
        with urllib.request.urlopen(url, timeout=1) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.URLError, TimeoutError):
        return False


def _launch_chrome_for_x() -> None:
    chrome_cmd = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "--remote-debugging-port=9222",
        f"--user-data-dir={os.getenv('USER_DATA_DIR')}",
    ]

    try:
        subprocess.Popen(
            chrome_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        print(
            "Failed to start Google Chrome.\n"
            "Make sure Chrome is installed at:\n"
            "  /Applications/Google Chrome.app\n",
            file=sys.stderr,
        )
        raise


def _ensure_chrome_cdp_available_for_x(timeout_seconds: int = 20) -> None:
    """
    Ensure that a Chrome instance is listening on the CDP endpoint for X posting.

    - If nothing is listening, start Chrome with the user-data-dir command.
    - Then wait (up to timeout_seconds) for the CDP endpoint to become available.
    """
    if not _is_cdp_available():
        _launch_chrome_for_x()

    start = time.time()
    while time.time() - start < timeout_seconds:
        if _is_cdp_available():
            return
        time.sleep(0.5)

    raise RuntimeError(
        f"Chrome did not start listening on {CDP_ENDPOINT} within {timeout_seconds} seconds.\n"
        "Check that Chrome can be launched with the configured command."
    )


async def post_to_x_with_playwright_async(content: str) -> bool:
    """
    Post content directly to X using Playwright attached to your real Chrome
    (via CDP) and the /compose/post page.

    This uses the same Chrome command/user-data-dir you've been using manually,
    so your existing X cookies and login should be reused.
    """
    try:
        _ensure_chrome_cdp_available_for_x()
    except Exception as e:
        print(
            f"❌ Could not start or connect to Chrome for X posting: {e}",
            file=sys.stderr,
        )
        return False

    try:
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(CDP_ENDPOINT)

            # Reuse existing context/page if available
            context = (
                browser.contexts[0] if browser.contexts else await browser.new_context()
            )
            page = context.pages[0] if context.pages else await context.new_page()

            # Go directly to the compose page
            await page.goto("https://x.com/compose/post")

            # Wait for the main tweet textarea
            try:
                textarea = await page.wait_for_selector(
                    'div[contenteditable="true"][data-testid="tweetTextarea_0"]',
                    timeout=15000,
                )
            except PlaywrightTimeoutError:
                print("❌ Could not find the X compose textarea.", file=sys.stderr)
                return False

            # Focus and type the content
            await textarea.click()
            await page.keyboard.type(content)

            # Try the primary tweet button selectors
            try:
                post_button = await page.wait_for_selector(
                    'div[data-testid="tweetButtonInline"], div[data-testid="tweetButton"]',
                    timeout=15000,
                )
            except PlaywrightTimeoutError:
                print("❌ Could not find the X Post button.", file=sys.stderr)
                return False

            await post_button.click()

            print("✅ Submitted post to X via real Chrome /compose/post")
            # Give X a moment to process the post
            await page.wait_for_timeout(3000)

            # Keep the browser alive; user can close Chrome manually
            return True

    except Exception as e:
        print(f"❌ Error posting to X via Playwright/CDP: {e}", file=sys.stderr)
        return False


async def post_to_x_async(content: str) -> bool:
    """
    Post content to X/Twitter using browser_use Agent with Stapply profile.
    Returns True if successful, False otherwise.
    """
    if Agent is None or Browser is None or ChatBrowserUse is None:
        print("❌ browser_use not available. Install with: pip install browser-use")
        return False

    try:
        # Expand ~ to full path for macOS Chrome user data directory
        chrome_user_data_dir = Path.home() / "Library/Application Support/Google/Chrome"

        # Note: Make sure Chrome is closed before running this script, as Chrome locks the profile
        # If you have multiple profiles, change "Default" to your profile name (e.g., "Profile 1", "Stapply", etc.)
        browser = Browser(
            executable_path="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            user_data_dir=str(chrome_user_data_dir),
            profile_directory="Default",  # Change this if your X/Twitter cookies are in a different profile
        )

        # Create task for the agent
        task = f"""Post the following content to X (Twitter):
        
{content}

Instructions:
1. Navigate to https://x.com/compose/post
2. Wait for the compose textarea to appear
3. Type the content exactly as provided
4. Click the "Post" button to publish
5. Verify the post was published successfully"""

        print("🤖 Starting browser agent to post to X...")
        print(f"📝 Content to post ({len(content)} characters):")
        print("-" * 50)
        print(content)
        print("-" * 50)

        llm = ChatBrowserUse(api_key=os.getenv("BROWSER_USE_API_KEY"))
        agent = Agent(
            task=task,
            llm=llm,
            browser=browser,
        )

        # Run the agent
        result = await agent.run()

        print("✅ Post submitted successfully")
        await browser.close()
        return True

    except Exception as e:
        print(f"❌ Error posting to X: {e}", file=sys.stderr)
        if "browser" in locals():
            try:
                await browser.close()
            except Exception:
                pass
        return False


def post_to_x(content: str) -> bool:
    """
    Synchronous wrapper for posting to X/Twitter.
    Returns True if successful, False otherwise.
    """
    if asyncio is None:
        print("❌ asyncio not available")
        return False

    try:
        # Prefer the Playwright/CDP flow using your real Chrome and /compose/post
        return asyncio.run(post_to_x_with_playwright_async(content))
    except Exception as e:
        print(f"❌ Error in async execution: {e}", file=sys.stderr)
        return False


def post_to_other_channels(content: str, channel: str) -> bool:
    """
    Post content to other channels (LinkedIn, Discord, etc.).
    Returns True if successful, False otherwise.
    """
    # TODO: Implement other channel integrations
    print(f"[{channel.upper()} POST] {content}")
    print(f"⚠️  {channel} posting not yet implemented - this is a placeholder")
    return False


def is_5pm_pst() -> bool:
    """Check if current time is 5 PM PST."""
    pst = timezone(timedelta(hours=-8))  # PST is UTC-8
    now = datetime.now(pst)
    return now.hour == 17 and now.minute < 5  # Within 5 minutes of 5 PM


def daily_digest_post():
    """Generate daily digest thread of all new jobs from today and write to file."""
    today_str = date.today().strftime("%d-%m-%Y")
    new_ai_path = ROOT_DIR / "new_ai.csv"

    # Get all jobs from today
    jobs = get_new_jobs_from_csv(new_ai_path, date_filter=today_str)

    if not jobs:
        print("No new jobs from today to post.")
        return

    # Build X thread (but do NOT publish)
    thread_posts = build_x_thread_for_jobs(jobs)

    # Write thread to a file
    out_path = ROOT_DIR / "daily_thread.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("=== X DAILY DIGEST THREAD ===\n")
        for i, post in enumerate(thread_posts, 1):
            f.write(f"\n[Post {i}]\n{post}\n")

    # Mark jobs as posted in our local tracking, since they are now part of the feed
    posted_urls = load_posted_jobs()
    for job in jobs:
        url = job.get("url", "").strip()
        if url:
            posted_urls.add(url)
    save_posted_jobs(posted_urls)
    print(f"✅ Generated daily digest thread for {len(jobs)} jobs → {out_path}")


def hourly_update_post():
    """Generate thread for new jobs since last run (hourly updates) and write to file."""
    new_ai_path = ROOT_DIR / "new_ai.csv"

    # Get all jobs that haven't been posted yet
    jobs = get_new_jobs_from_csv(new_ai_path, date_filter=None)

    if not jobs:
        print("No new jobs to include since last run.")
        return

    # Build X thread (but do NOT publish)
    thread_posts = build_x_thread_for_jobs(jobs)

    # Write thread to a file
    out_path = ROOT_DIR / "hourly_thread.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("=== X HOURLY UPDATE THREAD ===\n")
        for i, post in enumerate(thread_posts, 1):
            f.write(f"\n[Post {i}]\n{post}\n")

    # Mark jobs as posted
    posted_urls = load_posted_jobs()
    for job in jobs:
        url = job.get("url", "").strip()
        if url:
            posted_urls.add(url)
    save_posted_jobs(posted_urls)
    print(f"✅ Generated hourly update thread for {len(jobs)} jobs → {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Post job listings to social media platforms"
    )
    parser.add_argument(
        "--mode",
        choices=["daily", "hourly", "auto"],
        default="auto",
        help="Posting mode: daily (5 PM PST digest), hourly (new jobs since last run), or auto (detect based on time)",
    )
    parser.add_argument(
        "--channel",
        choices=["x", "twitter", "all"],
        default="x",
        help="Channel to post to (default: x)",
    )
    parser.add_argument(
        "--force-daily",
        action="store_true",
        help="Force daily digest post even if not 5 PM PST",
    )
    args = parser.parse_args()

    if args.mode == "auto":
        # Auto-detect: daily at 5 PM PST, otherwise hourly
        if is_5pm_pst() or args.force_daily:
            print("📅 Running daily digest post...")
            daily_digest_post()
        else:
            print("⏰ Running hourly update post...")
            hourly_update_post()
    elif args.mode == "daily":
        daily_digest_post()
    elif args.mode == "hourly":
        hourly_update_post()


if __name__ == "__main__":
    main()
