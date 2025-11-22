"""
SearXNG-Based Company Discovery
Self-hosted search alternative with no API limits.

Advantages:
- Self-hosted control
- No API limits or rate limiting
- No API keys needed
- Privacy-focused
- Aggregates results from multiple search engines
- No usage tracking

Requirements:
- SearXNG instance running (see SEARXNG_SETUP.md)
- SEARXNG_URL in .env pointing to your instance
"""

import requests
import pandas as pd
import re
import os
import shutil
import tempfile
from typing import Set, List
from dotenv import load_dotenv
import time

load_dotenv()

DEFAULT_ENGINES = (
    os.getenv("SEARXNG_ENGINES", "bing,brave,qwant,wikipedia").strip()
    or "bing,brave,qwant,wikipedia"
)

PRIMARY_ENGINE = DEFAULT_ENGINES.split(",")[0].strip() or "bing"

try:
    DEFAULT_REQUEST_DELAY = float(os.getenv("SEARXNG_REQUEST_DELAY", "1.0"))
except (TypeError, ValueError):
    DEFAULT_REQUEST_DELAY = 1.0

# Platform configurations
PLATFORMS = {
    "rippling": {
        "domains": ["ats.rippling.com"],
        "pattern": r"(https://ats\.rippling\.com/[^/?#]+/jobs)",
        "csv_column": "rippling_url",
        "output_file": "rippling/rippling_companies.csv",
    },
    "ashby": {
        "domains": ["jobs.ashbyhq.com"],
        "pattern": r"(https://jobs\.ashbyhq\.com/[^/?#]+)",
        "csv_column": "ashby_url",
        "output_file": "ashby/companies.csv",
    },
    "greenhouse": {
        "domains": ["job-boards.greenhouse.io", "boards.greenhouse.io"],
        "pattern": r"(https://(?:job-boards|boards)\.greenhouse\.io/[^/?#]+)",
        "csv_column": "greenhouse_url",
        "output_file": "greenhouse/greenhouse_companies.csv",
    },
    "lever": {
        "domains": ["jobs.lever.co"],
        "pattern": r"(https://jobs\.lever\.co/[^/?#]+)",
        "csv_column": "lever_url",
        "output_file": "lever/lever_companies.csv",
    },
    "workable": {
        "domains": ["apply.workable.com", "jobs.workable.com"],
        "pattern": [
            r"(https://apply\.workable\.com/[^/?#]+)",
            r"(https://jobs\.workable\.com/company/[^/?#]+/[^/?#]+)",
        ],
        "csv_column": "workable_url",
        "output_file": "workable/workable_companies.csv",
    },
}

SEARCH_STRATEGIES = [
    # Basic site search
    lambda domain: f"site:{domain}",
    lambda domain: f"site:{domain} careers",
    lambda domain: f"site:{domain} jobs",
    lambda domain: f"site:{domain} hiring",
    lambda domain: f'site:{domain} "we\'re hiring"',
    lambda domain: f"site:{domain} apply now",
    # High-value job roles (people willing to pay)
    lambda domain: f"site:{domain} software engineer",
    lambda domain: f"site:{domain} ai engineer",
    lambda domain: f"site:{domain} machine learning",
    lambda domain: f"site:{domain} data scientist",
    lambda domain: f"site:{domain} data engineer",
    lambda domain: f"site:{domain} devops",
    lambda domain: f"site:{domain} cloud engineer",
    lambda domain: f"site:{domain} security engineer",
    lambda domain: f"site:{domain} product manager",
    lambda domain: f"site:{domain} designer",
    lambda domain: f"site:{domain} sales",
    lambda domain: f"site:{domain} enterprise sales",
    lambda domain: f"site:{domain} account executive",
    lambda domain: f"site:{domain} marketing",
    lambda domain: f"site:{domain} finance",
    lambda domain: f"site:{domain} quant",
    lambda domain: f"site:{domain} trading",
    lambda domain: f"site:{domain} fintech",
    lambda domain: f'site:{domain} "engineering"',
    lambda domain: f'site:{domain} "product"',
    lambda domain: f'site:{domain} "data"',
    # Remote
    lambda domain: f"site:{domain} remote",
    # --- Rich Countries / Wealthy Cities ---
    # Europe Tier 1
    lambda domain: f'site:{domain} "Switzerland"',
    lambda domain: f'site:{domain} "Zurich"',
    lambda domain: f'site:{domain} "Geneva"',
    lambda domain: f'site:{domain} "Luxembourg"',
    lambda domain: f'site:{domain} "Monaco"',
    lambda domain: f'site:{domain} "Norway"',
    lambda domain: f'site:{domain} "Oslo"',
    lambda domain: f'site:{domain} "Denmark"',
    lambda domain: f'site:{domain} "Copenhagen"',
    lambda domain: f'site:{domain} "Sweden"',
    lambda domain: f'site:{domain} "Stockholm"',
    # Middle East wealthy hubs
    lambda domain: f'site:{domain} "Dubai"',
    lambda domain: f'site:{domain} "Abu Dhabi"',
    lambda domain: f'site:{domain} "Saudi Arabia"',
    lambda domain: f'site:{domain} "Riyadh"',
    lambda domain: f'site:{domain} "Qatar"',
    lambda domain: f'site:{domain} "Doha"',
    lambda domain: f'site:{domain} "Kuwait"',
    lambda domain: f'site:{domain} "Bahrain"',
    # Asia rich hubs
    lambda domain: f'site:{domain} "Singapore"',
    lambda domain: f'site:{domain} "Tokyo"',
    lambda domain: f'site:{domain} "Seoul"',
    lambda domain: f'site:{domain} "Hong Kong"',
    # North America rich hubs
    lambda domain: f'site:{domain} "San Francisco"',
    lambda domain: f'site:{domain} "Silicon Valley"',
    lambda domain: f'site:{domain} "Palo Alto"',
    lambda domain: f'site:{domain} "Seattle"',
    lambda domain: f'site:{domain} "New York"',
    lambda domain: f'site:{domain} "Toronto"',
    lambda domain: f'site:{domain} "Vancouver"',
    lambda domain: f'site:{domain} "Montreal"',
    # Australia rich hubs
    lambda domain: f'site:{domain} "Sydney"',
    lambda domain: f'site:{domain} "Melbourne"',
    # Regions
    lambda domain: f'site:{domain} "Middle East"',
    lambda domain: f'site:{domain} "Europe"',
    lambda domain: f'site:{domain} "North America"',
    lambda domain: f'site:{domain} "Asia"',
    # --- Startup / VC searches ---
    lambda domain: f"site:{domain} startup",
    lambda domain: f'site:{domain} YC OR "Y Combinator"',
    # Top VCs (A16Z ++)
    lambda domain: f'site:{domain} "a16z"',
    lambda domain: f'site:{domain} "Andreessen Horowitz"',
    lambda domain: f'site:{domain} "Sequoia Capital"',
    lambda domain: f'site:{domain} "Accel"',
    lambda domain: f'site:{domain} "Index Ventures"',
    lambda domain: f'site:{domain} "Benchmark"',
    lambda domain: f'site:{domain} "Greylock"',
    lambda domain: f'site:{domain} "Lightspeed"',
    lambda domain: f'site:{domain} "Founders Fund"',
    lambda domain: f'site:{domain} "Khosla Ventures"',
    lambda domain: f'site:{domain} "Tiger Global"',
    lambda domain: f"site:{domain} series A OR series B",
    lambda domain: f'site:{domain} "tech startup"',
    # --- Big Tech / Big Corp Searches ---
    # FAANG + BigTech patterns
    lambda domain: f"site:{domain} Google",
    lambda domain: f"site:{domain} Meta",
    lambda domain: f"site:{domain} Amazon",
    lambda domain: f"site:{domain} Apple",
    lambda domain: f"site:{domain} Microsoft",
    lambda domain: f"site:{domain} Netflix",
    lambda domain: f"site:{domain} Tesla",
    # Enterprise keywords
    lambda domain: f'site:{domain} "enterprise"',
    lambda domain: f'site:{domain} "corporate"',
    lambda domain: f'site:{domain} "global careers"',
    lambda domain: f'site:{domain} "fortune 500"',
    lambda domain: f'site:{domain} "multinational"',
    lambda domain: f'site:{domain} "global offices"',
    lambda domain: f'site:{domain} "corporate jobs"',
    # Combined high-paying patterns
    lambda domain: f'site:{domain} "San Francisco" software engineer',
    lambda domain: f'site:{domain} "New York" quant',
    lambda domain: f'site:{domain} "London" fintech',
    lambda domain: f'site:{domain} "Dubai" software engineer',
    lambda domain: f'site:{domain} "Singapore" ai engineer',
    lambda domain: f'site:{domain} "Zurich" finance',
    lambda domain: f'site:{domain} "Hong Kong" trading',
    lambda domain: f"site:{domain} remote",
    lambda domain: f'site:{domain} "San Francisco"',
    lambda domain: f'site:{domain} "New York"',
    lambda domain: f'site:{domain} "London"',
    lambda domain: f'site:{domain} "Paris"',
    lambda domain: f'site:{domain} "Berlin"',
    lambda domain: f'site:{domain} "Amsterdam"',
    lambda domain: f'site:{domain} "Stockholm"',
    lambda domain: f'site:{domain} "Warsaw"',
    lambda domain: f'site:{domain} "Brussels"',
    lambda domain: f'site:{domain} "Zurich"',
    lambda domain: f'site:{domain} "Delhi"',
    lambda domain: f'site:{domain} "Mumbai"',
    lambda domain: f'site:{domain} "Bangalore"',
    lambda domain: f'site:{domain} "Chennai"',
    lambda domain: f'site:{domain} "Hyderabad"',
    lambda domain: f'site:{domain} "Pune"',
    lambda domain: f'site:{domain} "Kolkata"',
    lambda domain: f'site:{domain} "Jaipur"',
    lambda domain: f'site:{domain} "Singapore"',
    lambda domain: f'site:{domain} "Dubai"',
    lambda domain: f'site:{domain} "Tokyo"',
    lambda domain: f'site:{domain} "Seoul"',
    lambda domain: f'site:{domain} "Hong Kong"',
    lambda domain: f'site:{domain} "Toronto"',
    lambda domain: f'site:{domain} "Montreal"',
    lambda domain: f'site:{domain} "Vancouver"',
    lambda domain: f'site:{domain} "Sydney"',
    lambda domain: f'site:{domain} "Europe"',
    lambda domain: f'site:{domain} "Asia"',
    lambda domain: f'site:{domain} "Middle East"',
    lambda domain: f'site:{domain} "North America"',
    lambda domain: f'site:{domain} "South America"',
]


def normalize_url(url: str) -> str:
    """Normalize URLs for case-insensitive comparisons"""
    if not isinstance(url, str):
        return ""
    return url.strip().rstrip("/").lower()


def standardize_rippling_url(url: str) -> str:
    """Standardize Rippling URLs to always have /jobs format"""
    if not isinstance(url, str):
        return ""

    url = url.strip().rstrip("/").lower()

    # Match rippling URLs
    rippling_pattern = r"^https://ats\.rippling\.com/([^/?#]+)(?:/jobs)?$"
    match = re.match(rippling_pattern, url)

    if match:
        slug = match.group(1)
        return f"https://ats.rippling.com/{slug}/jobs"

    return url


def create_temp_copy(source_path: str) -> str | None:
    """
    Create a temporary copy of the given file in the same directory.
    Returns the path to the copy, or None if copying fails.
    """
    temp_path = None
    try:
        temp_dir = os.path.dirname(source_path) or "."
        suffix = os.path.splitext(source_path)[1] or ".tmp"
        fd, temp_path = tempfile.mkstemp(prefix=".copy_", suffix=suffix, dir=temp_dir)
        os.close(fd)
        shutil.copy2(source_path, temp_path)
        return temp_path
    except OSError as e:
        print(f"⚠️  Failed to create temp copy for {source_path}: {e}")
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        return None


def write_dataframe_atomically(df: pd.DataFrame, target_path: str) -> None:
    """Write DataFrame to CSV via temp file and atomically replace the original."""
    target_dir = os.path.dirname(target_path) or "."
    os.makedirs(target_dir, exist_ok=True)

    fd, temp_path = tempfile.mkstemp(prefix=".tmp_", suffix=".csv", dir=target_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as tmp_file:
            df.to_csv(tmp_file, index=False)
        os.replace(temp_path, target_path)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise


def read_existing_urls(
    csv_file: str, column_name: str, platform_key: str = None
) -> Set[str]:
    """Read existing URLs from CSV file"""
    existing_urls: Set[str] = set()
    temp_copy = None
    if os.path.exists(csv_file):
        try:
            temp_copy = create_temp_copy(csv_file)
            read_path = temp_copy or csv_file
            df = pd.read_csv(read_path)
            urls_to_process = []
            if column_name in df.columns:
                urls_to_process = df[column_name].dropna().tolist()
            elif "url" in df.columns:
                urls_to_process = df["url"].dropna().tolist()

            # Standardize rippling URLs before normalization
            if platform_key == "rippling":
                urls_to_process = [
                    standardize_rippling_url(url) for url in urls_to_process
                ]

            existing_urls = {
                normalize_url(url) for url in urls_to_process if normalize_url(url)
            }

            if column_name in df.columns:
                print(f"📖 Found {len(existing_urls)} existing URLs in {csv_file}")
            else:
                print(
                    f"📖 Found {len(existing_urls)} existing URLs in {csv_file} (legacy format)"
                )
        except Exception as e:
            print(f"⚠️  Error reading {csv_file}: {e}")
        finally:
            if temp_copy and os.path.exists(temp_copy):
                try:
                    os.remove(temp_copy)
                except OSError:
                    pass
    return existing_urls


def extract_urls_from_results(
    results: List[dict], pattern: str | List[str], domains: List[str]
) -> Set[str]:
    """Extract company URLs from SearXNG search results"""
    urls = set()

    if not results:
        return urls

    for result in results:
        url = result.get("url", "")

        if not url:
            continue

        # Check if URL contains target domain
        if not any(domain in url for domain in domains):
            continue

        # Handle single pattern or list of patterns
        patterns = [pattern] if isinstance(pattern, str) else pattern

        for pat in patterns:
            match = re.match(pat, url)
            if match:
                urls.add(match.group(1))
                break

    return urls


def search_searxng(
    searxng_url: str,
    query: str,
    page: int = 1,
    engines: str = PRIMARY_ENGINE,
    max_retries: int = 3,
) -> List[dict]:
    """
    Perform search using SearXNG instance with retry logic for rate limiting

    Args:
        searxng_url: Base URL of SearXNG instance (e.g., http://localhost:8080)
        query: Search query
        page: Page number (default: 1)
        engines: Comma-separated list of search engines to use
        max_retries: Maximum number of retries for rate-limited requests

    Returns:
        List of search results
    """
    endpoint = f"{searxng_url.rstrip('/')}/search"

    params = {
        "q": query,
        "format": "json",
        "pageno": page,
        "engines": engines,
        "language": "en",
        "safesearch": 0,  # 0=off, 1=moderate, 2=strict
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(endpoint, params=params, timeout=30)

            # Handle rate limiting (429) with exponential backoff
            if response.status_code == 429:
                if attempt < max_retries - 1:
                    # Exponential backoff: 2^attempt seconds (2, 4, 8 seconds)
                    wait_time = 2 ** (attempt + 1)
                    print(
                        f"  ⏳ Rate limited (429), retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    print(
                        f"  ⚠️  Rate limited (429) after {max_retries} attempts, skipping this query"
                    )
                    return []

            response.raise_for_status()
            data = response.json()

            # Check for engine errors in the response
            errors = data.get("errors", [])
            if errors:
                # Log first few errors for debugging
                for error in errors[:3]:
                    engine_name = error.get("engine", "unknown")
                    error_msg = error.get("error", str(error))
                    print(f"    ⚠️  Engine '{engine_name}' error: {error_msg}")

            results = data.get("results", [])

            # Debug logging: Show response details if no results
            if not results:
                number_of_results = data.get("number_of_results", 0)
                infoboxes = data.get("infoboxes", [])
                answers = data.get("answers", [])

                # Show more detailed debug info for first failed query
                if page == 1:
                    print(f"    🔍 Debug: Response keys: {list(data.keys())}")
                    print(
                        f"    🔍 Debug: number_of_results={number_of_results}, errors={len(errors)}, infoboxes={len(infoboxes)}"
                    )
                    if number_of_results > 0:
                        print(
                            f"    ⚠️  SearXNG reports {number_of_results} total results but returned 0 in 'results' array"
                        )
                        # Check if results are in a different key
                        for key in data.keys():
                            if (
                                "result" in key.lower()
                                and isinstance(data[key], list)
                                and len(data[key]) > 0
                            ):
                                print(
                                    f"    💡 Found {len(data[key])} results in key '{key}' instead of 'results'"
                                )
                    elif errors:
                        print("    ⚠️  All engines failed with errors (see above)")
                    elif infoboxes or answers:
                        print("    ℹ️  Got infobox/answer data but no search results")
                    else:
                        query_preview = query[:50] + "..." if len(query) > 50 else query
                        print(f"    ℹ️  No results found for query: '{query_preview}'")

            return results

        except requests.exceptions.RequestException as e:
            # For 429 errors, we already handled above, so this is for other HTTP errors
            if "429" in str(e) and attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)
                print(
                    f"  ⏳ Rate limited, retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)
                continue
            elif attempt == max_retries - 1:
                print(f"  ⚠️  Error querying SearXNG: {e}")
                return []
            else:
                # For non-429 errors, don't retry
                print(f"  ⚠️  Error querying SearXNG: {e}")
                return []
        except Exception as e:
            print(f"  ⚠️  Unexpected error: {e}")
            return []

    return []


def discover_platform(
    platform_name: str,
    max_queries: int = 20,
    pages_per_query: int = 20,
    engines: str = DEFAULT_ENGINES,
    request_delay: float = DEFAULT_REQUEST_DELAY,
):
    """
    Discover companies using SearXNG

    Args:
        platform_name: Platform to discover
        max_queries: Maximum search queries to use (default: unlimited)
        pages_per_query: Pages per query (default: 3)
        engines: Search engines to use (default pulled from SEARXNG_ENGINES)
        request_delay: Seconds to wait between page fetches
    """

    platform_key = platform_name.lower()

    if platform_key not in PLATFORMS:
        print(f"❌ Unknown platform: {platform_name}")
        print(f"Available platforms: {', '.join(PLATFORMS.keys())}")
        return

    config = PLATFORMS[platform_key]

    print("=" * 80)
    print(f"🔍 SearXNG Discovery: {platform_key.upper()}")
    print(f"📊 Max queries: {max_queries}")
    print(f"📊 Pages per query: {pages_per_query}")
    print(f"🔧 Engines: {engines}")
    print(f"⏱️ Delay between page requests: {request_delay}s")
    print("=" * 80)

    query_cooldown = max(request_delay, 1.0)

    # Check for SearXNG URL
    searxng_url = os.getenv("SEARXNG_URL")
    if not searxng_url:
        print("\n❌ SEARXNG_URL not found in environment")
        print("\nSetup instructions:")
        print("1. Set up SearXNG (see SEARXNG_SETUP.md)")
        print("2. Add to .env file:")
        print("   SEARXNG_URL=http://localhost:8080")
        print("\nOr use a public instance (if available):")
        print("   SEARXNG_URL=https://searx.be")
        return

    # Test SearXNG connection
    print(f"\n🔗 Testing connection to {searxng_url}...")
    test_results = search_searxng(
        searxng_url, "test", page=1, engines=engines, max_retries=5
    )
    if not test_results:
        print("❌ Failed to connect to SearXNG or no results returned")
        print(
            "   This might be due to rate limiting - waiting 10 seconds and trying once more..."
        )
        time.sleep(10)
        test_results = search_searxng(
            searxng_url, "test", page=1, engines=engines, max_retries=3
        )
        if not test_results:
            print("❌ Still failing. Make sure:")
            print("   - SearXNG is running")
            print("   - JSON format is enabled in settings.yml")
            print("   - URL is correct in .env")
            print(
                "   - Rate limiter allows requests (check rate_limit in settings.yml)"
            )
            return
    print(f"✅ Connected! Got {len(test_results)} test results")

    # Read existing URLs
    existing_urls = read_existing_urls(
        config["output_file"], config["csv_column"], platform_key
    )

    discovered_norms = set()
    new_urls: Set[str] = set()
    queries_used = 0
    total_results_fetched = 0

    # Use search strategies
    strategies_to_use = (
        SEARCH_STRATEGIES if max_queries == -1 else SEARCH_STRATEGIES[:max_queries]
    )

    for strategy_idx, strategy_func in enumerate(strategies_to_use, 1):
        if max_queries != -1 and queries_used >= max_queries:
            print(
                f"\n⚠️  Reached query limit ({max_queries if max_queries != -1 else 'unlimited'})"
            )
            break

        query = strategy_func(config["domains"][0])
        print(
            f"\n[Query {queries_used + 1}/{max_queries if max_queries != -1 else 'unlimited'}] {query}"
        )

        query_norms = set()

        for page in range(1, pages_per_query + 1):
            try:
                # SearXNG search
                results = search_searxng(searxng_url, query, page=page, engines=engines)

                total_results_fetched += len(results)

                if not results:
                    print(f"  Page {page}: No results returned by engine mix")
                    if page == 1:
                        print(
                            f"    💡 Retrying with {PRIMARY_ENGINE} as a fallback engine..."
                        )
                        fallback_results = search_searxng(
                            searxng_url, query, page=page, engines=PRIMARY_ENGINE
                        )
                        if fallback_results:
                            print(
                                f"    ✅ Got {len(fallback_results)} results with {PRIMARY_ENGINE}"
                            )
                            results = fallback_results
                            total_results_fetched += len(fallback_results)
                        else:
                            print(
                                f"    ⚠️  {PRIMARY_ENGINE} fallback also returned no results"
                            )
                            break
                    else:
                        break

                # Extract URLs
                page_urls = extract_urls_from_results(
                    results, config["pattern"], config["domains"]
                )

                # Standardize rippling URLs to always have /jobs format
                if platform_key == "rippling":
                    page_urls = [standardize_rippling_url(url) for url in page_urls]

                normalized_page_urls = {
                    normalize_url(url) for url in page_urls if normalize_url(url)
                }

                new_in_page = normalized_page_urls - discovered_norms - query_norms
                query_norms.update(normalized_page_urls)

                new_candidates = normalized_page_urls - existing_urls - new_urls
                new_urls.update(new_candidates)

                print(
                    f"  Page {page}: {len(results)} results, {len(page_urls)} relevant URLs (+{len(new_in_page)} new)"
                )

                # Delay to avoid rate limiting (configurable via CLI/env)
                if request_delay > 0:
                    time.sleep(request_delay)

            except Exception as e:
                print(f"  ⚠️  Error on page {page}: {e}")
                break

        queries_used += 1
        new_from_query = query_norms - discovered_norms
        discovered_norms.update(query_norms)

        print(
            f"  Query total: +{len(new_from_query)} new URLs (cumulative: {len(discovered_norms)})"
        )

        # Delay between queries to avoid rate limiting
        if strategy_idx < len(strategies_to_use) and query_cooldown > 0:
            time.sleep(query_cooldown)

    print("\n📊 Discovery Summary:")
    print(f"  🔍 Queries used: {queries_used}")
    print(f"  📄 Total results fetched: {total_results_fetched}")
    new_count = len(new_urls)
    print(f"  🔍 Companies found: {len(discovered_norms)}")
    print(f"  🆕 New companies: {new_count}")

    # Save results
    combined_urls = existing_urls.union(new_urls)

    if new_count:
        print("\n🎉 Sample of new URLs (first 10):")
        # Normalized URLs are already in standardized format (standardized before normalization)
        # They're just lowercase, which is fine for display
        for url in sorted(new_urls)[:10]:
            print(f"  ✨ {url}")
        if new_count > 10:
            print(f"  ... and {new_count - 10} more")

    # Convert normalized URLs back to standardized format for rippling when saving
    # Normalized URLs are lowercase strings, but already in standardized format (with /jobs)
    # Just ensure they're all properly formatted
    if platform_key == "rippling":
        sorted_urls = []
        for norm_url in sorted(combined_urls):
            # norm_url is already normalized (lowercase, stripped) and standardized (has /jobs)
            # Just ensure it's in the proper format
            standardized = standardize_rippling_url(norm_url)
            if standardized:  # Only add if standardization succeeded
                sorted_urls.append(standardized)
        sorted_urls = sorted(
            set(sorted_urls)
        )  # Remove duplicates after standardization
    else:
        sorted_urls = sorted(combined_urls)

    df = pd.DataFrame({config["csv_column"]: sorted_urls})
    write_dataframe_atomically(df, config["output_file"])

    print(f"\n✅ Saved {len(df)} companies to {config['output_file']}")


def discover_all_platforms(
    max_queries_per_platform: int = -1,
    pages_per_query: int = 20,
    engines: str = DEFAULT_ENGINES,
    request_delay: float = DEFAULT_REQUEST_DELAY,
):
    """Discover all platforms using SearXNG"""

    print("=" * 80)
    print("🔍 SearXNG Discovery - All Platforms")
    print(f"📊 Queries per platform: {max_queries_per_platform}")
    print(f"📊 Pages per query: {pages_per_query}")
    print(f"🔧 Engines: {engines}")
    print(f"⏱️ Delay between page requests: {request_delay}s")
    print("=" * 80)

    platform_cooldown = max(5.0, request_delay * 4)

    for platform_name in PLATFORMS.keys():
        print("\n" + "=" * 80)
        discover_platform(
            platform_name,
            max_queries=max_queries_per_platform,
            pages_per_query=pages_per_query,
            engines=engines,
            request_delay=request_delay,
        )
        print("=" * 80)
        time.sleep(platform_cooldown)

    print("\n" + "=" * 80)
    print("✅ All platforms discovered!")
    print("=" * 80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="SearXNG-based company discovery (self-hosted)"
    )
    parser.add_argument(
        "--platform",
        type=str.lower,
        choices=list(PLATFORMS.keys()) + ["all"],
        default="all",
        help="Platform to discover (default: all)",
    )
    parser.add_argument(
        "--max-queries",
        type=int,
        default=-1,
        help="Maximum queries to use (default: unlimited!)",
    )
    parser.add_argument(
        "--pages", type=int, default=20, help="Pages per query (default: 20)"
    )
    parser.add_argument(
        "--engines",
        type=str,
        default=DEFAULT_ENGINES,
        help=f"Search engines to use (default: {DEFAULT_ENGINES})",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY,
        help=f"Seconds to sleep between page requests (default: {DEFAULT_REQUEST_DELAY})",
    )

    args = parser.parse_args()

    if args.platform == "all":
        discover_all_platforms(
            max_queries_per_platform=args.max_queries,
            pages_per_query=args.pages,
            engines=args.engines,
            request_delay=args.delay,
        )
    else:
        discover_platform(
            args.platform,
            max_queries=args.max_queries,
            pages_per_query=args.pages,
            engines=args.engines,
            request_delay=args.delay,
        )
