"""
Enhanced Company Discovery Script
Finds more companies by using multiple search strategies beyond basic site: queries
"""

from serpapi import GoogleSearch
import pandas as pd
import re
import os
from typing import Set, List, Tuple
import time
from dotenv import load_dotenv

load_dotenv()

# Platform configurations
PLATFORMS = {
    "ashby": {
        "domains": ["jobs.ashbyhq.com"],
        "pattern": r"(https://jobs\.ashbyhq\.com/[^/?#]+)",
        "csv_column": "ashby_url",
        "output_file": "ashby/companies.csv"
    },
    "greenhouse": {
        "domains": ["job-boards.greenhouse.io", "boards.greenhouse.io"],
        "pattern": r"(https://(?:job-boards|boards)\.greenhouse\.io/[^/?#]+)",
        "csv_column": "greenhouse_url",
        "output_file": "greenhouse/greenhouse_companies.csv"
    },
    "lever": {
        "domains": ["jobs.lever.co"],
        "pattern": r"(https://jobs\.lever\.co/[^/?#]+)",
        "csv_column": "lever_url",
        "output_file": "lever/lever_companies.csv"
    },
    "workable": {
        "domains": ["apply.workable.com", "jobs.workable.com"],
        "pattern": [
            r"(https://apply\.workable\.com/[^/?#]+)",
            r"(https://jobs\.workable\.com/company/[^/?#]+/[^/?#]+)"
        ],
        "csv_column": "workable_url",
        "output_file": "workable/workable_companies.csv"
    }
}

# Search query strategies to find more companies
SEARCH_STRATEGIES = [
    # Basic site search
    lambda domain: f"site:{domain}",

    # Job-related searches
    lambda domain: f"site:{domain} careers",
    lambda domain: f"site:{domain} jobs",
    lambda domain: f"site:{domain} hiring",
    lambda domain: f"site:{domain} \"we're hiring\"",
    lambda domain: f"site:{domain} apply now",

    # Role-based searches (helps find niche companies)
    lambda domain: f"site:{domain} software engineer",
    lambda domain: f"site:{domain} product manager",
    lambda domain: f"site:{domain} data scientist",
    lambda domain: f"site:{domain} designer",
    lambda domain: f"site:{domain} sales",
    lambda domain: f"site:{domain} marketing",
    lambda domain: f"site:{domain} \"engineering\"",
    lambda domain: f"site:{domain} \"product\"",
    lambda domain: f"site:{domain} \"data\"",
    lambda domain: f"site:{domain} \"design\"",
    lambda domain: f"site:{domain} \"sales\"",
    lambda domain: f"site:{domain} \"marketing\"",

    # Remote/location searches
    lambda domain: f"site:{domain} remote",
    lambda domain: f"site:{domain} \"San Francisco\"",
    lambda domain: f"site:{domain} \"New York\"",
    lambda domain: f"site:{domain} \"London\"",
    lambda domain: f"site:{domain} \"Paris\"",
    lambda domain: f"site:{domain} \"Berlin\"",
    lambda domain: f"site:{domain} \"Amsterdam\"",
    lambda domain: f"site:{domain} \"Stockholm\"",
    lambda domain: f"site:{domain} \"Warsaw\"",
    lambda domain: f"site:{domain} \"Brussels\"",
    lambda domain: f"site:{domain} \"Zurich\"",
    lambda domain: f"site:{domain} \"Delhi\"",
    lambda domain: f"site:{domain} \"Mumbai\"",
    lambda domain: f"site:{domain} \"Bangalore\"",
    lambda domain: f"site:{domain} \"Chennai\"",
    lambda domain: f"site:{domain} \"Hyderabad\"",
    lambda domain: f"site:{domain} \"Pune\"",
    lambda domain: f"site:{domain} \"Kolkata\"",
    lambda domain: f"site:{domain} \"Jaipur\"",
    lambda domain: f"site:{domain} \"Singapore\"",
    lambda domain: f"site:{domain} \"Dubai\"",
    lambda domain: f"site:{domain} \"Tokyo\"",
    lambda domain: f"site:{domain} \"Seoul\"",
    lambda domain: f"site:{domain} \"Hong Kong\"",
    lambda domain: f"site:{domain} \"Toronto\"",
    lambda domain: f"site:{domain} \"Montreal\"",
    lambda domain: f"site:{domain} \"Vancouver\"",
    lambda domain: f"site:{domain} \"Sydney\"",

    lambda domain: f"site:{domain} \"Europe\"",
    lambda domain: f"site:{domain} \"Asia\"",
    lambda domain: f"site:{domain} \"Middle East\"",
    lambda domain: f"site:{domain} \"North America\"",
    lambda domain: f"site:{domain} \"South America\"",

    # Company type searches
    lambda domain: f"site:{domain} startup",
    lambda domain: f"site:{domain} YC OR \"Y Combinator\"",
    lambda domain: f"site:{domain} series A OR series B",
    lambda domain: f"site:{domain} \"tech startup\"",
    lambda domain: f"site:{domain} \"tech company\"",
]


def read_existing_urls(csv_file: str, column_name: str) -> Set[str]:
    """Read existing URLs from CSV file to avoid duplicates"""
    existing_urls = set()
    if os.path.exists(csv_file):
        try:
            df = pd.read_csv(csv_file)
            if column_name in df.columns:
                existing_urls = set(df[column_name].dropna().tolist())
                print(f"📖 Found {len(existing_urls)} existing URLs in {csv_file}")
            elif "url" in df.columns:
                # Handle legacy format
                existing_urls = set(df["url"].dropna().tolist())
                print(f"📖 Found {len(existing_urls)} existing URLs in {csv_file} (legacy format)")
        except Exception as e:
            print(f"⚠️  Error reading {csv_file}: {e}")
    return existing_urls


def extract_urls_from_link(link: str, patterns: List[str] | str, domains: List[str]) -> Set[str]:
    """Extract company URLs from a search result link"""
    urls = set()

    # Skip if link doesn't contain any of the target domains
    if not any(domain in link for domain in domains):
        return urls

    # Handle single pattern or list of patterns
    if isinstance(patterns, str):
        patterns = [patterns]

    for pattern in patterns:
        match = re.match(pattern, link)
        if match:
            urls.add(match.group(1))

    return urls


def fetch_urls_with_strategies(
    platform: str,
    domains: List[str],
    patterns: List[str] | str,
    pages_per_strategy: int = 10,
    max_strategies: int = None
) -> Set[str]:
    """Fetch URLs using multiple search strategies"""

    all_urls = set()
    api_key = os.getenv("SERPAPI_API_KEY")

    if not api_key:
        print("⚠️  SERPAPI_API_KEY not found in environment")
        return all_urls

    strategies_to_use = SEARCH_STRATEGIES[:max_strategies] if max_strategies else SEARCH_STRATEGIES

    print(f"\n🔍 Starting discovery for {platform}")
    print(f"📊 Using {len(strategies_to_use)} search strategies with {pages_per_strategy} pages each")

    for strategy_idx, strategy_func in enumerate(strategies_to_use, 1):
        # Try strategy with each domain
        for domain in domains:
            query = strategy_func(domain)
            print(f"\n[{strategy_idx}/{len(strategies_to_use)}] Query: {query}")

            strategy_urls = set()

            for page in range(pages_per_strategy):
                try:
                    params = {
                        "engine": "google_light",
                        "q": query,
                        "start": page * 10,
                        "api_key": api_key,
                    }

                    search = GoogleSearch(params)
                    results = search.get_dict()

                    organic_results = results.get("organic_results", [])

                    if not organic_results:
                        print(f"  Page {page + 1}: No more results")
                        break

                    page_urls = set()
                    for res in organic_results:
                        link = res.get("link")
                        if link:
                            extracted = extract_urls_from_link(link, patterns, domains)
                            page_urls.update(extracted)

                    new_in_page = page_urls - all_urls - strategy_urls
                    strategy_urls.update(page_urls)

                    print(f"  Page {page + 1}: +{len(new_in_page)} new ({len(page_urls)} total on page)")

                    # Small delay to avoid rate limiting
                    time.sleep(0.5)

                except Exception as e:
                    print(f"  ⚠️  Error on page {page + 1}: {e}")
                    continue

            new_from_strategy = strategy_urls - all_urls
            all_urls.update(strategy_urls)

            print(f"  Strategy total: +{len(new_from_strategy)} new URLs (cumulative: {len(all_urls)})")

            # Delay between domains
            time.sleep(1)

    return all_urls


def save_to_csv(urls: Set[str], existing_urls: Set[str], output_file: str, column_name: str):
    """Save URLs to CSV, handling duplicates with existing data"""

    # Combine new and existing URLs
    all_urls = existing_urls.union(urls)
    new_urls = urls - existing_urls

    print(f"\n📈 Results:")
    print(f"  🆕 New URLs found: {len(new_urls)}")
    print(f"  📊 Total unique URLs: {len(all_urls)}")
    print(f"  📈 Growth: +{len(new_urls)} ({len(new_urls)/len(existing_urls)*100:.1f}% increase)" if existing_urls else "")

    if new_urls:
        print(f"\n🎉 Sample of new URLs (showing first 20):")
        for url in sorted(new_urls)[:20]:
            print(f"  ✨ {url}")

        if len(new_urls) > 20:
            print(f"  ... and {len(new_urls) - 20} more")

    # Save to CSV
    df = pd.DataFrame({column_name: sorted(all_urls)})
    df.to_csv(output_file, index=False)
    print(f"\n✅ Saved {len(df)} companies to {output_file}")


def discover_platform(
    platform_name: str,
    pages_per_strategy: int = 10,
    max_strategies: int = None
):
    """Run enhanced discovery for a specific platform"""

    if platform_name not in PLATFORMS:
        print(f"❌ Unknown platform: {platform_name}")
        print(f"Available platforms: {', '.join(PLATFORMS.keys())}")
        return

    config = PLATFORMS[platform_name]

    # Read existing URLs
    existing_urls = read_existing_urls(config["output_file"], config["csv_column"])

    # Fetch new URLs using multiple strategies
    new_urls = fetch_urls_with_strategies(
        platform=platform_name,
        domains=config["domains"],
        patterns=config["pattern"],
        pages_per_strategy=pages_per_strategy,
        max_strategies=max_strategies
    )

    # Save results
    save_to_csv(new_urls, existing_urls, config["output_file"], config["csv_column"])


def discover_all_platforms(pages_per_strategy: int = 10, max_strategies: int = 5):
    """Run enhanced discovery for all platforms"""

    print("=" * 80)
    print("🚀 Enhanced Company Discovery - All Platforms")
    print("=" * 80)

    for platform_name in PLATFORMS.keys():
        print("\n" + "=" * 80)
        discover_platform(platform_name, pages_per_strategy, max_strategies)
        print("=" * 80)

        # Delay between platforms to be respectful to SERP API
        time.sleep(2)

    print("\n" + "=" * 80)
    print("✅ All platforms discovered!")
    print("=" * 80)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Enhanced company discovery across ATS platforms")
    parser.add_argument(
        "--platform",
        choices=list(PLATFORMS.keys()) + ["all"],
        default="all",
        help="Platform to discover (default: all)"
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=10,
        help="Pages per search strategy (default: 10)"
    )
    parser.add_argument(
        "--strategies",
        type=int,
        default=5,
        help="Max number of search strategies to use (default: 5, max: 55)"
    )

    args = parser.parse_args()

    if args.platform == "all":
        discover_all_platforms(pages_per_strategy=args.pages, max_strategies=args.strategies)
    else:
        discover_platform(args.platform, pages_per_strategy=args.pages, max_strategies=args.strategies)
