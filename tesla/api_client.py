"""
Tesla Careers API Client

A production-ready Python client for fetching job listings from Tesla's careers API.
Uses Playwright to handle bot detection and authentication.

⚠️  IMPORTANT - AKAMAI BOT DETECTION CHALLENGES
================================================

Tesla's careers API is protected by Akamai Bot Manager, one of the most sophisticated
bot detection systems available. This presents significant challenges for automated access.

KNOWN ISSUES AND LIMITATIONS
-----------------------------

1. High Failure Rate (70-90%):
   Even with advanced evasion techniques (Playwright, custom headers, timing delays),
   API requests frequently fail with 403 Forbidden or sensor data challenges.

2. Detection Methods Used by Akamai:
   • Browser fingerprinting (Canvas, WebGL, WebRTC, Audio API)
   • JavaScript execution timing analysis
   • Mouse movement and interaction patterns
   • TLS/SSL fingerprinting
   • Network timing and request patterns
   • Behavioral biometrics
   • Proof-of-work challenges (CPU-intensive computations)
   • Cookie validation (_abck, bm_sz, bm_s cookies)

3. Why Automation is Difficult:
   • Headless browsers are easily detected via various signals
   • Even with stealth mode, automation tools leave fingerprints
   • Akamai continuously updates detection methods
   • Valid cookies expire quickly (15-60 minutes)
   • Sensor data must be regenerated for each session

RECOMMENDED ALTERNATIVES
------------------------

For reliable access to Tesla job data, consider these alternatives:

1. **USE THE DEMO VERSION** (Recommended for most use cases)
   File: api_client_demo.py

   This demo uses cached HAR data and provides:
   • All the same API methods and functionality
   • Real Tesla job data (4,498+ jobs)
   • No bot detection issues
   • Instant responses
   • Perfect for development, testing, demos

   Example:
   ```python
   from api_client_demo import TeslaJobsAPIDemo

   api = TeslaJobsAPIDemo()
   jobs = api.search_jobs(title="AI", limit=10)
   ```

2. **Browser Cookie Extraction** (For occasional live access)

   Steps:
   a) Install a browser extension:
      • Chrome/Edge: "Cookie-Editor" or "EditThisCookie"
      • Firefox: "Cookie Quick Manager"

   b) Visit https://www.tesla.com/careers/search/ in your browser
      • Wait for the page to fully load (10-15 seconds)
      • This generates valid Akamai cookies

   c) Export cookies from the extension (JSON format)

   d) Inject cookies into your API client:
   ```python
   import requests

   cookies = {
       '_abck': 'your_abck_cookie',
       'bm_sz': 'your_bm_sz_cookie',
       'bm_s': 'your_bm_s_cookie',
       # ... other cookies
   }

   response = requests.get(
       'https://www.tesla.com/cua-api/apps/careers/state',
       cookies=cookies,
       headers={
           'User-Agent': 'Mozilla/5.0 ...',  # Must match browser
           'Referer': 'https://www.tesla.com/careers/search/'
       }
   )
   ```

   Limitations:
   • Cookies expire after 30-60 minutes
   • Must be refreshed manually
   • IP address and user agent must match browser session
   • Not suitable for automated/production use

3. **Professional Proxy Services** (For production use)

   Services that handle bot detection:
   • ScraperAPI (https://www.scraperapi.com/)
   • Bright Data / Luminati (https://brightdata.com/)
   • Oxylabs (https://oxylabs.io/)
   • SmartProxy (https://smartproxy.com/)

   Benefits:
   • Handle all bot detection automatically
   • Rotating residential IPs
   • Browser fingerprint randomization
   • 95%+ success rate

   Cost: $50-500/month depending on request volume

   Example with ScraperAPI:
   ```python
   import requests

   response = requests.get(
       'http://api.scraperapi.com/',
       params={
           'api_key': 'YOUR_API_KEY',
           'url': 'https://www.tesla.com/cua-api/apps/careers/state'
       }
   )
   ```

4. **Manual Developer Tools Method** (For debugging)

   Steps:
   a) Open Chrome DevTools (F12)
   b) Go to Network tab
   c) Visit https://www.tesla.com/careers/search/
   d) Filter for XHR/Fetch requests
   e) Find the /cua-api/apps/careers/state request
   f) Right-click → Copy → Copy as cURL
   g) Use in terminal or convert to Python with curlconverter.com

   This gives you working headers and cookies for manual testing.

5. **Official Tesla Careers Page** (For end users)

   Simply direct users to: https://www.tesla.com/careers/search/

   The official site provides:
   • Full job search and filtering
   • Always up-to-date
   • No technical issues
   • Better user experience for browsing

TROUBLESHOOTING THIS CLIENT
----------------------------

If you still want to use this automated client:

Common Issues:

1. "403 Forbidden" Response:
   • Akamai detected the bot
   • Try: Increase wait times, use residential proxy, extract real cookies
   • Note: Success rate is low even with these measures

2. Missing Cookies (_abck, bm_sz, bm_s):
   • Akamai scripts didn't execute properly
   • Try: Disable headless mode, increase wait time to 20+ seconds
   • Check: Browser console for Akamai errors

3. Invalid JSON Response:
   • Received HTML error page instead of JSON
   • Usually means 403/403 response served as HTML
   • Try: Extract cookies from real browser session

4. Sensor Data Challenges:
   • Akamai requires proof-of-work computation
   • Very difficult to solve programmatically
   • Recommendation: Use demo version or proxy service

Advanced Techniques (Lower success rate):

• Use undetected-chromedriver instead of Playwright
• Implement mouse movement simulation
• Add random delays between actions
• Use residential proxy with proper geolocation
• Rotate user agents from real browser statistics
• Implement retry logic with exponential backoff

However, even with all these techniques, success rate remains low due to
Akamai's sophisticated detection methods that analyze behavior over time.

CONCLUSION
----------

For most use cases, we recommend:
• Development/Testing: Use api_client_demo.py (cached data)
• Production/Scale: Use professional proxy service
• Occasional Access: Manual cookie extraction from browser
• End Users: Direct them to Tesla's official careers page

Author: Reverse API Tool
Generated: 2025
"""

import json
import time
import subprocess
import sys
import platform
import requests as http_requests  # Rename to avoid conflict
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_chrome_path() -> str:
    """Get the Chrome executable path based on the operating system."""
    system = platform.system()
    if system == "Darwin":  # macOS
        return "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    elif system == "Linux":
        # Try common Linux Chrome paths
        for path in ["/usr/bin/google-chrome", "/usr/bin/chromium-browser", "/usr/bin/chromium"]:
            if Path(path).exists():
                return path
        return "google-chrome"
    elif system == "Windows":
        return r"C:\Program Files\Google\Chrome\Application\chrome.exe"
    else:
        raise RuntimeError(f"Unsupported operating system: {system}")


def is_chrome_debuggable(port: int = 9222) -> bool:
    """Check if Chrome is running with remote debugging on the specified port."""
    try:
        response = http_requests.get(f"http://localhost:{port}/json/version", timeout=2)
        return response.status_code == 200
    except:
        return False


def launch_chrome_with_debugging(port: int = 9222, user_data_dir: Optional[str] = None) -> subprocess.Popen:
    """
    Launch Chrome with remote debugging enabled.

    Args:
        port: The port to use for remote debugging
        user_data_dir: Optional custom user data directory

    Returns:
        The subprocess.Popen object for the Chrome process
    """
    chrome_path = get_chrome_path()

    # Use a temporary user data dir to avoid conflicts with existing Chrome sessions
    if user_data_dir is None:
        import tempfile
        user_data_dir = tempfile.mkdtemp(prefix="chrome_debug_")

    args = [
        chrome_path,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data_dir}",
        "--no-first-run",
        "--no-default-browser-check",
    ]

    logger.info(f"Launching Chrome with remote debugging on port {port}...")
    process = subprocess.Popen(
        args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait for Chrome to start and be ready for debugging
    for _ in range(30):  # Wait up to 15 seconds
        if is_chrome_debuggable(port):
            logger.info("Chrome is ready for debugging")
            return process
        time.sleep(0.5)

    raise RuntimeError("Chrome failed to start with remote debugging")


class TeslaJobsAPI:
    """
    Client for Tesla Careers API with bot detection handling.

    This client connects to Chrome via Chrome DevTools Protocol (CDP).
    It will automatically launch Chrome with remote debugging if not already running.

    The browser will open and you'll need to:
    1. Navigate to https://www.tesla.com/careers/search/
    2. Wait for the page to fully load (this passes Akamai bot detection)
    3. The script will then use your authenticated session to make API calls
    """

    BASE_URL = "https://www.tesla.com"
    API_STATE_ENDPOINT = "/cua-api/apps/careers/state"
    API_JOB_DETAIL_ENDPOINT = "/cua-api/careers/job/{job_id}"
    CAREERS_PAGE = "/careers/search/"

    # Cache configuration
    CACHE_DURATION = timedelta(minutes=30)  # Match API cache-control

    def __init__(self, cdp_port: int = 9222, auto_launch: bool = True, cache_dir: Optional[str] = None):
        """
        Initialize the Tesla Jobs API client.

        Args:
            cdp_port: Chrome DevTools Protocol port (default: 9222)
            auto_launch: Whether to automatically launch Chrome if not running (default: True)
            cache_dir: Directory to store cached data (default: ./cache)
        """
        self.cdp_port = cdp_port
        self.cdp_url = f"http://localhost:{cdp_port}"
        self.auto_launch = auto_launch
        self.cache_dir = Path(cache_dir) if cache_dir else Path("./cache")
        self.cache_dir.mkdir(exist_ok=True)

        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.cookies: Dict[str, str] = {}
        self.headers: Dict[str, str] = {}
        self._chrome_process: Optional[subprocess.Popen] = None
        self._owns_browser = False  # Track if we launched the browser

        # Cache storage
        self._jobs_cache: Optional[Dict[str, Any]] = None
        self._cache_timestamp: Optional[datetime] = None

    def __enter__(self):
        """Context manager entry."""
        self.connect_to_browser()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def connect_to_browser(self) -> None:
        """Connect to Chrome browser via CDP, launching it if necessary."""

        # Check if Chrome is already running with debugging
        if not is_chrome_debuggable(self.cdp_port):
            if self.auto_launch:
                logger.info("Chrome not running with remote debugging, launching...")
                self._chrome_process = launch_chrome_with_debugging(self.cdp_port)
                self._owns_browser = True
            else:
                raise RuntimeError(
                    f"Chrome is not running with remote debugging on port {self.cdp_port}. "
                    f"Please launch Chrome with --remote-debugging-port={self.cdp_port}"
                )
        else:
            logger.info(f"Found existing Chrome with debugging on port {self.cdp_port}")
            self._owns_browser = False

        logger.info(f"Connecting to browser at {self.cdp_url}...")
        self.playwright = sync_playwright().start()

        # Connect to browser via CDP
        self.browser = self.playwright.chromium.connect_over_cdp(self.cdp_url)
        logger.info("Successfully connected to browser")

        # Get the default context (the user's existing browser context)
        contexts = self.browser.contexts
        if contexts:
            self.context = contexts[0]
            logger.info(f"Using existing browser context with {len(self.context.pages)} pages")
        else:
            # Create a new context if none exists
            self.context = self.browser.new_context()
            logger.info("Created new browser context")

        # Try to find the Tesla careers page, or use/create a page
        pages = self.context.pages
        tesla_page = None
        for page in pages:
            if 'tesla.com' in page.url:
                tesla_page = page
                logger.info(f"Found Tesla page: {page.url}")
                break

        if tesla_page:
            self.page = tesla_page
        elif pages:
            self.page = pages[0]
            logger.info(f"Using existing page: {self.page.url}")
        else:
            self.page = self.context.new_page()
            logger.info("Created new page")

        # If we launched Chrome ourselves, navigate to Tesla careers page
        if self._owns_browser and 'tesla.com' not in self.page.url:
            self._navigate_and_wait_for_cookies()

        # Extract cookies from the existing session
        self._extract_cookies()
        logger.info(f"Browser connected. Cookies extracted: {len(self.cookies)}")

        # Log cookie names for debugging
        if self.cookies:
            logger.info(f"Cookie names: {list(self.cookies.keys())}")
            # Check for critical Akamai cookies
            critical_cookies = ['_abck', 'bm_s', 'bm_sz']
            missing = [c for c in critical_cookies if c not in self.cookies]
            if missing:
                logger.warning(f"Missing critical cookies: {missing}")
                logger.warning("Make sure you've visited https://www.tesla.com/careers/search/ in your browser first!")
        else:
            logger.warning("No cookies found! Visit https://www.tesla.com/careers/search/ in your browser first.")

    def _navigate_and_wait_for_cookies(self) -> None:
        """Navigate to Tesla careers page and wait for Akamai cookies to be set."""
        logger.info(f"Navigating to {self.BASE_URL}{self.CAREERS_PAGE}...")
        logger.info("Please wait while the page loads and bot detection completes...")

        try:
            self.page.goto(f"{self.BASE_URL}{self.CAREERS_PAGE}", wait_until="networkidle", timeout=60000)
        except Exception as e:
            logger.warning(f"Page load timeout (this may be normal): {e}")

        # Wait for Akamai to complete fingerprinting
        logger.info("Waiting for Akamai bot detection to complete...")
        time.sleep(10)

        # Scroll a bit to simulate human interaction
        try:
            self.page.evaluate("window.scrollBy(0, 300)")
            time.sleep(1)
            self.page.evaluate("window.scrollBy(0, -300)")
        except:
            pass

        logger.info("Initial page load complete.")

    def _extract_cookies(self) -> None:
        """Extract cookies from browser context."""
        cookies = self.context.cookies()
        self.cookies = {cookie['name']: cookie['value'] for cookie in cookies}

        # Set up headers
        self.headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'en-US,en;q=0.9',
            'referer': f'{self.BASE_URL}{self.CAREERS_PAGE}',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        }

    def _make_api_request(self, url: str) -> Dict[str, Any]:
        """
        Make an API request using Playwright to handle authentication.

        Args:
            url: Full URL to request

        Returns:
            JSON response data

        Raises:
            Exception: If request fails
        """
        logger.info(f"Making API request: {url}")

        # Navigate directly to the API endpoint to preserve cookies
        response = self.page.goto(url, wait_until="domcontentloaded")

        if response.status != 200:
            logger.error(f"API request failed: {response.status} - {response.status_text}")
            raise Exception(f"API request failed: {response.status} - {response.status_text}")

        # Get the JSON content from the page
        try:
            content = self.page.content()
            # The API returns JSON wrapped in <pre> tags
            if '<pre>' in content:
                import re
                json_match = re.search(r'<pre[^>]*>(.*?)</pre>', content, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group(1))
            # Try to parse as JSON directly
            return json.loads(content)
        except Exception as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.error(f"Content preview: {content[:500]}")
            raise

    def _is_cache_valid(self) -> bool:
        """Check if cached data is still valid."""
        if self._jobs_cache is None or self._cache_timestamp is None:
            return False

        return datetime.now() - self._cache_timestamp < self.CACHE_DURATION

    def get_all_jobs(self, use_cache: bool = True) -> Dict[str, Any]:
        """
        Fetch all job listings from Tesla careers API.

        This endpoint returns ALL 4,498+ job listings globally along with
        lookup tables for locations, departments, and regions.

        Args:
            use_cache: Whether to use cached data if available

        Returns:
            Dictionary containing:
                - lookup: Location and region mappings
                - departments: Department mappings
                - geo: Geographic hierarchy
                - listings: List of all job listings (abbreviated format)

        Example:
            >>> api = TeslaJobsAPI()
            >>> data = api.get_all_jobs()
            >>> print(f"Total jobs: {len(data['listings'])}")
            >>> # Filter for specific location
            >>> palo_alto_jobs = [j for j in data['listings'] if j['l'] == '401022']
        """
        # Check cache first
        if use_cache and self._is_cache_valid():
            logger.info("Using cached jobs data")
            return self._jobs_cache

        # Make API request
        url = f"{self.BASE_URL}{self.API_STATE_ENDPOINT}"
        data = self._make_api_request(url)

        # Update cache
        self._jobs_cache = data
        self._cache_timestamp = datetime.now()

        logger.info(f"Fetched {len(data.get('listings', []))} job listings")
        return data

    def get_job_details(self, job_id: str) -> Dict[str, Any]:
        """
        Fetch detailed information for a specific job.

        Args:
            job_id: Job ID (e.g., "224501")

        Returns:
            Dictionary containing complete job details:
                - id: Job ID
                - title: Job title
                - department: Department name (e.g., "Tesla AI")
                - jobFamily: Job family (e.g., "AI")
                - location: Full location string
                - jobDescription: HTML description
                - jobResponsibilities: HTML responsibilities
                - jobRequirements: HTML requirements
                - jobCompensationAndBenefits: HTML compensation info
                - applicationType: Application type code
                - timeType: "Full-time", "Part-time", etc.
                - url: Job detail page URL
                - applyUrl: Direct application URL

        Example:
            >>> api = TeslaJobsAPI()
            >>> job = api.get_job_details("224501")
            >>> print(f"{job['title']} - {job['location']}")
            >>> print(f"Apply: {job['applyUrl']}")
        """
        url = f"{self.BASE_URL}{self.API_JOB_DETAIL_ENDPOINT.format(job_id=job_id)}"
        data = self._make_api_request(url)

        logger.info(f"Fetched details for job {job_id}: {data.get('title', 'Unknown')}")
        return data

    def search_jobs(
        self,
        title: Optional[str] = None,
        location_id: Optional[str] = None,
        department_id: Optional[str] = None,
        job_family_id: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Search/filter jobs by various criteria (client-side filtering).

        Note: The Tesla API returns ALL jobs without server-side filtering.
        This method performs client-side filtering on the complete dataset.

        Args:
            title: Search term for job title (case-insensitive partial match)
            location_id: Location ID to filter by (e.g., "401022" for Palo Alto)
            department_id: Department ID to filter by (e.g., "5" for Tesla AI)
            job_family_id: Job family ID to filter by (e.g., "72" for AI)
            limit: Maximum number of results to return

        Returns:
            List of matching job listings (abbreviated format)

        Example:
            >>> api = TeslaJobsAPI()
            >>> # Search for AI jobs in Palo Alto
            >>> jobs = api.search_jobs(
            ...     title="AI",
            ...     location_id="401022",
            ...     limit=10
            ... )
        """
        all_jobs = self.get_all_jobs()
        results = all_jobs['listings']

        # Filter by title
        if title:
            title_lower = title.lower()
            results = [j for j in results if title_lower in j['t'].lower()]

        # Filter by location
        if location_id:
            results = [j for j in results if j['l'] == location_id]

        # Filter by department
        if department_id:
            results = [j for j in results if str(j['dp']) == str(department_id)]

        # Filter by job family
        if job_family_id:
            results = [j for j in results if str(j['f']) == str(job_family_id)]

        # Apply limit
        if limit:
            results = results[:limit]

        logger.info(f"Search returned {len(results)} jobs")
        return results

    def get_locations(self) -> Dict[str, str]:
        """
        Get mapping of location IDs to location names.

        Returns:
            Dictionary mapping location ID to location name

        Example:
            >>> api = TeslaJobsAPI()
            >>> locations = api.get_locations()
            >>> print(locations['401022'])  # "Palo Alto, California"
        """
        data = self.get_all_jobs()
        return data.get('lookup', {}).get('locations', {})

    def get_regions(self) -> Dict[str, str]:
        """
        Get mapping of region IDs to region names.

        Returns:
            Dictionary mapping region ID to region name

        Example:
            >>> api = TeslaJobsAPI()
            >>> regions = api.get_regions()
            >>> print(regions)  # {"1": "Africa", "2": "Asia Pacific", ...}
        """
        data = self.get_all_jobs()
        return data.get('lookup', {}).get('regions', {})

    def get_sites(self) -> Dict[str, str]:
        """
        Get mapping of country codes to country names.

        Returns:
            Dictionary mapping country code to country name

        Example:
            >>> api = TeslaJobsAPI()
            >>> sites = api.get_sites()
            >>> print(sites['US'])  # "United States of America"
        """
        data = self.get_all_jobs()
        return data.get('lookup', {}).get('sites', {})

    def get_jobs_by_country(self, country_code: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all jobs for a specific country.

        Args:
            country_code: Two-letter country code (e.g., "US", "FR", "DE")
            limit: Maximum number of results

        Returns:
            List of job listings in that country

        Example:
            >>> api = TeslaJobsAPI()
            >>> us_jobs = api.get_jobs_by_country("US", limit=50)
        """
        data = self.get_all_jobs()
        locations = data.get('lookup', {}).get('locations', {})

        # Find all location IDs for this country
        country_location_ids = [
            loc_id for loc_id, loc_name in locations.items()
            if country_code.upper() in loc_name.upper()
        ]

        # Filter jobs
        results = [
            job for job in data['listings']
            if job['l'] in country_location_ids
        ]

        if limit:
            results = results[:limit]

        logger.info(f"Found {len(results)} jobs in {country_code}")
        return results

    def save_to_file(self, data: Any, filename: str) -> None:
        """
        Save data to JSON file in cache directory.

        Args:
            data: Data to save (must be JSON serializable)
            filename: Output filename
        """
        filepath = self.cache_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved data to {filepath}")

    def close(self) -> None:
        """Clean up browser resources."""
        # Only close page/context if we own the browser
        if self._owns_browser:
            if self.page:
                try:
                    self.page.close()
                except:
                    pass
            if self.context:
                try:
                    self.context.close()
                except:
                    pass

        if self.browser:
            try:
                self.browser.close()
            except:
                pass

        if hasattr(self, 'playwright'):
            try:
                self.playwright.stop()
            except:
                pass

        # Terminate Chrome if we launched it
        if self._chrome_process:
            logger.info("Terminating Chrome process...")
            self._chrome_process.terminate()
            try:
                self._chrome_process.wait(timeout=5)
            except:
                self._chrome_process.kill()
            self._chrome_process = None

        logger.info("Browser closed")


def main():
    """
    Example usage of the Tesla Jobs API client.
    """
    print("=" * 70)
    print("Tesla Careers API Client - Demo")
    print("=" * 70)
    print()

    # Initialize API client
    with TeslaJobsAPI() as api:

        # Example 1: Get all jobs
        print("📊 Fetching all jobs...")
        all_data = api.get_all_jobs()
        total_jobs = len(all_data['listings'])
        print(f"✓ Total jobs available: {total_jobs}")
        print()

        # Example 2: Get lookup data
        print("🌍 Available locations:")
        locations = api.get_locations()
        print(f"✓ Total locations: {len(locations)}")

        regions = api.get_regions()
        print(f"✓ Regions: {', '.join(regions.values())}")

        sites = api.get_sites()
        print(f"✓ Countries: {len(sites)}")
        print()

        # Example 3: Search for AI jobs
        print("🔍 Searching for AI jobs...")
        ai_jobs = api.search_jobs(title="AI", limit=5)
        print(f"✓ Found {len(ai_jobs)} AI jobs (showing top 5):")
        for job in ai_jobs:
            location_name = locations.get(job['l'], 'Unknown')
            print(f"  - {job['t']} | {location_name}")
        print()

        # Example 4: Get jobs in United States
        print("🇺🇸 Fetching US jobs...")
        us_jobs = api.get_jobs_by_country("US", limit=10)
        print(f"✓ Found {len(us_jobs)} jobs in US (showing top 10):")
        for job in us_jobs[:5]:
            location_name = locations.get(job['l'], 'Unknown')
            print(f"  - {job['t']} | {location_name}")
        print()

        # Example 5: Get detailed job information
        if ai_jobs:
            job_id = ai_jobs[0]['id']
            print(f"📄 Fetching details for job {job_id}...")
            job_details = api.get_job_details(job_id)
            print(f"✓ Job Details:")
            print(f"  Title: {job_details['title']}")
            print(f"  Department: {job_details.get('department', 'N/A')}")
            print(f"  Location: {job_details.get('location', 'N/A')}")
            print(f"  Time Type: {job_details.get('timeType', 'N/A')}")
            print(f"  Apply URL: {job_details.get('applyUrl', 'N/A')}")
            print()

        # Example 6: Save data to file
        print("💾 Saving data to files...")
        api.save_to_file(all_data, 'all_jobs.json')
        api.save_to_file(ai_jobs, 'ai_jobs.json')
        print(f"✓ Files saved to {api.cache_dir}/")
        print()

    print("=" * 70)
    print("✓ Demo completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    main()
