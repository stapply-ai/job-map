"""
Uber Careers API Client

This module provides a clean interface to interact with Uber's careers/jobs API.
Reverse-engineered from HAR file analysis.

Author: Auto-generated from HAR analysis
Date: 2025-12-22
"""

import requests
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TimeType(Enum):
    """Job time type options"""
    FULL_TIME = "Full-Time"
    PART_TIME = "Part-Time"


@dataclass
class Location:
    """Represents a job location"""
    country: str
    country_name: str
    region: Optional[str] = None
    city: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Location':
        """Create Location from API response dict"""
        return cls(
            country=data.get('country', ''),
            country_name=data.get('countryName', ''),
            region=data.get('region'),
            city=data.get('city')
        )


@dataclass
class Job:
    """Represents a job posting"""
    id: int
    title: str
    description: str
    url: str
    location: Location
    department: str
    team: str
    level: str
    time_type: str
    creation_date: str
    updated_date: str
    status_name: str
    featured: bool = False
    type: str = ""
    program_and_platform: str = ""
    unique_skills: str = ""
    all_locations: List[Location] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Job':
        """Create Job from API response dict"""
        location = Location.from_dict(data.get('location', {}))
        all_locations = [Location.from_dict(loc) for loc in data.get('allLocations', [])]

        # Construct the job URL
        job_url = f"https://www.uber.com/global/en/careers/list/{data.get('id', '')}/"

        return cls(
            id=data.get('id', 0),
            title=data.get('title', ''),
            description=data.get('description', ''),
            url=job_url,
            location=location,
            department=data.get('department', ''),
            team=data.get('team', ''),
            level=data.get('level', ''),
            time_type=data.get('timeType', ''),
            creation_date=data.get('creationDate', ''),
            updated_date=data.get('updatedDate', ''),
            status_name=data.get('statusName', ''),
            featured=data.get('featured', False),
            type=data.get('type', ''),
            program_and_platform=data.get('programAndPlatform', ''),
            unique_skills=data.get('uniqueSkills', ''),
            all_locations=all_locations
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert Job to dictionary"""
        return {
            'id': self.id,
            'title': self.title,
            'description': self.description,
            'url': self.url,
            'location': {
                'country': self.location.country,
                'countryName': self.location.country_name,
                'region': self.location.region,
                'city': self.location.city
            },
            'department': self.department,
            'team': self.team,
            'level': self.level,
            'timeType': self.time_type,
            'creationDate': self.creation_date,
            'updatedDate': self.updated_date,
            'statusName': self.status_name,
            'featured': self.featured
        }


@dataclass
class FilterOptions:
    """Available filter options from the API"""
    locations: List[Location]
    departments: Dict[str, List[str]]
    line_of_business: List[str]
    time_types: List[str]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FilterOptions':
        """Create FilterOptions from API response"""
        locations = [Location.from_dict(loc) for loc in data.get('location', [])]
        return cls(
            locations=locations,
            departments=data.get('department', {}),
            line_of_business=data.get('lineOfBusinessName', []),
            time_types=data.get('timeType', [])
        )


class UberCareersAPI:
    """
    Client for interacting with Uber's Careers API.

    This class provides methods to search for jobs and retrieve filter options.
    No authentication is required for these public endpoints.
    """

    BASE_URL = "https://www.uber.com/api"
    DEFAULT_LOCALE = "en"

    def __init__(
        self,
        locale_code: str = DEFAULT_LOCALE,
        timeout: int = 30,
        rate_limit_delay: float = 0.5
    ):
        """
        Initialize the Uber Careers API client.

        Args:
            locale_code: Locale code for responses (default: "en")
            timeout: Request timeout in seconds (default: 30)
            rate_limit_delay: Delay between requests in seconds (default: 0.5)
        """
        self.locale_code = locale_code
        self.timeout = timeout
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()

        # Set default headers based on HAR analysis
        self.session.headers.update({
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/json',
            'Origin': 'https://www.uber.com',
            'Referer': 'https://www.uber.com/us/en/careers/',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'x-csrf-token': 'x',
            'x-uber-sites-page-edge-cache-enabled': 'true'
        })

    def _make_request(
        self,
        endpoint: str,
        method: str = 'POST',
        json_data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Make an API request with error handling.

        Args:
            endpoint: API endpoint path
            method: HTTP method (GET or POST)
            json_data: JSON body for POST requests
            params: Query parameters

        Returns:
            API response as dictionary

        Raises:
            requests.RequestException: If the request fails
        """
        url = f"{self.BASE_URL}/{endpoint}"

        if params is None:
            params = {}
        params['localeCode'] = self.locale_code

        try:
            logger.info(f"Making {method} request to {url}")

            if method.upper() == 'POST':
                response = self.session.post(
                    url,
                    json=json_data or {},
                    params=params,
                    timeout=self.timeout
                )
            else:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout
                )

            response.raise_for_status()

            # Rate limiting
            time.sleep(self.rate_limit_delay)

            return response.json()

        except requests.exceptions.Timeout:
            logger.error(f"Request timed out after {self.timeout} seconds")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            logger.error(f"Response: {e.response.text if e.response else 'No response'}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    def get_filter_options(self) -> FilterOptions:
        """
        Retrieve available filter options for job searches.

        Returns:
            FilterOptions object containing all available filters

        Example:
            >>> api = UberCareersAPI()
            >>> filters = api.get_filter_options()
            >>> print(f"Available locations: {len(filters.locations)}")
        """
        logger.info("Fetching filter options")

        response = self._make_request('loadFilterOptions', json_data={})

        if response.get('status') != 'success':
            raise ValueError(f"API returned error status: {response}")

        return FilterOptions.from_dict(response.get('data', {}))

    def search_jobs(
        self,
        limit: int = 10,
        page: int = 0,
        departments: Optional[List[str]] = None,
        line_of_business: Optional[List[str]] = None,
        locations: Optional[List[Dict[str, str]]] = None,
        programs: Optional[List[str]] = None,
        teams: Optional[List[str]] = None
    ) -> tuple[List[Job], int]:
        """
        Search for jobs with optional filters.

        Args:
            limit: Number of results per page (default: 10)
            page: Page number, 0-indexed (default: 0)
            departments: List of department names to filter by
            line_of_business: List of business lines to filter by
            locations: List of location dicts with 'country', 'region', 'city' keys
            programs: List of program/platform names to filter by
            teams: List of team names to filter by

        Returns:
            Tuple of (list of Job objects, total number of results)

        Example:
            >>> api = UberCareersAPI()
            >>> jobs, total = api.search_jobs(
            ...     limit=20,
            ...     locations=[{'country': 'USA', 'city': 'San Francisco'}]
            ... )
            >>> print(f"Found {total} jobs, showing first {len(jobs)}")
        """
        logger.info(f"Searching jobs: page={page}, limit={limit}")

        request_body = {
            'limit': limit,
            'page': page,
            'params': {
                'department': departments or [],
                'lineOfBusinessName': line_of_business or [],
                'location': locations or [],
                'programAndPlatform': programs or [],
                'team': teams or []
            }
        }

        response = self._make_request('loadSearchJobsResults', json_data=request_body)

        if response.get('status') != 'success':
            raise ValueError(f"API returned error status: {response}")

        data = response.get('data', {})
        results = data.get('results', [])
        total_results = data.get('totalResults', {})

        # Handle totalResults which can be a dict with 'low', 'high', 'unsigned' fields
        if isinstance(total_results, dict):
            total = total_results.get('low', 0)
        else:
            total = total_results

        jobs = [Job.from_dict(job_data) for job_data in results]

        logger.info(f"Retrieved {len(jobs)} jobs out of {total} total")

        return jobs, total

    def get_all_jobs(
        self,
        departments: Optional[List[str]] = None,
        line_of_business: Optional[List[str]] = None,
        locations: Optional[List[Dict[str, str]]] = None,
        programs: Optional[List[str]] = None,
        teams: Optional[List[str]] = None,
        max_results: Optional[int] = None,
        page_size: int = 50
    ) -> List[Job]:
        """
        Retrieve all jobs matching the given filters by paginating through results.

        Args:
            departments: List of department names to filter by
            line_of_business: List of business lines to filter by
            locations: List of location dicts
            programs: List of program/platform names to filter by
            teams: List of team names to filter by
            max_results: Maximum number of results to fetch (default: all)
            page_size: Number of results per page (default: 50)

        Returns:
            List of all Job objects matching filters

        Example:
            >>> api = UberCareersAPI()
            >>> all_jobs = api.get_all_jobs(
            ...     departments=['Engineering'],
            ...     max_results=100
            ... )
            >>> print(f"Retrieved {len(all_jobs)} engineering jobs")
        """
        logger.info("Fetching all jobs with pagination")

        all_jobs = []
        page = 0

        while True:
            jobs, total = self.search_jobs(
                limit=page_size,
                page=page,
                departments=departments,
                line_of_business=line_of_business,
                locations=locations,
                programs=programs,
                teams=teams
            )

            all_jobs.extend(jobs)

            # Check if we've fetched all results or reached max_results
            if len(jobs) < page_size:
                logger.info("No more results available")
                break

            if max_results and len(all_jobs) >= max_results:
                logger.info(f"Reached max_results limit: {max_results}")
                all_jobs = all_jobs[:max_results]
                break

            if len(all_jobs) >= total:
                logger.info("Fetched all available results")
                break

            page += 1
            logger.info(f"Fetching page {page + 1}...")

        logger.info(f"Total jobs retrieved: {len(all_jobs)}")
        return all_jobs


def main():
    """
    Example usage of the Uber Careers API client.
    """
    # Initialize the API client
    api = UberCareersAPI()

    # Example 1: Get available filter options
    print("=" * 80)
    print("Example 1: Fetching filter options")
    print("=" * 80)

    try:
        filters = api.get_filter_options()
        print(f"Available locations: {len(filters.locations)}")
        print(f"Sample locations: {filters.locations[:3]}")
        print(f"\nAvailable departments: {list(filters.departments.keys())[:5]}")
        print(f"\nLine of business options: {filters.line_of_business}")
    except Exception as e:
        logger.error(f"Failed to fetch filter options: {e}")

    # Example 2: Search for jobs in San Francisco
    print("\n" + "=" * 80)
    print("Example 2: Searching for jobs in San Francisco")
    print("=" * 80)

    try:
        jobs, total = api.search_jobs(
            limit=5,
            locations=[{'country': 'USA', 'region': 'California', 'city': 'San Francisco'}]
        )

        print(f"Found {total} total jobs in San Francisco")
        print(f"\nShowing first {len(jobs)} jobs:")

        for job in jobs:
            print(f"\n- {job.title}")
            print(f"  Department: {job.department}")
            print(f"  Team: {job.team}")
            print(f"  Location: {job.location.city}, {job.location.region}")
            print(f"  URL: {job.url}")
    except Exception as e:
        logger.error(f"Failed to search jobs: {e}")

    # Example 3: Get all Engineering jobs
    print("\n" + "=" * 80)
    print("Example 3: Fetching all Engineering jobs (limited to 20)")
    print("=" * 80)

    try:
        engineering_jobs = api.get_all_jobs(
            departments=['Engineering'],
            max_results=20,
            page_size=10
        )

        print(f"Retrieved {len(engineering_jobs)} Engineering jobs")

        # Group by team
        teams = {}
        for job in engineering_jobs:
            teams[job.team] = teams.get(job.team, 0) + 1

        print("\nJobs by team:")
        for team, count in sorted(teams.items(), key=lambda x: x[1], reverse=True):
            print(f"  {team}: {count}")
    except Exception as e:
        logger.error(f"Failed to fetch engineering jobs: {e}")


if __name__ == "__main__":
    main()
