# Tesla Careers API Client

This directory contains two versions of the Tesla Jobs API client:

1. **api_client_demo.py** - ✅ Demo client using cached data (RECOMMENDED)
2. **api_client.py** - ⚠️ Live API client with Playwright (challenging due to bot detection)

## 🚀 Quick Start - Demo Version (Recommended)

The demo version is ready to use immediately with no dependencies or bot detection issues.

```python
from api_client_demo import TeslaJobsAPIDemo

# Initialize the demo client
api = TeslaJobsAPIDemo()

# Search for AI jobs
ai_jobs = api.search_jobs(title="AI", limit=10)

# Get statistics
stats = api.get_statistics()
print(f"Total jobs: {stats['total_jobs']:,}")
```

Run the demo:
```bash
python3 api_client_demo.py
```

**Why use the demo version?**
- ✅ No bot detection challenges
- ✅ Instant responses
- ✅ Real Tesla job data (4,498+ jobs)
- ✅ All API methods work identically
- ✅ Perfect for development and testing

---

## 📂 File Overview

### 1. api_client_demo.py (Recommended)
**Status**: ✅ Ready to use
**Dependencies**: None (standard library only)
**Data**: Cached HAR file (tesla_jobs_cache.json)
**Jobs**: 4,498 real Tesla positions
**Locations**: 10,792 unique locations globally

**Use for:**
- Development and testing
- Demonstrations and prototypes
- Learning the API structure
- Avoiding bot detection headaches

### 2. api_client.py (Advanced)
**Status**: ⚠️ Challenging (Akamai bot detection)
**Dependencies**: Playwright, Chromium browser
**Data**: Live Tesla API
**Success Rate**: 10-30% without proxy services

**Use for:**
- Production with professional proxy services
- Real-time data requirements
- Custom cookie extraction solutions

See extensive troubleshooting documentation in the file header.

### 3. tesla_jobs_cache.json
**Size**: ~1.6 MB
**Source**: Extracted from HAR recording
**Contents**:
- 4,498 job listings
- 10,792 location mappings
- Department hierarchies
- Geographic data (58 countries, 5 regions)

Real Tesla job data in exact API format.

---

## 💡 Which Version Should I Use?

| Use Case | Recommended Version |
|----------|-------------------|
| Development & Testing | **api_client_demo.py** |
| Demonstrations | **api_client_demo.py** |
| Learning the API | **api_client_demo.py** |
| One-off data extraction | **api_client_demo.py** |
| Production (with proxy) | **api_client.py** + proxy service |
| Real-time monitoring | **api_client.py** + proxy service |
| Personal browsing | Tesla's official website |

---

## 🔍 APIs Discovered

### 1. **Get All Jobs** - `/cua-api/apps/careers/state`
- **Method:** GET
- **Returns:** All 4,498+ job listings globally
- **Cache:** 30 minutes
- **Response Size:** ~950KB uncompressed, ~205KB compressed
- **Data Included:**
  - Complete job listings (abbreviated format)
  - Location lookup tables (10,792 cities)
  - Region mappings (5 regions)
  - Country/site mappings (58 countries)
  - Geographic hierarchy

### 2. **Get Job Details** - `/cua-api/careers/job/{job_id}`
- **Method:** GET
- **Returns:** Complete details for a specific job
- **Data Included:**
  - Full job description (HTML)
  - Responsibilities (HTML)
  - Requirements (HTML)
  - Compensation & benefits (HTML)
  - Application URL
  - Department and location information

## 🔐 Authentication

The API uses **cookie-based authentication** with Akamai Bot Manager protection:

### Required Cookies:
- `_abck` - Akamai Bot Manager cookie (critical, 1-year expiry)
- `bm_s` - Bot detection session data (31 days)
- `bm_sz` - Bot detection fingerprint (4 hours)
- `bm_ss`, `bm_so`, `bm_lso`, `bm_sc` - Additional bot detection cookies
- `cua_sess` - Tesla CUA session cookie (3 days)

### Bot Detection Handling:
This client uses **Playwright** with a real Chromium browser to:
1. Load the Tesla careers page
2. Wait for Akamai bot detection to complete
3. Extract valid cookies automatically
4. Make API requests with proper authentication

## 📦 Installation

### For Demo Version (Recommended):
```bash
# No installation required! Just Python 3.7+
python3 api_client_demo.py
```

### For Live Version (Advanced):
```bash
# Install Python dependencies
pip install playwright

# Install Playwright browsers
playwright install chromium

# Run (expect challenges with bot detection)
python3 api_client.py
```

## 🚀 Usage Examples

### Demo Version (Recommended):

#### Basic Example:
```python
from api_client_demo import TeslaJobsAPIDemo

# Initialize demo client
api = TeslaJobsAPIDemo()

# Get all jobs
data = api.get_all_jobs()
print(f"Total jobs: {len(data['listings']):,}")

# Search for AI jobs
ai_jobs = api.search_jobs(title="AI", limit=10)
for job in ai_jobs:
    print(f"{job['t']} - {api.get_location_name(job['l'])}")

# Get job by ID
job = api.get_job_by_id("224501")
print(f"Job: {job['t']}")
print(f"Department: {api.get_department_name(str(job['dp']))}")
```

#### Advanced Filtering:
```python
from api_client_demo import TeslaJobsAPIDemo

api = TeslaJobsAPIDemo()

# Get all locations
locations = api.get_locations()

# Find Palo Alto location ID
palo_alto_id = [k for k, v in locations.items() if "Palo Alto" in v][0]

# Search for engineering jobs in Palo Alto
jobs = api.search_jobs(
    title="Engineer",
    location_id=palo_alto_id,
    limit=20
)

# Get jobs by country
us_jobs = api.get_jobs_by_country("US", limit=100)

# Get jobs by department
ai_jobs = api.get_jobs_by_department("Tesla AI", limit=50)

# Get statistics
stats = api.get_statistics()
print(f"Top locations: {stats['top_locations'][:5]}")
print(f"Top departments: {stats['top_departments'][:5]}")
```

#### Save Data to Files:
```python
from api_client_demo import TeslaJobsAPIDemo

api = TeslaJobsAPIDemo()
all_data = api.get_all_jobs()
stats = api.get_statistics()

api.save_to_file(all_data, "tesla_jobs.json")
api.save_to_file(stats, "job_statistics.json")
```

### Live Version (Advanced - see api_client.py header for troubleshooting):

```python
from api_client import TeslaJobsAPI

# Initialize API client (headless mode)
# WARNING: High failure rate due to Akamai bot detection
with TeslaJobsAPI(headless=True) as api:
    # Get all jobs (if bot detection allows)
    data = api.get_all_jobs()

    # Search for AI jobs
    ai_jobs = api.search_jobs(title="AI", limit=10)

    # Get job details (live API call)
    job_details = api.get_job_details("224501")
    print(f"Title: {job_details['title']}")
    print(f"Apply: {job_details['applyUrl']}")
```

**Note:** For reliable production use with live data, see the extensive documentation in `api_client.py` about using professional proxy services.

## 📊 Data Structure

### Job Listing (Abbreviated):
```json
{
  "id": "224501",
  "t": "AI Engineer, Manipulation, Optimus",
  "dp": "5",          // Department ID
  "f": "72",          // Job family ID
  "l": "401022",      // Location ID (Palo Alto)
  "y": 1,
  "sp": 1,            // Sort priority
  "pu": null          // Post until date
}
```

### Job Details (Complete):
```json
{
  "id": "224501",
  "title": "AI Engineer, Manipulation, Optimus",
  "department": "Tesla AI",
  "jobFamily": "AI",
  "location": "Palo Alto, California",
  "jobDescription": "<p>HTML content...</p>",
  "jobResponsibilities": "<ul>...</ul>",
  "jobRequirements": "<ul>...</ul>",
  "jobCompensationAndBenefits": "<div>$140,000 - $420,000/annual...</div>",
  "applicationType": "002",
  "timeType": "Full-time",
  "url": "/careers/search/job/...",
  "applyUrl": "https://tesla.avature.net/Careers/ApplicationMethods?jobId=224501"
}
```

### Lookup Tables:
```json
{
  "lookup": {
    "regions": {
      "1": "Africa",
      "2": "Asia Pacific",
      "3": "Europe",
      "4": "Middle East",
      "5": "North America"
    },
    "sites": {
      "US": "United States of America",
      "FR": "France",
      "DE": "Germany",
      ...
    },
    "locations": {
      "401022": "Palo Alto, California",
      "402001": "Austin, Texas",
      ...
    }
  }
}
```

## ⚙️ API Methods

Both clients implement the same core interface:

### Core Methods:

| Method | Demo | Live | Description | Returns |
|--------|------|------|-------------|---------|
| `get_all_jobs()` | ✅ | ✅ | Fetch all job listings | Dict with listings and lookup tables |
| `get_job_by_id(job_id)` | ✅ | - | Get job from cache by ID | Dict (abbreviated format) |
| `get_job_details(job_id)` | - | ✅ | Get detailed job info (live API call) | Dict with complete job details |
| `search_jobs(...)` | ✅ | ✅ | Filter jobs client-side | List of matching jobs |

### Search & Filter Methods:

| Method | Description | Parameters | Returns |
|--------|-------------|------------|---------|
| `search_jobs()` | Multi-criteria search | title, location_id, department_id, job_family_id, limit | List of jobs |
| `get_jobs_by_country()` | Filter by country code | country_code, limit | List of jobs |
| `get_jobs_by_department()` | Filter by department name | department_name, limit | List of jobs |

### Lookup Methods:

| Method | Description | Returns |
|--------|-------------|---------|
| `get_locations()` | Get location ID → name mapping | Dict[str, str] |
| `get_location_name(id)` | Get single location name | str |
| `get_regions()` | Get region ID → name mapping | Dict[str, str] |
| `get_sites()` | Get country code → name mapping | Dict[str, str] |
| `get_departments()` | Get department ID → name mapping | Dict[str, str] |
| `get_department_name(id)` | Get single department name | str |

### Statistics & Utility (Demo only):

| Method | Description | Returns |
|--------|-------------|---------|
| `get_statistics()` | Get job statistics and rankings | Dict with top locations/departments |
| `get_cache_info()` | Get cache metadata | Dict with cache details |
| `save_to_file(data, filename)` | Save data to JSON file | None |

## ⚠️ Important Notes

### Filtering & Pagination:
- ✅ The API returns **ALL jobs** in a single request (no server-side filtering)
- ✅ All filtering must be done **client-side**
- ✅ Use the built-in `search_jobs()` method for filtering
- ✅ Cache is valid for 30 minutes (matches API cache-control)

### Rate Limiting:
- ⏱️ Don't fetch more often than every 30 minutes
- ⏱️ The API response is cached server-side
- ⏱️ Excessive requests will trigger bot detection

### Bot Detection:
- 🤖 Heavy Akamai protection on all endpoints
- 🤖 Cookies must be generated by a real browser
- 🤖 Playwright handles this automatically
- 🤖 Session cookies expire after 3-31 days

### Performance:
- 📦 Initial request: ~950KB uncompressed data
- 📦 Compressed transfer: ~205KB
- 📦 Contains 4,498+ job listings
- 📦 10,792 location mappings included

## 🧪 Testing

Run the included demo:
```bash
python api_client.py
```

Expected output:
```
======================================================================
Tesla Careers API Client - Demo
======================================================================

📊 Fetching all jobs...
✓ Total jobs available: 4498

🌍 Available locations:
✓ Total locations: 10792
✓ Regions: Africa, Asia Pacific, Europe, Middle East, North America
✓ Countries: 58

🔍 Searching for AI jobs...
✓ Found 5 AI jobs (showing top 5):
  - AI Engineer, Manipulation, Optimus | Palo Alto, California
  ...

✓ Demo completed successfully!
```

## 🛠️ Troubleshooting

### Demo Version:

**Issue: "Cache file not found"**
- **Solution:** Ensure `tesla_jobs_cache.json` is in the same directory as `api_client_demo.py`

**Issue: "No results from search"**
- **Solution:** The search is case-insensitive partial match. Try broader terms or check location/department IDs.

### Live Version (api_client.py):

**Issue: "Request failed with 403 Forbidden"**
- **Cause:** Akamai detected the bot
- **Solutions:**
  1. Use the demo version instead (recommended)
  2. Extract cookies from real browser (see api_client.py header)
  3. Use professional proxy service (ScraperAPI, Bright Data)
  4. Run in non-headless mode (`headless=False`)
  5. Increase wait time to 20+ seconds

**Issue: "Missing critical cookies (_abck, bm_sz)"**
- **Cause:** Akamai scripts didn't execute properly
- **Solutions:**
  1. Disable headless mode
  2. Check for JavaScript errors in browser console
  3. Increase wait time after page load
  4. Use demo version to avoid this issue

**Issue: "No jobs returned"**
- **Cause:** API structure may have changed, or request was blocked
- **Solutions:**
  1. Check if `/cua-api/apps/careers/state` endpoint still exists
  2. Verify response format in HAR file
  3. Use demo version with cached data

**For all live API issues:** See the comprehensive troubleshooting guide in the `api_client.py` file header, which includes:
- Browser cookie extraction methods
- Proxy service recommendations
- Manual developer tools approach
- Alternative solutions

## 📝 License

This is a reverse-engineered API client for educational purposes.
Respect Tesla's terms of service and rate limits when using this tool.

## 🤝 Contributing

This client was automatically generated by the Reverse API Tool.
To improve or update:
1. Capture a new HAR file from tesla.com/careers
2. Re-run the reverse engineering process
3. Update API endpoints and authentication as needed

## 📚 Additional Resources

- Tesla Careers: https://www.tesla.com/careers
- HAR File Location: `/Users/kalilbouzigues/.reverse-api/runs/har/78c4747db520/recording.har`
- Generated: 2025-12-23
