# Tesla Jobs API - Quick Start Guide

## 🚀 Get Started in 30 Seconds

### Step 1: Run the Demo
```bash
cd /Users/kalilbouzigues/.reverse-api/runs/scripts/78c4747db520/
python3 api_client_demo.py
```

That's it! You'll see:
- 4,498 Tesla jobs loaded
- Search examples
- Statistics
- Sample results

---

## 💻 Use in Your Code

### Example 1: Search for Jobs
```python
from api_client_demo import TeslaJobsAPIDemo

api = TeslaJobsAPIDemo()

# Search for AI jobs
ai_jobs = api.search_jobs(title="AI", limit=10)

for job in ai_jobs:
    location = api.get_location_name(job['l'])
    department = api.get_department_name(str(job['dp']))
    print(f"{job['t']} - {location} ({department})")
```

### Example 2: Filter by Location
```python
from api_client_demo import TeslaJobsAPIDemo

api = TeslaJobsAPIDemo()

# Get all US jobs
us_jobs = api.get_jobs_by_country("US", limit=50)

# Or filter by specific location ID
locations = api.get_locations()
palo_alto_id = [k for k, v in locations.items() if "Palo Alto" in v][0]
pa_jobs = api.search_jobs(location_id=palo_alto_id)
```

### Example 3: Get Statistics
```python
from api_client_demo import TeslaJobsAPIDemo

api = TeslaJobsAPIDemo()
stats = api.get_statistics()

print(f"Total jobs: {stats['total_jobs']:,}")
print(f"\nTop 5 locations:")
for loc, count in stats['top_locations'][:5]:
    print(f"  {loc}: {count} jobs")
```

### Example 4: Filter by Department
```python
from api_client_demo import TeslaJobsAPIDemo

api = TeslaJobsAPIDemo()

# Get all Tesla AI jobs
ai_dept_jobs = api.get_jobs_by_department("Tesla AI")

# Or Manufacturing jobs
mfg_jobs = api.get_jobs_by_department("Manufacturing", limit=20)
```

### Example 5: Save Results
```python
from api_client_demo import TeslaJobsAPIDemo

api = TeslaJobsAPIDemo()

# Search and save
results = api.search_jobs(title="Engineer", limit=100)
api.save_to_file(results, "tesla_engineers.json")

# Get stats and save
stats = api.get_statistics()
api.save_to_file(stats, "job_stats.json")
```

---

## 📊 Available Methods

### Search & Filter
- `search_jobs(title, location_id, department_id, job_family_id, limit)` - Multi-criteria search
- `get_jobs_by_country(country_code, limit)` - Filter by country (e.g., "US", "FR")
- `get_jobs_by_department(department_name, limit)` - Filter by dept name

### Lookup & Info
- `get_all_jobs()` - Get full dataset with all jobs and lookups
- `get_job_by_id(job_id)` - Get specific job from cache
- `get_locations()` - Get all location ID → name mappings
- `get_location_name(location_id)` - Get single location name
- `get_departments()` - Get all department ID → name mappings
- `get_department_name(department_id)` - Get single department name
- `get_regions()` - Get region mappings
- `get_sites()` - Get country code → name mappings

### Statistics & Utils
- `get_statistics()` - Get job counts by location/department
- `get_cache_info()` - Get cache metadata
- `save_to_file(data, filename)` - Save data to JSON

---

## 🎯 Common Use Cases

### Find Jobs in Your City
```python
api = TeslaJobsAPIDemo()
locations = api.get_locations()

# Find your city's location ID
my_city_id = [k for k, v in locations.items() if "Austin" in v][0]

# Get all jobs in that location
local_jobs = api.search_jobs(location_id=my_city_id)
print(f"Found {len(local_jobs)} jobs in your area")
```

### Track Specific Department
```python
api = TeslaJobsAPIDemo()

# Get all engineering jobs
eng_jobs = api.get_jobs_by_department("Engineering & Information Technology")

# Filter further by title
software_jobs = [j for j in eng_jobs if "Software" in j['t']]
print(f"Found {len(software_jobs)} software engineering jobs")
```

### Export Jobs for Analysis
```python
api = TeslaJobsAPIDemo()

# Get all data
all_data = api.get_all_jobs()

# Export by country
for country_code in ['US', 'DE', 'CN', 'AU']:
    jobs = api.get_jobs_by_country(country_code)
    api.save_to_file(jobs, f"tesla_jobs_{country_code}.json")
```

### Monitor Hot Locations
```python
api = TeslaJobsAPIDemo()
stats = api.get_statistics()

print("Top hiring locations:")
for i, (location, count) in enumerate(stats['top_locations'][:10], 1):
    print(f"{i}. {location}: {count} open positions")
```

---

## ⚡ Pro Tips

1. **Cache is Fast** - The demo loads data in milliseconds. No API delays!

2. **Case-Insensitive Search** - Title search is case-insensitive and partial match:
   ```python
   api.search_jobs(title="ai")  # Finds "AI", "Training", "Maintenance", etc.
   ```

3. **Combine Filters** - Use multiple criteria:
   ```python
   jobs = api.search_jobs(
       title="Engineer",
       location_id="401022",  # Palo Alto
       limit=20
   )
   ```

4. **Get IDs from Lookups** - Use lookup methods to find IDs:
   ```python
   locations = api.get_locations()
   departments = api.get_departments()
   ```

5. **Export for Excel** - JSON files can be opened in Excel or converted to CSV

---

## ❓ FAQ

**Q: Is this live data from Tesla?**
A: It's real Tesla data from a HAR capture. Not real-time, but accurate snapshot.

**Q: How often is the cache updated?**
A: Currently static. Re-run HAR capture to get fresh data.

**Q: Can I get job descriptions?**
A: The cached data has abbreviated format. Full descriptions require live API access.

**Q: Why not use the live API client?**
A: Tesla's bot detection is extremely aggressive. The demo version is more reliable.

**Q: Can I modify the cache?**
A: Yes! `tesla_jobs_cache.json` is editable. You can add/remove jobs as needed.

---

## 🆘 Need Help?

1. **Check the README**: `/Users/kalilbouzigues/.reverse-api/runs/scripts/78c4747db520/README.md`
2. **Read Implementation Notes**: `IMPLEMENTATION_NOTES.md`
3. **View Code Examples**: Look at the `main()` function in `api_client_demo.py`

---

## 📦 What's in the Box?

```
78c4747db520/
├── api_client_demo.py          ← Use this! (demo version)
├── api_client.py               ← Advanced live version
├── tesla_jobs_cache.json       ← 4,498 Tesla jobs (1.5 MB)
├── README.md                   ← Full documentation
├── IMPLEMENTATION_NOTES.md     ← Technical details
├── QUICK_START.md             ← This file
└── requirements.txt            ← Dependencies (for live version)
```

---

## 🎉 You're Ready!

Start with:
```bash
python3 api_client_demo.py
```

Then customize for your needs. Happy job hunting! 🚗⚡
