# Workday Scraper

This module mirrors the other ATS collectors in the repo but is tailored to Workday-hosted job boards. It handles pagination, rate limiting, error-aware retries, and produces normalized JSON per company before exporting to CSV.

## Features

- Async HTML scraping via `aiohttp` with a Windows Chrome UA string and polite delays.
- Pagination via the `page` query param plus duplicate URL suppression.
- BeautifulSoup parsing for both list and detail pages, extracting metadata such as requisition IDs, full locations, job descriptions, and apply links.
- Result caching under `workday/companies/<slug>.json`, including:
  - `last_request` / `last_scraped` timestamps.
  - HTTP `status` of the last index request.
  - `job_count` and a `jobs` array with merged list/detail attributes.
- 24-hour skip window unless `--force` is supplied (previous requests shorter than 24 hours are ignored to reduce load).
- 500 responses immediately recorded without retries (other network errors still use exponential backoff).
- CSV export (`workday/export_to_csv.py`) that flattens cached JSON into `workday/jobs.csv` with deterministic job IDs using the shared `export_utils` helpers.

## Running the scraper

```bash
cd workday
python ../workday/main.py                # scrape every company (respecting 24h cache)
python ../workday/main.py abbott_wd5_myworkdayjobs_com_en-us
python ../workday/main.py --force        # ignore cache window
```

Tips:
- Each run saves or updates `companies/<slug>.json`. Empty responses (no jobs) are still cached with `status` and timestamps so tests/exports can rely on them.
- If a file already exists and was scraped less than 24 hours ago, the scraper prints a skip notice unless `--force` is provided.

## Exporting to CSV

After scraping, create a normalized CSV for downstream processing:

```bash
cd workday
python export_to_csv.py
```

This reads every JSON file, extracts the best available location string, prefers `apply_url` over `job_url`, and writes `workday/jobs.csv`. A timestamped diff file is generated whenever the output changes (leveraging `export_utils.write_jobs_csv`).

## JSON structure

Each cached file resembles:

```json
{
  "company": "Abbott",
  "url": "https://abbott.wd5.myworkdayjobs.com/en-us",
  "slug": "abbott_wd5_myworkdayjobs_com_en-us",
  "status": 500,
  "last_request": "2025-12-22T10:15:00.000000",
  "last_scraped": "2025-12-22T10:15:00.000000",
  "job_count": 0,
  "jobs": [
    {
      "job_id": "31075177",
      "job_title": "Data Engineer",
      "location_full": "Chicago, IL",
      "apply_url": "https://abbott.wd5.myworkdayjobs.com/en-us/job/31075177",
      "job_description_html": "<div>...</div>",
      "subtitle": ["Full-time", "Hybrid"],
      "posted_on": "Dec 22, 2025"
    }
  ]
}
```

Use `status` to spot failing tenants (500, 404, etc.) and `job_count` to monitor scrape completeness.

## Testing

A lightweight integration test randomly chooses a company from `workday_companies.csv`, forces a scrape, and asserts the JSON structure:

```bash
python -m unittest tests.test_workday_random
```

To debug an individual tenant, run the scraper with its slug plus `--force`, inspect the corresponding JSON, and rerun the exporter as needed.
