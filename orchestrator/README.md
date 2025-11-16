# Pipeline Orchestrator

Unified pipeline for job data aggregation across multiple ATS platforms with configurable steps.

## Overview

The orchestrator coordinates the entire workflow:

1. **Discovery** - Find companies using various search methods
2. **Scraping** - Fetch jobs from each company via ATS APIs
3. **CSV Consolidation** - Create simplified all_jobs.csv (url, title, location, company)
4. **Database Processing** - Save to PostgreSQL with embeddings (Ashby only currently)
5. **Export** - Export final data (placeholder for future features)

## Quick Start

### Default Behavior

By default, the pipeline runs: **Scraping → CSV Consolidation → Export**

Discovery and DB Processing are **skipped by default**.

```bash
# Default: scraping + CSV consolidation + export
python orchestrator/pipeline.py

# Same as above, specific platforms
python orchestrator/pipeline.py --platforms ashby,greenhouse
```

### Run Full Pipeline

To include discovery and DB processing, use `--all`:

```bash
# All 5 steps including discovery and DB processing
python orchestrator/pipeline.py --all

# Full pipeline with specific platforms
python orchestrator/pipeline.py --all --platforms ashby,greenhouse
```

### Run Specific Steps

```bash
# Discovery only
python orchestrator/pipeline.py --discovery-only --discovery-method searxng --max-queries 30

# Scraping only
python orchestrator/pipeline.py --scraping-only

# Scraping + CSV only (skip export)
python orchestrator/pipeline.py --skip-export
```

### Skip Steps

```bash
# Skip CSV consolidation (scraping + export only)
python orchestrator/pipeline.py --skip-csv

# Scraping only (skip CSV and export)
python orchestrator/pipeline.py --skip-csv --skip-export
```

## Pipeline Steps

### Step 1: Discovery

Find companies using various search APIs.

**Available Methods:**
- `searxng` - Self-hosted search engine (FREE unlimited!) ⭐
- `google` - Google Custom Search API (FREE 100/day)
- `firecrawl` - Firecrawl API ($16/mo, 500 free credits)
- `enhanced` - Enhanced SERP API (~$5-20/run)
- `optimized` - Optimized SERP with caching (~$2-5/run, 75% cheaper)
- `manual` - Skip automated discovery

**Examples:**
```bash
# SearXNG discovery (recommended - FREE unlimited)
python orchestrator/pipeline.py --discovery-only --discovery-method searxng --max-queries 20

# Google Custom Search (FREE 100/day)
python orchestrator/pipeline.py --discovery-only --discovery-method google --max-queries 100

# Firecrawl (cheaper than SERP API)
python orchestrator/pipeline.py --discovery-only --discovery-method firecrawl --max-queries 15
```

### Step 2: Scraping

Fetch jobs from each company via ATS APIs.

**Platforms:**
- Ashby - `ashby/main.py`
- Greenhouse - `greenhouse/main.py`
- Lever - `lever/main.py`
- Workable - `workable/main.py`

**Examples:**
```bash
# Scrape all platforms
python orchestrator/pipeline.py --scraping-only

# Scrape specific platforms
python orchestrator/pipeline.py --scraping-only --platforms ashby,greenhouse
```

**Output:**
- JSON files saved to `{platform}/companies/*.json`

### Step 3: CSV Consolidation

Create a simplified CSV with all jobs from all platforms.

**Output:** `all_jobs.csv`

**Columns:**
- `url` - Job application URL
- `title` - Job title
- `location` - Job location
- `company` - Company slug/name
- `platform` - ATS platform (ashby, greenhouse, lever, workable)

**Why?**
- Keeps file size small (only essential fields)
- Easy to review all jobs at a glance
- No complex JSON structures

**Examples:**
```bash
# Run consolidation manually
python orchestrator/consolidate_jobs.py --platforms all --output all_jobs.csv

# Via pipeline
python orchestrator/pipeline.py --skip-discovery --skip-scraping --skip-db-processing
```

### Step 4: Database Processing

Save jobs to PostgreSQL database with OpenAI embeddings for semantic search.

**Requirements:**
- `DATABASE_URL` environment variable
- `OPENAI_API_KEY` environment variable

**Currently Supported:**
- ✅ Ashby - Full processing with embeddings
- ⏳ Greenhouse - Coming soon
- ⏳ Lever - Coming soon
- ⏳ Workable - Coming soon

**Examples:**
```bash
# Process Ashby jobs to database
python orchestrator/pipeline.py --skip-discovery --skip-scraping --skip-csv --platforms ashby

# Full pipeline with DB processing
python orchestrator/pipeline.py --all --platforms ashby
```

### Step 5: Export

Placeholder for future centralized export features.

Currently, each platform handles its own exports during scraping.

## Usage Examples

### Common Workflows

#### Initial Setup - Discover Companies

```bash
# Use SearXNG for FREE unlimited discovery
python orchestrator/pipeline.py --discovery-only --discovery-method searxng --max-queries 30 --pages 5

# Result: Updated company CSV files for each platform
```

#### Daily Job Scraping

```bash
# Default: Scrape jobs + create consolidated CSV (discovery and DB are skipped by default)
python orchestrator/pipeline.py

# Result:
# - Fresh job data in {platform}/companies/*.json
# - Consolidated all_jobs.csv with all jobs
```

#### Full Pipeline with Database

```bash
# Discovery + Scraping + CSV + Database (Ashby only)
python orchestrator/pipeline.py --all --platforms ashby --discovery-method searxng --max-queries 20

# Result:
# - Discovered companies saved to ashby/companies.csv
# - Scraped jobs to ashby/companies/*.json
# - Consolidated CSV at all_jobs.csv
# - Jobs in PostgreSQL database with embeddings
```

#### Cost-Effective Discovery Rotation

```bash
# Week 1: SearXNG discovery (FREE)
python orchestrator/pipeline.py --discovery-only --discovery-method searxng --max-queries 50

# Week 2: Google CSE (FREE 100/day)
python orchestrator/pipeline.py --discovery-only --discovery-method google --max-queries 100

# Daily: Scrape all companies + CSV (default behavior)
python orchestrator/pipeline.py
```

## Configuration

### Platforms

Select which ATS platforms to process:

```bash
# All platforms
--platforms all

# Specific platforms (comma-separated)
--platforms ashby,greenhouse,lever,workable
```

### Discovery Options

```bash
# Discovery method (default: searxng)
--discovery-method {searxng,google,firecrawl,enhanced,optimized,manual}

# Max queries per platform (default: 20)
--max-queries 30

# Pages per query (default: 3, applies to searxng and enhanced)
--pages 5
```

### Execution Options

```bash
# Stop on first error (default: continue)
--stop-on-error

# Reduce output verbosity
--quiet
```

## Environment Variables

Required for different steps:

| Variable | Required For | Purpose |
|----------|--------------|---------|
| `SEARXNG_URL` | SearXNG discovery | Self-hosted SearXNG instance URL |
| `GOOGLE_API_KEY` | Google CSE discovery | Google Custom Search API key |
| `GOOGLE_CSE_ID` | Google CSE discovery | Custom Search Engine ID |
| `FIRECRAWL_API_KEY` | Firecrawl discovery | Firecrawl API key |
| `SERPAPI_API_KEY` | Enhanced/Optimized discovery | SERP API key |
| `DATABASE_URL` | Database processing | PostgreSQL connection string |
| `OPENAI_API_KEY` | Database processing | OpenAI API key for embeddings |

## Output Files

| File | Created By | Description |
|------|------------|-------------|
| `{platform}/companies.csv` | Discovery step | Company URLs per platform |
| `{platform}/companies/*.json` | Scraping step | Raw job data from ATS APIs |
| `all_jobs.csv` | CSV consolidation step | Simplified jobs CSV (all platforms) |
| Database records | DB processing step | Jobs with embeddings in PostgreSQL |

## Error Handling

By default, the pipeline continues on errors:
- If discovery fails → continues with existing companies
- If scraping fails for one platform → continues with other platforms
- If CSV consolidation fails → continues to DB processing
- If DB processing fails → continues to export

**Stop on first error:**
```bash
python orchestrator/pipeline.py --all --stop-on-error
```

## Help

```bash
# Show all available options
python orchestrator/pipeline.py --help
```

## Examples Summary

```bash
# 1. Default: scraping + CSV + export (most common use case)
python orchestrator/pipeline.py

# 2. Full pipeline including discovery and DB processing
python orchestrator/pipeline.py --all

# 3. Discovery only with SearXNG
python orchestrator/pipeline.py --discovery-only --discovery-method searxng --max-queries 30

# 4. Default for specific platforms
python orchestrator/pipeline.py --platforms ashby,greenhouse

# 5. Full pipeline for Ashby only
python orchestrator/pipeline.py --all --platforms ashby

# 6. Discovery with Google CSE (free tier)
python orchestrator/pipeline.py --discovery-only --discovery-method google --max-queries 100

# 7. CSV consolidation only
python orchestrator/consolidate_jobs.py --platforms all --output my_jobs.csv

# 8. Full pipeline, stop on error
python orchestrator/pipeline.py --all --stop-on-error
```

## Troubleshooting

### No companies found after discovery

- Check environment variables for discovery method
- Verify SearXNG instance is running (if using searxng)
- Check API keys are valid
- Try different discovery method

### Scraping fails

- Check if company CSV files exist
- Verify company URLs are valid
- Check internet connection
- Some companies may have removed their job boards

### CSV consolidation shows 0 jobs

- Run scraping step first
- Check if `{platform}/companies/*.json` files exist
- Verify JSON files contain valid job data

### Database processing fails

- Check `DATABASE_URL` and `OPENAI_API_KEY` environment variables
- Verify database is accessible
- Check OpenAI API quota
- Currently only Ashby is supported

## Architecture

```
orchestrator/
├── __init__.py           # Package init
├── config.py             # Configuration classes
├── pipeline.py           # Main orchestrator
├── consolidate_jobs.py   # CSV consolidation script
└── README.md             # This file
```

**Key Classes:**
- `PipelineOrchestrator` - Main orchestrator class
- `PipelineConfig` - Configuration data class
- `PipelineStep` - Enum of pipeline steps
- `Platform` - Enum of supported platforms
- `DiscoveryMethod` - Enum of discovery methods

## Future Improvements

- [ ] Add database processing for all platforms (not just Ashby)
- [ ] Parallel execution of platform scrapers
- [ ] Retry logic with exponential backoff
- [ ] Progress bars for long-running operations
- [ ] Email notifications on completion
- [ ] Centralized export to public CSV endpoint
- [ ] Incremental discovery (avoid re-discovering known companies)
- [ ] Company deduplication across platforms
- [ ] Job deduplication across platforms

## Support

For issues or questions:
1. Check this README
2. Check main project README
3. Open an issue on GitHub
4. Contact: https://stapply.ai
