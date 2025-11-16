# Stapply Job Data Aggregator

A data aggregator that collects job postings from multiple ATS (Applicant Tracking System) platforms and makes them publicly available.

## Data

The aggregated job data is available at: **https://storage.stapply.ai/jobs.csv**

## Supported Platforms

- **Ashby** 
- **Greenhouse** 
- **Lever** 
- **Workable**
- **Rippling** - Coming soon

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/stapply-ai/data.git
cd data

# Install dependencies using uv (recommended)
uv sync

# Or using pip
pip install -r requirements.txt
```

### Environment Setup

Create a `.env` file with your API keys:

```bash
cp env.example .env
```

Then edit `.env` with your keys:

```env
# Required for job processing and embeddings
DATABASE_URL=postgresql://user:pass@host:port/dbname
OPENAI_API_KEY=sk-...

# Required for company discovery
SERPAPI_API_KEY=your_serpapi_key

# Optional: Alternative discovery methods (cheaper/free)
SEARXNG_URL=http://localhost:8080 # SearXNG (self-hosted, FREE unlimited!)
FIRECRAWL_API_KEY=fc-your_key     # Firecrawl (500 free, $16/mo)
GOOGLE_API_KEY=your_google_key    # Google (FREE 100/day)
GOOGLE_CSE_ID=your_cse_id         # Google CSE ID
BING_API_KEY=your_bing_key        # Bing (FREE 1,000/mo)
```

## Pipeline Orchestrator (Recommended)

**NEW!** Unified pipeline to run the full workflow with configurable steps.

**Default behavior:** Scraping + CSV Consolidation + Export (skips discovery and DB processing)

### Quick Start

```bash
# Default: scraping + CSV consolidation (most common use case)
python orchestrator/pipeline.py

# Run full pipeline including discovery and DB processing
python orchestrator/pipeline.py --all

# Discovery only with SearXNG (FREE unlimited!)
python orchestrator/pipeline.py --discovery-only --discovery-method searxng --max-queries 30
```

### Pipeline Steps

1. **Discovery** - Find companies using search APIs (SearXNG, Google CSE, Firecrawl, SERP API)
2. **Scraping** - Fetch jobs from each company via ATS APIs
3. **CSV Consolidation** - Create simplified `all_jobs.csv` (url, title, location, company)
4. **DB Processing** - Save to PostgreSQL with embeddings (Ashby only currently)
5. **Export** - Export final data

### Benefits

- ✅ Coordinated workflow across all platforms
- ✅ Skip any step as needed
- ✅ Automatic error handling
- ✅ Progress tracking and summaries
- ✅ Simple CSV output for easy review

**See [orchestrator/README.md](orchestrator/README.md) for full documentation.**

## Company Discovery

Find more companies using different ATS platforms.

### Option 1: Enhanced Discovery (Recommended)

Uses 55+ search strategies to find the most companies:

```bash
# Discover all platforms (uses SERPAPI_API_KEY)
python enhanced_discovery.py --platform all --pages 10 --strategies 10

# Discover specific platform
python enhanced_discovery.py --platform ashby --pages 15 --strategies 20

# Deep discovery (expensive but comprehensive)
python enhanced_discovery.py --platform greenhouse --pages 20 --strategies 55
```

**Search Strategies Include:**
- Basic site searches
- Job-related keywords (careers, hiring, jobs)
- Role-based (software engineer, product manager, data scientist, designer, sales, marketing)
- Location-based (50+ global cities: SF, NYC, London, Paris, Berlin, Bangalore, Singapore, Tokyo, etc.)
- Region-based (Europe, Asia, Middle East, North America, South America)
- Company type (startup, YC, series A/B, tech startup)

### Option 2: SearXNG Discovery (FREE - Unlimited!) ⭐

Use your own self-hosted search engine with NO limits or costs:

```bash
# Setup: Follow SEARXNG_SETUP.md (10 minutes)
# Runs on localhost or any server

# Discover all platforms (unlimited queries!)
python searxng_discovery.py --platform all --max-queries 20

# Deep discovery (no cost concerns!)
python searxng_discovery.py --platform all --max-queries 100 --pages 10

# Use specific search engines
python searxng_discovery.py --platform ashby --engines "google,duckduckgo,bing"
```

**Advantages:**
- Completely FREE (self-hosted)
- UNLIMITED queries (no rate limits!)
- No API keys needed
- Privacy-focused (you control everything)
- Aggregates from multiple search engines
- Can run on localhost or $5/month VPS

**Cost:** $0 (localhost) or $5-10/month (VPS) - unlimited usage!

### Option 3: Google Custom Search (FREE - 100/day)

Use Google's free tier API (3,000 queries/month):

```bash
# Setup: See SERP_ALTERNATIVES.md for Google API setup (5 minutes)

# Discover using free tier
python google_custom_search.py --platform all --max-queries 100

# Daily discovery (100 free queries per day)
python google_custom_search.py --platform ashby --max-queries 100
```

**Cost:** FREE for first 100 queries/day, then $5 per 1,000 queries

### Option 4: Firecrawl Discovery (SERP Alternative)

Use Firecrawl's search endpoint as an alternative to SERP API:

```bash
# Setup: Get API key from https://firecrawl.dev/ (500 free credits)

# Discover using Firecrawl search
python firecrawl_discovery.py --platform all --max-queries 15

# Discover specific platform
python firecrawl_discovery.py --platform ashby --max-queries 20
```

**Advantages:**
- Returns full content (not just snippets like Google)
- Combined search + scrape in one call
- Cheaper than SERP API ($16/mo vs $50/mo)
- 500 free credits to start (no credit card)
- 2 credits per 10 search results

### Option 5: Optimized Discovery (Minimal Cost)

Minimize SERP API costs with query caching and smart strategies:

```bash
# Uses only top 5 strategies with caching
python optimized_serp_discovery.py --platform all --max-queries 25

# Check cache stats
python optimized_serp_discovery.py --cache-stats
```

**Features:**
- SQLite query caching (avoid duplicate API calls)
- Top 5 most effective strategies only
- 75% cost reduction vs enhanced discovery

### Manual Discovery

Find companies from curated sources (FREE):

**Sources:**
- [Y Combinator Companies](https://www.ycombinator.com/companies) - 4,000+ startups
- [BuiltIn Jobs](https://builtin.com/jobs) - 50,000+ tech jobs
- [Wellfound](https://wellfound.com/jobs) - Startup jobs
- [AngelList](https://angel.co/jobs) - Tech startup jobs

**Method:**
1. Visit company's careers page
2. View page source (Ctrl+U / Cmd+U)
3. Search for ATS domains:
   - `ashbyhq.com` → Add to `ashby/companies.csv`
   - `greenhouse.io` → Add to `greenhouse/greenhouse_companies.csv`
   - `lever.co` → Add to `lever/lever_companies.csv`
   - `workable.com` → Add to `workable/workable_companies.csv`

## Job Scraping

Scrape job postings from discovered companies:

```bash
# Ashby (includes database processing with embeddings)
cd ashby
python main.py
python process_ashby.py  # Process to database with OpenAI embeddings

# Greenhouse
cd greenhouse
python main.py

# Lever
cd lever
python main.py

# Workable
cd workable
python main.py
```

## Project Structure

```
data/
├── ashby/                          # Ashby platform (816 companies)
│   ├── main.py                    # Job scraper
│   ├── process_ashby.py           # Database processor with embeddings
│   ├── serp.py                    # Company discovery
│   ├── extract_companies_ashby.py # Company extraction
│   └── companies.csv              # Company list
├── greenhouse/                     # Greenhouse platform (310 companies)
│   ├── main.py                    # Job scraper
│   ├── serp.py                    # Company discovery
│   └── greenhouse_companies.csv   # Company list
├── lever/                          # Lever platform (444 companies)
│   ├── main.py                    # Job scraper
│   ├── serp.py                    # Company discovery
│   └── lever_companies.csv        # Company list
├── workable/                       # Workable platform (218 companies)
│   ├── main.py                    # Job scraper
│   ├── serp.py                    # Company discovery
│   └── workable_companies.csv     # Company list
├── rippling/                       # Rippling platform (coming soon)
│   └── rippling.txt               # URL collection
├── models/                         # Pydantic data models
│   ├── db.py                      # Database models
│   ├── ashby.py                   # Ashby API models
│   ├── gh.py                      # Greenhouse models
│   ├── lever.py                   # Lever models
│   └── workable.py                # Workable models
├── orchestrator/                   # Pipeline orchestrator (NEW!)
│   ├── __init__.py                # Package init
│   ├── config.py                  # Pipeline configuration
│   ├── pipeline.py                # Main orchestrator
│   ├── consolidate_jobs.py        # CSV consolidation script
│   └── README.md                  # Orchestrator documentation
├── enhanced_discovery.py           # 55+ search strategies (recommended)
├── searxng_discovery.py            # SearXNG self-hosted search (FREE unlimited!)
├── firecrawl_discovery.py          # Firecrawl search API (SERP alternative)
├── google_custom_search.py         # Free Google API discovery
├── optimized_serp_discovery.py     # Cost-optimized SERP discovery
└── alternative_discovery.py        # Alternative discovery methods
```

## Documentation

- **[orchestrator/README.md](orchestrator/README.md)** - Pipeline orchestrator full documentation
- **[SEARXNG_SETUP.md](SEARXNG_SETUP.md)** - Complete guide to setting up SearXNG (self-hosted search)
- **[DISCOVERY_QUICK_START.md](DISCOVERY_QUICK_START.md)** - Quick reference for company discovery
- **[IMPROVEMENTS.md](IMPROVEMENTS.md)** - Code quality improvements and recommendations

## Discovery Cost Comparison

| Method | Cost | Companies Found | Setup Time |
|--------|------|----------------|------------|
| **Enhanced Discovery** | $5-20/run | 500-2,000 | 0 min |
| **SearXNG Discovery** ⭐ | $0 (unlimited!) | 500-2,000 | 10 min |
| **Firecrawl Discovery** | $0-3/run | 300-800 | 5 min |
| **Google Custom Search** | FREE (100/day) | 500-1,000 | 5 min |
| **Optimized Discovery** | $2-5/run | 500-1,000 | 0 min |
| **Manual Curation** | FREE | 500-1,000 | 30-60 min |

### Recommended Workflow (Cost-Effective)

**Week 1:** Enhanced discovery with limited strategies
```bash
python enhanced_discovery.py --platform all --pages 10 --strategies 10
# Cost: ~$4, Result: +500-1,000 companies
```

**Week 2:** Google Custom Search (FREE)
```bash
python google_custom_search.py --platform all --max-queries 100
# Cost: $0, Result: +300-500 companies
```

**Week 3:** Manual curation (30 mins)
```
Visit YC directory, BuiltIn, etc.
# Cost: $0, Result: +200-500 companies
```

**Monthly total: 1,000-2,000 new companies at $4-8/month**

## Features

- **Multi-platform support** - Scrape jobs from 5 different ATS platforms
- **Semantic search** - OpenAI embeddings for job title and description (Ashby)
- **Incremental processing** - Checkpoint system prevents duplicate processing
- **Job lifecycle tracking** - Automatically detect and mark inactive jobs
- **Public data export** - Jobs published to public CSV endpoint
- **Cost-optimized discovery** - Multiple discovery methods from FREE to paid

## Tech Stack

- **Python 3.12+**
- **aiohttp** - Async HTTP client for concurrent scraping
- **Pydantic** - Data validation and modeling
- **SQLAlchemy** - Database ORM
- **PostgreSQL** - Data storage
- **OpenAI API** - Text embeddings for semantic search
- **SERP API / SearXNG / Google Custom Search / Firecrawl** - Company discovery
- **pandas** - Data processing
- **BeautifulSoup** - HTML parsing (if needed)

## Environment Variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `DATABASE_URL` | For job processing | PostgreSQL connection string |
| `OPENAI_API_KEY` | For embeddings | OpenAI API key for semantic search |
| `SERPAPI_API_KEY` | For discovery | SerpAPI key for company discovery |
| `SEARXNG_URL` | Optional | SearXNG instance URL (self-hosted, FREE unlimited) |
| `FIRECRAWL_API_KEY` | Optional | Firecrawl API (500 free credits, $16/mo) |
| `GOOGLE_API_KEY` | Optional | Google Custom Search API (FREE tier) |
| `GOOGLE_CSE_ID` | Optional | Custom Search Engine ID |
| `BING_API_KEY` | Optional | Bing Search API (FREE tier) |

## API Endpoints

**Public Jobs CSV:** https://storage.stapply.ai/jobs.csv

This CSV contains all aggregated job postings with:
- Job title, description, location
- Company name and ATS platform
- Salary range (if available)
- Remote/hybrid/onsite status
- Post date and update timestamps
- Semantic embeddings (for search)

## Contributing

We welcome contributions! Here's how you can help:

### Add More Companies

1. Find companies using ATS platforms
2. Add URLs to the appropriate CSV:
   - `ashby/companies.csv`
   - `greenhouse/greenhouse_companies.csv`
   - `lever/lever_companies.csv`
   - `workable/workable_companies.csv`
3. Submit a pull request

### Improve Discovery Scripts

- Add new search strategies to `enhanced_discovery.py`
- Improve cost optimization in `optimized_serp_discovery.py`
- Add new free discovery sources

### Add New ATS Platforms

- Create scraper in new platform directory
- Add Pydantic models in `models/`
- Add platform to discovery scripts
- Update documentation

## License

Please feel free to make requests to improve or add more companies. Contributions are welcome!

## Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Submit a pull request
- Contact: https://stapply.ai

---

**Current Status:** Actively maintained | 1,788 companies tracked | Updated daily
