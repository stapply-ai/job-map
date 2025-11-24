# 🚀 Stapply Job Data Aggregator

<div align="center">

**A powerful, open-source job aggregator that collects job postings from multiple ATS platforms and makes them publicly available.**

</div>

---

## Map

We built a map of the jobs:

- [Explore the interactive job map](https://map.stapply.ai)

![Preview of the Stapply job map](job-map/public/opengraph-image.jpeg)


## 📊 Public Data

The aggregated job data is available at: **https://storage.stapply.ai/jobs.csv**

This CSV is updated regularly and contains job postings from all supported platforms.

## ✨ Features

- **🌐 Multi-Platform Support** - Scrape jobs from 5+ different ATS platforms (Ashby, Greenhouse, Lever, Workable, Rippling)
- **🔍 Smart Discovery** - Multiple discovery methods to find companies using ATS platforms
- **📦 Structured Data** - Clean, normalized job data with consistent schema across platforms
- **🔄 Incremental Processing** - Checkpoint system prevents duplicate processing
- **📈 Job Lifecycle Tracking** - Automatically detect and mark inactive jobs
- **🎯 Semantic Search** - OpenAI embeddings for job title and description (Ashby)
- **🐳 Self-Hosted Options** - Use SearXNG for unlimited discovery without API limits
- **📝 Export Formats** - CSV exports with automatic diff tracking for new/updated jobs

## 🏗️ Architecture

The project follows a modular, platform-based architecture:

```
data/
├── 📁 Platform Modules (ashby/, greenhouse/, lever/, workable/)
│   ├── main.py              # Platform-specific scraper
│   ├── export_to_csv.py     # CSV export utility
│   ├── companies/           # JSON files (one per company)
│   ├── jobs.csv             # Platform-specific job export
│   └── *_companies.csv      # Company URL registry
│
├── 📁 Core Components
│   ├── models/              # Pydantic data models for each platform
│   ├── classifier/          # Job classification tools
│   ├── searxng-docker/      # SearXNG self-hosted setup
│   └── gather_jobs.py       # Job consolidation utility
│
├── 🔍 Discovery Scripts
│   ├── searxng_discovery.py      # Self-hosted search (unlimited!)
│   ├── discovery.py              # Discovery utilities
│   └── [other discovery methods]
│
└── 📄 Configuration
    ├── pyproject.toml       # Python dependencies
    ├── env.example           # Environment variables template
    └── README.md            # This file
```

## 🚀 Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip
- PostgreSQL (optional, for database processing)
- SearXNG instance (optional, for unlimited discovery)

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

Create a `.env` file from the example:

```bash
cp env.example .env
```

Edit `.env` with your configuration:

```env
# Required for job processing and embeddings
DATABASE_URL=postgresql://user:pass@host:port/dbname
OPENAI_API_KEY=sk-...

# Optional: Discovery methods
SEARXNG_URL=http://localhost:8080        # Self-hosted search (unlimited!)
SERPAPI_API_KEY=your_serpapi_key        # SerpAPI for discovery
FIRECRAWL_API_KEY=fc-your_key           # Firecrawl search API
GOOGLE_API_KEY=your_google_key          # Google Custom Search API
GOOGLE_CSE_ID=your_cse_id               # Custom Search Engine ID
BING_API_KEY=your_bing_key              # Bing Search API
```

## 📖 Usage

### Job Scraping

Scrape jobs from discovered companies:

```bash
# Ashby
cd ashby
python main.py              # Scrapes jobs to companies/ directory as JSON
python export_to_csv.py     # Exports jobs from JSON to jobs.csv
```

### Consolidate Jobs

Merge all platform CSVs into a single file:

```bash
python gather_jobs.py
```

This creates a unified `jobs.csv` at the root with jobs from all platforms.

## 📝 License

This project is open source. Contributions are welcome!

## 🌐 Links

- **Public Jobs CSV**: https://storage.stapply.ai/jobs.csv
- **Website**: https://stapply.ai
- **Issues**: [GitHub Issues](https://github.com/stapply-ai/data/issues)