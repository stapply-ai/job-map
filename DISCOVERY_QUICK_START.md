# Company Discovery Quick Start Guide

## TL;DR - What to Run Right Now

### Option 1: Enhanced Discovery (Recommended) 🌟
```bash
# Uses 55+ search strategies to find the most companies
python enhanced_discovery.py --platform all --pages 10 --strategies 10
# Cost: ~$4 (SERPAPI_API_KEY required), Time: 15 minutes
# Finds: 500-1,000 companies
```

### Option 2: FREE API Tier (No cost!)
```bash
# Google Custom Search (100/day = 3,000/month FREE)
# Setup: See SERP_ALTERNATIVES.md for Google API setup (5 minutes)
python google_custom_search.py --platform all --max-queries 100
# Cost: $0, Time: 10 minutes
# Finds: 500-1,000 companies
```

### Option 3: Optimized Discovery (Minimal cost)
```bash
# Uses query caching and top 5 strategies
python optimized_serp_discovery.py --platform all --max-queries 25
# Cost: ~$2.50 (25 queries × 4 platforms × $0.01)
# Time: 15 minutes
# Finds: 500-1,000 companies
```

### Option 4: Manual Curation (FREE, time-intensive)
```bash
# Visit company directories and check careers pages
# Y Combinator: https://www.ycombinator.com/companies
# BuiltIn: https://builtin.com/jobs
# Cost: $0, Time: 30-60 minutes
# Finds: 500-1,000 companies
```

## Setup Instructions

### 1. Enhanced Discovery (SERPAPI setup)
```bash
# Add to .env file
echo "SERPAPI_API_KEY=your_key_here" >> .env

# Run discovery
python enhanced_discovery.py --platform all --pages 10 --strategies 10
```

Get your SERPAPI key from: https://serpapi.com/ (100 free queries/month, then $50/mo for 5,000)

### 2. Google Custom Search API (5 min setup, FREE)

**Step 1: Create Custom Search Engine**
1. Go to: https://programmablesearchengine.google.com/
2. Click "Add"
3. Name: "ATS Company Discovery"
4. Search: "Entire web"
5. Click "Create"
6. Copy your **Search Engine ID** (looks like: `a1b2c3d4e5f6g7h8i`)

**Step 2: Get API Key**
1. Go to: https://console.cloud.google.com/apis/credentials
2. Click "Create Credentials" → "API Key"
3. Copy your **API Key**
4. Click "Restrict Key"
5. Under "API restrictions", select "Custom Search API"
6. Save

**Step 3: Add to .env**
```bash
echo "GOOGLE_API_KEY=your_api_key_here" >> .env
echo "GOOGLE_CSE_ID=your_search_engine_id" >> .env
```

**Step 4: Test**
```bash
python google_custom_search.py --platform ashby --max-queries 10
```

**Free Tier:** 100 searches/day = 3,000/month

### 3. Bing Search API (5 min setup, FREE)

**Step 1: Create Azure Account**
1. Go to: https://portal.azure.com/
2. Sign up (free account, no credit card for free tier)
3. Search for "Bing Search v7"
4. Click "Create"
5. Choose "Free" pricing tier (1,000 searches/month)
6. Copy your **API Key**

**Step 2: Add to .env**
```bash
echo "BING_API_KEY=your_api_key_here" >> .env
```

**Free Tier:** 1,000 searches/month

### 4. SerpAPI (Optional, $50/month)

Only if you need paid discovery:
1. Go to: https://serpapi.com/
2. Sign up
3. Get your API key
4. Add to .env:
```bash
echo "SERPAPI_API_KEY=your_api_key" >> .env
```
