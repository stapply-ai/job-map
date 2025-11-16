# SearXNG Setup Guide

Complete guide to setting up your own FREE, self-hosted search engine for unlimited company discovery.

## What is SearXNG?

**SearXNG** is a privacy-respecting metasearch engine that:
- ✅ Aggregates results from **multiple search engines** (Google, DuckDuckGo, Bing, etc.)
- ✅ **Completely FREE** - no API costs or limits
- ✅ **Self-hosted** - you control everything
- ✅ **No API keys** needed
- ✅ **Privacy-focused** - no tracking
- ✅ **JSON API** for programmatic access

**Perfect for:** Unlimited company discovery at $0 cost!

---

## Quick Start (Docker - Recommended)

### Prerequisites

- **Docker** and **Docker Compose** installed
- **5-10 minutes** of setup time
- **Minimal server** (can run on localhost)

### Step 1: Clone SearXNG Docker Repository

```bash
# Clone official SearXNG docker setup
git clone https://github.com/searxng/searxng-docker.git
cd searxng-docker
```

### Step 2: Generate Secret Key

```bash
# Generate a random secret key
sed -i "s|ultrasecretkey|$(openssl rand -hex 32)|g" searxng/settings.yml
```

**On macOS:**
```bash
sed -i '' "s|ultrasecretkey|$(openssl rand -hex 32)|g" searxng/settings.yml
```

### Step 3: Enable JSON API

Edit `searxng/settings.yml` to enable JSON format:

```bash
# Open settings file
nano searxng/settings.yml  # or vim, or any editor
```

Find the `search:` section and add `json` format:

```yaml
search:
  # Search formats (remove formats to deny access)
  formats:
    - html
    - json  # ← ADD THIS LINE!
```

### Step 4: Start SearXNG

```bash
# Start in background
docker compose up -d

# Check logs
docker compose logs -f searxng
```

### Step 5: Test Your Instance

```bash
# Test HTML interface
curl http://localhost:8080/

# Test JSON API
curl "http://localhost:8080/search?q=test&format=json" | jq
```

If you see JSON results, you're all set! 🎉

### Step 6: Configure Environment

Add to your `.env` file:

```bash
echo "SEARXNG_URL=http://localhost:8080" >> .env
```

### Step 7: Test Discovery Script

```bash
# Test with one platform
python searxng_discovery.py --platform ashby --max-queries 5

# Run full discovery (unlimited, $0 cost!)
python searxng_discovery.py --platform all --max-queries 20
```

---

## Detailed Setup Options

### Option 1: Localhost (Development/Testing)

**Best for:** Personal use, development, testing

```bash
# Default configuration works out of the box
docker compose up -d

# Access at: http://localhost:8080
# Set SEARXNG_URL=http://localhost:8080 in .env
```

**Pros:**
- ✅ Easiest setup
- ✅ No network configuration
- ✅ Instant start

**Cons:**
- ❌ Only accessible from same machine
- ❌ No HTTPS

### Option 2: Server with Domain (Production)

**Best for:** Remote access, production use, multiple users

#### 2.1. Update docker-compose.yml

Edit `docker-compose.yaml`:

```yaml
services:
  caddy:
    container_name: caddy
    image: docker.io/library/caddy:2-alpine
    network_mode: host
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy-data:/data:rw
      - caddy-config:/config:rw
    environment:
      - SEARXNG_HOSTNAME=search.yourdomain.com  # ← Change this
      - SEARXNG_TLS=your@email.com              # ← Your email
    cap_drop:
      - ALL
    cap_add:
      - NET_BIND_SERVICE
```

#### 2.2. Configure DNS

Point your domain to your server:

```
A record: search.yourdomain.com → your_server_ip
```

#### 2.3. Start with HTTPS

```bash
docker compose up -d
```

Caddy will automatically get Let's Encrypt certificate!

**Access at:** `https://search.yourdomain.com`

**Set in .env:**
```bash
SEARXNG_URL=https://search.yourdomain.com
```

### Option 3: Behind Existing Reverse Proxy

**Best for:** If you already have Nginx/Traefik/etc.

#### 3.1. Simplified Docker Compose

Remove Caddy from `docker-compose.yaml`:

```yaml
services:
  redis:
    # ... keep as is

  searxng:
    container_name: searxng
    image: docker.io/searxng/searxng:latest
    ports:
      - "127.0.0.1:8080:8080"  # Expose only on localhost
    # ... rest of config
```

#### 3.2. Nginx Configuration Example

```nginx
server {
    listen 443 ssl http2;
    server_name search.yourdomain.com;

    # SSL configuration
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

---

## Advanced Configuration

### Customize Search Engines

Edit `searxng/settings.yml`:

```yaml
engines:
  # Enable/disable specific engines
  - name: google
    disabled: false  # Set to true to disable

  - name: duckduckgo
    disabled: false

  - name: bing
    disabled: false

  # Add more engines as needed
```

### Optimize for Company Discovery

```yaml
search:
  # Number of results per page
  default_results: 20

  # Safe search
  safe_search: 0  # 0=off, 1=moderate, 2=strict

  # Autocomplete
  autocomplete: "google"

  # Formats
  formats:
    - html
    - json
    - csv  # Optional: also enable CSV
```

### Performance Tuning

Edit `docker-compose.yaml` to add more resources:

```yaml
searxng:
  # ... existing config
  deploy:
    resources:
      limits:
        cpus: '2'
        memory: 2G
      reservations:
        cpus: '1'
        memory: 512M
```

### Enable Rate Limiting (Optional)

Edit `searxng/settings.yml`:

```yaml
server:
  limiter: true  # Enable rate limiting
  rate_limit:
    # Limit per IP address
    http: 200/minute
    search: 100/minute
```

---

## Troubleshooting

### Issue: JSON API Returns HTML

**Problem:** Getting HTML instead of JSON results

**Solution:**

1. Check that JSON is enabled in `searxng/settings.yml`:
   ```yaml
   search:
     formats:
       - html
       - json  # Must be here!
   ```

2. Restart container:
   ```bash
   docker compose restart searxng
   ```

3. Test with format parameter:
   ```bash
   curl "http://localhost:8080/search?q=test&format=json"
   ```

### Issue: No Results Returned

**Problem:** Search returns empty results

**Solution:**

1. Check search engines are enabled:
   ```bash
   docker compose exec searxng cat /etc/searxng/settings.yml | grep -A 20 "engines:"
   ```

2. Test individual engines:
   ```bash
   curl "http://localhost:8080/search?q=test&format=json&engines=google"
   ```

3. Check logs:
   ```bash
   docker compose logs -f searxng
   ```

### Issue: Container Won't Start

**Problem:** Docker container fails to start

**Solution:**

1. Check logs:
   ```bash
   docker compose logs searxng
   ```

2. Verify settings.yml syntax:
   ```bash
   docker compose exec searxng cat /etc/searxng/settings.yml
   ```

3. Rebuild:
   ```bash
   docker compose down
   docker compose up -d --force-recreate
   ```

### Issue: Slow Search Results

**Problem:** Searches taking too long

**Solution:**

1. Reduce number of engines:
   ```yaml
   # In settings.yml, disable slower engines
   engines:
     - name: some_slow_engine
       disabled: true
   ```

2. Increase timeout:
   ```yaml
   outgoing:
     request_timeout: 3.0  # seconds (default: 2.0)
   ```

3. Enable caching:
   ```yaml
   server:
     cache:
       enabled: true
   ```

---

## Security Best Practices

### 1. Use HTTPS in Production

Always use HTTPS with a valid certificate (Caddy handles this automatically).

### 2. Restrict Access (Optional)

If you want to restrict who can use your instance:

**Option A: Basic Auth with Caddy**

Edit `Caddyfile`:

```caddy
{$SEARXNG_HOSTNAME} {
    tls {$SEARXNG_TLS}

    basicauth {
        username hashed_password  # Generate with: caddy hash-password
    }

    reverse_proxy localhost:8080
}
```

**Option B: IP Whitelist**

```caddy
{$SEARXNG_HOSTNAME} {
    tls {$SEARXNG_TLS}

    @allowed {
        remote_ip 1.2.3.4 5.6.7.8  # Your IPs
    }

    handle @allowed {
        reverse_proxy localhost:8080
    }

    handle {
        respond "Access denied" 403
    }
}
```

### 3. Keep Updated

```bash
# Update SearXNG regularly
cd searxng-docker
docker compose pull
docker compose up -d
```

### 4. Monitor Resource Usage

```bash
# Check resource usage
docker stats searxng

# Check disk usage
docker system df
```

---

## Cost Analysis

### SearXNG (Self-Hosted) vs Paid APIs

| Service | Setup | Monthly Cost | Queries | Limits |
|---------|-------|--------------|---------|--------|
| **SearXNG (Self-hosted)** | 10 min | **$0** | Unlimited | None! |
| **SearXNG (VPS)** | 30 min | **$5-10** | Unlimited | Server only |
| SERP API | 0 min | $50 | 5,000 | API limits |
| Google CSE | 5 min | $0 | 100/day | Daily limit |
| Firecrawl | 5 min | $16 | 10,000 credits | Credit-based |

### Hosting Options

**Localhost (Free):**
- Your own computer
- $0/month
- Good for: Development, testing, personal use

**VPS (Cheap):**
- DigitalOcean, Linode, Vultr, Hetzner
- $5-10/month (smallest droplet)
- Good for: Remote access, production, sharing

**Free Tier Cloud:**
- Oracle Cloud (Always Free tier)
- Google Cloud ($300 free credit)
- AWS (12 months free)
- $0/month (with limits)
- Good for: Testing cloud deployment

### Recommended Setup

**For Personal Use:**
```
Localhost + Docker = $0/month, unlimited queries
```

**For Production:**
```
$5 VPS + SearXNG = $5/month, unlimited queries
(vs $50/month for SERP API with 5,000 query limit)
```

**ROI:** Save $45-540/year!

---

## Maintenance

### Updating SearXNG

```bash
cd searxng-docker

# Pull latest images
docker compose pull

# Restart with new version
docker compose up -d
```

### Backup Configuration

```bash
# Backup settings
cp searxng/settings.yml searxng/settings.yml.backup

# Backup entire directory
tar -czf searxng-backup-$(date +%Y%m%d).tar.gz searxng-docker/
```

### Monitor Logs

```bash
# Follow logs
docker compose logs -f

# Last 100 lines
docker compose logs --tail=100

# Specific service
docker compose logs -f searxng
```

### Check Resource Usage

```bash
# Container stats
docker stats

# Disk usage
docker system df

# Clean up old images
docker system prune -a
```

---

## Alternative: Public SearXNG Instances

If you don't want to self-host, some public instances allow JSON API access.

**Popular Public Instances:**
- https://searx.be
- https://search.sapti.me
- https://searx.tiekoetter.com
- https://searx.ninja

**⚠️ Limitations:**
- May not allow JSON API
- May have rate limits
- Less reliable (can go down)
- Privacy concerns (not your server)

**To use public instance:**
```bash
# Test if JSON works
curl "https://searx.be/search?q=test&format=json"

# If it works, add to .env
SEARXNG_URL=https://searx.be
```

**Recommendation:** Self-host for best results!

---

## Using SearXNG for Company Discovery

### Basic Usage

```bash
# Discover all platforms (20 queries per platform, 3 pages each)
python searxng_discovery.py --platform all --max-queries 20 --pages 3

# Single platform with more queries
python searxng_discovery.py --platform ashby --max-queries 30 --pages 5

# Use specific search engines
python searxng_discovery.py --platform greenhouse --engines "google,bing"

# Deep discovery (unlimited!)
python searxng_discovery.py --platform all --max-queries 50 --pages 10
```

### Optimizing Results

**Use Multiple Search Engines:**
```bash
# More engines = more diverse results
python searxng_discovery.py --platform all --engines "google,duckduckgo,bing,qwant"
```

**Increase Pages:**
```bash
# More pages = more results per query
python searxng_discovery.py --platform all --pages 10
```

**More Queries:**
```bash
# Unlike paid APIs, you can use unlimited queries!
python searxng_discovery.py --platform all --max-queries 100
```

### Expected Results

| Queries | Pages | Expected Companies | Time | Cost |
|---------|-------|-------------------|------|------|
| 20 | 3 | 400-800 | 10 min | $0 |
| 30 | 5 | 600-1,200 | 15 min | $0 |
| 50 | 10 | 1,000-2,000 | 30 min | $0 |
| 100 | 10 | 2,000-4,000 | 60 min | $0 |

**No limits, no costs - run as often as you want!**

---

## FAQ

### Q: How much does SearXNG cost?

**A:** $0 if self-hosted on your computer. $5-10/month if you use a VPS for remote access. Either way, **unlimited queries at no additional cost!**

### Q: Is it legal to self-host a search engine?

**A:** Yes! SearXNG is open-source (AGPL-3.0). It's a metasearch engine that queries other search engines, which is completely legal.

### Q: How does it compare to Google Custom Search API?

| Feature | SearXNG | Google CSE |
|---------|---------|------------|
| Cost | $0 | $0 for 100/day |
| Queries | Unlimited | 100/day limit |
| Setup | 10 min | 5 min |
| Privacy | Complete | Google tracks |
| Engines | Multiple | Google only |

### Q: Can I use it for commercial purposes?

**A:** Yes! The AGPL-3.0 license allows commercial use. If self-hosted, you have no restrictions on usage.

### Q: How long does setup take?

**A:** 10-15 minutes for basic setup. 30 minutes if configuring domain and HTTPS.

### Q: Do I need a powerful server?

**A:** No! SearXNG runs fine on:
- Your laptop (localhost)
- Raspberry Pi
- $5/month VPS (1 vCPU, 1GB RAM)
- Free tier cloud instances

### Q: What if my self-hosted instance goes down?

**A:** You can:
1. Restart it (docker compose restart)
2. Fall back to a public instance temporarily
3. Use other discovery methods (Firecrawl, Google CSE)

---

## Summary

### Why Use SearXNG for Company Discovery?

✅ **$0 cost** - completely free
✅ **Unlimited queries** - no rate limits
✅ **No API keys** - self-hosted
✅ **Multiple search engines** - better coverage
✅ **Privacy-focused** - no tracking
✅ **Easy setup** - 10 minutes with Docker
✅ **Flexible** - run on laptop or VPS

### Quick Start Checklist

- [ ] Install Docker and Docker Compose
- [ ] Clone searxng-docker repository
- [ ] Enable JSON format in settings.yml
- [ ] Start with `docker compose up -d`
- [ ] Add `SEARXNG_URL=http://localhost:8080` to .env
- [ ] Test with `python searxng_discovery.py --platform ashby --max-queries 5`
- [ ] Run full discovery: `python searxng_discovery.py --platform all`

### Next Steps

1. Set up SearXNG (follow this guide)
2. Test with small query first
3. Run full discovery on all platforms
4. Compare results with other methods
5. Enjoy unlimited, free company discovery! 🎉

---

## Support

**Documentation:** https://docs.searxng.org/
**GitHub:** https://github.com/searxng/searxng
**Docker Setup:** https://github.com/searxng/searxng-docker

**For this project:**
- See `searxng_discovery.py` for usage
- Check `.env.example` for configuration
- Read `README.md` for integration with other discovery methods
