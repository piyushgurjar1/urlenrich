import asyncio
import logging
import random
import time
import cloudscraper
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, quote_plus

_logger = logging.getLogger(__name__)

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

def _make_scraper():
    """Create a cloudscraper instance to bypass TLS fingerprinting."""
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )
    scraper.headers.update({
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://duckduckgo.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
    })
    return scraper

def _ddg_fetch_url_for_property(address: str, source: str) -> str | None:
    """Fetch a property URL from DuckDuckGo HTML search using cloudscraper."""
    query = quote_plus(f"{address.strip()} {source.strip()}")
    url = f"https://html.duckduckgo.com/html/?q={query}"
    target = source.lower().replace(" ", "").replace(".com", "").replace("www.", "")

    for attempt in range(1, 4):
        scraper = _make_scraper()
        try:
            # Add a small jitter before the actual request
            time.sleep(random.uniform(1, 3))
            res = scraper.get(url, timeout=20)

            if res.status_code in (202, 403):
                 wait = random.uniform(15, 30) * attempt
                 _logger.warning("DDG Challenge (Status %s) for '%s'. Backing off %.1fs", res.status_code, address, wait)
                 time.sleep(wait)
                 continue

            if res.status_code != 200:
                _logger.debug("Non-200 status %s for '%s'", res.status_code, address)
                time.sleep(random.uniform(5, 10))
                continue

            soup = BeautifulSoup(res.text, "html.parser")
            results = soup.select("a.result__a")

            if not results:
                _logger.debug("No results for '%s'", address)
                continue

            for a in results:
                raw = a.get("href")
                if not raw:
                    continue

                full_url = urljoin("https://duckduckgo.com", raw)
                parsed = urlparse(full_url)
                actual_url = parse_qs(parsed.query).get("uddg", [None])[0]

                if not actual_url:
                    continue

                domain = urlparse(actual_url).netloc.lower()
                if target in domain:
                    return actual_url

        except Exception as e:
            _logger.error("Scraping error for '%s': %s", address, e)
            time.sleep(random.uniform(5, 10))
        finally:
            try:
                scraper.close()
            except:
                pass

    return None

async def enrich(data: dict) -> dict:
    """Enrich items with URLs. STRICTLY SEQUENTIAL."""
    items = data.get("clean_sold_comps", []) + data.get("clean_active_listings", [])
    results = []
    
    _logger.info("Starting batch enrichment for %d items", len(items))
    
    for i, item in enumerate(items):
        address = item.get("address")
        source = item.get("source")
        
        if not address or not source:
            results.append({**item, "url": None})
            continue

        if i > 0:
            delay = random.uniform(25, 45)  # Human-like delay
            _logger.info("Coiling per-item delay: %.1fs", delay)
            await asyncio.sleep(delay)

        url = await asyncio.to_thread(_ddg_fetch_url_for_property, address, source)
        results.append({**item, "url": url, "source_url": url})

        if url:
            _logger.info("Enriched item %d: %s -> %s", i+1, address, url)
        else:
            _logger.warning("FAILED item %d: %s", i+1, address)

    return {"results": results}