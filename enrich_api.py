import asyncio
import logging
import random
import time

from fastapi import FastAPI
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin, urlparse, parse_qs, quote_plus

app = FastAPI()
_logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://duckduckgo.com/",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}

def _make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD", "OPTIONS"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

session = _make_session()

def _ddg_fetch_url_for_property(address: str, source: str) -> str | None:
    query = quote_plus(f"{address.strip()} {source.strip()}")
    url = f"https://html.duckduckgo.com/html/?q={query}"
    target = source.lower().replace(" ", "").replace(".com", "").replace("www.", "")

    for attempt in range(1, 4):
        try:
            time.sleep(random.uniform(1.0, 2.5))

            res = session.get(url, timeout=(15, 25))

            if res.status_code in (202, 403, 429):
                wait = min(20, (2 ** attempt) + random.uniform(1, 3))
                _logger.warning(
                    "DDG blocked/challenged address=%s source=%s status=%s wait=%.1fs",
                    address, source, res.status_code, wait
                )
                time.sleep(wait)
                continue

            if res.status_code != 200:
                _logger.warning(
                    "DDG non-200 address=%s source=%s status=%s",
                    address, source, res.status_code
                )
                time.sleep(random.uniform(2, 4))
                continue

            soup = BeautifulSoup(res.text, "html.parser")
            results = soup.select("a.result__a")

            if not results:
                _logger.info("No DDG results for address=%s source=%s", address, source)
                time.sleep(random.uniform(1, 2))
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

        except requests.exceptions.ConnectTimeout as e:
            wait = min(20, (2 ** attempt) + random.uniform(1, 3))
            _logger.warning(
                "DDG connect timeout address=%s source=%s attempt=%s error=%s wait=%.1fs",
                address, source, attempt, e, wait
            )
            time.sleep(wait)

        except requests.exceptions.ReadTimeout as e:
            wait = min(15, attempt + random.uniform(1, 2))
            _logger.warning(
                "DDG read timeout address=%s source=%s attempt=%s error=%s wait=%.1fs",
                address, source, attempt, e, wait
            )
            time.sleep(wait)

        except requests.RequestException as e:
            wait = min(10, attempt + random.uniform(1, 2))
            _logger.warning(
                "DDG request error address=%s source=%s attempt=%s error=%s wait=%.1fs",
                address, source, attempt, e, wait
            )
            time.sleep(wait)

        except Exception as e:
            _logger.exception(
                "Unexpected DDG error address=%s source=%s attempt=%s error=%s",
                address, source, attempt, e
            )
            time.sleep(random.uniform(1, 2))

    return None

async def process_item(item: dict) -> dict:
    address = item.get("address")
    source = item.get("source")

    if not address or not source:
        return {**item, "url": None, "source_url": None}

    url = await asyncio.to_thread(_ddg_fetch_url_for_property, address, source)
    return {**item, "url": url, "source_url": url}

@app.post("/enrich")
async def enrich(data: dict):
    items = data.get("clean_sold_comps", []) + data.get("clean_active_listings", [])

    semaphore = asyncio.Semaphore(3)

    async def sem_task(item):
        async with semaphore:
            await asyncio.sleep(random.uniform(1, 3))
            return await process_item(item)

    results = await asyncio.gather(*(sem_task(item) for item in items))
    return {"results": results}
