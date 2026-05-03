import os
import re
import asyncio
import logging
from typing import Optional
from urllib.parse import urlparse

import requests
from fastapi import FastAPI

app = FastAPI()
_logger = logging.getLogger(__name__)

SERPAPI_KEY = os.getenv("SERPAPI_KEY")  # set this in Render env vars
SERPAPI_URL = "https://serpapi.com/search"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

session = requests.Session()
session.headers.update(HEADERS)


def normalize_text(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value


def source_domain(source: str) -> str:
    s = source.lower().strip()
    if "zillow" in s:
        return "zillow.com"
    if "redfin" in s:
        return "redfin.com"
    if "realtor" in s:
        return "realtor.com"
    return s.replace("www.", "").replace(".com", "") + ".com"


def is_source_match(link: str, source: str) -> bool:
    domain = urlparse(link).netloc.lower()
    target = source_domain(source)
    return target in domain


def score_result(result: dict, address: str, source: str) -> int:
    link = result.get("link", "") or ""
    title = result.get("title", "") or ""
    snippet = result.get("snippet", "") or ""

    address_n = normalize_text(address)
    title_n = normalize_text(title)
    snippet_n = normalize_text(snippet)
    link_n = normalize_text(link)

    score = 0

    if is_source_match(link, source):
        score += 50

    if address_n in title_n:
        score += 40
    elif any(part in title_n for part in address_n.split()[:4]):
        score += 10

    if address_n in snippet_n:
        score += 20

    if address_n in link_n:
        score += 10

    if "/homedetails/" in link.lower() or "/home/" in link.lower():
        score += 10

    return score


def serpapi_search_exact_url(address: str, source: str) -> Optional[str]:
    if not SERPAPI_KEY:
        _logger.error("SERPAPI_KEY is missing")
        return None

    domain = source_domain(source)

    queries = [
        f'site:{domain} "{address}"',
        f'site:{domain} {address}',
        f'"{address}" "{source}"',
    ]

    best_link = None
    best_score = -1

    for q in queries:
        params = {
            "engine": "google",
            "q": q,
            "api_key": SERPAPI_KEY,
            "num": 10,
            "hl": "en",
            "gl": "us",
            "no_cache": "true",
        }

        try:
            res = session.get(SERPAPI_URL, params=params, timeout=(15, 30))
            res.raise_for_status()
            data = res.json()

            organic_results = data.get("organic_results", []) or []
            for result in organic_results:
                link = result.get("link")
                if not link:
                    continue

                score = score_result(result, address, source)
                if score > best_score:
                    best_score = score
                    best_link = link

                if score >= 90:
                    return link

        except requests.RequestException as e:
            _logger.warning("SerpApi request failed address=%s source=%s error=%s", address, source, e)
        except Exception as e:
            _logger.exception("Unexpected SerpApi error address=%s source=%s error=%s", address, source, e)

    return best_link


async def process_item(item: dict) -> dict:
    address = item.get("address")
    source = item.get("source")

    if not address or not source:
        return {**item, "url": None, "source_url": None}

    url = await asyncio.to_thread(serpapi_search_exact_url, address, source)
    return {**item, "url": url, "source_url": url}


@app.post("/enrich")
async def enrich(data: dict):
    items = data.get("clean_sold_comps", []) + data.get("clean_active_listings", [])

    semaphore = asyncio.Semaphore(5)

    async def sem_task(item):
        async with semaphore:
            return await process_item(item)

    results = await asyncio.gather(*(sem_task(item) for item in items))
    return {"results": results}
