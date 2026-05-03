import asyncio
import logging
import requests

_logger = logging.getLogger(__name__)

SERP_API_KEY = "93c3e3d8c48efa1bbd866a2380659bb75397812d704190ebfaf4f1970d84e308"
SERP_API_ENDPOINT = "https://serpapi.com/search"


def _serp_fetch_url_for_property(address: str, source: str) -> str | None:
    """
    Fetch the best matching property URL using SerpAPI Google Search.
    Searches for '<address> <source>' and returns the first result
    whose domain matches the source.
    """
    query = f"{address.strip()} {source.strip()}"
    target = (
        source.lower()
        .replace(" ", "")
        .replace(".com", "")
        .replace("www.", "")
    )

    params = {
        "engine": "google",
        "q": query,
        "api_key": SERP_API_KEY,
        "hl": "en",
        "gl": "us",
        "num": 10,
    }

    try:
        response = requests.get(SERP_API_ENDPOINT, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        organic_results = data.get("organic_results", [])

        if not organic_results:
            _logger.debug("No organic results returned for '%s'", address)
            return None

        # First pass: find exact domain match
        for result in organic_results:
            link = result.get("link")
            if not link:
                continue
            from urllib.parse import urlparse
            domain = urlparse(link).netloc.lower()
            if target in domain:
                _logger.debug("Matched URL for '%s': %s", address, link)
                return link

        # Fallback: return position-1 result if available
        fallback = organic_results[0].get("link")
        _logger.warning(
            "No domain match for '%s' on source '%s'. Falling back to: %s",
            address, source, fallback,
        )
        return fallback

    except requests.exceptions.HTTPError as e:
        _logger.error("HTTP error fetching SERP for '%s': %s", address, e)
    except requests.exceptions.RequestException as e:
        _logger.error("Request error fetching SERP for '%s': %s", address, e)
    except Exception as e:
        _logger.error("Unexpected error fetching SERP for '%s': %s", address, e)

    return None


async def enrich(data: dict) -> dict:
    """Enrich items with URLs using SerpAPI. Runs sequentially to respect API rate limits."""
    items = data.get("clean_sold_comps", []) + data.get("clean_active_listings", [])
    results = []

    _logger.info("Starting SERP enrichment for %d items", len(items))

    for i, item in enumerate(items):
        address = item.get("address")
        source = item.get("source")

        if not address or not source:
            results.append({**item, "url": None, "source_url": None})
            continue

        url = await asyncio.to_thread(_serp_fetch_url_for_property, address, source)
        results.append({**item, "url": url, "source_url": url})

        if url:
            _logger.info("Enriched item %d: %s -> %s", i + 1, address, url)
        else:
            _logger.warning("FAILED item %d: %s", i + 1, address)

    return {"results": results}
