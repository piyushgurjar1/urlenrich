import asyncio
import logging
import os
import re
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests

_logger = logging.getLogger(__name__)

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(levelname)s:%(name)s:%(message)s",
    )

# ---------------------------------------------------------------------------
# Serper API config
# ---------------------------------------------------------------------------
SERPER_API_KEY      = os.getenv("SERPER_API_KEY", "27377e7bc4e37f9fd4cd11051fdc61bae97c11a6")
SERPER_API_ENDPOINT = os.getenv("SERPER_API_URL", "https://google.serper.dev/search")
SERPER_TIMEOUT      = float(os.getenv("SERPER_TIMEOUT", "30"))
SERPER_NUM_RESULTS  = int(os.getenv("SERPER_NUM_RESULTS", "10"))
_MIN_RESULT_SCORE   = int(os.getenv("SERPER_MIN_RESULT_SCORE", "60"))


# ---------------------------------------------------------------------------
# Domain helpers
# ---------------------------------------------------------------------------
def _normalize_domain(value: str) -> str:
    value = (value or "").strip().lower()
    value = value.replace("https://", "").replace("http://", "")
    value = value.replace("www.", "").strip("/")
    return value


def _source_domain_variants(source: str) -> List[str]:
    cleaned = _normalize_domain(source)
    variants = set()

    if cleaned:
        variants.add(cleaned)
    if cleaned and "." not in cleaned:
        variants.add(f"{cleaned}.com")

    alias_map = {
        "zillow":      {"zillow.com"},
        "zillow.com":  {"zillow.com"},
        "realtor":     {"realtor.com"},
        "realtor.com": {"realtor.com"},
        "redfin":      {"redfin.com"},
        "redfin.com":  {"redfin.com"},
        "trulia":      {"trulia.com"},
        "trulia.com":  {"trulia.com"},
        "homes":       {"homes.com"},
        "homes.com":   {"homes.com"},
    }

    for variant in list(variants):
        variants.update(alias_map.get(variant, set()))

    return sorted({v.replace("www.", "") for v in variants if v})


def _tokenize_address(address: str) -> List[str]:
    tokens = re.findall(r"[a-z0-9]+", (address or "").lower())
    ignore = {
        "st", "street", "ave", "avenue", "rd", "road", "dr", "drive", "ln", "lane",
        "ct", "court", "ter", "terrace", "blvd", "boulevard", "cir", "circle",
        "port", "charlotte", "fl",
    }
    return [t for t in tokens if (t.isdigit() or len(t) >= 4) and t not in ignore]


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def _score_candidate(url: str, title: str, snippet: str, address: str, source: str) -> int:
    parsed = urlparse(url)
    domain = parsed.netloc.lower().replace("www.", "")
    path   = parsed.path.lower()

    score = 0
    source_domains = _source_domain_variants(source)

    if domain in source_domains:
        score += 70
    elif any(domain.endswith("." + s) for s in source_domains):
        score += 50
    elif any(s in domain for s in source_domains):
        score += 25
    else:
        score -= 80

    combined       = f"{url.lower()} {title.lower()} {snippet.lower()}"
    address_tokens = _tokenize_address(address)
    token_hits     = sum(1 for token in address_tokens if token in combined)
    score += min(token_hits * 6, 30)

    house_number = next((t for t in address_tokens if t.isdigit()), None)
    if house_number and house_number in path:
        score += 12

    propertyish_paths = ["/homedetails/", "/realestateandhomes-detail/", "/property/", "/listing/"]
    if any(marker in path for marker in propertyish_paths):
        score += 10

    bad_paths = ["/search", "/agent", "/agents", "/directory", "/profile", "/sitemap"]
    if any(marker in path for marker in bad_paths):
        score -= 25

    if path in ("", "/"):
        score -= 20

    return score


# ---------------------------------------------------------------------------
# Core Serper API fetch
# ---------------------------------------------------------------------------
def _serper_fetch_url_for_property(address: str, source: str) -> Optional[str]:
    address = (address or "").strip()
    source  = (source  or "").strip()

    if not address or not source:
        return None

    query   = f"{address} {source}"
    headers = {
        "X-API-KEY":    SERPER_API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "q":   query,
        "num": SERPER_NUM_RESULTS,
    }

    _logger.info("Serper request start | address=%s | source=%s", address, source)

    try:
        response = requests.post(
            SERPER_API_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=SERPER_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        _logger.info(
            "Serper response received | address=%s | status=%s | credits_used=%s",
            address, response.status_code, data.get("credits", "?"),
        )

        # Serper uses "organic" key (not "organic_results")
        organic_results: List[Dict] = data.get("organic", [])

        if not organic_results:
            _logger.warning("Serper no organic results | address=%s | source=%s", address, source)
            return None

        # Score every organic result
        scored = []
        for result in organic_results:
            url     = result.get("link", "")
            title   = result.get("title", "")
            snippet = result.get("snippet", "")

            if not url:
                continue

            score = _score_candidate(url, title, snippet, address, source)
            scored.append((score, url))
            _logger.debug(
                "Serper candidate | address=%s | score=%s | url=%s",
                address, score, url,
            )

        if not scored:
            _logger.warning("Serper no scorable candidates | address=%s", address)
            return None

        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_url = scored[0]

        _logger.info(
            "Serper best candidate | address=%s | score=%s | url=%s",
            address, best_score, best_url,
        )

        if best_score >= _MIN_RESULT_SCORE:
            return best_url

        _logger.warning(
            "Serper low score | address=%s | source=%s | score=%s | url=%s",
            address, source, best_score, best_url,
        )
        return None

    except requests.exceptions.HTTPError as exc:
        _logger.error("Serper HTTP error | address=%s | error=%s", address, exc)
    except requests.exceptions.ConnectTimeout as exc:
        _logger.warning("Serper connect timeout | address=%s | error=%s", address, exc)
    except requests.exceptions.ReadTimeout as exc:
        _logger.warning("Serper read timeout | address=%s | error=%s", address, exc)
    except requests.exceptions.RequestException as exc:
        _logger.warning("Serper request exception | address=%s | error=%s", address, exc)
    except Exception as exc:
        _logger.exception("Serper unexpected error | address=%s | error=%s", address, exc)

    return None


# ---------------------------------------------------------------------------
# Enrich
# ---------------------------------------------------------------------------
async def enrich(data: dict) -> dict:
    items = data.get("clean_sold_comps", []) + data.get("clean_active_listings", [])
    results = []

    _logger.info("Starting Serper batch enrichment for %d items", len(items))

    for i, item in enumerate(items):
        address = item.get("address")
        source  = item.get("source")

        if not address or not source:
            results.append({**item, "url": None, "source_url": None})
            continue

        url = await asyncio.to_thread(_serper_fetch_url_for_property, address, source)

        results.append({**item, "url": url, "source_url": url})

        if url:
            _logger.info("Enriched item %d: %s -> %s", i + 1, address, url)
        else:
            _logger.warning("FAILED item %d: %s", i + 1, address)

    return {"results": results}
