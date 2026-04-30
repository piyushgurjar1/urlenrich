import asyncio
import logging
import os
import random
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

import cloudscraper
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectTimeout, ReadTimeout, RequestException
from urllib3.util.retry import Retry

app = FastAPI()
_logger = logging.getLogger("enrich_api")

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(levelname)s:%(name)s:%(message)s",
    )

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

_DDG_HTML_URL = os.getenv("DDG_HTML_URL", "https://html.duckduckgo.com/html/")
_CONNECT_TIMEOUT = float(os.getenv("DDG_CONNECT_TIMEOUT", "8"))
_READ_TIMEOUT = float(os.getenv("DDG_READ_TIMEOUT", "20"))
_MAX_RETRIES = int(os.getenv("DDG_MAX_RETRIES", "3"))
_CONCURRENCY = int(os.getenv("ENRICH_CONCURRENCY", "3"))
_PRE_REQUEST_DELAY_MIN = float(os.getenv("DDG_PRE_REQUEST_DELAY_MIN", "1"))
_PRE_REQUEST_DELAY_MAX = float(os.getenv("DDG_PRE_REQUEST_DELAY_MAX", "3"))
_RETRY_BACKOFF_MIN = float(os.getenv("DDG_RETRY_BACKOFF_MIN", "4"))
_RETRY_BACKOFF_MAX = float(os.getenv("DDG_RETRY_BACKOFF_MAX", "9"))
_PER_ITEM_DELAY_MIN = float(os.getenv("ENRICH_DELAY_MIN", "0"))
_PER_ITEM_DELAY_MAX = float(os.getenv("ENRICH_DELAY_MAX", "0"))
_USE_CLOUDSCRAPER_FALLBACK = os.getenv("USE_CLOUDSCRAPER_FALLBACK", "true").lower() == "true"
_MIN_SCORE = int(os.getenv("DDG_MIN_SCORE", "65"))

_thread_local = threading.local()


def _mask_text(value: str, keep: int = 6) -> str:
    if not value:
        return value
    if len(value) <= keep:
        return value
    return value[:keep] + "..."


def _build_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def _mount_adapters(session: requests.Session) -> None:
    retry = Retry(
        total=0,
        connect=0,
        read=0,
        redirect=0,
        status=0,
        backoff_factor=0,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=10,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)


def _make_requests_session() -> requests.Session:
    session = requests.Session()
    _mount_adapters(session)
    session.headers.update(_build_headers())
    return session


def _make_cloudscraper_session() -> requests.Session:
    session = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )
    _mount_adapters(session)
    session.headers.update(_build_headers())
    return session


def _get_requests_session() -> requests.Session:
    session = getattr(_thread_local, "requests_session", None)
    if session is None:
        session = _make_requests_session()
        _thread_local.requests_session = session
    return session


def _get_cloudscraper_session() -> requests.Session:
    session = getattr(_thread_local, "cloudscraper_session", None)
    if session is None:
        session = _make_cloudscraper_session()
        _thread_local.cloudscraper_session = session
    return session


def _clean_source(source: str) -> str:
    source = source.strip().lower()
    source = source.replace("https://", "").replace("http://", "")
    source = source.replace("www.", "")
    source = source.strip("/")
    return source


def _source_variants(source: str) -> List[str]:
    cleaned = _clean_source(source)
    variants = set()

    if cleaned:
        variants.add(cleaned)

    if "." not in cleaned and cleaned:
        variants.add(f"{cleaned}.com")

    alias_map = {
        "zillow": {"zillow.com"},
        "zillow.com": {"zillow.com"},
        "realtor": {"realtor.com"},
        "realtor.com": {"realtor.com"},
        "redfin": {"redfin.com"},
        "redfin.com": {"redfin.com"},
        "trulia": {"trulia.com"},
        "trulia.com": {"trulia.com"},
        "homes": {"homes.com"},
        "homes.com": {"homes.com"},
    }

    for variant in list(variants):
        variants.update(alias_map.get(variant, set()))

    expanded = set()
    for variant in variants:
        variant = variant.replace("www.", "")
        expanded.add(variant)
        expanded.add(f"www.{variant}")

    return sorted(expanded)


def _tokenize_address(address: str) -> List[str]:
    raw_tokens = re.findall(r"[a-z0-9]+", address.lower())
    ignore = {
        "st", "street", "ave", "avenue", "rd", "road", "dr", "drive", "ln", "lane",
        "ct", "court", "ter", "terrace", "blvd", "boulevard", "cir", "circle",
        "port", "charlotte", "fl", "usa"
    }
    tokens = []
    for token in raw_tokens:
        if token.isdigit() or len(token) >= 4:
            if token not in ignore:
                tokens.append(token)
    return tokens


def _extract_actual_url(raw_href: Optional[str]) -> Optional[str]:
    if not raw_href:
        return None

    full_url = urljoin("https://duckduckgo.com", raw_href)
    parsed = urlparse(full_url)

    if "duckduckgo.com" in parsed.netloc:
        actual_url = parse_qs(parsed.query).get("uddg", [None])[0]
        if actual_url:
            return actual_url

    if parsed.scheme in ("http", "https"):
        return full_url

    return None


def _looks_like_bad_property_page(path: str) -> bool:
    bad_markers = [
        "/search",
        "/agent",
        "/agents",
        "/profile",
        "/directory",
        "/sitemap",
        "/homes/",
        "/apartments/",
        "/community/",
        "/neighborhood/",
    ]
    return any(marker in path for marker in bad_markers)


def _score_candidate(
    actual_url: str,
    title: str,
    snippet: str,
    address: str,
    source: str,
) -> Tuple[int, str]:
    parsed = urlparse(actual_url)
    domain = parsed.netloc.lower().replace("www.", "")
    path = parsed.path.lower()
    title_l = title.lower()
    snippet_l = snippet.lower()
    combined = f"{actual_url.lower()} {title_l} {snippet_l}"

    score = 0
    reasons: List[str] = []
    source_domains = [d.replace("www.", "") for d in _source_variants(source)]

    if domain in source_domains:
        score += 70
        reasons.append("exact-domain")
    elif any(domain.endswith("." + sd) for sd in source_domains):
        score += 55
        reasons.append("subdomain-match")
    elif any(sd in domain for sd in source_domains):
        score += 35
        reasons.append("partial-domain")
    else:
        score -= 80
        reasons.append("wrong-domain")

    tokens = _tokenize_address(address)
    token_hits = sum(1 for token in tokens if token in combined)
    score += min(token_hits * 6, 30)
    if token_hits:
        reasons.append(f"token-hits:{token_hits}")

    street_number = next((t for t in tokens if t.isdigit()), None)
    if street_number and street_number in path:
        score += 15
        reasons.append("street-number-in-path")

    if path in ("", "/"):
        score -= 25
        reasons.append("homepage-penalty")

    if _looks_like_bad_property_page(path):
        score -= 25
        reasons.append("non-property-penalty")

    if any(marker in path for marker in ["/homedetails/", "/homes/", "/property/", "/realestateandhomes-detail/"]):
        score += 10
        reasons.append("propertyish-path")

    return score, ",".join(reasons)


def _parse_ddg_results(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: List[Dict[str, str]] = []

    for anchor in soup.select("a.result__a"):
        raw_href = anchor.get("href")
        actual_url = _extract_actual_url(raw_href)
        if not actual_url:
            continue

        title = anchor.get_text(" ", strip=True)
        snippet = ""
        parent = anchor.find_parent()
        if parent:
            result_container = parent
            for _ in range(4):
                if result_container is None:
                    break
                snippet_node = result_container.select_one(".result__snippet")
                if snippet_node:
                    snippet = snippet_node.get_text(" ", strip=True)
                    break
                result_container = result_container.parent

        candidates.append(
            {
                "title": title,
                "snippet": snippet,
                "url": actual_url,
            }
        )

    return candidates


def _request_with_client(
    client: requests.Session,
    url: str,
    client_name: str,
    address: str,
    attempt: int,
) -> Optional[requests.Response]:
    headers = _build_headers()
    delay = random.uniform(_PRE_REQUEST_DELAY_MIN, _PRE_REQUEST_DELAY_MAX)
    _logger.info(
        "ddg_request_start address=%s attempt=%s client=%s delay=%.1f",
        _mask_text(address, 18),
        attempt,
        client_name,
        delay,
    )
    time.sleep(delay)

    response = client.get(
        url,
        headers=headers,
        timeout=(_CONNECT_TIMEOUT, _READ_TIMEOUT),
        allow_redirects=True,
    )

    _logger.info(
        "ddg_response address=%s attempt=%s client=%s status=%s",
        _mask_text(address, 18),
        attempt,
        client_name,
        response.status_code,
    )
    return response


def _ddg_fetch_url_for_property(address: str, source: str) -> Optional[str]:
    address = (address or "").strip()
    source = (source or "").strip()

    if not address or not source:
        return None

    query = quote_plus(f"{address} {source}")
    url = f"{_DDG_HTML_URL}?q={query}"

    last_error: Optional[str] = None

    for attempt in range(1, _MAX_RETRIES + 1):
        clients: List[Tuple[str, requests.Session]] = [("requests", _get_requests_session())]
        if _USE_CLOUDSCRAPER_FALLBACK:
            clients.append(("cloudscraper", _get_cloudscraper_session()))

        for client_name, client in clients:
            try:
                response = _request_with_client(client, url, client_name, address, attempt)

                if response.status_code in (202, 403, 429):
                    last_error = f"challenge-status-{response.status_code}"
                    wait = random.uniform(_RETRY_BACKOFF_MIN, _RETRY_BACKOFF_MAX) * attempt
                    _logger.warning(
                        "ddg_challenge address=%s attempt=%s client=%s status=%s wait=%.1f",
                        _mask_text(address, 18),
                        attempt,
                        client_name,
                        response.status_code,
                        wait,
                    )
                    time.sleep(wait)
                    continue

                if response.status_code != 200:
                    last_error = f"http-{response.status_code}"
                    wait = random.uniform(_RETRY_BACKOFF_MIN, _RETRY_BACKOFF_MAX)
                    _logger.warning(
                        "ddg_non_200 address=%s attempt=%s client=%s status=%s wait=%.1f",
                        _mask_text(address, 18),
                        attempt,
                        client_name,
                        response.status_code,
                        wait,
                    )
                    time.sleep(wait)
                    continue

                candidates = _parse_ddg_results(response.text)
                _logger.info(
                    "ddg_candidates address=%s attempt=%s client=%s count=%s",
                    _mask_text(address, 18),
                    attempt,
                    client_name,
                    len(candidates),
                )

                if not candidates:
                    last_error = "no-candidates"
                    continue

                scored: List[Tuple[int, str, Dict[str, str]]] = []
                for candidate in candidates:
                    score, reason = _score_candidate(
                        candidate["url"],
                        candidate["title"],
                        candidate["snippet"],
                        address,
                        source,
                    )
                    scored.append((score, reason, candidate))

                scored.sort(key=lambda item: item[0], reverse=True)
                best_score, best_reason, best_candidate = scored[0]

                _logger.info(
                    "ddg_best_candidate address=%s attempt=%s client=%s score=%s reason=%s url=%s",
                    _mask_text(address, 18),
                    attempt,
                    client_name,
                    best_score,
                    best_reason,
                    best_candidate["url"],
                )

                if best_score >= _MIN_SCORE:
                    return best_candidate["url"]

                last_error = f"low-score-{best_score}"

            except ConnectTimeout as exc:
                last_error = f"connect-timeout:{exc.__class__.__name__}"
                _logger.warning(
                    "ddg_connect_timeout address=%s attempt=%s client=%s error=%s",
                    _mask_text(address, 18),
                    attempt,
                    client_name,
                    str(exc),
                )
            except ReadTimeout as exc:
                last_error = f"read-timeout:{exc.__class__.__name__}"
                _logger.warning(
                    "ddg_read_timeout address=%s attempt=%s client=%s error=%s",
                    _mask_text(address, 18),
                    attempt,
                    client_name,
                    str(exc),
                )
            except RequestException as exc:
                last_error = f"request-exception:{exc.__class__.__name__}"
                _logger.warning(
                    "ddg_request_exception address=%s attempt=%s client=%s error=%s",
                    _mask_text(address, 18),
                    attempt,
                    client_name,
                    str(exc),
                )
            except Exception as exc:
                last_error = f"unexpected:{exc.__class__.__name__}"
                _logger.exception(
                    "ddg_unexpected address=%s attempt=%s client=%s error=%s",
                    _mask_text(address, 18),
                    attempt,
                    client_name,
                    str(exc),
                )

        if attempt < _MAX_RETRIES:
            wait = random.uniform(_RETRY_BACKOFF_MIN, _RETRY_BACKOFF_MAX) * attempt
            _logger.info(
                "ddg_retry_backoff address=%s attempt=%s wait=%.1f last_error=%s",
                _mask_text(address, 18),
                attempt,
                wait,
                last_error,
            )
            time.sleep(wait)

    _logger.error(
        "ddg_failed address=%s source=%s last_error=%s",
        _mask_text(address, 18),
        source,
        last_error,
    )
    return None


async def _process_item(item: Dict[str, Any]) -> Dict[str, Any]:
    address = item.get("address")
    source = item.get("source")

    if not address or not source:
        return {
            **item,
            "url": None,
            "source_url": None,
            "matched_domain": None,
            "debug_reason": "missing-address-or-source",
        }

    url = await asyncio.to_thread(_ddg_fetch_url_for_property, address, source)
    matched_domain = urlparse(url).netloc.lower() if url else None

    return {
        **item,
        "url": url,
        "source_url": url,
        "matched_domain": matched_domain,
        "debug_reason": "ok" if url else "not-found-or-low-confidence",
    }


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/enrich")
async def enrich(data: Dict[str, Any]) -> Dict[str, Any]:
    items = data.get("clean_sold_comps", []) + data.get("clean_active_listings", [])
    semaphore = asyncio.Semaphore(_CONCURRENCY)

    _logger.info("batch_start count=%s concurrency=%s", len(items), _CONCURRENCY)

    async def sem_task(index: int, item: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            if _PER_ITEM_DELAY_MAX > 0:
                delay = random.uniform(_PER_ITEM_DELAY_MIN, _PER_ITEM_DELAY_MAX)
                _logger.info("item_delay index=%s delay=%.1f", index, delay)
                await asyncio.sleep(delay)

            result = await _process_item(item)
            if result.get("url"):
                _logger.info("item_success index=%s address=%s url=%s", index, item.get("address"), result["url"])
            else:
                _logger.warning("item_failed index=%s address=%s reason=%s", index, item.get("address"), result.get("debug_reason"))
            return result

    results = await asyncio.gather(*(sem_task(i, item) for i, item in enumerate(items)))
    _logger.info("batch_done count=%s", len(results))
    return {"results": results}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "enrich_api:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=False,
    )
