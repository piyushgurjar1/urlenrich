import logging
from typing import Any

_logger = logging.getLogger(__name__)

async def _enrich_source_urls(stage2b: dict[str, Any]) -> None:
    from enrich_api import enrich

    sold_items = stage2b.get("clean_sold_comps", [])
    active_items = stage2b.get("clean_active_listings", [])

    # Build payload — only items with both address and source
    payload = {
        "clean_sold_comps": [
            {"address": it.get("address"), "source": it.get("source")}
            for it in sold_items if it.get("address") and it.get("source")
        ],
        "clean_active_listings": [
            {"address": it.get("address"), "source": it.get("source")}
            for it in active_items if it.get("address") and it.get("source")
        ],
    }

    total = len(payload["clean_sold_comps"]) + len(payload["clean_active_listings"])
    if total == 0:
        _logger.info("No items to enrich.")
        return

    _logger.info(f"Starting enrichment for {total} items...")
    try:
        data = await enrich(payload)
    except Exception as exc:
        _logger.warning("URL enrichment failed: %s", exc)
        return

    if not data or not isinstance(data, dict):
        _logger.warning("Received invalid data from enrich")
        return

    # Build lookup from results
    results = data.get("results", [])
    url_map: dict[str, str] = {}
    for row in results:
        if not isinstance(row, dict):
            continue
        addr = (row.get("address") or "").strip().lower()
        url = row.get("source_url") or row.get("url")
        if addr and url:
            url_map[addr] = url

    # Write URLs back into original items
    for item in sold_items + active_items:
        addr = (item.get("address") or "").strip().lower()
        if addr and addr in url_map:
            item["source_url"] = url_map[addr]
            _logger.info(f"Updated item with URL: {item['source_url']}")