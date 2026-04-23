import asyncio
import logging
from fastapi import FastAPI, BackgroundTasks
from service import _enrich_source_urls

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

app = FastAPI()

demo_data = {
    "clean_sold_comps": [
        {"address": "364 Lindon St, Port Charlotte, FL 33954", "source": "Realtor.com"},
        {"address": "22238 Clinton Ave, Port Charlotte, FL 33954", "source": "Zillow"},
    ],
    "clean_active_listings": [
        {"address": "286 Salisbury St, Port Charlotte, FL 33954", "source": "Realtor.com"},
        {"address": "286 Salisbury St, Port Charlotte, FL 33954", "source": "Redfin"},
    ],
}

@app.on_event("startup")
async def startup_event():
    _logger.info("FastAPI starting up... triggering demo enrichment test.")
    asyncio.create_task(_enrich_source_urls(demo_data))

@app.post("/enrich")
async def enrich_endpoint(data: dict):
    """
    Manually trigger enrichment via API.
    Expects JSON body like demo_data above.
    """
    _logger.info("Received manual enrichment request via /enrich")
    # We call it and return the modified data
    await _enrich_source_urls(data)
    return data

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/")
async def root():
    return {"status": "running", "demo": "/enrich (POST) or /health (GET)"}
