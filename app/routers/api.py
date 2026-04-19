"""
Operational API endpoints — consumed by the Lay Engine and other FSUs.

  GET /api/markets                       List all cached markets (filterable)
  GET /api/markets/{market_id}           Single market summary + runner prices
  GET /api/markets/{market_id}/prices    Runner best-available prices (compact)
  GET /api/markets/{market_id}/book      Full order book for all runners
  GET /api/stream/status                 Current streaming connection state

All data is served from in-memory state — no on-demand Betfair REST calls.
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.market_cache import market_cache
from app.state import app_state

router = APIRouter()


# ---------------------------------------------------------------------------
# Market list
# ---------------------------------------------------------------------------

@router.get("/markets")
async def list_markets(
    event_type_id: Optional[str] = Query(None, description="Filter by Betfair event type ID"),
    status: Optional[str] = Query(None, description="Filter by market status (e.g. OPEN, SUSPENDED)"),
    in_play: Optional[bool] = Query(None, description="Filter in-play markets"),
    market_type: Optional[str] = Query(None, description="Filter by market type (e.g. WIN, MATCH_ODDS)"),
    country_code: Optional[str] = Query(None, description="Filter by country code"),
):
    markets = await market_cache.get_all_markets()
    result = []
    for ms in markets:
        s = ms.to_summary_dict()
        if event_type_id is not None and s.get("eventTypeId") != event_type_id:
            continue
        if status is not None and s.get("status") != status:
            continue
        if in_play is not None and s.get("inPlay") != in_play:
            continue
        if market_type is not None and s.get("marketType") != market_type:
            continue
        if country_code is not None and s.get("countryCode") != country_code:
            continue
        result.append(s)

    return {"markets": result, "count": len(result)}


# ---------------------------------------------------------------------------
# Single market
# ---------------------------------------------------------------------------

@router.get("/markets/{market_id}")
async def get_market(market_id: str):
    """Full market detail with runner prices (best 3 levels each side)."""
    ms = await market_cache.get_market(market_id)
    if not ms:
        raise HTTPException(status_code=404, detail=f"Market {market_id} not found in cache")
    return ms.to_detail_dict()


@router.get("/markets/{market_id}/prices")
async def get_market_prices(market_id: str):
    """
    Compact runner prices — the primary endpoint consumed by the Lay Engine.
    Returns best 3 levels of batl/batb for every runner.

    Response shape the Lay Engine expects:
      {
        "marketId": "1.234567890",
        "status": "OPEN",
        "inPlay": false,
        "totalMatched": 12345.67,
        "runners": [
          {
            "selectionId": 12345,
            "handicap": 0.0,
            "status": "ACTIVE",
            "lastPriceTraded": 4.5,
            "totalMatched": 1234.56,
            "bestAvailableToBack": [[5.0, 200.0], [4.8, 150.0], [4.6, 100.0]],
            "bestAvailableToLay":  [[4.5, 50.0],  [4.6, 80.0],  [4.8, 120.0]],
            "spNear": null,
            "spFar": null,
            "spActual": null
          }
        ]
      }
    """
    ms = await market_cache.get_market(market_id)
    if not ms:
        raise HTTPException(status_code=404, detail=f"Market {market_id} not found in cache")

    return {
        "marketId": market_id,
        "status": ms.status or ms.market_definition.get("status"),
        "inPlay": ms.market_definition.get("inPlay", False),
        "totalMatched": ms.total_volume,
        "runners": [r.to_prices_dict() for r in ms.runners.values()],
    }


@router.get("/markets/{market_id}/book")
async def get_market_book(market_id: str):
    """
    Full order book — all available/lay/traded price points for every runner,
    plus the raw marketDefinition. Used by downstream consumers that need depth
    beyond the best 3 levels.
    """
    ms = await market_cache.get_market(market_id)
    if not ms:
        raise HTTPException(status_code=404, detail=f"Market {market_id} not found in cache")
    return ms.to_book_dict()


# ---------------------------------------------------------------------------
# Stream diagnostics
# ---------------------------------------------------------------------------

@router.get("/stream/status")
async def stream_status():
    """Current streaming connection state — useful for Lay Engine health checks."""
    lm = app_state.last_message_at
    return {
        "status": app_state.stream_status,
        "connectionId": app_state.connection_id,
        "latencyIndicator": app_state.stream_latency,
        "lastMessageAt": lm.isoformat() if lm else None,
        "reconnectCount": app_state.reconnect_count,
        "marketCount": app_state.market_count,
    }
