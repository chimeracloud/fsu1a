"""
In-memory market cache with full Betfair ESA delta reconstruction.

Delta encoding rules (per Betfair Exchange Streaming API spec):

Level-based ladders — batb, batl, bdatb, bdatl
  Each update is a triple: [level, price, size]
  Keyed by level (int). Level 0 = best price.
  size == 0 → remove that level entry.

Price-point ladders — atb, atl, trd, spb, spl
  Each update is a pair: [price, size]
  Keyed by price (float). size == 0 → remove that price entry.

Scalar fields — ltp, tv, spn, spf
  Sent only when changed; absence means unchanged.

img == True on a MarketChange → replace entire market state from scratch.

sp sub-object within a RunnerChange:
  sp.spn, sp.spf — scalar SP near/far
  sp.spb, sp.spl — price-point SP back/lay bets placed
  sp.bsp         — calculated BSP

Segmentation (segmentType: SEG_START | SEG | SEG_END | null):
  Apply all mc[] deltas immediately regardless of segment.
  Only update stored clk on non-segmented (null) or SEG_END messages.

Status 503 in an MCM message: high-latency indicator — do NOT disconnect.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)
UTC = timezone.utc

# Type aliases for clarity
LevelLadder = dict   # int   → [price: float, size: float]
PriceLadder = dict   # float → size: float


@dataclass
class RunnerState:
    """All in-memory state for a single runner (selection)."""

    selection_id: int
    handicap: float = 0.0
    status: str = "ACTIVE"

    # Level-based ladders (EX_BEST_OFFERS)
    batb: LevelLadder = field(default_factory=dict)   # best available to back
    batl: LevelLadder = field(default_factory=dict)   # best available to lay
    bdatb: LevelLadder = field(default_factory=dict)  # best display ATB
    bdatl: LevelLadder = field(default_factory=dict)  # best display ATL

    # Price-point ladders
    atb: PriceLadder = field(default_factory=dict)    # full available to back
    atl: PriceLadder = field(default_factory=dict)    # full available to lay
    trd: PriceLadder = field(default_factory=dict)    # traded volume by price
    spb: PriceLadder = field(default_factory=dict)    # SP back bets
    spl: PriceLadder = field(default_factory=dict)    # SP lay bets

    # Scalar fields
    ltp: Optional[float] = None   # last traded price
    tv: Optional[float] = None    # total traded volume
    spn: Optional[float] = None   # SP near
    spf: Optional[float] = None   # SP far
    bsp: Optional[float] = None   # calculated BSP

    # ------------------------------------------------------------------

    def apply_runner_change(self, rc: dict) -> None:
        """Apply a RunnerChange delta in-place."""

        # Scalars (sent only when changed)
        if "status" in rc:
            self.status = rc["status"]
        if "ltp" in rc:
            self.ltp = rc["ltp"]
        if "tv" in rc:
            self.tv = rc["tv"]
        if "spn" in rc:
            self.spn = rc["spn"]
        if "spf" in rc:
            self.spf = rc["spf"]

        # Level-based ladders: [level, price, size]
        for fname in ("batb", "batl", "bdatb", "bdatl"):
            if fname not in rc:
                continue
            ladder: LevelLadder = getattr(self, fname)
            for triple in rc[fname]:
                level, price, size = int(triple[0]), float(triple[1]), float(triple[2])
                if size == 0:
                    ladder.pop(level, None)
                else:
                    ladder[level] = [price, size]

        # Price-point ladders: [price, size]
        for fname in ("atb", "atl", "trd"):
            if fname not in rc:
                continue
            ladder: PriceLadder = getattr(self, fname)
            for pair in rc[fname]:
                price, size = float(pair[0]), float(pair[1])
                if size == 0:
                    ladder.pop(price, None)
                else:
                    ladder[price] = size

        # sp sub-object (Starting Price data)
        if "sp" in rc:
            sp = rc["sp"]
            if "spn" in sp:
                self.spn = sp["spn"]
            if "spf" in sp:
                self.spf = sp["spf"]
            if "bsp" in sp:
                self.bsp = sp["bsp"]
            for fname in ("spb", "spl"):
                if fname not in sp:
                    continue
                ladder: PriceLadder = getattr(self, fname)
                for pair in sp[fname]:
                    price, size = float(pair[0]), float(pair[1])
                    if size == 0:
                        ladder.pop(price, None)
                    else:
                        ladder[price] = size

    # ------------------------------------------------------------------
    # Derived views used by API endpoints
    # ------------------------------------------------------------------

    def best_available_to_lay(self, depth: int = 3) -> list:
        """
        Top N lay prices.  Primary source: batl (level-based, level 0 = best).
        Falls back to atl (price-point) sorted ascending (lower price = better for layer).
        Returns list of [price, size] pairs.
        """
        if self.batl:
            return [self.batl[lvl] for lvl in sorted(self.batl)[:depth]]
        if self.atl:
            prices = sorted(self.atl.keys())[:depth]
            return [[p, self.atl[p]] for p in prices]
        return []

    def best_available_to_back(self, depth: int = 3) -> list:
        """
        Top N back prices.  Primary source: batb (level-based, level 0 = best).
        Falls back to atb (price-point) sorted descending (higher price = better for backer).
        Returns list of [price, size] pairs.
        """
        if self.batb:
            return [self.batb[lvl] for lvl in sorted(self.batb)[:depth]]
        if self.atb:
            prices = sorted(self.atb.keys(), reverse=True)[:depth]
            return [[p, self.atb[p]] for p in prices]
        return []

    def traded_ladder(self) -> list:
        """All traded price points as [[price, size], ...] sorted by price."""
        return [[p, self.trd[p]] for p in sorted(self.trd.keys())]

    def to_prices_dict(self) -> dict:
        """Compact representation for /api/markets/{id}/prices."""
        return {
            "selectionId": self.selection_id,
            "handicap": self.handicap,
            "status": self.status,
            "lastPriceTraded": self.ltp,
            "totalMatched": self.tv,
            "spNear": self.spn,
            "spFar": self.spf,
            "spActual": self.bsp,
            "bestAvailableToBack": self.best_available_to_back(3),
            "bestAvailableToLay": self.best_available_to_lay(3),
        }

    def to_book_dict(self) -> dict:
        """Full order-book representation for /api/markets/{id}/book."""
        return {
            "selectionId": self.selection_id,
            "handicap": self.handicap,
            "status": self.status,
            "lastPriceTraded": self.ltp,
            "totalMatched": self.tv,
            "spNear": self.spn,
            "spFar": self.spf,
            "spActual": self.bsp,
            "bestAvailableToBack": self.best_available_to_back(3),
            "bestAvailableToLay": self.best_available_to_lay(3),
            "availableToBack": sorted(
                [[p, s] for p, s in self.atb.items()], key=lambda x: -x[0]
            ),
            "availableToLay": sorted(
                [[p, s] for p, s in self.atl.items()], key=lambda x: x[0]
            ),
            "traded": self.traded_ladder(),
        }


@dataclass
class MarketState:
    """All in-memory state for a single market."""

    market_id: str
    market_definition: dict = field(default_factory=dict)
    runners: dict = field(default_factory=dict)   # selection_id (int) → RunnerState
    status: Optional[str] = None
    total_volume: Optional[float] = None
    last_update_at: Optional[datetime] = None

    # ------------------------------------------------------------------

    def _get_or_create_runner(self, selection_id: int, handicap: float = 0.0) -> RunnerState:
        if selection_id not in self.runners:
            self.runners[selection_id] = RunnerState(
                selection_id=selection_id, handicap=handicap
            )
        return self.runners[selection_id]

    def apply_market_change(self, mc: dict) -> None:
        """
        Apply a MarketChange dict to this market's state.
        img=True means full image — wipe and rebuild from scratch.
        """
        if mc.get("img"):
            self.runners.clear()
            self.market_definition = {}
            self.status = None
            self.total_volume = None

        if "marketDefinition" in mc:
            md = mc["marketDefinition"]
            self.market_definition.update(md)
            # Sync runner metadata from marketDefinition.runners
            for rd in md.get("runners", []):
                sid = int(rd["id"])
                runner = self._get_or_create_runner(sid, float(rd.get("hc", 0.0)))
                if "status" in rd:
                    runner.status = rd["status"]

        if "status" in mc:
            self.status = mc["status"]

        if "tv" in mc:
            self.total_volume = mc["tv"]

        # Apply runner-level changes
        for rc in mc.get("rc", []):
            sid = int(rc["id"])
            hc = float(rc.get("hc", 0.0))
            runner = self._get_or_create_runner(sid, hc)
            runner.apply_runner_change(rc)

        self.last_update_at = datetime.now(UTC)

    # ------------------------------------------------------------------
    # Derived views
    # ------------------------------------------------------------------

    def _effective_status(self) -> str:
        return self.status or self.market_definition.get("status", "UNKNOWN")

    def _market_name(self) -> str:
        md = self.market_definition
        market_time = md.get("marketTime", "")
        venue = md.get("venue", "")
        market_type = md.get("marketType", "")
        if market_time and venue:
            try:
                t = datetime.fromisoformat(market_time.replace("Z", "+00:00"))
                return f"{t.strftime('%H:%M')} {venue}"
            except Exception:
                pass
        return f"{venue} {market_type}".strip() or self.market_id

    def to_summary_dict(self) -> dict:
        md = self.market_definition
        return {
            "marketId": self.market_id,
            "status": self._effective_status(),
            "marketType": md.get("marketType"),
            "eventTypeId": md.get("eventTypeId"),
            "eventId": md.get("eventId"),
            "countryCode": md.get("countryCode"),
            "venue": md.get("venue"),
            "name": self._market_name(),
            "marketTime": md.get("marketTime"),
            "suspendTime": md.get("suspendTime"),
            "totalMatched": self.total_volume,
            "inPlay": md.get("inPlay", False),
            "bspMarket": md.get("bspMarket", False),
            "runnerCount": len(self.runners),
            "lastUpdateAt": self.last_update_at.isoformat() if self.last_update_at else None,
        }

    def to_detail_dict(self) -> dict:
        d = self.to_summary_dict()
        d["runners"] = [r.to_prices_dict() for r in self.runners.values()]
        return d

    def to_book_dict(self) -> dict:
        d = self.to_summary_dict()
        d["marketDefinition"] = self.market_definition
        d["runners"] = [r.to_book_dict() for r in self.runners.values()]
        return d


class MarketCache:
    """
    Thread-safe in-memory store for all active market states.
    The asyncio.Lock means all mutations are serialised within the event loop.
    """

    def __init__(self) -> None:
        self._markets: dict[str, MarketState] = {}
        self._lock = asyncio.Lock()

    async def apply_mcm(self, msg: dict) -> None:
        """
        Apply a full MarketChangeMessage (op=mcm) to the cache.
        Handles segmentation transparently — deltas are always applied immediately.
        """
        async with self._lock:
            for mc in msg.get("mc", []):
                mid: str = mc["id"]
                if mid not in self._markets:
                    self._markets[mid] = MarketState(market_id=mid)
                self._markets[mid].apply_market_change(mc)

    async def get_market(self, market_id: str) -> Optional[MarketState]:
        async with self._lock:
            return self._markets.get(market_id)

    async def get_all_markets(self) -> list[MarketState]:
        async with self._lock:
            return list(self._markets.values())

    async def count(self) -> int:
        async with self._lock:
            return len(self._markets)

    async def remove_closed(self) -> int:
        """Remove CLOSED markets. Returns number removed."""
        async with self._lock:
            to_remove = [
                mid
                for mid, ms in self._markets.items()
                if ms.market_definition.get("status") == "CLOSED"
            ]
            for mid in to_remove:
                del self._markets[mid]
            return len(to_remove)


# Module-level singleton
market_cache = MarketCache()
