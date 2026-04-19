"""
Singleton application state — shared across all request handlers and the stream client.
No external dependencies; safe to import everywhere.
"""

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

UTC = timezone.utc


class AppState:
    def __init__(self) -> None:
        # Runtime settings (loaded from Firestore at startup)
        self.settings: dict = {}
        self.mode: str = "active"

        # Betfair streaming connection state
        self.stream_status: str = "disconnected"   # disconnected | connecting | connected | reconnecting
        self.connection_id: Optional[str] = None
        self.stream_latency: bool = False           # True when server sends status=503
        self.last_message_at: Optional[datetime] = None
        self.reconnect_count: int = 0
        self.market_count: int = 0

        # Betfair session state (managed by betfair_auth.py)
        self.session_token: Optional[str] = None
        self.session_acquired_at: Optional[datetime] = None

        # Request counters (used for error_rate and metrics)
        self.request_count: int = 0
        self.error_count: int = 0

        # Service birth timestamp
        self.started_at: datetime = datetime.now(UTC)

        # Structured log ring buffer (maxlen=10000 matches FSU1E)
        self._logs: deque = deque(maxlen=10_000)

        # SSE pub/sub
        self._subscribers: list[asyncio.Queue] = []
        self._subscriber_lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Derived properties
    # ------------------------------------------------------------------

    @property
    def health(self) -> str:
        if self.mode == "drain":
            return "draining"
        if self.stream_status == "connected" and not self.stream_latency:
            return "healthy"
        if self.stream_status in ("connecting", "reconnecting"):
            return "degraded"
        return "unhealthy"

    @property
    def error_rate(self) -> float:
        if self.request_count == 0:
            return 0.0
        return self.error_count / self.request_count

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

    def add_log(self, level: str, message: str, **extra) -> None:
        entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": level,
            "message": message,
            **extra,
        }
        self._logs.append(entry)

    def get_logs(self, limit: int = 100, offset: int = 0) -> tuple[list, int]:
        snapshot = list(self._logs)
        total = len(snapshot)
        return snapshot[offset : offset + limit], total

    # ------------------------------------------------------------------
    # SSE pub/sub
    # ------------------------------------------------------------------

    async def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        async with self._subscriber_lock:
            self._subscribers.append(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue) -> None:
        async with self._subscriber_lock:
            self._subscribers = [s for s in self._subscribers if s is not q]

    async def broadcast(self, event: dict) -> None:
        async with self._subscriber_lock:
            for q in self._subscribers:
                try:
                    q.put_nowait(event)
                except asyncio.QueueFull:
                    pass  # slow consumer — drop rather than block


# Module-level singleton
app_state = AppState()
