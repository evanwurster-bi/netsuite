"""Tiny in-memory TTL cache for lookups that rarely change.

Used to skip repeated NetSuite/HubSpot round-trips for values like a customer's shared
subsidiaries or a HubSpot owner -> NetSuite employee mapping. The cache lives on a
module-level instance, so it survives across warm Lambda invocations.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional, Tuple


class TTLCache:
    """Minimal key -> value cache with per-entry expiry. Not thread-safe by design — the
    processor handles one message per invocation (BatchSize 1)."""

    def __init__(self, ttl_seconds: float):
        self._ttl = ttl_seconds
        self._store: Dict[Any, Tuple[float, Any]] = {}

    def get(self, key: Any) -> Optional[Any]:
        entry = self._store.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.time() >= expires_at:
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: Any, value: Any) -> None:
        self._store[key] = (time.time() + self._ttl, value)

    def get_or_compute(self, key: Any, compute: Callable[[], Any]) -> Any:
        """Return the cached value, or compute + cache it. A ``None`` result is not cached
        (so a transient miss is retried next time rather than stuck)."""
        cached = self.get(key)
        if cached is not None:
            return cached
        value = compute()
        if value is not None:
            self.set(key, value)
        return value
