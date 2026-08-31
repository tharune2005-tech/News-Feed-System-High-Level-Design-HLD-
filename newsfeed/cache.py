"""Bounded TTL cache standing in for Redis (LRU + TTL)."""

from __future__ import annotations

import time
from collections import OrderedDict
from typing import Generic, Optional, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class LruTtlCache(Generic[K, V]):
    def __init__(self, max_size: int = 1024, default_ttl_seconds: float = 60.0):
        self.max_size = max_size
        self.default_ttl_seconds = default_ttl_seconds
        self._data: OrderedDict[K, tuple[V, float]] = OrderedDict()

    def get(self, key: K) -> Optional[V]:
        item = self._data.get(key)
        if item is None:
            return None
        value, expires_at = item
        if expires_at < time.time():
            del self._data[key]
            return None
        self._data.move_to_end(key)
        return value

    def set(self, key: K, value: V, ttl_seconds: Optional[float] = None) -> None:
        ttl = self.default_ttl_seconds if ttl_seconds is None else ttl_seconds
        self._data[key] = (value, time.time() + ttl)
        self._data.move_to_end(key)
        while len(self._data) > self.max_size:
            self._data.popitem(last=False)

    def delete(self, key: K) -> None:
        self._data.pop(key, None)

    def clear(self) -> None:
        self._data.clear()
