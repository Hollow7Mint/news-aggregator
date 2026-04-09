"""News aggregator database — Feed management."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class NewsDatabase:
    """Feed database for the news-aggregator application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._headline = self._cfg.get("headline", None)
        logger.debug("NewsDatabase ready (store=%s)", type(store).__name__)

    def fetch_feed(
        self, headline: Any, url: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Feed record."""
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "headline":   headline,
            "url":   url,
            "status":     "active",
            "created_at": datetime.utcnow().isoformat(),
            **extra,
        }
        saved = self._store.put(record)
        logger.info("fetch_feed: created %s", saved["id"])
        return saved

    def get_feed(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Feed by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_feed: %s not found", record_id)
        return record

    def parse_feed(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Feed."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Feed not found: {record_id}")
        record.update(changes)
        record["updated_at"] = datetime.utcnow().isoformat()
        return self._store.put(record)

    def classify_feed(self, record_id: str) -> bool:
        """Remove a Feed record; returns True if deleted."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("classify_feed: removed %s", record_id)
        return True

    def list_feeds(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return a filtered, paginated list of Feed records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_feeds: %d results", len(results))
        return results

    def iter_feeds(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Feed records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_feeds(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size
