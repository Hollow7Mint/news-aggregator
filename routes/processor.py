"""News aggregator processor — Source management."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class NewsProcessor:
    """Source processor for the news-aggregator application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._category = self._cfg.get("category", None)
        logger.debug("NewsProcessor ready (store=%s)", type(store).__name__)

    def publish_source(
        self, category: Any, score: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Source record."""
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "category":   category,
            "score":   score,
            "status":     "active",
            "created_at": datetime.utcnow().isoformat(),
            **extra,
        }
        saved = self._store.put(record)
        logger.info("publish_source: created %s", saved["id"])
        return saved

    def get_source(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Source by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_source: %s not found", record_id)
        return record

    def subscribe_source(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Source."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Source not found: {record_id}")
        record.update(changes)
        record["updated_at"] = datetime.utcnow().isoformat()
        return self._store.put(record)

    def rank_source(self, record_id: str) -> bool:
        """Remove a Source record; returns True if deleted."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("rank_source: removed %s", record_id)
        return True

    def list_sources(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return a filtered, paginated list of Source records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_sources: %d results", len(results))
        return results

    def iter_sources(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Source records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_sources(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size
