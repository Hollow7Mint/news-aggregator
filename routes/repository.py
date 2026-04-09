"""News aggregator repository — Article management."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class NewsRepository:
    """Article repository for the news-aggregator application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._source_id = self._cfg.get("source_id", None)
        logger.debug("NewsRepository ready (store=%s)", type(store).__name__)

    def rank_article(
        self, source_id: Any, headline: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Article record."""
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "source_id":   source_id,
            "headline":   headline,
            "status":     "active",
            "created_at": datetime.utcnow().isoformat(),
            **extra,
        }
        saved = self._store.put(record)
        logger.info("rank_article: created %s", saved["id"])
        return saved

    def get_article(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Article by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_article: %s not found", record_id)
        return record

    def fetch_article(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Article."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Article not found: {record_id}")
        record.update(changes)
        record["updated_at"] = datetime.utcnow().isoformat()
        return self._store.put(record)

    def parse_article(self, record_id: str) -> bool:
        """Remove a Article record; returns True if deleted."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("parse_article: removed %s", record_id)
        return True

    def list_articles(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return a filtered, paginated list of Article records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_articles: %d results", len(results))
        return results

    def iter_articles(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Article records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_articles(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size
