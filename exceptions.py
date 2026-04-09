"""News aggregator exceptions — publish configuration."""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_DEFAULTS: Dict[str, Any] = {
    "category": None,
    "score": 30,
    "source_id": True,
    "max_retries": 3,
    "timeout":     60,
}


class NewsExceptions:
    """Feed exceptions for the news-aggregator system."""

    def __init__(self, **kwargs: Any) -> None:
        self._data: Dict[str, Any] = dict(_DEFAULTS)
        self._data.update(kwargs)
        self._from_env()
        logger.debug("NewsExceptions initialised")

    def _from_env(self) -> None:
        prefix = "NEWS_AGGREGATOR_"
        for key in _DEFAULTS:
            val = os.environ.get(prefix + key.upper())
            if val is not None:
                self._data[key] = val

    def get(self, key: str, default: Any = None) -> Any:
        """Return the category value for *key*."""
        return self._data.get(key, default)

    def update(self, **kwargs: Any) -> None:
        """Update exceptions settings in place."""
        self._data.update(kwargs)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise exceptions to a plain dict."""
        return dict(self._data)

    def __repr__(self) -> str:
        return f"NewsExceptions({self._data!r})"


def load_feed_exceptions(path: Optional[str] = None) -> NewsExceptions:
    """Load Feed exceptions from *path* or environment."""
    kwargs: Dict[str, Any] = {}
    if path and os.path.exists(path):
        import json
        with open(path) as fh:
            kwargs = json.load(fh)
        logger.info("Loaded exceptions from %s", path)
    return NewsExceptions(**kwargs)
