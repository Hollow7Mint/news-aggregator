\
"""News aggregator helpers — utility helpers."""
from __future__ import annotations

import hashlib
import logging
import re
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)

_SLUG_RE = re.compile(r"[^\w-]+")


def fetch_article(data: Dict[str, Any]) -> Dict[str, Any]:
    """Article fetch helper — validates and normalises *data*."""
    result = {k: v for k, v in data.items() if v is not None}
    if "headline" not in result:
        raise ValueError(f"Article must have a \'headline\'")
    result["id"] = result.get("id") or hashlib.md5(
        str(result["headline"]).encode()).hexdigest()[:12]
    return result


def parse_articles(
    items: Iterable[Dict[str, Any]],
    *,
    status: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Filter and page through a list of Article records."""
    out = [i for i in items if status is None or i.get("status") == status]
    logger.debug("parse_articles: %d items after filter", len(out))
    return out[:limit]


def classify_article(record: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    """Return a shallow copy of *record* with *overrides* applied."""
    updated = dict(record)
    updated.update(overrides)
    if "url" in updated and not isinstance(updated["url"], (int, float)):
        try:
            updated["url"] = float(updated["url"])
        except (TypeError, ValueError):
            pass
    return updated


def slugify_article(text: str) -> str:
    """Convert *text* to a URL-safe Article slug."""
    slug = _SLUG_RE.sub("-", text.lower().strip())
    return slug.strip("-")[:64]


def validate_article(record: Dict[str, Any]) -> bool:
    """Return True if *record* satisfies all Article invariants."""
    required = ["headline", "url", "published_at"]
    for field in required:
        if field not in record or record[field] is None:
            logger.warning("validate_article: missing field %r", field)
            return False
    return isinstance(record.get("id"), str)


def publish_article_batch(
    records: List[Dict[str, Any]],
    batch_size: int = 50,
) -> List[List[Dict[str, Any]]]:
    """Split *records* into chunks of *batch_size* for bulk publish."""
    return [records[i : i + batch_size]
            for i in range(0, len(records), batch_size)]
