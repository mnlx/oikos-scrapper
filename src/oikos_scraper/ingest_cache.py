from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import structlog

from oikos_scraper.settings import get_setting

LOGGER = structlog.get_logger(__name__)

try:  # pragma: no cover - exercised in runtime environments
    from redis import Redis
    from redis.exceptions import RedisError
except Exception:  # pragma: no cover - local environments may not have redis installed
    Redis = None

    class RedisError(Exception):
        pass


def normalize_page_url(page_url: str) -> str:
    parts = urlsplit(page_url)
    scheme = parts.scheme.lower()
    hostname = (parts.hostname or "").lower()
    port = parts.port
    if port is not None and ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        port = None
    netloc = hostname
    if parts.username:
        netloc = parts.username
        if parts.password:
            netloc = f"{netloc}:{parts.password}"
        netloc = f"{netloc}@{hostname}"
    if port is not None:
        netloc = f"{netloc}:{port}"
    path = parts.path or "/"
    if path != "/":
        path = path.rstrip("/") or "/"
    query = urlencode(parse_qsl(parts.query, keep_blank_values=True), doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


def ingest_cache_enabled() -> bool:
    raw = get_setting("OIKOS_INGEST_CACHE_ENABLED", "false") or "false"
    return raw.lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class IngestCache:
    client: object
    prefix: str
    ttl_seconds: int
    enabled: bool = True

    def listing_key_for(self, source_code: str, external_id: str) -> str:
        return f"{self.prefix}:listing:{source_code}:{external_id.strip()}"

    def page_key_for(self, source_code: str, page_url: str) -> str:
        normalized = normalize_page_url(page_url)
        return f"{self.prefix}:page:{source_code}:{normalized}"

    def reserve_listing(self, source_code: str, external_id: str) -> bool:
        if not self.enabled:
            return True
        key = self.listing_key_for(source_code, external_id)
        reserved = self.client.set(key, "1", nx=True, ex=self.ttl_seconds)
        return bool(reserved)

    def reserve_page(self, source_code: str, page_url: str) -> bool:
        if not self.enabled:
            return True
        key = self.page_key_for(source_code, page_url)
        reserved = self.client.set(key, "1", nx=True, ex=self.ttl_seconds)
        return bool(reserved)

    def release_listing(self, source_code: str, external_id: str) -> None:
        if not self.enabled:
            return
        self.client.delete(self.listing_key_for(source_code, external_id))

    def release_page(self, source_code: str, page_url: str) -> None:
        if not self.enabled:
            return
        self.client.delete(self.page_key_for(source_code, page_url))


@dataclass(slots=True)
class NullIngestCache:
    enabled: bool = False

    def listing_key_for(self, source_code: str, external_id: str) -> str:
        return f"{source_code}:{external_id.strip()}"

    def page_key_for(self, source_code: str, page_url: str) -> str:
        return normalize_page_url(page_url)

    def reserve_listing(self, source_code: str, external_id: str) -> bool:
        return True

    def reserve_page(self, source_code: str, page_url: str) -> bool:
        return True

    def release_listing(self, source_code: str, external_id: str) -> None:
        return

    def release_page(self, source_code: str, page_url: str) -> None:
        return


def build_ingest_cache() -> IngestCache | NullIngestCache:
    if not ingest_cache_enabled():
        return NullIngestCache()
    if Redis is None:
        LOGGER.warning("ingest_cache_unavailable", reason="redis_dependency_missing")
        return NullIngestCache()
    cache_url = get_setting("OIKOS_INGEST_CACHE_URL")
    if not cache_url:
        LOGGER.warning("ingest_cache_unavailable", reason="missing_cache_url")
        return NullIngestCache()
    prefix = get_setting("OIKOS_INGEST_CACHE_PREFIX", "oikos:ingest-page") or "oikos:ingest-page"
    ttl_seconds = int(get_setting("OIKOS_INGEST_CACHE_TTL_SECONDS", "86400") or "86400")
    return IngestCache(
        client=Redis.from_url(cache_url, decode_responses=True),
        prefix=prefix,
        ttl_seconds=ttl_seconds,
    )
