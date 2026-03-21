from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from oikos_scraper.object_store import BronzePathSpec, build_bronze_object_store, offering_hash
from oikos_scraper.types import ListingDraft


@dataclass(slots=True)
class RawHtmlObject:
    bucket: str
    key: str
    endpoint: str
    secure: bool
    size: int


class RawHtmlStore:
    def __init__(self, store) -> None:  # noqa: ANN001
        self.store = store
        self.endpoint = store.endpoint
        self.bucket = store.bucket
        self.secure = store.secure

    def upload_listing_html(self, listing: ListingDraft, html: str) -> RawHtmlObject:
        base_hash = offering_hash(listing.source_code, listing.external_id)
        key = BronzePathSpec(
            layer="bronze",
            category="html",
            run_at=datetime.now(UTC),
            base_hash=base_hash,
            extension=".html",
        ).object_key()
        uploaded = self.store.put_text(payload=html, key=key, content_type="text/html; charset=utf-8")
        return RawHtmlObject(
            bucket=uploaded.bucket,
            key=uploaded.key,
            endpoint=uploaded.endpoint,
            secure=uploaded.secure,
            size=uploaded.size,
        )


def build_raw_html_store() -> RawHtmlStore | None:
    store = build_bronze_object_store()
    if store is None:
        return None
    return RawHtmlStore(store)
