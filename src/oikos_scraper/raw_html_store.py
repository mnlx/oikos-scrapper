from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO

from minio import Minio

from oikos_scraper.heuristics import slugify
from oikos_scraper.settings import get_setting
from oikos_scraper.types import ListingDraft


@dataclass(slots=True)
class RawHtmlObject:
    bucket: str
    key: str
    endpoint: str
    secure: bool
    size: int


class RawHtmlStore:
    def __init__(
        self,
        *,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool,
    ) -> None:
        self.endpoint = endpoint
        self.bucket = bucket
        self.secure = secure
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def ensure_bucket(self) -> None:
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def object_key_for(self, listing: ListingDraft) -> str:
        return (
            f"{slugify(listing.source_code)}/"
            f"{slugify(listing.city)}/"
            f"{slugify(listing.external_id)}.html"
        )

    def upload_listing_html(self, listing: ListingDraft, html: str) -> RawHtmlObject:
        payload = html.encode("utf-8")
        key = self.object_key_for(listing)
        self.ensure_bucket()
        self.client.put_object(
            self.bucket,
            key,
            data=BytesIO(payload),
            length=len(payload),
            content_type="text/html; charset=utf-8",
        )
        return RawHtmlObject(
            bucket=self.bucket,
            key=key,
            endpoint=self.endpoint,
            secure=self.secure,
            size=len(payload),
        )


def build_raw_html_store() -> RawHtmlStore | None:
    endpoint = get_setting("OIKOS_RAW_HTML_S3_ENDPOINT")
    access_key = get_setting("OIKOS_RAW_HTML_S3_ACCESS_KEY")
    secret_key = get_setting("OIKOS_RAW_HTML_S3_SECRET_KEY")
    bucket = get_setting("OIKOS_RAW_HTML_S3_BUCKET")
    secure_value = (get_setting("OIKOS_RAW_HTML_S3_SECURE", "false") or "false").lower()

    if not endpoint or not access_key or not secret_key or not bucket:
        return None

    return RawHtmlStore(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket=bucket,
        secure=secure_value in {"1", "true", "yes", "on"},
    )
