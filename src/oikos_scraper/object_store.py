from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from mimetypes import guess_extension
from pathlib import PurePosixPath
from urllib.parse import urlparse

import httpx
from minio import Minio

from oikos_scraper.settings import get_setting
from oikos_scraper.types import StoredObject


def _secure_from_setting(value: str | None) -> bool:
    return (value or "false").strip().lower() in {"1", "true", "yes", "on"}


def offering_hash(agency: str, offering: str) -> str:
    return sha256(f"{agency}|{offering}".encode("utf-8")).hexdigest()


@dataclass(slots=True)
class BronzePathSpec:
    layer: str
    category: str
    run_at: datetime
    base_hash: str
    extension: str
    index: int | None = None

    def object_key(self) -> str:
        stamp = self.run_at.astimezone(UTC)
        folder = PurePosixPath(
            self.layer,
            "ingestion",
            "listings",
            self.category,
            f"{stamp.year:04d}",
            f"{stamp.month:02d}",
            f"{stamp.day:02d}",
        )
        suffix = ""
        if self.index is not None:
            suffix = f"-{self.index:02d}"
        return str(folder / f"{self.base_hash}{suffix}{self.extension}")


class BronzeObjectStore:
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

    def _put_bytes(
        self,
        *,
        payload: bytes,
        key: str,
        content_type: str,
    ) -> StoredObject:
        self.ensure_bucket()
        self.client.put_object(
            self.bucket,
            key,
            data=BytesIO(payload),
            length=len(payload),
            content_type=content_type,
        )
        return StoredObject(
            bucket=self.bucket,
            key=key,
            endpoint=self.endpoint,
            secure=self.secure,
            size=len(payload),
            content_type=content_type,
            checksum_sha256=sha256(payload).hexdigest(),
        )

    def put_text(self, *, payload: str, key: str, content_type: str) -> StoredObject:
        return self._put_bytes(payload=payload.encode("utf-8"), key=key, content_type=content_type)

    def put_bytes(self, *, payload: bytes, key: str, content_type: str) -> StoredObject:
        return self._put_bytes(payload=payload, key=key, content_type=content_type)

    def get_text(self, key: str) -> str:
        response = self.client.get_object(self.bucket, key)
        try:
            return response.read().decode("utf-8")
        finally:
            response.close()
            response.release_conn()

    def infer_extension(self, source_url: str | None, content_type: str) -> str:
        if source_url:
            parsed = urlparse(source_url)
            suffix = PurePosixPath(parsed.path).suffix
            if suffix:
                return suffix.lower()
        guessed = guess_extension(content_type.split(";")[0].strip()) or ""
        return guessed.lower() or ".bin"

    def fetch_and_store(
        self,
        *,
        client: httpx.Client,
        source_url: str,
        key: str,
        default_content_type: str = "application/octet-stream",
    ) -> StoredObject:
        response = client.get(source_url)
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", default_content_type)
        return self.put_bytes(payload=response.content, key=key, content_type=content_type)


def build_bronze_object_store() -> BronzeObjectStore | None:
    endpoint = get_setting("OIKOS_BRONZE_S3_ENDPOINT") or get_setting("OIKOS_RAW_HTML_S3_ENDPOINT")
    access_key = get_setting("OIKOS_BRONZE_S3_ACCESS_KEY") or get_setting("OIKOS_RAW_HTML_S3_ACCESS_KEY")
    secret_key = get_setting("OIKOS_BRONZE_S3_SECRET_KEY") or get_setting("OIKOS_RAW_HTML_S3_SECRET_KEY")
    bucket = get_setting("OIKOS_BRONZE_S3_BUCKET") or get_setting("OIKOS_RAW_HTML_S3_BUCKET")
    secure = _secure_from_setting(get_setting("OIKOS_BRONZE_S3_SECURE") or get_setting("OIKOS_RAW_HTML_S3_SECURE"))
    if not endpoint or not access_key or not secret_key or not bucket:
        return None
    return BronzeObjectStore(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket=bucket,
        secure=secure,
    )
