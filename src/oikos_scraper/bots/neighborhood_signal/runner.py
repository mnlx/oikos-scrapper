from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from decimal import Decimal
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import structlog
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from oikos_scraper.config import AppConfig, SourceDefinition
from oikos_scraper.db.repository import (
    complete_scrape_run,
    create_scrape_run,
    delete_neighborhood_signals_for_source_url,
    insert_neighborhood_signal,
    list_neighborhood_files,
    update_neighborhood_file_parse_status,
    upsert_neighborhood_artifact,
    upsert_neighborhood_file,
)
from oikos_scraper.db.session import create_session_factory
from oikos_scraper.heuristics import ASSET_SUFFIXES, extract_asset_links
from oikos_scraper.object_store import BronzePathSpec, build_bronze_object_store, offering_hash

LOGGER = structlog.get_logger(__name__)


@dataclass(slots=True)
class NeighborhoodSignalSummary:
    source_code: str
    items_seen: int
    items_inserted: int
    error_count: int


@dataclass(slots=True)
class NeighborhoodAssetEnrichmentSummary:
    source_code: str
    assets_seen: int
    assets_scrapped: int
    assets_reused: int
    error_count: int


class NeighborhoodSignalRunner:
    def __init__(self, config: AppConfig, database_url: str | None = None) -> None:
        self.config = config
        self.database_url = database_url
        self.session_factory = None
        self.object_store = build_bronze_object_store()
        self.enable_screenshots = True

    def _project_root(self) -> Path:
        return Path(__file__).resolve().parents[4]

    def _session_factory(self):
        if self.session_factory is None:
            self.session_factory = create_session_factory(self.database_url)
        return self.session_factory

    def _http_client(self) -> httpx.Client:
        return httpx.Client(
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                )
            },
            timeout=45.0,
        )

    def _artifact_key(self, *, category: str, base_hash: str, extension: str) -> str:
        return BronzePathSpec(
            layer="bronze",
            dataset="neighborhood_signal",
            category=category,
            run_at=datetime.now(UTC),
            base_hash=base_hash,
            extension=extension,
        ).object_key()

    def _dedupe_links(self, values: list[str]) -> list[str]:
        seen: set[str] = set()
        deduped: list[str] = []
        for value in values:
            normalized = value.split("#", 1)[0]
            if normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    def _extract_urls_from_json(self, value: Any) -> list[str]:
        found: list[str] = []
        if isinstance(value, dict):
            for item in value.values():
                found.extend(self._extract_urls_from_json(item))
            return found
        if isinstance(value, list):
            for item in value:
                found.extend(self._extract_urls_from_json(item))
            return found
        if not isinstance(value, str):
            return found
        candidate = value.strip()
        parsed = urlparse(candidate)
        if parsed.scheme not in {"http", "https"}:
            return found
        suffix = parsed.path.lower()
        if any(suffix.endswith(ext) for ext in ASSET_SUFFIXES):
            found.append(candidate)
        return found

    def _extract_neighborhood_asset_links(
        self,
        *,
        source_url: str,
        content_type: str,
        response_text: str | None,
        payload: Any,
    ) -> list[str]:
        asset_links: list[str] = []
        lowered = content_type.lower()
        if response_text and ("html" in lowered or "text/" in lowered):
            asset_links.extend(extract_asset_links(response_text, source_url))
        if payload is not None:
            asset_links.extend(self._extract_urls_from_json(payload))
        return self._dedupe_links(asset_links)

    def _asset_type(self, asset_url: str, content_type: str | None = None) -> str:
        lowered_type = (content_type or "").split(";", 1)[0].strip().lower()
        if lowered_type.startswith("image/"):
            return "image"
        if lowered_type == "application/pdf":
            return "pdf"
        path = urlparse(asset_url).path.lower()
        if any(path.endswith(ext) for ext in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp", ".tif", ".tiff", ".avif", ".heic"}):
            return "image"
        if path.endswith(".pdf"):
            return "pdf"
        if any(path.endswith(ext) for ext in ASSET_SUFFIXES):
            return "asset"
        return "asset"

    def _capture_screenshot(self, url: str) -> bytes | None:
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                try:
                    page = browser.new_page(viewport={"width": 1440, "height": 1200})
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    try:
                        page.wait_for_load_state("networkidle", timeout=5000)
                    except PlaywrightTimeoutError:
                        pass
                    return page.screenshot(full_page=True, type="png")
                finally:
                    browser.close()
        except Exception:
            return None

    def _fetch_html_with_browser(self, url: str) -> str | None:
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                try:
                    page = browser.new_page(viewport={"width": 1440, "height": 1200})
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    try:
                        page.wait_for_load_state("networkidle", timeout=5000)
                    except PlaywrightTimeoutError:
                        pass
                    return page.content()
                finally:
                    browser.close()
        except Exception:
            return None

    def ingest_sources(
        self,
        source_codes: list[str] | None = None,
        trigger_type: str = "scheduled",
    ) -> list[NeighborhoodSignalSummary]:
        if self.object_store is None:
            raise RuntimeError("Bronze object store is not configured")
        selected = [self.config.find_source(code) for code in source_codes] if source_codes else self.config.active_sources()
        summaries: list[NeighborhoodSignalSummary] = []
        for source in selected:
            summaries.append(self.ingest_source(source, trigger_type=trigger_type))
        return summaries

    def ingest_source(self, source: SourceDefinition, trigger_type: str) -> NeighborhoodSignalSummary:
        with self._session_factory()() as session:
            run = create_scrape_run(
                session,
                source.code,
                trigger_type,
                strategy="httpx",
                pipeline_stage="neighborhood_ingest",
            )
        seen = 0
        inserted = 0
        errors = 0
        with self._http_client() as client:
            for source_url in source.urls:
                seen += 1
                try:
                    response = client.get(source_url)
                    response.raise_for_status()
                    content_type = response.headers.get("Content-Type", "application/octet-stream")
                    response_content = response.content
                    response_text = response.text
                    json_payload: Any = None
                    if "json" in content_type.lower():
                        try:
                            json_payload = response.json()
                        except Exception:
                            json_payload = None
                except Exception:
                    browser_html = self._fetch_html_with_browser(source_url)
                    if not browser_html:
                        raise
                    content_type = "text/html; charset=utf-8"
                    response_content = browser_html.encode("utf-8")
                    response_text = browser_html
                    json_payload = None
                try:
                    base_hash = offering_hash(source.code, source_url)
                    html_uri = None
                    json_uri = None
                    screenshot_uri = None
                    file_uri = None
                    checksum_sha256 = None
                    size_bytes = None
                    asset_links = self._extract_neighborhood_asset_links(
                        source_url=source_url,
                        content_type=content_type,
                        response_text=response_text,
                        payload=json_payload,
                    )

                    if "json" in content_type:
                        stored = self.object_store.put_bytes(
                            payload=response_content,
                            key=self._artifact_key(category="json", base_hash=base_hash, extension=".json"),
                            content_type=content_type,
                        )
                        json_uri = stored.uri
                        checksum_sha256 = stored.checksum_sha256
                        size_bytes = stored.size
                    elif "html" in content_type or "text/" in content_type:
                        stored = self.object_store.put_text(
                            payload=response_text,
                            key=self._artifact_key(category="html", base_hash=base_hash, extension=".html"),
                            content_type="text/html; charset=utf-8",
                        )
                        html_uri = stored.uri
                        checksum_sha256 = stored.checksum_sha256
                        size_bytes = stored.size
                        screenshot = self._capture_screenshot(source_url)
                        if screenshot is not None:
                            screenshot_object = self.object_store.put_bytes(
                                payload=screenshot,
                                key=self._artifact_key(category="screenshots", base_hash=base_hash, extension=".png"),
                                content_type="image/png",
                            )
                            screenshot_uri = screenshot_object.uri
                    else:
                        extension = self.object_store.infer_extension(source_url, content_type)
                        stored = self.object_store.put_bytes(
                            payload=response_content,
                            key=self._artifact_key(category="files", base_hash=base_hash, extension=extension),
                            content_type=content_type,
                        )
                        file_uri = stored.uri
                        checksum_sha256 = stored.checksum_sha256
                        size_bytes = stored.size

                    metadata_payload = {
                        "source_code": source.code,
                        "source_name": source.name,
                        "source_url": source_url,
                        "fetched_at": datetime.now(UTC).isoformat(),
                        "content_type": content_type,
                        "asset_links": asset_links,
                    }
                    metadata = self.object_store.put_text(
                        payload=json.dumps(metadata_payload, ensure_ascii=True, indent=2),
                        key=self._artifact_key(category="metadata", base_hash=base_hash, extension=".json"),
                        content_type="application/json",
                    )
                    with self._session_factory()() as session:
                        upsert_neighborhood_file(
                            session,
                            source=source,
                            source_url=source_url,
                            city=source.cities[0] if source.cities else None,
                            neighborhood=None,
                            content_type=content_type,
                            html_uri=html_uri,
                            json_uri=json_uri,
                            screenshot_uri=screenshot_uri,
                            file_uri=file_uri,
                            metadata_uri=metadata.uri,
                            checksum_sha256=checksum_sha256,
                            size_bytes=size_bytes,
                            parse_status="pending",
                            reference_date=None,
                            metadata_json=metadata_payload,
                        )
                    inserted += 1
                except Exception as exc:
                    errors += 1
                    LOGGER.warning("neighborhood_ingest_failed", source=source.code, url=source_url, error=str(exc))

        with self._session_factory()() as session:
            complete_scrape_run(
                session,
                run,
                status="success" if errors == 0 else "partial_success",
                items_seen=seen,
                items_inserted=inserted,
                items_updated=0,
                error_count=errors,
            )
        return NeighborhoodSignalSummary(
            source_code=source.code,
            items_seen=seen,
            items_inserted=inserted,
            error_count=errors,
        )

    def enrich_assets_sources(self, source_codes: list[str] | None = None) -> list[NeighborhoodAssetEnrichmentSummary]:
        if self.object_store is None:
            raise RuntimeError("Bronze object store is not configured")
        selected = [self.config.find_source(code) for code in source_codes] if source_codes else self.config.active_sources()
        summaries: list[NeighborhoodAssetEnrichmentSummary] = []
        for source in selected:
            summaries.append(self.enrich_assets_source(source))
        return summaries

    def enrich_assets_source(self, source: SourceDefinition) -> NeighborhoodAssetEnrichmentSummary:
        strategy_name = "asset_enrichment"
        with self._session_factory()() as session:
            run = create_scrape_run(
                session,
                source.code,
                "scheduled",
                strategy_name,
                pipeline_stage="neighborhood_enriching_assets",
            )

        assets_seen = 0
        assets_scrapped = 0
        assets_reused = 0
        error_count = 0
        try:
            with self._session_factory()() as session:
                files = list_neighborhood_files(session, source_codes=[source.code], only_pending=False)
            with self._http_client() as client:
                for file_row in files:
                    asset_links = self._dedupe_links(list((file_row.metadata_json or {}).get("asset_links") or []))
                    for asset_id, asset_url in enumerate(asset_links, start=1):
                        assets_seen += 1
                        default_extension = self.object_store.infer_extension(asset_url, "application/octet-stream")
                        asset_hash = f"{offering_hash(file_row.source_code, file_row.source_url)}-asset-{asset_id:02d}"
                        key = self._artifact_key(category="assets", base_hash=asset_hash, extension=default_extension)
                        asset_uri = self.object_store.uri_for_key(key)
                        is_scrapped = self.object_store.object_exists(key)
                        stored = None
                        if is_scrapped:
                            assets_reused += 1
                        else:
                            try:
                                stored = self.object_store.fetch_and_store(
                                    client=client,
                                    source_url=asset_url,
                                    key=key,
                                )
                                asset_uri = stored.uri
                                is_scrapped = True
                                assets_scrapped += 1
                            except Exception:
                                LOGGER.warning(
                                    "neighborhood_asset_fetch_failed",
                                    source_code=file_row.source_code,
                                    source_url=file_row.source_url,
                                    asset_url=asset_url,
                                )
                                error_count += 1

                        with self._session_factory()() as session:
                            upsert_neighborhood_artifact(
                                session,
                                file_row=file_row,
                                asset_id=asset_id,
                                asset_type=self._asset_type(asset_url, stored.content_type if stored else None),
                                asset_url=asset_url,
                                asset_uri=asset_uri,
                                is_scrapped=is_scrapped,
                                content_type=stored.content_type if stored else None,
                                checksum_sha256=stored.checksum_sha256 if stored else None,
                                size_bytes=stored.size if stored else None,
                            )

            with self._session_factory()() as session:
                complete_scrape_run(
                    session,
                    run,
                    status="success" if error_count == 0 else "partial_success",
                    items_seen=assets_seen,
                    items_inserted=assets_scrapped,
                    items_updated=assets_reused,
                    error_count=error_count,
                )
            return NeighborhoodAssetEnrichmentSummary(
                source_code=source.code,
                assets_seen=assets_seen,
                assets_scrapped=assets_scrapped,
                assets_reused=assets_reused,
                error_count=error_count,
            )
        except Exception as exc:
            with self._session_factory()() as session:
                complete_scrape_run(
                    session,
                    run,
                    status="failed",
                    items_seen=assets_seen,
                    items_inserted=assets_scrapped,
                    items_updated=assets_reused,
                    error_count=error_count + 1,
                    last_error=str(exc),
                )
            LOGGER.exception("neighborhood_asset_enrichment_failed", source=source.code)
            return NeighborhoodAssetEnrichmentSummary(
                source_code=source.code,
                assets_seen=assets_seen,
                assets_scrapped=assets_scrapped,
                assets_reused=assets_reused,
                error_count=error_count + 1,
            )

    def parse_sources(self, source_codes: list[str] | None = None) -> list[NeighborhoodSignalSummary]:
        if self.object_store is None:
            raise RuntimeError("Bronze object store is not configured")
        summaries: list[NeighborhoodSignalSummary] = []
        grouped: dict[str, list[Any]] = {}
        with self._session_factory()() as session:
            for row in list_neighborhood_files(session, source_codes=source_codes, only_pending=True):
                grouped.setdefault(row.source_code, []).append(row)

        for source_code, rows in grouped.items():
            inserted = 0
            errors = 0
            for row in rows:
                try:
                    parsed = self._parse_file_row(row)
                    with self._session_factory()() as session:
                        if parsed:
                            delete_neighborhood_signals_for_source_url(session, row.source_url)
                            for item in parsed:
                                insert_neighborhood_signal(session, **item)
                            update_neighborhood_file_parse_status(session, row, parse_status="parsed")
                            inserted += len(parsed)
                        else:
                            update_neighborhood_file_parse_status(session, row, parse_status="unparsed")
                except Exception as exc:
                    errors += 1
                    with self._session_factory()() as session:
                        update_neighborhood_file_parse_status(session, row, parse_status="failed", last_error=str(exc))
            summaries.append(
                NeighborhoodSignalSummary(
                    source_code=source_code,
                    items_seen=len(rows),
                    items_inserted=inserted,
                    error_count=errors,
                )
            )
        return summaries

    def _parse_file_row(self, row) -> list[dict[str, Any]]:  # noqa: ANN001
        payload = None
        html = None
        if row.json_uri is not None:
            payload = json.loads(self.object_store.get_text(row.json_uri.removeprefix(f"s3://{self.object_store.bucket}/")))
        if row.html_uri is not None:
            html = self.object_store.get_text(row.html_uri.removeprefix(f"s3://{self.object_store.bucket}/"))

        custom = self._parse_source_specific(row, payload=payload, html=html)
        if custom is not None:
            return custom
        if payload is None:
            return []
        flattened = list(self._flatten_scalar_values(payload))
        parsed: list[dict[str, Any]] = []
        for key, value in flattened:
            signal_name = key.replace(".", " ").replace("_", " ").title()
            if isinstance(value, (int, float, Decimal)):
                parsed.append(
                    {
                        "city": row.city or "Grande Florianopolis",
                        "neighborhood": row.neighborhood,
                        "geographic_scope": row.geographic_scope,
                        "signal_category": row.signal_category or "market",
                        "signal_code": f"{row.source_code}.{key}",
                        "signal_name": signal_name,
                        "source_name": row.source_name,
                        "source_type": row.source_type,
                        "publisher": row.publisher,
                        "source_url": row.source_url,
                        "reference_date": row.reference_date,
                        "value_numeric": value,
                        "value_text": None,
                        "unit": None,
                        "priority": 0,
                        "metadata_json": {"file_id": row.id},
                    }
                )
            elif isinstance(value, str) and value.strip():
                parsed.append(
                    {
                        "city": row.city or "Grande Florianopolis",
                        "neighborhood": row.neighborhood,
                        "geographic_scope": row.geographic_scope,
                        "signal_category": row.signal_category or "market",
                        "signal_code": f"{row.source_code}.{key}",
                        "signal_name": signal_name,
                        "source_name": row.source_name,
                        "source_type": row.source_type,
                        "publisher": row.publisher,
                        "source_url": row.source_url,
                        "reference_date": row.reference_date,
                        "value_numeric": None,
                        "value_text": value[:1000],
                        "unit": None,
                        "priority": 0,
                        "metadata_json": {"file_id": row.id},
                    }
                )
        return parsed

    def _flatten_scalar_values(self, value: Any, prefix: str = "") -> list[tuple[str, Any]]:
        rows: list[tuple[str, Any]] = []
        if isinstance(value, dict):
            for key, item in value.items():
                next_prefix = f"{prefix}.{key}" if prefix else str(key)
                rows.extend(self._flatten_scalar_values(item, next_prefix))
            return rows
        if isinstance(value, list):
            for index, item in enumerate(value):
                next_prefix = f"{prefix}.{index}" if prefix else str(index)
                rows.extend(self._flatten_scalar_values(item, next_prefix))
            return rows
        if isinstance(value, bool) or value is None:
            return rows
        if isinstance(value, str) and len(value.strip()) > 1000:
            return rows
        if prefix:
            rows.append((prefix, value))
        return rows

    def _parse_source_specific(self, row, *, payload: Any, html: str | None) -> list[dict[str, Any]] | None:  # noqa: ANN001
        source_code = row.source_code
        if source_code == "ibge_localidades" and payload is not None:
            return self._parse_ibge_localidades(row, payload)
        if html is None:
            return None
        if source_code == "geofloripa":
            return self._parse_geofloripa(row, html)
        if source_code == "sao_jose_observatorio_imobiliario":
            return self._parse_sao_jose_observatorio(row, html)
        if source_code == "ssp_sc_seguranca_numeros":
            return self._parse_ssp_sc(row, html)
        if source_code == "sao_jose_pmrr":
            return self._parse_sao_jose_pmrr(row, html)
        if source_code == "palhoca_enchentes":
            return self._parse_palhoca_home(row, html)
        if source_code == "biguacu_reurb":
            return self._parse_biguacu_reurb(row, html)
        if source_code == "opendatasus_cnes":
            return self._parse_opendatasus(row, html)
        return None

    def _signal_row(
        self,
        row,
        *,
        signal_code: str,
        signal_name: str,
        value_numeric: int | float | Decimal | None = None,
        value_text: str | None = None,
        unit: str | None = None,
        neighborhood: str | None = None,
        priority: int = 0,
        metadata_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "city": row.city or "Grande Florianopolis",
            "neighborhood": neighborhood or row.neighborhood,
            "geographic_scope": row.geographic_scope,
            "signal_category": row.signal_category or "market",
            "signal_code": signal_code,
            "signal_name": signal_name,
            "source_name": row.source_name,
            "source_type": row.source_type,
            "publisher": row.publisher,
            "source_url": row.source_url,
            "reference_date": row.reference_date,
            "value_numeric": value_numeric,
            "value_text": value_text,
            "unit": unit,
            "priority": priority,
            "metadata_json": metadata_json or {"file_id": row.id},
        }

    def _text(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        return " ".join(soup.get_text(" ", strip=True).split())

    def _extract_number(self, text: str, pattern: str) -> Decimal | None:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            return None
        normalized = match.group(1).replace(".", "").replace(",", ".")
        try:
            return Decimal(normalized)
        except Exception:
            return None

    def _parse_ibge_localidades(self, row, payload: dict[str, Any]) -> list[dict[str, Any]]:  # noqa: ANN001
        rows: list[dict[str, Any]] = []
        rows.append(
            self._signal_row(
                row,
                signal_code="ibge_localidades.municipio_id",
                signal_name="IBGE Municipio ID",
                value_numeric=payload.get("id"),
                unit="id",
                priority=10,
            )
        )
        region = payload.get("regiao-imediata", {})
        inter = region.get("regiao-intermediaria", {})
        uf = inter.get("UF", {})
        rows.append(
            self._signal_row(
                row,
                signal_code="ibge_localidades.regiao_imediata_nome",
                signal_name="Regiao Imediata",
                value_text=str(region.get("nome") or ""),
            )
        )
        rows.append(
            self._signal_row(
                row,
                signal_code="ibge_localidades.regiao_intermediaria_nome",
                signal_name="Regiao Intermediaria",
                value_text=str(inter.get("nome") or ""),
            )
        )
        rows.append(
            self._signal_row(
                row,
                signal_code="ibge_localidades.uf_sigla",
                signal_name="UF Sigla",
                value_text=str(uf.get("sigla") or ""),
            )
        )
        return rows

    def _parse_geofloripa(self, row, html: str) -> list[dict[str, Any]]:
        text = self._text(html)
        features = [
            ("mapa_interativo", "Mapa Interativo"),
            ("emissao_documentos", "Emissão de Documentos"),
            ("geoservicos", "Geoserviços"),
            ("downloads", "Downloads"),
            ("consulta_ambiental", "Consulta Ambiental"),
            ("consulta_viabilidade", "Consulta de Viabilidade"),
        ]
        rows = [
            self._signal_row(
                row,
                signal_code=f"geofloripa.{code}_disponivel",
                signal_name=label,
                value_numeric=1 if label.lower() in text.lower() else 0,
                unit="flag",
                priority=5,
            )
            for code, label in features
        ]
        rows.append(
            self._signal_row(
                row,
                signal_code="geofloripa.feature_count",
                signal_name="Geoportal Feature Count",
                value_numeric=sum(1 for _, label in features if label.lower() in text.lower()),
                unit="features",
                priority=5,
            )
        )
        return rows

    def _parse_sao_jose_observatorio(self, row, html: str) -> list[dict[str, Any]]:
        text = self._text(html)
        rows: list[dict[str, Any]] = []
        increase = self._extract_number(text, r"aumento de\s+([\d\.,]+)% na base de cálculo")
        if increase is not None:
            rows.append(
                self._signal_row(
                    row,
                    signal_code="sao_jose_observatorio.itbi_base_increase_pct",
                    signal_name="ITBI Base Increase",
                    value_numeric=increase,
                    unit="percent",
                    priority=9,
                )
            )
        rows.append(
            self._signal_row(
                row,
                signal_code="sao_jose_observatorio.daily_listing_collection",
                signal_name="Coleta Diaria de Anuncios",
                value_numeric=1 if "coleta diária de anúncios" in text.lower() else 0,
                unit="flag",
                priority=8,
            )
        )
        rows.append(
            self._signal_row(
                row,
                signal_code="sao_jose_observatorio.map_interface",
                signal_name="Interface de Busca com Mapa",
                value_numeric=1 if "dados em mapas" in text.lower() else 0,
                unit="flag",
                priority=6,
            )
        )
        return rows

    def _parse_ssp_sc(self, row, html: str) -> list[dict[str, Any]]:
        text = self._text(html)
        rows: list[dict[str, Any]] = []
        for year in (2026, 2025, 2024, 2023):
            count = len(re.findall(rf"(JANEIRO|FEVEREIRO|MARÇO|ABRIL|MAIO|JUNHO|JULHO|AGOSTO|SETEMBRO|OUTUBRO|NOVEMBRO|DEZEMBRO)\s+{year}", text))
            rows.append(
                self._signal_row(
                    row,
                    signal_code=f"ssp_sc.boletins_publicados_{year}",
                    signal_name=f"Boletins Publicados {year}",
                    value_numeric=count,
                    unit="boletins",
                    priority=7,
                )
            )
        rows.append(
            self._signal_row(
                row,
                signal_code="ssp_sc.violencia_domestica_series",
                signal_name="Series de Violencia Domestica Disponiveis",
                value_numeric=len(re.findall(r"violência doméstica", text.lower())),
                unit="series",
                priority=6,
            )
        )
        return rows

    def _parse_sao_jose_pmrr(self, row, html: str) -> list[dict[str, Any]]:
        text = self._text(html)
        mappings = [
            ("areas_risco", "Areas em Situacao de Risco", r"identificou\s+(\d+)\s+áreas em situação de risco", "areas"),
            ("setores_risco", "Setores Mapeados", r"totalizando\s+(\d+)\s+setores", "setores"),
            ("alto_risco", "Setores Alto ou Muito Alto Risco", r"Destas,\s+(\d+)\s+foram classificadas", "setores"),
            ("obras_contemplacao_solicitada", "Recursos Solicitados para Obras", r"R\$\s*([\d\.,]+)\s+milhões", "milhoes_brl"),
            ("cidades_selecionadas", "Cidades Selecionadas", r"entre as\s+(\d+)\s+cidades brasileiras", "cidades"),
        ]
        rows: list[dict[str, Any]] = []
        for code, name, pattern, unit in mappings:
            value = self._extract_number(text, pattern)
            if value is not None:
                rows.append(
                    self._signal_row(
                        row,
                        signal_code=f"sao_jose_pmrr.{code}",
                        signal_name=name,
                        value_numeric=value,
                        unit=unit,
                        priority=10,
                    )
                )
        for neighborhood in ("Forquilhas", "Forquilhinha", "Serraria", "Jardim Cidade de Florianópolis"):
            rows.append(
                self._signal_row(
                    row,
                    signal_code=f"sao_jose_pmrr.bairro_mencionado.{neighborhood.lower().replace(' ', '_')}",
                    signal_name=f"Bairro Mencionado {neighborhood}",
                    value_numeric=1 if neighborhood.lower() in text.lower() else 0,
                    unit="flag",
                    neighborhood=neighborhood,
                    priority=7,
                )
            )
        return rows

    def _parse_palhoca_home(self, row, html: str) -> list[dict[str, Any]]:
        text = self._text(html)
        rows: list[dict[str, Any]] = []
        rows.append(
            self._signal_row(
                row,
                signal_code="palhoca_enchentes.programa_prevencao_noticia",
                signal_name="Programa de Prevenção de Enchentes em Destaque",
                value_numeric=1 if "programa de prevenção de enchentes" in text.lower() else 0,
                unit="flag",
                priority=8,
            )
        )
        rows.append(
            self._signal_row(
                row,
                signal_code="palhoca_enchentes.avenida_das_torres",
                signal_name="Avenida das Torres Recebe Acao",
                value_numeric=1 if "avenida das torres recebe ação do programa de prevenção de enchentes".lower() in text.lower() else 0,
                unit="flag",
                priority=8,
            )
        )
        rows.append(
            self._signal_row(
                row,
                signal_code="palhoca_servicos_digitais.count",
                signal_name="Servicos Digitais em Destaque",
                value_numeric=sum(1 for label in ["consulta de licitações", "consulta de processos", "prefeitura digital", "ouvidoria", "trânsito", "iptu"] if label in text.lower()),
                unit="servicos",
                priority=4,
            )
        )
        return rows

    def _parse_biguacu_reurb(self, row, html: str) -> list[dict[str, Any]]:
        text = self._text(html)
        rows: list[dict[str, Any]] = []
        families = self._extract_number(text, r"realidade para\s+(\d+)\s+famílias")
        if families is not None:
            rows.append(
                self._signal_row(
                    row,
                    signal_code="biguacu_reurb.familias_beneficiadas",
                    signal_name="Familias Beneficiadas",
                    value_numeric=families,
                    unit="familias",
                    priority=10,
                    neighborhood="Foz do Rio Biguacu",
                )
            )
        rows.append(
            self._signal_row(
                row,
                signal_code="biguacu_reurb.registro_publico",
                signal_name="Regularizacao com Registro Publico",
                value_numeric=1 if "registro público" in text.lower() else 0,
                unit="flag",
                priority=8,
                neighborhood="Foz do Rio Biguacu",
            )
        )
        rows.append(
            self._signal_row(
                row,
                signal_code="biguacu_reurb.reurb_social",
                signal_name="REURB Social Municipal",
                value_numeric=1 if "reurb social" in text.lower() else 0,
                unit="flag",
                priority=8,
                neighborhood="Foz do Rio Biguacu",
            )
        )
        return rows

    def _parse_opendatasus(self, row, html: str) -> list[dict[str, Any]]:
        text = self._text(html)
        patterns = [
            ("assistencia_saude", "Assistencia a Saude", r"Assistência à saúde\s+(\d+)\s+Conjuntos"),
            ("atencao_primaria", "Atencao Primaria", r"Atenção Primária\s+(\d+)\s+Conjuntos"),
            ("ciencia_tecnologia", "Ciencia e Tecnologia", r"Ciência & Tecnologia\s+(\d+)\s+Conjuntos"),
            ("diagnosticos_tratamentos", "Diagnosticos e Tratamentos", r"Diagnósticos e Tratamentos\s+(\d+)\s+Conjuntos"),
            ("arboviroses", "Arboviroses", r"Arboviroses\s+(\d+)\s+Conjuntos"),
        ]
        rows: list[dict[str, Any]] = []
        for code, name, pattern in patterns:
            value = self._extract_number(text, pattern)
            if value is not None:
                rows.append(
                    self._signal_row(
                        row,
                        signal_code=f"opendatasus.{code}_datasets",
                        signal_name=f"{name} Datasets",
                        value_numeric=value,
                        unit="datasets",
                        priority=5,
                    )
                )
        rows.append(
            self._signal_row(
                row,
                signal_code="opendatasus.pda_2024_2026",
                signal_name="Plano de Dados Abertos 2024-2026",
                value_numeric=1 if "Plano de Dados Abertos - Ministério da Saúde - 2024-2026" in text else 0,
                unit="flag",
                priority=4,
            )
        )
        return rows

    def run_dbt_build(self, select: str | None = None) -> subprocess.CompletedProcess[str]:
        profiles_dir = self._project_root() / "dbt"
        command = [
            "dbt",
            "build",
            "--project-dir",
            str(self._project_root()),
            "--profiles-dir",
            str(profiles_dir),
        ]
        if select:
            command.extend(["--select", select])
        return subprocess.run(command, check=True, text=True, capture_output=True, env=None)
