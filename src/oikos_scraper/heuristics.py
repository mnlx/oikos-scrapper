from __future__ import annotations

import json
import re
from collections.abc import Iterable
from decimal import Decimal, InvalidOperation
from html import unescape
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from selectolax.parser import HTMLParser


PRICE_RE = re.compile(r"R\$\s*([\d\.\,]+)")
NUMBER_RE = re.compile(r"(\d+)")
AREA_RE = re.compile(r"(\d+(?:[\.,]\d+)?)\s*m")
DETAIL_PATH_RE = re.compile(r"(imovel|imoveis|apartamento|casa|apto|cobertura|sobrado)")
SCRIPT_PATTERNS = [
    re.compile(r"__NEXT_DATA__"),
    re.compile(r"__NUXT__"),
    re.compile(r"INITIAL_STATE"),
    re.compile(r"window\.__data"),
]

CITY_MAP = {
    "florianopolis": "Florianopolis",
    "sao jose": "Sao Jose",
    "sao-jose": "Sao Jose",
    "biguacu": "Biguacu",
    "palhoca": "Palhoca",
}


def compact_text(text: str | None) -> str:
    if not text:
        return ""
    return " ".join(unescape(text).split())


def slugify(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", "-", value.lower()).strip("-")
    return value or "listing"


def parse_money(text: str | None) -> Decimal | None:
    if not text:
        return None
    match = PRICE_RE.search(text)
    if not match:
        return None
    raw = match.group(1).replace(".", "").replace(",", ".")
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def parse_int(text: str | None) -> int | None:
    if not text:
        return None
    match = NUMBER_RE.search(text)
    return int(match.group(1)) if match else None


def parse_area(text: str | None) -> Decimal | None:
    if not text:
        return None
    match = AREA_RE.search(text.lower())
    if not match:
        return None
    raw = match.group(1).replace(".", "").replace(",", ".")
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def normalize_city(text: str | None, fallback: str | None = None) -> str:
    candidate = compact_text(text).lower()
    for key, city in CITY_MAP.items():
        if key in candidate:
            return city
    return fallback or "Florianopolis"


def detect_transaction_type(*values: str) -> str:
    haystack = " ".join(value.lower() for value in values if value)
    if "alug" in haystack or "rent" in haystack:
        return "rent"
    return "sale"


def detect_property_type(*values: str) -> str:
    haystack = " ".join(value.lower() for value in values if value)
    if "apart" in haystack or "apto" in haystack:
        return "apartment"
    return "house"


def extract_detail_links(html: str, base_url: str) -> list[str]:
    tree = HTMLParser(html)
    seen: set[str] = set()
    links: list[str] = []
    for node in tree.css("a[href]"):
        href = node.attributes.get("href", "")
        if not href:
            continue
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue
        if absolute in seen:
            continue
        if DETAIL_PATH_RE.search(absolute):
            seen.add(absolute)
            links.append(absolute)
    return links


def extract_text_blocks(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    texts = [compact_text(text) for text in soup.stripped_strings]
    return [text for text in texts if text]


def extract_title_from_html(html: str) -> str | None:
    tree = HTMLParser(html)
    for selector in ("h1", "title", "[data-testid='ad-title']", ".title", ".property-title"):
        node = tree.css_first(selector)
        if node and compact_text(node.text()):
            return compact_text(node.text())
    return None


def extract_description_from_html(html: str) -> str | None:
    tree = HTMLParser(html)
    for selector in (".description", "[data-testid='description']", ".property-description"):
        node = tree.css_first(selector)
        if node and compact_text(node.text()):
            return compact_text(node.text())
    return None


def extract_numeric_features(texts: Iterable[str]) -> dict[str, Decimal | int | None]:
    data: dict[str, Decimal | int | None] = {
        "bedrooms": None,
        "bathrooms": None,
        "parking_spaces": None,
        "area_m2": None,
    }
    for text in texts:
        lower = text.lower()
        if data["bedrooms"] is None and ("quarto" in lower or "dorm" in lower):
            data["bedrooms"] = parse_int(text)
        if data["bathrooms"] is None and "banh" in lower:
            data["bathrooms"] = parse_int(text)
        if data["parking_spaces"] is None and ("vaga" in lower or "garagem" in lower):
            data["parking_spaces"] = parse_int(text)
        if data["area_m2"] is None and "m" in lower:
            data["area_m2"] = parse_area(text)
    return data


def find_price_candidates(texts: Iterable[str]) -> list[Decimal]:
    prices = []
    for text in texts:
        parsed = parse_money(text)
        if parsed is not None:
            prices.append(parsed)
    return prices


def collect_json_blobs(html: str) -> list[object]:
    soup = BeautifulSoup(html, "html.parser")
    blobs: list[object] = []
    for script in soup.find_all("script"):
        script_type = script.attrs.get("type", "")
        script_id = script.attrs.get("id", "")
        content = script.string or script.get_text()
        if not content:
            continue
        content = content.strip()
        if script_type in {"application/ld+json", "application/json"}:
            try:
                blobs.append(json.loads(content))
            except json.JSONDecodeError:
                pass
            else:
                continue
        if script_id == "__NEXT_DATA__":
            try:
                blobs.append(json.loads(content))
            except json.JSONDecodeError:
                pass
            else:
                continue
        if any(pattern.search(content) for pattern in SCRIPT_PATTERNS):
            for candidate in find_json_objects_in_text(content):
                blobs.append(candidate)
    return blobs


def find_json_objects_in_text(text: str) -> list[object]:
    decoder = json.JSONDecoder()
    results: list[object] = []
    for start in (idx for idx, char in enumerate(text) if char in "[{"):
        try:
            obj, _ = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            continue
        results.append(obj)
    return results


def walk_json(data: object) -> Iterable[dict]:
    if isinstance(data, dict):
        yield data
        for value in data.values():
            yield from walk_json(value)
    elif isinstance(data, list):
        for item in data:
            yield from walk_json(item)


def maybe_listing_object(item: dict) -> bool:
    keys = {key.lower() for key in item.keys()}
    return bool(
        {"title", "name", "url"} & keys
        and any(key in keys for key in {"price", "pricinginfos", "address", "geo", "listing"})
    )


def safe_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        return parse_money(value) or parse_area(value)
    return None
