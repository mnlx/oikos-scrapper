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
DECIMAL_RE = re.compile(r"-?\d+(?:\.\d+)?")
DETAIL_PATH_RE = re.compile(
    r"(imovel|imoveis|apartamento|casa|apto|cobertura|sobrado|empreendimento|empreendimentos|lancamento|lancamentos|unidade)"
)
ADDRESS_HINT_RE = re.compile(
    r"\b(rua|r\.|avenida|av\.|travessa|servid[aã]o|rodovia|estrada|alameda|pra[cç]a)\b",
    re.IGNORECASE,
)
NEIGHBORHOOD_HINT_RE = re.compile(r"\b(bairro)\b", re.IGNORECASE)
LATITUDE_RE = re.compile(
    r"(?:latitude|lat)[\"'=\s:>{\[]+(?P<value>-?\d{1,2}\.\d{4,})",
    re.IGNORECASE,
)
LONGITUDE_RE = re.compile(
    r"(?:longitude|lng|lon)[\"'=\s:>{\[]+(?P<value>-?\d{1,3}\.\d{4,})",
    re.IGNORECASE,
)
COORDINATE_PAIR_RE = re.compile(
    r"(?P<lat>-?\d{1,2}\.\d{4,})\s*[,;]\s*(?P<lng>-?\d{1,3}\.\d{4,})"
)
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


def parse_decimal_text(text: str | None) -> Decimal | None:
    if not text:
        return None
    match = DECIMAL_RE.search(text.replace(",", "."))
    if not match:
        return None
    try:
        return Decimal(match.group(0))
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


def extract_address_from_html(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    selectors = (
        "[itemprop='streetAddress']",
        "[data-testid*='address']",
        ".address",
        ".endereco",
        ".property-address",
        ".property-location",
        ".location",
        ".localizacao",
    )
    for selector in selectors:
        for node in soup.select(selector):
            text = compact_text(node.get_text(" ", strip=True))
            if text and ADDRESS_HINT_RE.search(text):
                return text[:500]

    for text in extract_text_blocks(html):
        if len(text) > 140:
            continue
        if ADDRESS_HINT_RE.search(text):
            return text[:500]
    return None


def extract_neighborhood_from_html(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    selectors = (
        ".neighborhood",
        ".bairro",
        "[data-testid*='neighborhood']",
    )
    for selector in selectors:
        for node in soup.select(selector):
            text = compact_text(node.get_text(" ", strip=True))
            if text:
                return text[:255]

    for text in extract_text_blocks(html):
        if NEIGHBORHOOD_HINT_RE.search(text):
            parts = re.split(r"[:\-]", text, maxsplit=1)
            candidate = compact_text(parts[-1] if len(parts) > 1 else text)
            if candidate:
                return candidate[:255]
    return None


def extract_coordinates_from_html(html: str) -> tuple[Decimal | None, Decimal | None]:
    soup = BeautifulSoup(html, "html.parser")
    latitude: Decimal | None = None
    longitude: Decimal | None = None

    for selector, attr, target in (
        ("meta[property='place:location:latitude']", "content", "lat"),
        ("meta[property='place:location:longitude']", "content", "lng"),
        ("meta[name='ICBM']", "content", "pair"),
        ("[data-lat][data-lng]", "data", "pair-attr"),
        ("[data-latitude][data-longitude]", "data", "pair-attr-alt"),
    ):
        for node in soup.select(selector):
            if target == "lat":
                latitude = parse_decimal_text(node.get(attr))
            elif target == "lng":
                longitude = parse_decimal_text(node.get(attr))
            elif target == "pair":
                pair = compact_text(node.get(attr) or "")
                match = COORDINATE_PAIR_RE.search(pair)
                if match:
                    latitude = parse_decimal_text(match.group("lat"))
                    longitude = parse_decimal_text(match.group("lng"))
            elif target == "pair-attr":
                latitude = parse_decimal_text(node.get("data-lat"))
                longitude = parse_decimal_text(node.get("data-lng"))
            else:
                latitude = parse_decimal_text(node.get("data-latitude"))
                longitude = parse_decimal_text(node.get("data-longitude"))
            if latitude is not None and longitude is not None:
                return latitude, longitude

    lat_match = LATITUDE_RE.search(html)
    lng_match = LONGITUDE_RE.search(html)
    if lat_match and lng_match:
        latitude = parse_decimal_text(lat_match.group("value"))
        longitude = parse_decimal_text(lng_match.group("value"))
        if latitude is not None and longitude is not None:
            return latitude, longitude

    pair_match = COORDINATE_PAIR_RE.search(html)
    if pair_match:
        latitude = parse_decimal_text(pair_match.group("lat"))
        longitude = parse_decimal_text(pair_match.group("lng"))
    return latitude, longitude


def extract_location_fields_from_html(html: str, fallback_city: str | None = None) -> dict[str, object]:
    address = extract_address_from_html(html)
    neighborhood = extract_neighborhood_from_html(html)
    latitude, longitude = extract_coordinates_from_html(html)
    city_source = " ".join(
        value for value in (address, neighborhood, extract_title_from_html(html), extract_description_from_html(html)) if value
    )
    return {
        "address": address,
        "neighborhood": neighborhood,
        "city": normalize_city(city_source, fallback=fallback_city),
        "latitude": latitude,
        "longitude": longitude,
    }


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
        return parse_money(value) or parse_area(value) or parse_decimal_text(value)
    return None
