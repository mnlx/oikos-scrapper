from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal
from urllib.parse import urljoin

import httpx

from oikos_scraper.config import SourceDefinition
from oikos_scraper.heuristics import collect_json_blobs, maybe_listing_object, safe_decimal, walk_json
from oikos_scraper.normalizer import normalize_listing
from oikos_scraper.strategies.base import ScrapeStrategy
from oikos_scraper.types import ListingDraft, StrategyResult


def _first_decimal(*values: object) -> Decimal | None:
    for value in values:
        parsed = safe_decimal(value)
        if parsed is not None and parsed != Decimal("0"):
            return parsed
    return None


def _first_value(*values: object) -> object | None:
    for value in values:
        if value not in (None, "", [], {}):
            return value
    return None


def _coerce_price_block(raw: object) -> dict:
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                return item
        return {}
    if isinstance(raw, dict):
        return raw
    return {}


def _listing_score(raw: dict) -> int:
    keys = {key.lower() for key in raw.keys()}
    score = 0
    if "id" in keys:
        score += 3
    if "url" in keys or "link" in keys:
        score += 3
    if "title" in keys or "name" in keys:
        score += 2
    if any(key in keys for key in {"price", "pricinginfos", "rentprice", "saleprice", "totalcost"}):
        score += 6
    if any(key in keys for key in {"bedrooms", "bathrooms", "area", "usablearea", "usableareas"}):
        score += 2
    if any(key in keys for key in {"address", "city", "neighborhood", "geo", "coordinates"}):
        score += 2
    return score


def listing_from_json(source: SourceDefinition, raw: dict, seed_url: str) -> ListingDraft | None:
    address = raw.get("address") or raw.get("location")
    geo = raw.get("geo") or raw.get("coordinates") or {}
    address_text = address
    neighborhood = raw.get("neighborhood")
    state = raw.get("state") or "SC"
    latitude = None
    longitude = None
    if isinstance(address, dict):
        address_text = ", ".join(
            str(value).strip()
            for value in (
                address.get("streetAddress"),
                address.get("street"),
                address.get("number"),
                address.get("neighborhood"),
                address.get("city"),
                address.get("addressLocality"),
            )
            if value
        )
        neighborhood = neighborhood or address.get("neighborhood")
        state = address.get("stateAcronym") or address.get("addressRegion") or state
        latitude = safe_decimal(address.get("lat") or address.get("latitude"))
        longitude = safe_decimal(address.get("lng") or address.get("longitude"))
    price = _coerce_price_block(raw.get("price") or raw.get("pricingInfos") or raw.get("pricinginfos"))
    sale = _first_decimal(
        raw.get("salePrice"),
        raw.get("businessPrice"),
        raw.get("listPrice"),
        raw.get("priceValue"),
        price.get("salePrice"),
        price.get("price"),
        price.get("amount"),
        price.get("businessPrice"),
        price.get("value"),
        raw.get("price") if not price else None,
    )
    rent = _first_decimal(
        raw.get("rentPrice"),
        raw.get("rentalPrice"),
        raw.get("totalRent"),
        raw.get("totalCost"),
        price.get("rentPrice"),
        price.get("rentalTotalPrice"),
        price.get("rentalPrice"),
        price.get("totalCost"),
    )
    condo_fee = _first_decimal(
        raw.get("condoPrice"),
        raw.get("condominiumFee"),
        price.get("condoPrice"),
        price.get("monthlyCondoFee"),
    )
    iptu = _first_decimal(
        raw.get("iptu"),
        raw.get("propertyTax"),
        price.get("yearlyIptu"),
        price.get("iptu"),
    )

    canonical_url = _first_value(raw.get("url"), raw.get("link"), raw.get("href"), seed_url)
    if isinstance(canonical_url, str):
        canonical_url = urljoin(seed_url, canonical_url)
    else:
        canonical_url = seed_url
    generated_description = raw.get("generatedDescription")
    if isinstance(generated_description, dict):
        generated_description = _first_value(
            generated_description.get("shortRentDescription"),
            generated_description.get("shortSaleDescription"),
            generated_description.get("longDescription"),
        )

    normalized = normalize_listing(
        source,
        {
            "external_id": raw.get("id") or raw.get("listingId"),
            "canonical_url": canonical_url,
            "title": _first_value(
                raw.get("title"),
                raw.get("name"),
                raw.get("displayAddress"),
                generated_description,
                raw.get("remarks"),
            ),
            "description": raw.get("description"),
            "city": raw.get("city") or (address.get("city") if isinstance(address, dict) else None) or str(address_text),
            "state": state,
            "neighborhood": neighborhood,
            "address": address_text,
            "latitude": latitude or safe_decimal(geo.get("latitude") if isinstance(geo, dict) else None),
            "longitude": longitude or safe_decimal(geo.get("longitude") if isinstance(geo, dict) else None),
            "price_sale": safe_decimal(sale),
            "price_rent": safe_decimal(rent),
            "condo_fee": safe_decimal(condo_fee),
            "iptu": safe_decimal(iptu),
            "bedrooms": raw.get("bedrooms") or raw.get("dormitories"),
            "bathrooms": raw.get("bathrooms"),
            "parking_spaces": raw.get("parkingSpaces") or raw.get("suites"),
            "area_m2": safe_decimal(raw.get("usableAreas") or raw.get("usableArea") or raw.get("area")),
            "broker_name": raw.get("account") or raw.get("publisher") or raw.get("realEstateName"),
            "raw_payload": raw,
        },
        seed_url,
    )
    return normalized


def extract_from_json_blobs(source: SourceDefinition, blobs: Iterable[object], seed_url: str) -> list[ListingDraft]:
    candidates: list[dict] = []
    for blob in blobs:
        for item in walk_json(blob):
            if isinstance(item, dict) and maybe_listing_object(item):
                candidates.append(item)

    listings: list[ListingDraft] = []
    seen: set[str] = set()
    seen_urls: set[str] = set()
    for item in sorted(candidates, key=_listing_score, reverse=True):
        listing = listing_from_json(source, item, seed_url)
        if listing is None or listing.external_id in seen or listing.canonical_url in seen_urls:
            continue
        seen.add(listing.external_id)
        seen_urls.add(listing.canonical_url)
        listings.append(listing)
    return listings


class EmbeddedDataStrategy(ScrapeStrategy):
    name = "embedded_data"

    def scrape_seed(self, client: httpx.Client, source: SourceDefinition, seed_url: str) -> StrategyResult:
        response = client.get(seed_url)
        response.raise_for_status()
        blobs = collect_json_blobs(response.text)
        listings = extract_from_json_blobs(source, blobs, seed_url)
        return StrategyResult(
            strategy=self.name,
            listings=listings,
            diagnostics={"json_blobs": len(blobs), "seed_url": seed_url},
        )
