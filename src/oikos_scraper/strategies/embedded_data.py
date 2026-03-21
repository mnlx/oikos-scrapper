from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal

import httpx

from oikos_scraper.config import SourceDefinition
from oikos_scraper.heuristics import collect_json_blobs, maybe_listing_object, safe_decimal, walk_json
from oikos_scraper.normalizer import normalize_listing
from oikos_scraper.strategies.base import ScrapeStrategy
from oikos_scraper.types import ListingDraft, StrategyResult


def listing_from_json(source: SourceDefinition, raw: dict, seed_url: str) -> ListingDraft | None:
    address = raw.get("address") or raw.get("location")
    geo = raw.get("geo") or raw.get("coordinates") or {}
    price = raw.get("price") or raw.get("pricingInfos") or raw.get("pricinginfos") or {}
    if isinstance(price, dict):
        sale = (
            price.get("price")
            or price.get("amount")
            or price.get("businessPrice")
            or price.get("value")
        )
        rent = price.get("rentalTotalPrice") or price.get("monthlyCondoFee")
        condo_fee = price.get("monthlyCondoFee")
        iptu = price.get("yearlyIptu")
    else:
        sale = price
        rent = None
        condo_fee = None
        iptu = None

    normalized = normalize_listing(
        source,
        {
            "external_id": raw.get("id") or raw.get("listingId"),
            "canonical_url": raw.get("url") or raw.get("link"),
            "title": raw.get("title") or raw.get("name"),
            "description": raw.get("description"),
            "city": raw.get("city") or str(address),
            "state": raw.get("state") or "SC",
            "neighborhood": raw.get("neighborhood"),
            "address": address,
            "latitude": safe_decimal(geo.get("latitude") if isinstance(geo, dict) else None),
            "longitude": safe_decimal(geo.get("longitude") if isinstance(geo, dict) else None),
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
    listings: list[ListingDraft] = []
    seen: set[str] = set()
    for blob in blobs:
        for item in walk_json(blob):
            if not maybe_listing_object(item):
                continue
            listing = listing_from_json(source, item, seed_url)
            if listing is None or listing.external_id in seen:
                continue
            seen.add(listing.external_id)
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
