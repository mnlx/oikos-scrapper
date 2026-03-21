from __future__ import annotations

from urllib.parse import urljoin

import httpx

from oikos_scraper.config import SourceDefinition
from oikos_scraper.heuristics import (
    compact_text,
    extract_description_from_html,
    extract_detail_links,
    extract_location_fields_from_html,
    extract_numeric_features,
    extract_text_blocks,
    extract_title_from_html,
    find_price_candidates,
)
from oikos_scraper.normalizer import normalize_listing
from oikos_scraper.strategies.base import ScrapeStrategy
from oikos_scraper.types import ListingDraft, StrategyResult


def extract_listing_from_detail(source: SourceDefinition, html: str, detail_url: str, seed_url: str) -> ListingDraft | None:
    texts = extract_text_blocks(html)
    prices = find_price_candidates(texts[:40])
    features = extract_numeric_features(texts[:80])
    location = extract_location_fields_from_html(html, fallback_city=source.cities[0] if source.cities else None)
    return normalize_listing(
        source,
        {
            "canonical_url": detail_url,
            "title": extract_title_from_html(html),
            "description": extract_description_from_html(html),
            "city": location["city"] or " ".join(texts[:20]),
            "neighborhood": location["neighborhood"],
            "address": location["address"],
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "price_sale": prices[0] if prices else None,
            "price_rent": prices[0] if prices else None,
            "bedrooms": features["bedrooms"],
            "bathrooms": features["bathrooms"],
            "parking_spaces": features["parking_spaces"],
            "area_m2": features["area_m2"],
            "raw_payload": {
                "seed_url": seed_url,
                "detail_url": detail_url,
                "raw_html": html,
                "excerpt": texts[:40],
            },
        },
        seed_url,
    )


def enrich_listing_from_detail_html(listing: ListingDraft, html: str) -> ListingDraft:
    location = extract_location_fields_from_html(html, fallback_city=listing.city)
    if not listing.address and location["address"]:
        listing.address = compact_text(str(location["address"])) or None
    if not listing.neighborhood and location["neighborhood"]:
        listing.neighborhood = compact_text(str(location["neighborhood"])) or None
    if listing.latitude is None and location["latitude"] is not None:
        listing.latitude = location["latitude"]
    if listing.longitude is None and location["longitude"] is not None:
        listing.longitude = location["longitude"]
    if location["city"] and listing.city != location["city"]:
        listing.city = str(location["city"])

    listing.raw_payload = {
        **listing.raw_payload,
        "detail_enrichment": {
            "address": listing.address,
            "neighborhood": listing.neighborhood,
            "latitude": str(listing.latitude) if listing.latitude is not None else None,
            "longitude": str(listing.longitude) if listing.longitude is not None else None,
        },
        "raw_html": html,
    }
    return listing


class StaticHTMLStrategy(ScrapeStrategy):
    name = "static_html"

    def scrape_seed(self, client: httpx.Client, source: SourceDefinition, seed_url: str) -> StrategyResult:
        response = client.get(seed_url)
        response.raise_for_status()
        detail_links = extract_detail_links(response.text, seed_url)[:25]
        listings: list[ListingDraft] = []
        for detail_url in detail_links:
            detail_response = client.get(urljoin(seed_url, detail_url))
            if detail_response.is_error:
                continue
            listing = extract_listing_from_detail(source, detail_response.text, detail_response.url, seed_url)
            if listing is not None:
                listings.append(listing)
        return StrategyResult(
            strategy=self.name,
            listings=listings,
            diagnostics={"detail_links": len(detail_links), "seed_url": seed_url},
        )
