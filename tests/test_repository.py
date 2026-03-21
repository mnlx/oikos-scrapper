from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from oikos_scraper.config import load_config
from oikos_scraper.db.repository import ensure_sources
from oikos_scraper.db.session import create_session_factory


@pytest.mark.skipif("TEST_DATABASE_URL" not in os.environ, reason="TEST_DATABASE_URL not set")
def test_ensure_sources_round_trip() -> None:
    session_factory = create_session_factory(os.environ["TEST_DATABASE_URL"])
    with session_factory() as session:
        session.execute(text("select 1"))
        mapping = ensure_sources(session, load_config("config/sources.yaml").sources)
    assert "olx" in mapping
