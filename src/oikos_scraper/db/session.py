from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from oikos_scraper.settings import load_environment


def get_database_url() -> str:
    load_environment()
    try:
        return os.environ["DATABASE_URL"]
    except KeyError as exc:
        raise RuntimeError("DATABASE_URL is required") from exc


def create_session_factory(url: str | None = None) -> sessionmaker[Session]:
    engine = create_engine(url or get_database_url(), future=True)
    return sessionmaker(engine, expire_on_commit=False)
