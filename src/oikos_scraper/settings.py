from __future__ import annotations

import os

from dotenv import load_dotenv


def load_environment() -> None:
    load_dotenv(override=False)


def get_setting(name: str, default: str | None = None) -> str | None:
    load_environment()
    return os.environ.get(name, default)
