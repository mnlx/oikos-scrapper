from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict

from alembic import command
from alembic.config import Config

from oikos_scraper.config import load_config
from oikos_scraper.logging import configure_logging
from oikos_scraper.runner import ScrapeRunner
from oikos_scraper.settings import get_setting, load_environment


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="oikos")
    subparsers = parser.add_subparsers(dest="command", required=True)
    source_config_default = get_setting("OIKOS_SOURCE_CONFIG", "config/sources.yaml") or "config/sources.yaml"

    migrate = subparsers.add_parser("migrate")
    migrate.add_argument("--config", default="alembic.ini")

    scrape = subparsers.add_parser("scrape")
    scrape.add_argument("--config", default=source_config_default)
    scrape.add_argument("--run-mode", default="scheduled")
    scrape.add_argument("--source", action="append", default=[])
    scrape.add_argument("--group", choices=["agency", "developer"], default=None)

    scrape_source = subparsers.add_parser("scrape-source")
    scrape_source.add_argument("--config", default=source_config_default)
    scrape_source.add_argument("--source", required=True)
    scrape_source.add_argument("--run-mode", default="manual")

    benchmark = subparsers.add_parser("benchmark-source")
    benchmark.add_argument("--config", default=source_config_default)
    benchmark.add_argument("--source", required=True)

    return parser


def run_migrations(config_path: str) -> None:
    alembic_config = Config(config_path)
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        alembic_config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(alembic_config, "head")


def main() -> None:
    load_environment()
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(get_setting("OIKOS_LOG_LEVEL", "INFO") or "INFO")

    if args.command == "migrate":
        run_migrations(args.config)
        return

    config = load_config(args.config)
    runner = ScrapeRunner(config)

    if args.command == "scrape":
        summaries = runner.scrape_sources(
            args.source or None,
            trigger_type=args.run_mode,
            group=args.group,
        )
        print(json.dumps([asdict(summary) for summary in summaries], indent=2))
        return

    if args.command == "scrape-source":
        summary = runner.scrape_sources([args.source], trigger_type=args.run_mode)
        print(json.dumps([asdict(item) for item in summary], indent=2))
        return

    if args.command == "benchmark-source":
        print(json.dumps(runner.benchmark_source(args.source), indent=2))
        return


if __name__ == "__main__":
    main()
