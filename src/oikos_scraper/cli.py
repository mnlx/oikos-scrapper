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
    source_config_default = (
        get_setting("OIKOS_SOURCE_CONFIG", "bots/realestate-listings/sources.yaml")
        or "bots/realestate-listings/sources.yaml"
    )
    neighborhood_config_default = (
        get_setting("OIKOS_NEIGHBORHOOD_SOURCE_CONFIG", "bots/neighborhood-signal/sources.yaml")
        or "bots/neighborhood-signal/sources.yaml"
    )

    migrate = subparsers.add_parser("migrate")
    migrate.add_argument("--config", default="alembic.ini")

    scrape = subparsers.add_parser("scrape")
    scrape.add_argument("--config", default=source_config_default)
    scrape.add_argument("--run-mode", default="scheduled")
    scrape.add_argument("--source", action="append", default=[])
    scrape.add_argument("--group", choices=["agency", "developer"], default=None)

    ingest = subparsers.add_parser("ingest")
    ingest.add_argument("--config", default=source_config_default)
    ingest.add_argument("--run-mode", default="scheduled")
    ingest.add_argument("--source", action="append", default=[])
    ingest.add_argument("--group", choices=["agency", "developer"], default=None)

    parse = subparsers.add_parser("parse")
    parse.add_argument("--config", default=source_config_default)
    parse.add_argument("--source", action="append", default=[])
    parse.add_argument("--run-dbt", action="store_true")

    enrich_assets = subparsers.add_parser("enriching-assets")
    enrich_assets.add_argument("--config", default=source_config_default)
    enrich_assets.add_argument("--source", action="append", default=[])

    enrich_geocodes = subparsers.add_parser("enrich-geocodes")
    enrich_geocodes.add_argument("--config", default=source_config_default)
    enrich_geocodes.add_argument("--source", action="append", default=[])
    enrich_geocodes.add_argument("--limit", type=int, default=200)

    publish = subparsers.add_parser("publish")
    publish.add_argument("--select", default="tag:gold")

    scrape_source = subparsers.add_parser("scrape-source")
    scrape_source.add_argument("--config", default=source_config_default)
    scrape_source.add_argument("--source", required=True)
    scrape_source.add_argument("--run-mode", default="manual")

    benchmark = subparsers.add_parser("benchmark-source")
    benchmark.add_argument("--config", default=source_config_default)
    benchmark.add_argument("--source", required=True)

    llm_enrich = subparsers.add_parser("llm-enrich")
    llm_enrich.add_argument("--config", default=source_config_default)
    llm_enrich.add_argument("--source", action="append", default=[])
    llm_enrich.add_argument("--limit", type=int, default=50)

    enrich_prices = subparsers.add_parser("enrich-prices")
    enrich_prices.add_argument("--config", default=source_config_default)
    enrich_prices.add_argument("--source", action="append", default=[])
    enrich_prices.add_argument("--limit", type=int, default=100)

    neighborhood_ingest = subparsers.add_parser("neighborhood-ingest")
    neighborhood_ingest.add_argument("--config", default=neighborhood_config_default)
    neighborhood_ingest.add_argument("--run-mode", default="scheduled")
    neighborhood_ingest.add_argument("--source", action="append", default=[])

    neighborhood_parse = subparsers.add_parser("neighborhood-parse")
    neighborhood_parse.add_argument("--config", default=neighborhood_config_default)
    neighborhood_parse.add_argument("--source", action="append", default=[])

    neighborhood_enrich_assets = subparsers.add_parser("neighborhood-enriching-assets")
    neighborhood_enrich_assets.add_argument("--config", default=neighborhood_config_default)
    neighborhood_enrich_assets.add_argument("--source", action="append", default=[])

    neighborhood_publish = subparsers.add_parser("neighborhood-publish")
    neighborhood_publish.add_argument("--select", default="mart_neighborhood_signals mart_neighborhood_files")

    return parser


def run_migrations(config_path: str) -> None:
    alembic_config = Config(config_path)
    database_url = os.environ.get("DATABASE_URL")
    if database_url:
        alembic_config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(alembic_config, "head")


def build_neighborhood_runner(config_path: str):  # noqa: ANN201
    from oikos_scraper.bots.neighborhood_signal import NeighborhoodSignalRunner

    config = load_config(config_path)
    return NeighborhoodSignalRunner(config)


def main() -> None:
    load_environment()
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(get_setting("OIKOS_LOG_LEVEL", "INFO") or "INFO")

    if args.command == "migrate":
        run_migrations(args.config)
        return

    if args.command == "scrape":
        config = load_config(args.config)
        runner = ScrapeRunner(config)
        summaries = runner.scrape_sources(
            args.source or None,
            trigger_type=args.run_mode,
            group=args.group,
        )
        print(json.dumps([asdict(summary) for summary in summaries], indent=2))
        return

    if args.command == "ingest":
        config = load_config(args.config)
        runner = ScrapeRunner(config)
        summaries = runner.ingest_sources(
            args.source or None,
            trigger_type=args.run_mode,
            group=args.group,
        )
        print(json.dumps([asdict(summary) for summary in summaries], indent=2))
        return

    if args.command == "parse":
        config = load_config(args.config)
        runner = ScrapeRunner(config)
        summaries = runner.parse_sources(args.source or None)
        response: dict[str, object] = {"summaries": [asdict(summary) for summary in summaries]}
        if args.run_dbt:
            dbt_result = runner.run_dbt_build(select="tag:silver")
            response["dbt"] = {
                "returncode": dbt_result.returncode,
                "stdout": dbt_result.stdout,
                "stderr": dbt_result.stderr,
            }
        print(json.dumps(response, indent=2))
        return

    if args.command == "enriching-assets":
        config = load_config(args.config)
        runner = ScrapeRunner(config)
        summaries = runner.enrich_assets_sources(args.source or None)
        print(json.dumps([asdict(summary) for summary in summaries], indent=2))
        return

    if args.command == "enrich-geocodes":
        config = load_config(args.config)
        runner = ScrapeRunner(config)
        summaries = runner.enrich_geocodes(
            source_codes=args.source or None,
            limit=args.limit,
        )
        print(json.dumps([asdict(summary) for summary in summaries], indent=2))
        return

    if args.command == "publish":
        config = load_config(source_config_default)
        runner = ScrapeRunner(config)
        result = runner.run_dbt_build(select=args.select)
        print(
            json.dumps(
                {
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                },
                indent=2,
            )
        )
        return

    if args.command == "scrape-source":
        config = load_config(args.config)
        runner = ScrapeRunner(config)
        summary = runner.scrape_sources([args.source], trigger_type=args.run_mode)
        print(json.dumps([asdict(item) for item in summary], indent=2))
        return

    if args.command == "benchmark-source":
        config = load_config(args.config)
        runner = ScrapeRunner(config)
        print(json.dumps(runner.benchmark_source(args.source), indent=2))
        return

    if args.command == "llm-enrich":
        config = load_config(args.config)
        runner = ScrapeRunner(config)
        summaries = runner.enrich_with_llm(
            source_codes=args.source or None,
            limit=args.limit,
        )
        print(json.dumps([asdict(summary) for summary in summaries], indent=2))
        return

    if args.command == "enrich-prices":
        config = load_config(args.config)
        runner = ScrapeRunner(config)
        summaries = runner.enrich_prices(
            source_codes=args.source or None,
            limit=args.limit,
        )
        print(json.dumps([asdict(summary) for summary in summaries], indent=2))
        return

    if args.command == "neighborhood-ingest":
        runner = build_neighborhood_runner(args.config)
        summaries = runner.ingest_sources(args.source or None, trigger_type=args.run_mode)
        print(json.dumps([asdict(summary) for summary in summaries], indent=2))
        return

    if args.command == "neighborhood-parse":
        runner = build_neighborhood_runner(args.config)
        summaries = runner.parse_sources(args.source or None)
        print(json.dumps([asdict(summary) for summary in summaries], indent=2))
        return

    if args.command == "neighborhood-enriching-assets":
        runner = build_neighborhood_runner(args.config)
        summaries = runner.enrich_assets_sources(args.source or None)
        print(json.dumps([asdict(summary) for summary in summaries], indent=2))
        return

    if args.command == "neighborhood-publish":
        runner = build_neighborhood_runner(neighborhood_config_default)
        result = runner.run_dbt_build(select=args.select)
        print(
            json.dumps(
                {
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                },
                indent=2,
            )
        )
        return


if __name__ == "__main__":
    main()
