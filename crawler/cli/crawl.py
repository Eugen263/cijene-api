#!/usr/bin/env python3
import argparse
import asyncio
import datetime
import logging
import sys
from pathlib import Path

from crawler.crawl import crawl, get_chains


def parse_date(date_str):
    """Parse a date string in YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("Date must be in YYYY-MM-DD format")


def setup_logging(log_level, db_direct: bool = False):
    """Configure logging for the crawler package (and ingest package when db_direct)."""
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    level = level_map.get(log_level.lower(), logging.WARNING)
    logging.basicConfig(
        level=level,
        format="%(asctime)s:%(name)s:%(levelname)s:%(message)s",
        stream=sys.stderr,
    )

    allowed_prefixes = ("crawler",) if not db_direct else ("crawler", "ingest", "service.db")

    for handler in logging.root.handlers:
        handler.addFilter(
            lambda record, p=allowed_prefixes: any(record.name.startswith(p) for p in p)
        )

    for logger_name in logging.root.manager.loggerDict:
        if not any(logger_name.startswith(p) for p in allowed_prefixes):
            logging.getLogger(logger_name).setLevel(logging.ERROR)


def main():
    parser = argparse.ArgumentParser(
        description="Crawl retail chains for product pricing data",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "output_path",
        nargs="?",
        type=Path,
        default=None,
        help="Output directory path where data will be stored.\n(Required unless -l/--list is used)",
    )
    parser.add_argument(
        "-d",
        "--date",
        type=parse_date,
        help="Date for which to crawl (format: YYYY-MM-DD, defaults to today)",
    )
    parser.add_argument(
        "-c",
        "--chain",
        help="Comma-separated list of retail chains to crawl (defaults to all)",
    )
    parser.add_argument(
        "-l",
        "--list",
        action="store_true",
        help="List supported retail chains and exit (output_path is not required)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        choices=["debug", "info", "warning", "error", "critical"],
        default="warning",
        help="Set verbosity level (default: warning)",
    )
    parser.add_argument(
        "--db-direct",
        action="store_true",
        help=(
            "Write crawled data directly to the database (requires DB_DSN env var).\n"
            "CSV files and ZIP archive are still saved as backup."
        ),
    )
    parser.add_argument(
        "--skip-stats",
        action="store_true",
        help="Skip computing chain stats after DB import (only with --db-direct)",
    )

    args = parser.parse_args()

    setup_logging(args.verbose, db_direct=args.db_direct)

    if args.list:
        print("Supported retail chains:")
        for chain_name in get_chains():
            print(f"  - {chain_name}")
        return 0

    if args.output_path is None:
        parser.error("output_path is required; use -h/--help for more info")

    if args.output_path.is_file():
        parser.error(f"Output path '{args.output_path}' is a file.")

    if not args.output_path.exists():
        args.output_path.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {args.output_path}")

    chains_to_crawl = None
    if args.chain:
        chains_to_crawl = [chain.strip() for chain in args.chain.split(",")]
        available_chains = get_chains()
        for chain_name in chains_to_crawl:
            if chain_name not in available_chains:
                parser.error(
                    f"Unknown chain '{chain_name}'. Available chains: {', '.join(available_chains)}"
                )

    try:
        crawl_date = args.date
        chains_txt = (
            ", ".join(chains_to_crawl) if chains_to_crawl else "all retail chains"
        )
        date_txt = args.date.strftime("%Y-%m-%d") if args.date else "today"
        print(f"Fetching price data from {chains_txt} for {date_txt} ...", flush=True)

        zip_path, chain_stores = crawl(
            args.output_path,
            crawl_date,
            chains_to_crawl,
            db_direct=args.db_direct,
        )
        print(f"Archive created: {zip_path}")

        if args.db_direct and chain_stores:
            # Lazy import keeps the crawler usable without asyncpg installed
            from service.db.ingest import ingest_crawl_results

            effective_date = crawl_date or datetime.date.today()
            print(f"Ingesting data into database for {effective_date:%Y-%m-%d} ...", flush=True)
            asyncio.run(
                ingest_crawl_results(
                    effective_date,
                    chain_stores,
                    compute_stats_flag=not args.skip_stats,
                )
            )
            print("Database import complete.")

        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
