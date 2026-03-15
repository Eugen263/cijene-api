#!/usr/bin/env python3
"""
Statistics computation service for retail chain price data.

This module provides functionality to compute statistics for retail chain price data
stored in the database. It can be used independently or as part of the import process.
"""

import argparse
import asyncio
import logging
from datetime import date, datetime
from time import time
from typing import Union

from service.config import settings

logger = logging.getLogger("stats")

db = settings.get_db()


async def compute_stats(price_date: Union[date, datetime, str]) -> None:
    """
    Compute statistics for the given date.

    Args:
        price_date: A date/datetime object or a date string in YYYY-MM-DD format
    """
    if isinstance(price_date, str):
        try:
            price_date = datetime.strptime(price_date, "%Y-%m-%d").date()
        except ValueError:
            logger.error(f"Invalid date format: {price_date}. Expected YYYY-MM-DD")
            return
    elif isinstance(price_date, datetime):
        price_date = price_date.date()

    logger.info(f"Computing statistics for {price_date:%Y-%m-%d}")

    t0 = time()

    logger.debug(f"Computing average chain prices for {price_date:%Y-%m-%d}")
    await db.compute_chain_prices(price_date)

    logger.debug(f"Computing chain stats for {price_date:%Y-%m-%d}")
    await db.compute_chain_stats(price_date)

    dt = int(time() - t0)
    logger.info(f"Computed statistics for {price_date:%Y-%m-%d} in {dt} seconds")


async def main():
    """
    Standalone CLI for computing statistics.

    This allows running statistics computation independently from the import process.
    Useful for recomputing stats on existing data or batch processing multiple dates.
    """
    parser = argparse.ArgumentParser(
        description="Compute statistics for retail chain price data",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "dates",
        help="One or more dates (YYYY-MM-DD) to compute statistics for",
        nargs="+",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s:%(name)s:%(levelname)s:%(message)s",
    )

    await db.connect()

    try:
        for date_str in args.dates:
            await compute_stats(date_str)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
