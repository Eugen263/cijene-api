#!/usr/bin/env python3
"""Geocode stores without lat/lon coordinates using OpenStreetMap Nominatim."""
import argparse
import asyncio
import logging
from typing import Optional

import httpx

from service.config import settings

logger = logging.getLogger("geocoder")

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_DELAY = 1.1  # seconds between requests (Nominatim policy: max 1/sec)
NOMINATIM_USER_AGENT = "cijene-api/1.0 (https://cijene.dev)"


async def geocode_address(
    client: httpx.AsyncClient,
    address: str,
    city: str,
    zipcode: Optional[str] = None,
) -> tuple[float, float] | None:
    """
    Query Nominatim to get lat/lon for a store address.

    Args:
        client: HTTP client to use for the request.
        address: Street address of the store.
        city: City where the store is located.
        zipcode: Optional postal code for better accuracy.

    Returns:
        A (lat, lon) tuple, or None if geocoding failed.
    """
    parts = [address]
    if zipcode:
        parts.append(zipcode)
    parts.append(city)
    parts.append("Croatia")
    query = ", ".join(p for p in parts if p)

    try:
        response = await client.get(
            NOMINATIM_URL,
            params={"q": query, "format": "json", "limit": 1, "countrycodes": "hr"},
        )
        response.raise_for_status()
        results = response.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        logger.warning(f"Geocoding failed for '{query}': {e}")
    return None


async def geocode_stores(dry_run: bool = False) -> None:
    """
    Geocode all stores in the database that are missing lat/lon.

    Args:
        dry_run: If True, log what would be done without updating the database.
    """
    db = settings.get_db()
    await db.connect()

    try:
        await db.create_tables()
        stores = await db.get_stores_without_location()

        if not stores:
            logger.info("All stores already have location data")
            return

        logger.info(f"Geocoding {len(stores)} stores...")

        headers = {"User-Agent": NOMINATIM_USER_AGENT}
        async with httpx.AsyncClient(headers=headers, timeout=10.0) as client:
            updated = 0
            failed = 0

            for i, store in enumerate(stores):
                if not store.address and not store.city:
                    logger.debug(f"Skipping store {store.code} (no address or city)")
                    continue

                result = await geocode_address(
                    client,
                    store.address or "",
                    store.city or "",
                    store.zipcode or None,
                )

                if result:
                    lat, lon = result
                    if dry_run:
                        logger.info(
                            f"[dry-run] Would set store {store.code} "
                            f"({store.address}, {store.city}) → ({lat}, {lon})"
                        )
                    else:
                        await db.update_store(
                            store.chain_id, store.code, lat=lat, lon=lon
                        )
                        logger.debug(
                            f"Geocoded store {store.code} "
                            f"({store.address}, {store.city}) → ({lat}, {lon})"
                        )
                    updated += 1
                else:
                    logger.warning(
                        f"No location found for store {store.code}: "
                        f"{store.address}, {store.city}"
                    )
                    failed += 1

                if i < len(stores) - 1:
                    await asyncio.sleep(NOMINATIM_DELAY)

        logger.info(
            f"Geocoding complete: {updated} updated, {failed} failed "
            f"out of {len(stores)} stores"
        )
    finally:
        await db.close()


async def main() -> None:
    """Geocode stores that are missing lat/lon coordinates."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be geocoded without updating the database",
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

    await geocode_stores(dry_run=args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
