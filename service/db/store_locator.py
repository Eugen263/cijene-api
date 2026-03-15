#!/usr/bin/env python3
"""
Enrich the stores table with data from OpenStreetMap Overpass API.

For each known Croatian grocery chain, fetch all OSM nodes/ways tagged with
the corresponding brand name, then:
  1. Match each OSM result against an existing DB store (same chain, within
     300 m or same city+address).
  2. If matched: fill in any missing lat/lon, address, city, zipcode, phone
     using COALESCE semantics (never overwrite existing official data).
  3. If unmatched: insert a new store record with code "osm:{osm_id}".

All proximity matching is done in Python after loading each chain's stores in
one query — no per-store DB round trips.

Run with: python3 -m service.db.store_locator [--dry-run] [--debug]
"""
import argparse
import asyncio
import logging
import math
from dataclasses import dataclass
from typing import Optional

import httpx

from service.config import settings
from service.db.models import Chain, Store as DbStore, StoreWithId

logger = logging.getLogger("store_locator")

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_TIMEOUT = 60
HTTP_TIMEOUT = 90.0

# Maps chain code → list of OSM brand name variants to search for.
CHAIN_BRANDS: dict[str, list[str]] = {
    "boso": ["Boso"],
    "brodokomerc": ["Brodokomerc"],
    "dm": ["dm", "dm drogerie markt"],
    "eurospin": ["Eurospin"],
    "jadranka": ["Jadranka Trgovina", "Jadranka"],
    "kaufland": ["Kaufland"],
    "konzum": ["Konzum"],
    "ktc": ["KTC"],
    "lidl": ["Lidl"],
    "lorenco": ["Lorenco"],
    "metro": ["Metro", "METRO"],
    "ntl": ["NTL"],
    "plodine": ["Plodine"],
    "ribola": ["Ribola"],
    "roto": ["Roto"],
    "spar": ["Spar", "SPAR", "Interspar", "INTERSPAR"],
    "studenac": ["Studenac"],
    "tommy": ["Tommy"],
    "trgocentar": ["Trgocentar"],
    "trgovina_krk": ["Trgovina Krk", "KRK"],
    "vrutak": ["Vrutak"],
    "zabac": ["Žabac", "Zabac"],
}

# Radius for proximity matching in km
MATCH_RADIUS_KM = 0.3


@dataclass
class OsmStore:
    osm_id: str          # "node/12345" or "way/67890"
    chain_code: str
    name: Optional[str]
    lat: float
    lon: float
    city: Optional[str]
    address: Optional[str]
    zipcode: Optional[str]
    phone: Optional[str]


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _find_nearby(
    osm: OsmStore,
    db_stores: list[StoreWithId],
    radius_km: float = MATCH_RADIUS_KM,
) -> Optional[StoreWithId]:
    """
    Return the closest DB store within radius_km, or None.

    Only considers stores that already have lat/lon. City+address fallback
    matching is intentionally skipped to keep the logic simple and avoid
    false positives between stores in the same city.
    """
    best: Optional[StoreWithId] = None
    best_dist = radius_km

    for store in db_stores:
        if store.lat is None or store.lon is None:
            continue
        dist = _haversine_km(osm.lat, osm.lon, store.lat, store.lon)
        if dist < best_dist:
            best_dist = dist
            best = store

    return best


def _build_overpass_query() -> str:
    """Build a single Overpass QL query for all chain brands in Croatia."""
    unions = []
    for brands in CHAIN_BRANDS.values():
        for brand in brands:
            escaped = brand.replace('"', '\\"')
            unions.append(f'  node["brand"="{escaped}"]["shop"](area.croatia);')
            unions.append(f'  way["brand"="{escaped}"]["shop"](area.croatia);')
            # Catch stores tagged by name only (smaller/local chains)
            unions.append(f'  node["name"="{escaped}"]["shop"](area.croatia);')
            unions.append(f'  way["name"="{escaped}"]["shop"](area.croatia);')

    return f"""
[out:json][timeout:{OVERPASS_TIMEOUT}];
area["ISO3166-1"="HR"]->.croatia;
(
{chr(10).join(unions)}
);
out center tags;
"""


def _classify_element(tags: dict) -> Optional[str]:
    """Return the chain_code that best matches this OSM element's tags."""
    brand = tags.get("brand", "").lower()
    name = tags.get("name", "").lower()

    for chain_code, variants in CHAIN_BRANDS.items():
        for variant in variants:
            v = variant.lower()
            if brand == v or name == v:
                return chain_code
    return None


def _parse_osm_element(
    elem: dict, chain_code: str, osm_type: str, osm_id: int
) -> Optional[OsmStore]:
    """Extract OsmStore from an OSM element dict. Returns None if unusable."""
    if osm_type == "node":
        lat = elem.get("lat", 0.0)
        lon = elem.get("lon", 0.0)
    elif osm_type == "way":
        center = elem.get("center", {})
        lat = center.get("lat", 0.0)
        lon = center.get("lon", 0.0)
    else:
        return None

    if lat is None or lon is None:
        return None

    tags = elem.get("tags", {})
    street = tags.get("addr:street", "")
    housenumber = tags.get("addr:housenumber", "")
    address = f"{street} {housenumber}".strip() or None
    city = (tags.get("addr:city")
            or tags.get("addr:town")
            or tags.get("addr:village")
            or None)

    return OsmStore(
        osm_id=f"{osm_type}/{osm_id}",
        chain_code=chain_code,
        name=tags.get("name") or tags.get("brand") or None,
        lat=lat,
        lon=lon,
        city=city,
        address=address,
        zipcode=tags.get("addr:postcode") or None,
        phone=tags.get("phone") or tags.get("contact:phone") or None,
    )


async def fetch_osm_stores(client: httpx.AsyncClient) -> dict[str, list[OsmStore]]:
    """
    Fetch all grocery stores in Croatia from OSM grouped by chain code.

    Returns a dict mapping chain_code → list[OsmStore].
    """
    query = _build_overpass_query()
    logger.info("Querying Overpass API for all chain stores in Croatia...")

    try:
        response = await client.post(
            OVERPASS_URL,
            data={"data": query},
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
    except httpx.HTTPError as e:
        logger.error(f"Overpass API request failed: {e}")
        return {}

    data = response.json()
    elements = data.get("elements", [])
    logger.info(f"Overpass returned {len(elements)} elements")

    result: dict[str, list[OsmStore]] = {code: [] for code in CHAIN_BRANDS}
    seen: set[str] = set()

    for elem in elements:
        osm_type = elem.get("type", "")
        osm_id = elem.get("id", 0)
        tags = elem.get("tags", {})

        chain_code = _classify_element(tags)
        if not chain_code:
            continue

        osm_key = f"{osm_type}/{osm_id}"
        if osm_key in seen:
            continue
        seen.add(osm_key)

        store = _parse_osm_element(elem, chain_code, osm_type, osm_id)
        if store:
            result[chain_code].append(store)

    for code, stores in result.items():
        if stores:
            logger.debug(f"  {code}: {len(stores)} OSM stores")

    return result


async def locate_stores(dry_run: bool = False) -> None:
    """
    Match OSM stores to the DB and enrich/insert as needed.

    For each chain:
    - Loads all existing DB stores in one query.
    - For each OSM store, checks proximity in Python (no per-store DB calls).
    - If a nearby DB store is found: fills in missing lat/lon/address/phone.
    - If not found: inserts a new store with code "osm:{osm_id}".
    """
    db = settings.get_db()
    await db.connect()

    try:
        await db.create_tables()

        async with httpx.AsyncClient(
            headers={"User-Agent": "cijene-api/1.0 (https://cijene.dev)"},
            timeout=HTTP_TIMEOUT,
        ) as client:
            osm_by_chain = await fetch_osm_stores(client)

        total_osm = sum(len(v) for v in osm_by_chain.values())
        logger.info(f"Processing {total_osm} OSM stores across {len(CHAIN_BRANDS)} chains")

        n_enriched = 0
        n_inserted = 0
        n_skipped = 0

        for chain_code, osm_stores in osm_by_chain.items():
            if not osm_stores:
                continue

            # Load all DB stores for this chain in one query
            chain_id = await db.add_chain(Chain(code=chain_code))
            db_stores = await db.list_stores(chain_code)

            for osm in osm_stores:
                existing = _find_nearby(osm, db_stores)

                if existing:
                    needs_update = (
                        (osm.name and existing.name is None)
                        or (osm.lat and existing.lat is None)
                        or (osm.lon and existing.lon is None)
                        or (osm.address and existing.address is None)
                        or (osm.city and existing.city is None)
                        or (osm.zipcode and existing.zipcode is None)
                        or (osm.phone and existing.phone is None)
                    )

                    if not needs_update:
                        n_skipped += 1
                        continue

                    if dry_run:
                        logger.info(
                            f"[dry-run] [{chain_code}] Would enrich {existing.code} "
                            f"({existing.city or osm.city}) "
                            f"lat={osm.lat:.4f}, lon={osm.lon:.4f}"
                        )
                    else:
                        await db.update_store(
                            chain_id,
                            existing.code,
                            name=osm.name if existing.name is None else None,
                            address=osm.address if existing.address is None else None,
                            city=osm.city if existing.city is None else None,
                            zipcode=osm.zipcode if existing.zipcode is None else None,
                            lat=osm.lat if existing.lat is None else None,
                            lon=osm.lon if existing.lon is None else None,
                            phone=osm.phone if existing.phone is None else None,
                        )
                        logger.debug(
                            f"[{chain_code}] Enriched {existing.code} "
                            f"({existing.city or osm.city}) from OSM"
                        )
                    n_enriched += 1

                else:
                    osm_code = f"osm:{osm.osm_id}"

                    if dry_run:
                        logger.info(
                            f"[dry-run] [{chain_code}] Would insert {osm_code} "
                            f"'{osm.name}' in {osm.city} "
                            f"({osm.lat:.4f}, {osm.lon:.4f})"
                        )
                    else:
                        await db.add_store(DbStore(
                            chain_id=chain_id,
                            code=osm_code,
                            name=osm.name or None,
                            address=osm.address or None,
                            city=osm.city or None,
                            zipcode=osm.zipcode or None,
                            lat=osm.lat,
                            lon=osm.lon,
                            phone=osm.phone or None,
                        ))
                        logger.debug(
                            f"[{chain_code}] Inserted {osm_code} "
                            f"'{osm.name}' in {osm.city}"
                        )
                    n_inserted += 1

        logger.info(
            f"Store locator complete: "
            f"{n_enriched} enriched, {n_inserted} inserted, {n_skipped} already complete"
        )

    finally:
        await db.close()


async def main() -> None:
    """Locate and enrich stores using OpenStreetMap data."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing to the database",
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

    await locate_stores(dry_run=args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
