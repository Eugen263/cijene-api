#!/usr/bin/env python3
"""
Direct ingest of crawler Store models into the database.

This module is the DB-direct counterpart to service/db/import.py.
Instead of reading from CSV files, it accepts crawler Store model objects
and writes them straight to the database — preserving full type safety
and skipping the CSV serialization/deserialization round-trip.

service/db/import.py is kept for manual re-imports of historical ZIP archives.
"""
import logging
from datetime import date
from decimal import Decimal
from time import time
from typing import Optional

from crawler.store.models import Store as CrawlerStore
from crawler.store.output import transform_products
from service.config import settings
from service.db.models import Chain, ChainProduct, Price, Store as DbStore
from service.db.stats import compute_stats

logger = logging.getLogger("ingest")

db = settings.get_db()


def _clean_barcode(data: dict, chain_code: str) -> dict:
    """
    Ensure the barcode is valid; fall back to a chain-namespaced code.
    Mirrors the same logic in service/db/import.py.
    """
    barcode = data.get("barcode", "").strip()

    if ":" in barcode:
        return data

    if len(barcode) >= 8 and barcode.isdigit():
        return data

    product_id = data.get("product_id", "")
    if not product_id:
        logger.warning(f"Product has no barcode: {data}")
        return data

    data["barcode"] = f"{chain_code}:{product_id}"
    return data


def _clean_price(value: Optional[str]) -> Optional[Decimal]:
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    dval = Decimal(value)
    return dval if dval != 0 else None


async def ingest_chain(
    price_date: date,
    chain_code: str,
    stores: list[CrawlerStore],
    barcodes: dict[str, int],
) -> int:
    """
    Ingest all data for one chain directly from crawler Store model objects.

    Args:
        price_date: Date for which prices are valid.
        chain_code: Chain identifier (e.g. "konzum", "lidl").
        stores: Crawler Store objects with product/price data.
        barcodes: Shared EAN→product_id mapping (mutated in place as new EANs are added).

    Returns:
        Number of price records inserted.
    """
    store_list, product_list, price_list = transform_products(stores)

    chain_id = await db.add_chain(Chain(code=chain_code))

    # --- stores ---
    store_map: dict[str, int] = {}
    for s in store_list:
        db_store = DbStore(
            chain_id=chain_id,
            code=s["store_id"],
            type=s["type"] or None,
            address=s["address"] or None,
            city=s["city"] or None,
            zipcode=s["zipcode"] or None,
        )
        store_map[s["store_id"]] = await db.add_store(db_store)

    logger.debug(f"[{chain_code}] Processed {len(store_list)} stores")

    # --- products ---
    chain_product_map = await db.get_chain_product_map(chain_id)

    new_products = [
        _clean_barcode(p, chain_code)
        for p in product_list
        if p["product_id"] not in chain_product_map
    ]

    if new_products:
        n_new_barcodes = 0
        for p in new_products:
            barcode = p["barcode"]
            if barcode not in barcodes:
                barcodes[barcode] = await db.add_ean(barcode)
                n_new_barcodes += 1

        if n_new_barcodes:
            logger.debug(f"[{chain_code}] Added {n_new_barcodes} new EAN codes")

        products_to_create = [
            ChainProduct(
                chain_id=chain_id,
                product_id=barcodes[p["barcode"]],
                code=p["product_id"],
                name=p["name"],
                brand=(p["brand"] or "").strip() or None,
                category=(p["category"] or "").strip() or None,
                unit=(p["unit"] or "").strip() or None,
                quantity=(p["quantity"] or "").strip() or None,
            )
            for p in new_products
        ]
        await db.add_many_chain_products(products_to_create)
        logger.debug(f"[{chain_code}] Imported {len(new_products)} new products")

        chain_product_map = await db.get_chain_product_map(chain_id)

    # --- prices ---
    prices_to_create = []
    for p in price_list:
        product_id = chain_product_map.get(p["product_id"])
        if product_id is None:
            logger.warning(f"[{chain_code}] Skipping price for unknown product {p['product_id']}")
            continue
        prices_to_create.append(
            Price(
                chain_product_id=product_id,
                store_id=store_map[p["store_id"]],
                price_date=price_date,
                regular_price=Decimal(str(p["price"])),
                special_price=_clean_price(p.get("special_price")),
                unit_price=_clean_price(p.get("unit_price")),
                best_price_30=_clean_price(p.get("best_price_30")),
                anchor_price=_clean_price(p.get("anchor_price")),
            )
        )

    n_inserted = await db.add_many_prices(prices_to_create)
    logger.info(f"[{chain_code}] Imported {n_inserted} prices")
    return n_inserted


async def ingest_crawl_results(
    price_date: date,
    chain_stores: dict[str, list[CrawlerStore]],
    compute_stats_flag: bool = True,
) -> None:
    """
    Ingest crawl results from all chains directly into the database.

    Args:
        price_date: Date for which the prices are valid.
        chain_stores: Mapping of chain_code -> list of crawler Store objects.
        compute_stats_flag: Whether to compute chain stats after import.
    """
    await db.connect()

    try:
        await db.create_tables()

        t0 = time()
        barcodes = await db.get_product_barcodes()

        for chain_code, stores in chain_stores.items():
            if not stores:
                logger.warning(f"[{chain_code}] No stores to ingest, skipping")
                continue
            try:
                n_prices = sum(len(s.items) for s in stores)
                logger.info(f"[{chain_code}] Ingesting {len(stores)} stores, {n_prices} prices ...")
                await ingest_chain(price_date, chain_code, stores, barcodes)
            except Exception as e:
                logger.error(f"[{chain_code}] Ingest failed: {e}", exc_info=True)

        dt = int(time() - t0)
        logger.info(f"Ingested {len(chain_stores)} chains in {dt}s")

        if compute_stats_flag:
            await compute_stats(price_date)
        else:
            logger.debug(f"Skipping stats for {price_date:%Y-%m-%d}")
    finally:
        await db.close()
