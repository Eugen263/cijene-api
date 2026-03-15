#!/usr/bin/env python3
"""
Direct ingest of crawler Store models into the database.

This module is the DB-direct counterpart to service/db/import.py.
Instead of reading from CSV files, it accepts crawler Store model objects
and writes them straight to the database — preserving full type safety
and skipping the CSV serialization/deserialization round-trip.

Data is written incrementally: each store's record, new products, and prices
are committed as soon as that store is processed, so partial progress is never
lost if the run is interrupted.

service/db/import.py is kept for manual re-imports of historical ZIP archives.
"""
import logging
from datetime import date
from decimal import Decimal
from time import time
from typing import Optional

from crawler.store.models import Product as CrawlerProduct
from crawler.store.models import Store as CrawlerStore
from service.config import settings
from service.db.models import Chain, ChainProduct, Price, Store as DbStore
from service.db.stats import compute_stats

logger = logging.getLogger("ingest")

db = settings.get_db()


def _get_barcode(product: CrawlerProduct, chain_code: str) -> str:
    """
    Return a valid barcode for the product, falling back to chain:product_id
    if the product has no real EAN.
    """
    barcode = (product.barcode or "").strip()

    if ":" in barcode:
        return barcode

    if len(barcode) >= 8 and barcode.isdigit():
        return barcode

    if not product.product_id:
        logger.warning(f"[{chain_code}] Product has no barcode or product_id: {product}")
        return barcode

    return f"{chain_code}:{product.product_id}"


# kept for tests and backward compatibility
def _clean_barcode(data: dict, chain_code: str) -> dict:
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
    Ingest all data for one chain store by store.

    Each store's record, new products, and prices are written to the database
    before moving on to the next store. This means partial progress is committed
    incrementally and visible in real time.

    Args:
        price_date: Date for which prices are valid.
        chain_code: Chain identifier (e.g. "konzum", "lidl").
        stores: Crawler Store objects with product/price data.
        barcodes: Shared EAN→product_id mapping (mutated in place as new EANs are added).

    Returns:
        Total number of price records inserted across all stores.
    """
    chain_id = await db.add_chain(Chain(code=chain_code))
    chain_product_map = await db.get_chain_product_map(chain_id)

    n_total_prices = 0

    for store in stores:
        # --- store record ---
        store_db_id = await db.add_store(DbStore(
            chain_id=chain_id,
            code=store.store_id,
            name=store.name or None,
            type=store.store_type or None,
            address=store.street_address or None,
            city=store.city or None,
            zipcode=store.zipcode or None,
        ))

        # --- new products for this store ---
        new_chain_products = []
        for product in store.items:
            if product.product_id in chain_product_map:
                continue

            barcode = _get_barcode(product, chain_code)
            if barcode not in barcodes:
                barcodes[barcode] = await db.add_ean(barcode)

            new_chain_products.append(ChainProduct(
                chain_id=chain_id,
                product_id=barcodes[barcode],
                code=product.product_id,
                name=product.product,
                brand=product.brand.strip() or None if product.brand else None,
                category=product.category.strip() or None if product.category else None,
                unit=product.unit.strip() or None if product.unit else None,
                quantity=product.quantity.strip() or None if product.quantity else None,
            ))

        if new_chain_products:
            await db.add_many_chain_products(new_chain_products)
            # Fetch only the newly inserted codes to update the map
            new_codes = [cp.code for cp in new_chain_products]
            new_ids = await db.get_chain_products_by_codes(chain_id, new_codes)
            chain_product_map.update(new_ids)

        # --- prices for this store ---
        prices = []
        for product in store.items:
            product_db_id = chain_product_map.get(product.product_id)
            if product_db_id is None:
                logger.warning(f"[{chain_code}] Skipping price for unknown product {product.product_id}")
                continue
            prices.append(Price(
                chain_product_id=product_db_id,
                store_id=store_db_id,
                price_date=price_date,
                regular_price=product.price,
                special_price=product.special_price,
                unit_price=product.unit_price,
                best_price_30=product.best_price_30,
                anchor_price=product.anchor_price,
            ))

        n_inserted = await db.add_many_prices(prices)
        n_total_prices += n_inserted
        logger.info(
            f"[{chain_code}] {store.name} ({store.city}): "
            f"{len(new_chain_products)} new products, {n_inserted} prices"
        )

    n_enriched = await db.enrich_products_from_chain_data(chain_id)
    if n_enriched:
        logger.info(f"[{chain_code}] Enriched {n_enriched} products with chain data")

    return n_total_prices


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
