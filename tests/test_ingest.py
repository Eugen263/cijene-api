"""
Tests for service/db/ingest.py pure functions and crawler/store/output.py transform_products.
Run with: python -m pytest tests/ or python -m unittest tests/test_ingest.py
"""
import sys
import os
import unittest
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── _clean_barcode ─────────────────────────────────────────────────────────────

# Import only the pure helper — avoids pulling in asyncpg / service config
from service.db.ingest import _clean_barcode, _clean_price


class TestCleanBarcode(unittest.TestCase):
    def test_valid_ean13(self):
        """13-digit numeric barcode is kept as-is."""
        data = {"barcode": "3850151240124", "product_id": "ABC"}
        result = _clean_barcode(data, "konzum")
        self.assertEqual(result["barcode"], "3850151240124")

    def test_valid_ean8(self):
        """8-digit numeric barcode is kept as-is."""
        data = {"barcode": "12345678", "product_id": "ABC"}
        result = _clean_barcode(data, "konzum")
        self.assertEqual(result["barcode"], "12345678")

    def test_already_namespaced(self):
        """Barcode containing ':' is kept as-is (already chain-namespaced)."""
        data = {"barcode": "konzum:0001234", "product_id": "0001234"}
        result = _clean_barcode(data, "konzum")
        self.assertEqual(result["barcode"], "konzum:0001234")

    def test_invalid_barcode_replaced(self):
        """Short/non-numeric barcode is replaced with chain:product_id."""
        data = {"barcode": "XYZ", "product_id": "0000006"}
        result = _clean_barcode(data, "trgovina-krk")
        self.assertEqual(result["barcode"], "trgovina-krk:0000006")

    def test_empty_barcode_replaced(self):
        """Empty barcode is replaced with chain:product_id."""
        data = {"barcode": "", "product_id": "0000007"}
        result = _clean_barcode(data, "studenac")
        self.assertEqual(result["barcode"], "studenac:0000007")

    def test_no_product_id_left_as_is(self):
        """If product_id is also missing, data is returned unchanged."""
        data = {"barcode": "bad", "product_id": ""}
        result = _clean_barcode(data, "lidl")
        self.assertEqual(result["barcode"], "bad")

    def test_short_numeric_replaced(self):
        """Numeric barcode shorter than 8 digits is replaced."""
        data = {"barcode": "123", "product_id": "P001"}
        result = _clean_barcode(data, "spar")
        self.assertEqual(result["barcode"], "spar:P001")

    def test_mutates_dict_in_place(self):
        """_clean_barcode mutates and returns the same dict object."""
        data = {"barcode": "bad", "product_id": "P001"}
        result = _clean_barcode(data, "spar")
        self.assertIs(result, data)


# ── _clean_price ───────────────────────────────────────────────────────────────

class TestCleanPrice(unittest.TestCase):
    def test_valid_price(self):
        self.assertEqual(_clean_price("12.99"), Decimal("12.99"))

    def test_zero_returns_none(self):
        self.assertIsNone(_clean_price("0"))

    def test_zero_decimal_returns_none(self):
        self.assertIsNone(_clean_price("0.00"))

    def test_empty_string_returns_none(self):
        self.assertIsNone(_clean_price(""))

    def test_none_returns_none(self):
        self.assertIsNone(_clean_price(None))

    def test_whitespace_returns_none(self):
        self.assertIsNone(_clean_price("   "))

    def test_strips_whitespace(self):
        self.assertEqual(_clean_price("  5.50  "), Decimal("5.50"))


# ── transform_products ─────────────────────────────────────────────────────────

from crawler.store.output import transform_products
from crawler.store.models import Store, Product


def make_product(**kwargs) -> Product:
    defaults = dict(
        product="Test Product",
        product_id="P001",
        brand="TestBrand",
        quantity="500g",
        unit="kg",
        price=Decimal("9.99"),
        unit_price=Decimal("19.98"),
        barcode="3850151240124",
        category="food",
    )
    defaults.update(kwargs)
    return Product(**defaults)


def make_store(**kwargs) -> Store:
    defaults = dict(
        chain="konzum",
        store_id="ST01",
        name="Konzum Zagreb",
        store_type="supermarket",
        city="Zagreb",
        street_address="Ilica 1",
        zipcode="10000",
        items=[make_product()],
    )
    defaults.update(kwargs)
    return Store(**defaults)


class TestTransformProducts(unittest.TestCase):
    def test_basic_structure(self):
        stores = [make_store()]
        store_list, product_list, price_list = transform_products(stores)

        self.assertEqual(len(store_list), 1)
        self.assertEqual(len(product_list), 1)
        self.assertEqual(len(price_list), 1)

    def test_store_fields(self):
        stores = [make_store()]
        store_list, _, _ = transform_products(stores)
        s = store_list[0]

        self.assertEqual(s["store_id"], "ST01")
        self.assertEqual(s["type"], "supermarket")
        self.assertEqual(s["address"], "Ilica 1")
        self.assertEqual(s["city"], "Zagreb")
        self.assertEqual(s["zipcode"], "10000")

    def test_product_fields(self):
        stores = [make_store()]
        _, product_list, _ = transform_products(stores)
        p = product_list[0]

        self.assertEqual(p["product_id"], "P001")
        self.assertEqual(p["barcode"], "3850151240124")
        self.assertEqual(p["name"], "Test Product")
        self.assertEqual(p["brand"], "TestBrand")

    def test_price_fields(self):
        stores = [make_store()]
        _, _, price_list = transform_products(stores)
        p = price_list[0]

        self.assertEqual(p["store_id"], "ST01")
        self.assertEqual(p["product_id"], "P001")
        self.assertEqual(p["price"], Decimal("9.99"))

    def test_products_deduplicated_across_stores(self):
        """Same product_id in two stores should appear once in product_list."""
        stores = [
            make_store(store_id="ST01"),
            make_store(store_id="ST02"),
        ]
        _, product_list, price_list = transform_products(stores)

        self.assertEqual(len(product_list), 1)   # deduplicated
        self.assertEqual(len(price_list), 2)     # one price per store

    def test_missing_barcode_uses_chain_key(self):
        """Product with empty barcode should fall back to chain:product_id."""
        product = make_product(barcode="")
        stores = [make_store(items=[product])]
        _, product_list, _ = transform_products(stores)

        self.assertEqual(product_list[0]["barcode"], "konzum:P001")

    def test_empty_stores_returns_empty_lists(self):
        store_list, product_list, price_list = transform_products([])
        self.assertEqual(store_list, [])
        self.assertEqual(product_list, [])
        self.assertEqual(price_list, [])

    def test_store_with_no_products(self):
        stores = [make_store(items=[])]
        store_list, product_list, price_list = transform_products(stores)

        self.assertEqual(len(store_list), 1)
        self.assertEqual(len(product_list), 0)
        self.assertEqual(len(price_list), 0)


# ── temp table collision simulation ───────────────────────────────────────────

class TestTempTableSQL(unittest.TestCase):
    """
    Verifies that the SQL fix (IF NOT EXISTS + TRUNCATE) handles the case
    where a previous failed transaction left a temp table on the connection.

    Uses simple string checks — no DB connection required.
    """

    def _get_create_sql(self, path):
        """Extract the CREATE TEMP TABLE statement from psql.py."""
        import ast
        with open(path) as f:
            source = f.read()
        # Check the fixed patterns are present
        return source

    def test_chain_products_uses_if_not_exists(self):
        source = self._get_create_sql("service/db/psql.py")
        self.assertIn(
            "CREATE TEMP TABLE IF NOT EXISTS temp_chain_products",
            source,
            "add_many_chain_products must use IF NOT EXISTS to survive connection reuse",
        )

    def test_chain_products_truncates(self):
        source = self._get_create_sql("service/db/psql.py")
        # TRUNCATE must appear after the temp_chain_products table creation
        idx_create = source.find("CREATE TEMP TABLE IF NOT EXISTS temp_chain_products")
        idx_truncate = source.find("TRUNCATE temp_chain_products", idx_create)
        self.assertGreater(
            idx_truncate, idx_create,
            "TRUNCATE temp_chain_products must follow the CREATE TEMP TABLE",
        )

    def test_prices_uses_if_not_exists(self):
        source = self._get_create_sql("service/db/psql.py")
        self.assertIn(
            "CREATE TEMP TABLE IF NOT EXISTS temp_prices",
            source,
            "add_many_prices must use IF NOT EXISTS to survive connection reuse",
        )

    def test_prices_truncates(self):
        source = self._get_create_sql("service/db/psql.py")
        idx_create = source.find("CREATE TEMP TABLE IF NOT EXISTS temp_prices")
        idx_truncate = source.find("TRUNCATE temp_prices", idx_create)
        self.assertGreater(
            idx_truncate, idx_create,
            "TRUNCATE temp_prices must follow the CREATE TEMP TABLE",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
