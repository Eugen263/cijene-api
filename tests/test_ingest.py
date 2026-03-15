"""
Tests for service/db/ingest.py pure functions and crawler/store/output.py transform_products.
Run with: python -m pytest tests/ or python -m unittest tests/test_ingest.py
"""
import datetime
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


# ── _write_github_summary ──────────────────────────────────────────────────────

import io
import tempfile
from crawler.crawl import CrawlResult
from crawler.cli.crawl import _write_github_summary


class TestWriteGithubSummary(unittest.TestCase):
    def _run(self, results: dict, date_str: str = "2026-03-15") -> str:
        """Write summary to a temp file and return its contents."""
        price_date = datetime.date.fromisoformat(date_str)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
            tmp_path = f.name
        import os
        os.environ["GITHUB_STEP_SUMMARY"] = tmp_path
        try:
            _write_github_summary(results, price_date)
            with open(tmp_path) as f:
                return f.read()
        finally:
            del os.environ["GITHUB_STEP_SUMMARY"]
            os.unlink(tmp_path)

    def test_no_env_var_does_nothing(self):
        """No output when GITHUB_STEP_SUMMARY is not set."""
        import os
        os.environ.pop("GITHUB_STEP_SUMMARY", None)
        # Should not raise and return immediately
        _write_github_summary({}, datetime.date(2026, 3, 15))

    def test_header_contains_date(self):
        results = {"konzum": CrawlResult(n_stores=5, n_products=100, n_prices=500, elapsed_time=2.0)}
        output = self._run(results)
        self.assertIn("2026-03-15", output)
        self.assertIn("## Crawl Summary", output)

    def test_successful_chain_shows_checkmark(self):
        results = {"konzum": CrawlResult(n_stores=3, n_products=50, n_prices=150, elapsed_time=1.5)}
        output = self._run(results)
        self.assertIn("✅", output)
        self.assertIn("konzum", output)

    def test_failed_chain_shows_cross(self):
        results = {"dm": CrawlResult(n_stores=0, n_products=0, n_prices=0, elapsed_time=0.0)}
        output = self._run(results)
        self.assertIn("❌", output)
        self.assertIn("Failed chains", output)
        self.assertIn("dm", output)

    def test_totals_line(self):
        results = {
            "konzum": CrawlResult(n_stores=2, n_products=10, n_prices=20, elapsed_time=1.0),
            "lidl": CrawlResult(n_stores=3, n_products=15, n_prices=45, elapsed_time=1.5),
        }
        output = self._run(results)
        self.assertIn("5 stores", output)
        self.assertIn("25 products", output)
        self.assertIn("65 prices", output)

    def test_mixed_results(self):
        results = {
            "konzum": CrawlResult(n_stores=5, n_products=100, n_prices=500, elapsed_time=2.0),
            "dm": CrawlResult(n_stores=0, n_products=0, n_prices=0, elapsed_time=0.1),
        }
        output = self._run(results)
        self.assertIn("✅", output)
        self.assertIn("❌", output)


# ── Store model has name field ─────────────────────────────────────────────────

from service.db.models import Store as DbStore


class TestStoreModel(unittest.TestCase):
    def test_store_accepts_name(self):
        store = DbStore(chain_id=1, code="ST01", name="Konzum Ilica")
        self.assertEqual(store.name, "Konzum Ilica")

    def test_store_name_defaults_to_none(self):
        store = DbStore(chain_id=1, code="ST01")
        self.assertIsNone(store.name)

    def test_store_with_all_fields(self):
        store = DbStore(
            chain_id=1, code="ST01", name="Test Store",
            type="supermarket", address="Ilica 1", city="Zagreb",
            zipcode="10000", lat=45.81, lon=15.98,
        )
        self.assertEqual(store.name, "Test Store")
        self.assertEqual(store.city, "Zagreb")


# ── enrich SQL pattern check ───────────────────────────────────────────────────

class TestEnrichSQL(unittest.TestCase):
    def _get_psql_source(self):
        with open("service/db/psql.py") as f:
            return f.read()

    def test_enrich_method_exists(self):
        source = self._get_psql_source()
        self.assertIn("enrich_products_from_chain_data", source)

    def test_enrich_uses_coalesce(self):
        source = self._get_psql_source()
        idx = source.find("enrich_products_from_chain_data")
        snippet = source[idx:idx + 500]
        self.assertIn("COALESCE", snippet)

    def test_get_chain_products_by_codes_exists(self):
        source = self._get_psql_source()
        self.assertIn("get_chain_products_by_codes", source)

    def test_get_chain_products_by_codes_uses_any(self):
        source = self._get_psql_source()
        idx = source.find("get_chain_products_by_codes")
        snippet = source[idx:idx + 500]
        self.assertIn("ANY", snippet)


if __name__ == "__main__":
    unittest.main(verbosity=2)
