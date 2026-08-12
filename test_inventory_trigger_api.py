import sqlite3
import unittest
from datetime import date
from decimal import Decimal
from unittest import mock

import inventory_trigger_api as pipeline


def _execute_sold_units_query(product_ids, report_end_date, calculate_30d, orders, returns):
    sql, params = pipeline._build_sold_units_query(
        product_ids, report_end_date, calculate_30d
    )

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE shopify_orders (
            order_id TEXT,
            product_id INTEGER,
            variant_id INTEGER,
            created_date TEXT,
            line_item_quantity INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE returns_fact (
            order_id TEXT,
            event_date TEXT,
            event_type TEXT,
            amount REAL
        )
        """
    )

    cur.executemany(
        """
        INSERT INTO shopify_orders (order_id, product_id, variant_id, created_date, line_item_quantity)
        VALUES (?, ?, ?, ?, ?)
        """,
        orders,
    )
    cur.executemany(
        """
        INSERT INTO returns_fact (order_id, event_date, event_type, amount)
        VALUES (?, ?, ?, ?)
        """,
        returns,
    )

    sqlite_sql = sql.replace("%s", "?")
    cur.execute(sqlite_sql, params)
    rows = cur.fetchall()
    conn.close()
    return {
        (int(row["product_id"]), int(row["variant_id"])): dict(row)
        for row in rows
    }


class SoldUnitsQueryTests(unittest.TestCase):
    def test_7d_parameter_order_matches_placeholder_order(self):
        sql, params = pipeline._build_sold_units_query(
            [11, 22], date(2026, 8, 11), False
        )
        self.assertEqual(sql.count("%s"), len(params))
        self.assertEqual(
            params,
            ("2026-08-05", "2026-08-11", 11, 22, "2026-08-05", "2026-08-11"),
        )

    def test_30d_parameter_order_matches_placeholder_order(self):
        sql, params = pipeline._build_sold_units_query(
            [11, 22], date(2026, 8, 11), True
        )
        self.assertEqual(sql.count("%s"), len(params))
        self.assertEqual(
            params,
            (
                "2026-07-13",
                "2026-08-11",
                11,
                22,
                "2026-08-05",
                "2026-08-11",
                "2026-07-13",
                "2026-08-11",
                "2026-08-05",
                "2026-08-11",
                "2026-07-13",
                "2026-08-11",
                "2026-08-05",
                "2026-08-11",
            ),
        )

    def test_single_variant_product_receives_full_sales(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 11),
            False,
            [
                ("o1", 111, 1001, "2026-08-06", 40),
                ("o2", 111, 1001, "2026-08-08", 60),
            ],
            [],
        )
        self.assertEqual(rows[(111, 1001)]["sold_units_7d"], 100)
        self.assertEqual(rows[(111, 1001)]["sold_units_30d"], 0)

    def test_multi_variant_product_is_not_aggregated_to_product_total(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 11),
            False,
            [
                ("o1", 111, 1001, "2026-08-06", 172),
                ("o2", 111, 1002, "2026-08-07", 48),
            ],
            [],
        )
        self.assertEqual(rows[(111, 1001)]["sold_units_7d"], 172)
        self.assertEqual(rows[(111, 1002)]["sold_units_7d"], 48)
        self.assertNotEqual(rows[(111, 1001)]["sold_units_7d"], 220)
        self.assertNotEqual(rows[(111, 1002)]["sold_units_7d"], 220)

    def test_same_order_cancel_and_refund_subtracts_variant_quantity_once(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 11),
            False,
            [
                ("o1", 111, 1001, "2026-08-06", 1),
                ("o2", 111, 1001, "2026-08-07", 99),
            ],
            [
                ("o1", "2026-08-08", "CANCEL", 100.0),
                ("o1", "2026-08-09", "REFUND", 100.0),
            ],
        )
        self.assertEqual(rows[(111, 1001)]["gross_sold_units_7d"], 100)
        self.assertEqual(rows[(111, 1001)]["reversed_units_7d"], 1)
        self.assertEqual(rows[(111, 1001)]["sold_units_7d"], 99)

    def test_reversal_outside_window_is_ignored(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 11),
            False,
            [
                ("o1", 111, 1001, "2026-08-06", 1),
                ("o2", 111, 1001, "2026-08-07", 99),
            ],
            [("o1", "2026-08-12", "CANCEL", 100.0)],
        )
        self.assertEqual(rows[(111, 1001)]["sold_units_7d"], 100)

    def test_multi_variant_reversal_case_matches_production_evidence(self):
        rows = _execute_sold_units_query(
            [7987757285546],
            date(2026, 8, 11),
            False,
            [
                ("o1", 7987757285546, 48229753290922, "2026-08-06", 168),
                ("o2", 7987757285546, 48229753290922, "2026-08-07", 4),
                ("o3", 7987757285546, 48229753323690, "2026-08-07", 47),
                ("o4", 7987757285546, 48229753323690, "2026-08-08", 1),
            ],
            [
                ("o2", "2026-08-08", "REFUND", 100.0),
                ("o4", "2026-08-09", "CANCEL", 50.0),
            ],
        )
        self.assertEqual(rows[(7987757285546, 48229753290922)]["gross_sold_units_7d"], 172)
        self.assertEqual(rows[(7987757285546, 48229753290922)]["reversed_units_7d"], 4)
        self.assertEqual(rows[(7987757285546, 48229753290922)]["sold_units_7d"], 168)
        self.assertEqual(rows[(7987757285546, 48229753323690)]["gross_sold_units_7d"], 48)
        self.assertEqual(rows[(7987757285546, 48229753323690)]["reversed_units_7d"], 1)
        self.assertEqual(rows[(7987757285546, 48229753323690)]["sold_units_7d"], 47)

    def test_multi_product_multi_variant_order_uses_accepted_approximation(self):
        rows = _execute_sold_units_query(
            [111, 222],
            date(2026, 8, 11),
            False,
            [
                ("o1", 111, 1001, "2026-08-06", 2),
                ("o1", 222, 2001, "2026-08-06", 3),
            ],
            [("o1", "2026-08-07", "CANCEL", 100.0)],
        )
        self.assertEqual(rows[(111, 1001)]["sold_units_7d"], 0)
        self.assertEqual(rows[(222, 2001)]["sold_units_7d"], 0)

    def test_30d_calc_false_keeps_30d_zero(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 11),
            False,
            [
                ("o1", 111, 1001, "2026-07-15", 10),
                ("o2", 111, 1001, "2026-08-06", 90),
            ],
            [],
        )
        self.assertEqual(rows[(111, 1001)]["sold_units_7d"], 90)
        self.assertEqual(rows[(111, 1001)]["sold_units_30d"], 0)

    def test_30d_calc_true_applies_variant_level_net_logic_to_both_windows(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 11),
            True,
            [
                ("o1", 111, 1001, "2026-08-06", 100),
                ("o2", 111, 1001, "2026-08-07", 1),
                ("o3", 111, 1001, "2026-07-15", 10),
                ("o4", 111, 1001, "2026-07-20", 2),
                ("o5", 111, 1002, "2026-08-07", 50),
            ],
            [
                ("o2", "2026-08-08", "CANCEL", 100.0),
                ("o4", "2026-07-21", "REFUND", 100.0),
            ],
        )
        self.assertEqual(rows[(111, 1001)]["sold_units_7d"], 100)
        self.assertEqual(rows[(111, 1001)]["sold_units_30d"], 110)
        self.assertEqual(rows[(111, 1002)]["sold_units_7d"], 50)
        self.assertEqual(rows[(111, 1002)]["sold_units_30d"], 50)

    def test_end_to_end_regression_fixture_neither_variant_receives_product_total(self):
        rows = _execute_sold_units_query(
            [7987757285546],
            date(2026, 8, 11),
            False,
            [
                ("o1", 7987757285546, 48229753290922, "2026-08-06", 168),
                ("o2", 7987757285546, 48229753290922, "2026-08-07", 4),
                ("o3", 7987757285546, 48229753323690, "2026-08-07", 47),
                ("o4", 7987757285546, 48229753323690, "2026-08-08", 1),
            ],
            [
                ("o2", "2026-08-08", "REFUND", 100.0),
                ("o4", "2026-08-09", "CANCEL", 50.0),
            ],
        )
        variant_a = rows[(7987757285546, 48229753290922)]["sold_units_7d"]
        variant_b = rows[(7987757285546, 48229753323690)]["sold_units_7d"]
        self.assertEqual(variant_a, 168)
        self.assertEqual(variant_b, 47)
        self.assertEqual(variant_a + variant_b, 215)
        self.assertNotEqual(variant_a, 215)
        self.assertNotEqual(variant_b, 215)

    def test_calculate_domain_items_uses_variant_mapping_not_product_total(self):
        brand = pipeline.BrandContext(
            brand_id=1,
            brand_tag="x",
            brand_name="Test",
            database="test",
            shop_domain="test.myshopify.com",
        )
        source_items = [
            pipeline.SourceInventoryItem("test.myshopify.com", 111, "P", 1001, "A", "SKU-A", 20),
            pipeline.SourceInventoryItem("test.myshopify.com", 111, "P", 1002, "B", "SKU-B", 8),
        ]
        fake_sales = {
            (111, 1001): {"sold_units_7d": 168, "sold_units_30d": 0},
            (111, 1002): {"sold_units_7d": 47, "sold_units_30d": 0},
        }

        class DummyCursor:
            pass

        class DummyCtx:
            def __enter__(self):
                return (brand, DummyCursor(), None)

            def __exit__(self, exc_type, exc, tb):
                return False

        with mock.patch.object(pipeline, "get_db_cursor", return_value=DummyCtx()):
            with mock.patch.object(pipeline, "bulk_fetch_sold_units", return_value=fake_sales):
                items = pipeline.calculate_domain_items(brand, source_items)

        self.assertEqual(items[0].sold_units_7d, 168)
        self.assertEqual(items[1].sold_units_7d, 47)
        self.assertEqual(items[0].drr_7d, Decimal("24.00"))
        self.assertEqual(items[1].drr_7d, Decimal("6.71"))

    def test_invalid_variant_never_uses_product_level_fallback(self):
        brand = pipeline.BrandContext(
            brand_id=1,
            brand_tag="x",
            brand_name="Test",
            database="test",
            shop_domain="test.myshopify.com",
        )
        source_items = [
            pipeline.SourceInventoryItem("test.myshopify.com", 111, "P", 0, "Broken", "SKU-X", 20),
        ]

        class DummyCursor:
            pass

        class DummyCtx:
            def __enter__(self):
                return (brand, DummyCursor(), None)

            def __exit__(self, exc_type, exc, tb):
                return False

        with mock.patch.object(pipeline, "get_db_cursor", return_value=DummyCtx()):
            with mock.patch.object(
                pipeline,
                "bulk_fetch_sold_units",
                return_value={(111, 1001): {"sold_units_7d": 215, "sold_units_30d": 0}},
            ):
                items = pipeline.calculate_domain_items(brand, source_items)

        self.assertEqual(items[0].sold_units_7d, 0)
        self.assertEqual(items[0].drr_7d, Decimal("0.00"))


if __name__ == "__main__":
    unittest.main()
