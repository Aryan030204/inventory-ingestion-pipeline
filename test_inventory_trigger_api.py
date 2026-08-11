import sqlite3
import unittest
from datetime import date

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
        INSERT INTO shopify_orders (order_id, product_id, created_date, line_item_quantity)
        VALUES (?, ?, ?, ?)
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
    return {int(row["product_id"]): dict(row) for row in rows}


class SoldUnitsQueryTests(unittest.TestCase):
    def test_7d_parameter_order_matches_placeholder_order(self):
        sql, params = pipeline._build_sold_units_query(
            [11, 22], date(2026, 8, 10), False
        )
        self.assertEqual(sql.count("%s"), len(params))
        self.assertEqual(
            params,
            ("2026-08-04", "2026-08-10", 11, 22, "2026-08-04", "2026-08-10"),
        )

    def test_30d_parameter_order_matches_placeholder_order(self):
        sql, params = pipeline._build_sold_units_query(
            [11, 22], date(2026, 8, 10), True
        )
        self.assertEqual(sql.count("%s"), len(params))
        self.assertEqual(
            params,
            (
                "2026-07-12",
                "2026-08-10",
                11,
                22,
                "2026-08-04",
                "2026-08-10",
                "2026-07-12",
                "2026-08-10",
                "2026-08-04",
                "2026-08-10",
                "2026-07-12",
                "2026-08-10",
                "2026-08-04",
                "2026-08-10",
            ),
        )

    def test_no_reversal_keeps_gross_units(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 10),
            False,
            [
                ("o1", 111, "2026-08-05", 40),
                ("o2", 111, "2026-08-07", 60),
            ],
            [],
        )
        self.assertEqual(rows[111]["sold_units_7d"], 100)
        self.assertEqual(rows[111]["sold_units_30d"], 0)

    def test_one_cancelled_order_reduces_net_units(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 10),
            False,
            [
                ("o1", 111, "2026-08-05", 99),
                ("o2", 111, "2026-08-06", 1),
            ],
            [("o2", "2026-08-08", "CANCEL", 100.0)],
        )
        self.assertEqual(rows[111]["sold_units_7d"], 99)

    def test_one_refunded_order_reduces_net_units(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 10),
            False,
            [
                ("o1", 111, "2026-08-05", 99),
                ("o2", 111, "2026-08-06", 1),
            ],
            [("o2", "2026-08-08", "REFUND", 100.0)],
        )
        self.assertEqual(rows[111]["sold_units_7d"], 99)

    def test_same_order_cancel_and_refund_subtracts_once(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 10),
            False,
            [
                ("o1", 111, "2026-08-06", 1),
                ("o2", 111, "2026-08-07", 99),
            ],
            [
                ("o1", "2026-08-08", "CANCEL", 100.0),
                ("o1", "2026-08-09", "REFUND", 100.0),
            ],
        )
        self.assertEqual(rows[111]["gross_sold_units_7d"], 100)
        self.assertEqual(rows[111]["reversed_units_7d"], 1)
        self.assertEqual(rows[111]["sold_units_7d"], 99)

    def test_reversal_outside_window_is_ignored(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 10),
            False,
            [
                ("o1", 111, "2026-08-05", 1),
                ("o2", 111, "2026-08-07", 99),
            ],
            [("o1", "2026-08-11", "CANCEL", 100.0)],
        )
        self.assertEqual(rows[111]["sold_units_7d"], 100)

    def test_multi_product_order_uses_accepted_approximation(self):
        rows = _execute_sold_units_query(
            [111, 222],
            date(2026, 8, 10),
            False,
            [
                ("o1", 111, "2026-08-05", 2),
                ("o1", 222, "2026-08-05", 3),
            ],
            [("o1", "2026-08-06", "CANCEL", 100.0)],
        )
        self.assertEqual(rows[111]["sold_units_7d"], 0)
        self.assertEqual(rows[222]["sold_units_7d"], 0)
        self.assertEqual(rows[111]["reversed_units_7d"], 2)
        self.assertEqual(rows[222]["reversed_units_7d"], 3)

    def test_30d_calc_false_keeps_30d_zero(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 10),
            False,
            [
                ("o1", 111, "2026-07-15", 10),
                ("o2", 111, "2026-08-05", 90),
            ],
            [],
        )
        self.assertEqual(rows[111]["sold_units_7d"], 90)
        self.assertEqual(rows[111]["sold_units_30d"], 0)

    def test_30d_calc_true_applies_gross_minus_reversals_to_both_windows(self):
        rows = _execute_sold_units_query(
            [111],
            date(2026, 8, 10),
            True,
            [
                ("o1", 111, "2026-08-05", 100),
                ("o2", 111, "2026-08-06", 1),
                ("o3", 111, "2026-07-15", 10),
                ("o4", 111, "2026-07-20", 2),
            ],
            [
                ("o2", "2026-08-08", "CANCEL", 100.0),
                ("o4", "2026-07-21", "REFUND", 100.0),
            ],
        )
        self.assertEqual(rows[111]["gross_sold_units_7d"], 101)
        self.assertEqual(rows[111]["reversed_units_7d"], 1)
        self.assertEqual(rows[111]["sold_units_7d"], 100)
        self.assertEqual(rows[111]["gross_sold_units_30d"], 113)
        self.assertEqual(rows[111]["reversed_units_30d"], 3)
        self.assertEqual(rows[111]["sold_units_30d"], 110)

    def test_regression_fixture_matches_289_to_285(self):
        rows = _execute_sold_units_query(
            [7987757842602],
            date(2026, 8, 10),
            False,
            [
                ("o1", 7987757842602, "2026-08-05", 280),
                ("o2", 7987757842602, "2026-08-06", 5),
                ("o3", 7987757842602, "2026-08-07", 4),
            ],
            [("o3", "2026-08-08", "REFUND", 100.0)],
        )
        self.assertEqual(rows[7987757842602]["gross_sold_units_7d"], 289)
        self.assertEqual(rows[7987757842602]["reversed_units_7d"], 4)
        self.assertEqual(rows[7987757842602]["sold_units_7d"], 285)


if __name__ == "__main__":
    unittest.main()
