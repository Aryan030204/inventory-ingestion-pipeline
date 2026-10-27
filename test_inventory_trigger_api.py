import sqlite3
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest import mock

import inventory_trigger_api as pipeline


def _make_brand(**overrides):
    values = {
        "brand_id": 1,
        "brand_tag": "x",
        "brand_name": "Test",
        "database": "test",
        "shop_domain": "test.myshopify.com",
        "shop_name": "test",
        "access_token": "token",
        "api_version": "2025-10",
        "store_timezone": "Asia/Kolkata",
    }
    values.update(overrides)
    return pipeline.BrandContext(**values)


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
        brand = _make_brand()
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
        brand = _make_brand()
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


class TopProductSalesTests(unittest.TestCase):
    def test_brand_sales_window_uses_store_timezone_and_excludes_today(self):
        brand = _make_brand(store_timezone="Asia/Kolkata")
        window_start, window_end = pipeline.get_brand_sales_window(
            brand,
            now_utc=datetime(2026, 8, 16, 20, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(window_start, "2026-08-10")
        self.assertEqual(window_end, "2026-08-17")

    def test_shopifyql_query_uses_exclusive_until_end_date(self):
        query = pipeline.build_top_product_sales_shopifyql(
            "2026-08-10",
            "2026-08-17",
        )
        self.assertIn("SINCE 2026-08-10 UNTIL 2026-08-17", query)
        self.assertNotIn("SINCE 2026-08-10 UNTIL 2026-08-10", query)
        self.assertIn("product_variant_sku", query)

    def test_shopifyql_query_supports_offset_pagination(self):
        query = pipeline.build_top_product_sales_shopifyql(
            "2026-08-10",
            "2026-08-17",
            limit=1000,
            offset=1000,
        )
        self.assertIn("LIMIT 1000 OFFSET 1000", query)

    def test_next_top_product_sales_run_is_0023_ist(self):
        # 06:30 UTC = 12:00 IST on the 17th -> the next run is 00:23 IST on the 18th (was 00:00).
        next_run = pipeline.get_next_top_product_sales_run_at(
            datetime(2026, 8, 17, 6, 30, tzinfo=timezone.utc)
        )
        self.assertEqual(
            next_run.astimezone(pipeline.TOP_PRODUCT_SALES_SCHEDULE_TIMEZONE).isoformat(),
            "2026-08-18T00:23:00+05:30",
        )

    def test_next_top_product_sales_run_just_after_midnight_is_the_same_night(self):
        # 18:40 UTC = 00:10 IST on the 18th, before 00:23 -> still due tonight, not a day later.
        next_run = pipeline.get_next_top_product_sales_run_at(
            datetime(2026, 8, 17, 18, 40, tzinfo=timezone.utc)
        )
        self.assertEqual(
            next_run.astimezone(pipeline.TOP_PRODUCT_SALES_SCHEDULE_TIMEZONE).isoformat(),
            "2026-08-18T00:23:00+05:30",
        )

    def test_next_top_product_sales_run_after_the_run_is_tomorrow(self):
        # 18:55 UTC = 00:25 IST on the 18th, after 00:23 -> the 19th.
        next_run = pipeline.get_next_top_product_sales_run_at(
            datetime(2026, 8, 17, 18, 55, tzinfo=timezone.utc)
        )
        self.assertEqual(
            next_run.astimezone(pipeline.TOP_PRODUCT_SALES_SCHEDULE_TIMEZONE).isoformat(),
            "2026-08-19T00:23:00+05:30",
        )

    def test_startup_cycles_wait_for_their_slot_before_running(self):
        import threading
        from unittest import mock

        stop = threading.Event()
        with mock.patch.object(pipeline, "INVENTORY_STARTUP_DELAY_S", 150), \
             mock.patch.object(stop, "wait", return_value=True) as wait:
            self.assertTrue(pipeline._wait_for_startup_slot(stop, pipeline.CACHE_STARTUP_OFFSET_S))
        wait.assert_called_once_with(210)
        offsets = {pipeline.INGESTION_STARTUP_OFFSET_S, pipeline.CACHE_STARTUP_OFFSET_S,
                   pipeline.TOP_PRODUCT_SALES_STARTUP_OFFSET_S}
        self.assertEqual(len(offsets), 3, "the three startup cycles must not share a slot")

    def test_workers_do_not_run_a_cycle_when_stopped_during_the_startup_wait(self):
        import threading
        from unittest import mock

        stop = threading.Event()
        stop.set()
        with mock.patch.object(pipeline, "get_redis_client", return_value=mock.Mock()), \
             mock.patch.object(pipeline, "run_ingestion_cycle_exclusive") as ingestion, \
             mock.patch.object(pipeline, "run_cache_cycle") as cache:
            pipeline.ingestion_worker(stop)
            pipeline.cache_worker(stop)
        ingestion.assert_not_called()
        cache.assert_not_called()

    def test_format_shopifyql_table_parses_object_rows(self):
        rows = pipeline._format_shopifyql_table(
            {
                "columns": [
                    {"name": "product_id", "dataType": "STRING"},
                    {"name": "gross_sales", "dataType": "DECIMAL"},
                ],
                "rows": [
                    {"product_id": "gid://shopify/Product/100", "gross_sales": "1000.50"},
                    {"product_id": "gid://shopify/Product/101", "gross_sales": "500"},
                ],
            }
        )
        self.assertEqual(rows[0]["product_id"], "gid://shopify/Product/100")
        self.assertEqual(rows[0]["gross_sales"], 1000.5)
        self.assertEqual(rows[1]["gross_sales"], 500.0)

    def test_duplicate_variant_rows_are_aggregated_to_product_level(self):
        records = pipeline.normalize_top_product_sales_rows(
            [
                {
                    "product_id": "gid://shopify/Product/100",
                    "gross_sales": "1000",
                    "product_variant_id": "gid://shopify/ProductVariant/1",
                },
                {
                    "product_id": "gid://shopify/Product/100",
                    "gross_sales": "250.25",
                    "product_variant_id": "gid://shopify/ProductVariant/2",
                },
                {
                    "product_id": "gid://shopify/Product/101",
                    "gross_sales": "500",
                    "product_variant_id": "gid://shopify/ProductVariant/3",
                },
            ]
        )
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].product_id, 100)
        self.assertEqual(records[0].product_title, "")
        self.assertEqual(records[0].sales, Decimal("1250.25"))
        self.assertEqual(records[0].rank, 1)

    def test_fetch_all_top_product_sales_rows_uses_pagination(self):
        brand = _make_brand()
        first_page = [{"product_id": str(index), "gross_sales": "1"} for index in range(1000)]
        second_page = [{"product_id": "1001", "gross_sales": "1"}]
        with mock.patch.object(
            pipeline.TOP_PRODUCT_SALES_SERVICE,
            "fetch_shopifyql_table_data",
            side_effect=[first_page, second_page],
        ) as fetch_mock:
            rows = pipeline.fetch_all_top_product_sales_rows(
                brand,
                "2026-08-10",
                "2026-08-17",
            )
        self.assertEqual(len(rows), 1001)
        self.assertEqual(fetch_mock.call_count, 2)
        first_query = fetch_mock.call_args_list[0].args[1]
        second_query = fetch_mock.call_args_list[1].args[1]
        self.assertIn("LIMIT 1000", first_query)
        self.assertNotIn("OFFSET 1000", first_query)
        self.assertIn("LIMIT 1000 OFFSET 1000", second_query)

    def test_fetch_top_product_sales_for_brand_aggregates_all_pages_to_true_product_top_20(self):
        brand = _make_brand()
        first_page = []
        for variant_id in range(1000):
            first_page.append(
                {
                    "product_id": "gid://shopify/Product/100",
                    "gross_sales": "1",
                    "product_variant_id": str(variant_id),
                    "product_variant_sku": f"SKU-{variant_id}",
                }
            )
        second_page = [
            {
                "product_id": "gid://shopify/Product/200",
                "gross_sales": "900",
                "product_variant_id": "2001",
                "product_variant_sku": "SKU-2001",
            },
            {
                "product_id": "gid://shopify/Product/300",
                "gross_sales": "800",
                "product_variant_id": "3001",
                "product_variant_sku": "SKU-3001",
            },
        ]
        with mock.patch.object(
            pipeline.TOP_PRODUCT_SALES_SERVICE,
            "fetch_shopifyql_table_data",
            side_effect=[first_page, second_page],
        ):
            window_start, window_end, records = pipeline.fetch_top_product_sales_for_brand(
                brand,
                now_utc=datetime(2026, 8, 16, 20, 0, tzinfo=timezone.utc),
            )
        self.assertEqual(window_start, "2026-08-10")
        self.assertEqual(window_end, "2026-08-17")
        self.assertEqual(records[0].product_id, 100)
        self.assertEqual(records[0].product_title, "")
        self.assertEqual(records[0].sales, Decimal("1000.00"))
        self.assertEqual(records[1].product_id, 200)
        self.assertEqual(records[1].sales, Decimal("900.00"))

    def test_top_20_limit_and_deterministic_ranking(self):
        rows = [{"product_id": str(product_id), "gross_sales": "100"} for product_id in range(3, 5)]
        for product_id in range(1, 25):
            if product_id in {3, 4}:
                continue
            rows.append(
                {
                    "product_id": str(product_id),
                    "gross_sales": str(99 - product_id),
                }
            )
        records = pipeline.normalize_top_product_sales_rows(rows)
        self.assertEqual(len(records), 20)
        equal_sales_records = [
            record for record in records if record.sales == Decimal("100.00")
        ]
        self.assertEqual([record.product_id for record in equal_sales_records], [3, 4])
        self.assertEqual(records[0].rank, 1)
        self.assertEqual(records[-1].rank, 20)

    def test_invalid_rows_are_ignored(self):
        records = pipeline.normalize_top_product_sales_rows(
            [
                {"product_id": "", "gross_sales": "100"},
                {"product_id": None, "gross_sales": "200"},
                {"product_id": "gid://shopify/Product/123", "gross_sales": "50"},
            ]
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].product_id, 123)

    def test_build_payload_contains_expected_fields(self):
        payload = pipeline.build_top_product_sales_payload(
            "2026-08-10",
            "2026-08-17",
            [
                pipeline.TopProductSalesRecord(
                    product_id=123,
                    product_title="Build Your Own Small Box",
                    sales=Decimal("125000.50"),
                    rank=1,
                    source_rows=(
                        {
                            "product_id": "123",
                            "product_title": "Build Your Own Small Box",
                            "product_variant_id": "456",
                            "product_variant_title": "Default Title",
                            "product_variant_sku": "PCB30ML",
                            "gross_sales": 125000.5,
                        },
                    ),
                )
            ],
            updated_at="2026-08-17T00:00:00Z",
        )
        self.assertEqual(payload["window_start_date"], "2026-08-10")
        self.assertEqual(payload["window_end_date_exclusive"], "2026-08-17")
        self.assertEqual(payload["updated_at"], "2026-08-17T00:00:00Z")
        self.assertEqual(
            payload["products"],
            [
                {
                    "product_id": "123",
                    "product_title": "Build Your Own Small Box",
                    "sales": 125000.5,
                    "rank": 1,
                    "source_rows": [
                        {
                            "product_id": "123",
                            "product_title": "Build Your Own Small Box",
                            "product_variant_id": "456",
                            "product_variant_title": "Default Title",
                            "product_variant_sku": "PCB30ML",
                            "gross_sales": 125000.5,
                        }
                    ],
                }
            ],
        )

    def test_cache_top_product_sales_writes_ttl(self):
        redis_client = mock.Mock()
        payload = {
            "window_start_date": "2026-08-10",
            "window_end_date_exclusive": "2026-08-17",
            "updated_at": "2026-08-17T00:00:00Z",
            "products": [],
        }
        key = pipeline.cache_top_product_sales(
            redis_client,
            "test.myshopify.com",
            payload,
        )
        self.assertEqual(key, "inventory_top_product_sales:test.myshopify.com")
        redis_client.set.assert_called_once()
        args, kwargs = redis_client.set.call_args
        self.assertEqual(args[0], key)
        self.assertEqual(kwargs["ex"], 172800)

    def test_run_top_product_sales_cycle_skips_when_same_date_cached(self):
        redis_client = mock.Mock()
        brand = _make_brand()
        with mock.patch.object(
            pipeline.TOP_PRODUCT_SALES_SERVICE,
            "get_active_brands",
            return_value=[brand],
        ):
            with mock.patch.object(
                pipeline.TOP_PRODUCT_SALES_SERVICE,
                "get_cached_top_product_sales_date",
                return_value="2026-08-17",
            ):
                with mock.patch.object(
                    pipeline.TOP_PRODUCT_SALES_SERVICE,
                    "fetch_top_product_sales_for_brand",
                ) as fetch_mock:
                    counts = pipeline.run_top_product_sales_cycle(
                        redis_client,
                        now_utc=datetime(2026, 8, 16, 20, 0, tzinfo=timezone.utc),
                    )
        self.assertEqual(counts["cached"], 0)
        self.assertEqual(counts["skipped"], 1)
        fetch_mock.assert_not_called()

    def test_run_top_product_sales_cycle_refreshes_next_day(self):
        redis_client = mock.Mock()
        brand = _make_brand()
        with mock.patch.object(
            pipeline.TOP_PRODUCT_SALES_SERVICE,
            "get_active_brands",
            return_value=[brand],
        ):
            with mock.patch.object(
                pipeline.TOP_PRODUCT_SALES_SERVICE,
                "get_cached_top_product_sales_date",
                return_value="2026-08-16",
            ):
                with mock.patch.object(
                    pipeline.TOP_PRODUCT_SALES_SERVICE,
                    "fetch_top_product_sales_for_brand",
                    return_value=(
                        "2026-08-10",
                        "2026-08-17",
                        [
                            pipeline.TopProductSalesRecord(
                                product_id=123,
                                product_title="Test Product",
                                sales=Decimal("99.00"),
                                rank=1,
                                source_rows=(),
                            )
                        ],
                    ),
                ) as fetch_mock:
                    with mock.patch.object(
                        pipeline.TOP_PRODUCT_SALES_SERVICE,
                        "cache_top_product_sales",
                        return_value="inventory_top_product_sales:test.myshopify.com",
                    ) as cache_mock:
                        counts = pipeline.run_top_product_sales_cycle(
                            redis_client,
                            now_utc=datetime(2026, 8, 16, 20, 0, tzinfo=timezone.utc),
                        )
        self.assertEqual(counts["cached"], 1)
        self.assertEqual(counts["failed"], 0)
        fetch_mock.assert_called_once()
        cache_mock.assert_called_once()

    def test_shopify_failure_is_counted_without_stopping_other_brands(self):
        redis_client = mock.Mock()
        brand_a = _make_brand(brand_id=1, brand_name="A", shop_domain="a.myshopify.com", shop_name="a")
        brand_b = _make_brand(brand_id=2, brand_name="B", shop_domain="b.myshopify.com", shop_name="b")

        def fetch_side_effect(brand, now_utc=None):
            if brand.brand_id == 1:
                raise RuntimeError("shopify failed")
            return (
                "2026-08-10",
                "2026-08-17",
                [
                    pipeline.TopProductSalesRecord(
                        product_id=555,
                        product_title="B Product",
                        sales=Decimal("10.00"),
                        rank=1,
                        source_rows=(),
                    )
                ],
            )

        with mock.patch.object(
            pipeline.TOP_PRODUCT_SALES_SERVICE,
            "get_active_brands",
            return_value=[brand_a, brand_b],
        ):
            with mock.patch.object(
                pipeline.TOP_PRODUCT_SALES_SERVICE,
                "get_cached_top_product_sales_date",
                return_value=None,
            ):
                with mock.patch.object(
                    pipeline.TOP_PRODUCT_SALES_SERVICE,
                    "fetch_top_product_sales_for_brand",
                    side_effect=fetch_side_effect,
                ):
                    with mock.patch.object(
                        pipeline.TOP_PRODUCT_SALES_SERVICE,
                        "cache_top_product_sales",
                        return_value="inventory_top_product_sales:b.myshopify.com",
                    ) as cache_mock:
                        counts = pipeline.run_top_product_sales_cycle(
                            redis_client,
                            now_utc=datetime(2026, 8, 16, 20, 0, tzinfo=timezone.utc),
                        )
        self.assertEqual(counts["failed"], 1)
        self.assertEqual(counts["cached"], 1)
        self.assertEqual(cache_mock.call_count, 1)

    def test_redis_failure_is_counted(self):
        redis_client = mock.Mock()
        brand = _make_brand()
        with mock.patch.object(
            pipeline.TOP_PRODUCT_SALES_SERVICE,
            "get_active_brands",
            return_value=[brand],
        ):
            with mock.patch.object(
                pipeline.TOP_PRODUCT_SALES_SERVICE,
                "get_cached_top_product_sales_date",
                return_value=None,
            ):
                with mock.patch.object(
                    pipeline.TOP_PRODUCT_SALES_SERVICE,
                    "fetch_top_product_sales_for_brand",
                    return_value=(
                        "2026-08-10",
                        "2026-08-17",
                        [
                            pipeline.TopProductSalesRecord(
                                product_id=1,
                                product_title="Test Product",
                                sales=Decimal("5.00"),
                                rank=1,
                                source_rows=(),
                            )
                        ],
                    ),
                ):
                    with mock.patch.object(
                        pipeline.TOP_PRODUCT_SALES_SERVICE,
                        "cache_top_product_sales",
                        side_effect=RuntimeError("redis failed"),
                    ):
                        counts = pipeline.run_top_product_sales_cycle(
                            redis_client,
                            now_utc=datetime(2026, 8, 16, 20, 0, tzinfo=timezone.utc),
                        )
        self.assertEqual(counts["failed"], 1)


class CacheWorkerTests(unittest.TestCase):
    def test_build_inventory_cache_key_uses_shop_slug(self):
        self.assertEqual(
            pipeline.build_inventory_cache_key("ajmal-perfumes-india"),
            "Inventory_cache:ajmal-perfumes-india",
        )
        self.assertEqual(
            pipeline.build_inventory_cache_key("ajmal-perfumes-india.myshopify.com"),
            "Inventory_cache:ajmal-perfumes-india",
        )

    def test_build_inventory_cache_payload_contains_metadata_and_all_columns(self):
        brand = _make_brand(shop_name="ajmal-perfumes-india")
        payload = pipeline.build_inventory_cache_payload(
            brand,
            [
                {
                    "product_id": 1,
                    "product_title": "Product",
                    "variant_id": 2,
                    "variant_title": "Default Title",
                    "sku": "SKU-1",
                    "inventory_available": 10,
                    "sold_units_7d": 7,
                    "sold_units_30d": 0,
                    "sold_units_90d": 0,
                    "drr_7d": 1.0,
                    "drr_30d": 0.0,
                    "drr_90d": 0.0,
                    "doh_7d": 10.0,
                    "doh_30d": 0.0,
                    "doh_90d": 0.0,
                    "updated_at": "2026-08-18 08:11:36",
                }
            ],
            cached_at="2026-08-18T08:30:00Z",
        )
        self.assertEqual(payload["shop"], "ajmal-perfumes-india")
        self.assertEqual(payload["cached_at"], "2026-08-18T08:30:00Z")
        self.assertEqual(payload["row_count"], 1)
        self.assertEqual(
            payload["items"][0],
            {
                "product_id": 1,
                "product_title": "Product",
                "variant_id": 2,
                "variant_title": "Default Title",
                "sku": "SKU-1",
                "inventory_available": 10,
                "sold_units_7d": 7,
                "sold_units_30d": 0,
                "sold_units_90d": 0,
                "drr_7d": 1.0,
                "drr_30d": 0.0,
                "drr_90d": 0.0,
                "doh_7d": 10.0,
                "doh_30d": 0.0,
                "doh_90d": 0.0,
                "updated_at": "2026-08-18 08:11:36",
            },
        )

    def test_cache_top_products_inventory_snapshot_writes_without_ttl(self):
        redis_client = mock.Mock()
        brand = _make_brand(shop_name="ajmal-perfumes-india")
        payload = {"shop": "ajmal-perfumes-india", "cached_at": "x", "row_count": 0, "items": []}
        key = pipeline.cache_top_products_inventory_snapshot(redis_client, brand, payload)
        self.assertEqual(key, "Inventory_cache:ajmal-perfumes-india")
        redis_client.set.assert_called_once()
        args, kwargs = redis_client.set.call_args
        self.assertEqual(args[0], key)
        self.assertEqual(kwargs, {})

    def test_fetch_top_products_inventory_rows_serializes_db_values(self):
        brand = _make_brand()
        rows = [
            {
                "product_id": 1,
                "product_title": "Product",
                "variant_id": 2,
                "variant_title": "Default Title",
                "sku": "SKU-1",
                "inventory_available": 10,
                "sold_units_7d": 7,
                "sold_units_30d": 0,
                "sold_units_90d": 0,
                "drr_7d": Decimal("1.23"),
                "drr_30d": Decimal("0.00"),
                "drr_90d": Decimal("0.00"),
                "doh_7d": Decimal("8.13"),
                "doh_30d": Decimal("0.00"),
                "doh_90d": Decimal("0.00"),
                "updated_at": datetime(2026, 8, 18, 8, 11, 36),
            }
        ]

        class DummyCursor:
            def __init__(self):
                self.executed_sql = None

            def execute(self, sql):
                self.executed_sql = sql

            def fetchall(self):
                return rows

        dummy_cursor = DummyCursor()

        class DummyCtx:
            def __enter__(self):
                return (brand, dummy_cursor, None)

            def __exit__(self, exc_type, exc, tb):
                return False

        with mock.patch.object(pipeline, "get_db_cursor", return_value=DummyCtx()):
            result = pipeline.fetch_top_products_inventory_rows(brand)

        self.assertIn("ORDER BY", dummy_cursor.executed_sql)
        self.assertIn("updated_at DESC", dummy_cursor.executed_sql)
        self.assertEqual(
            result,
            [
                {
                    "product_id": 1,
                    "product_title": "Product",
                    "variant_id": 2,
                    "variant_title": "Default Title",
                    "sku": "SKU-1",
                    "inventory_available": 10,
                    "sold_units_7d": 7,
                    "sold_units_30d": 0,
                    "sold_units_90d": 0,
                    "drr_7d": 1.23,
                    "drr_30d": 0.0,
                    "drr_90d": 0.0,
                    "doh_7d": 8.13,
                    "doh_30d": 0.0,
                    "doh_90d": 0.0,
                    "updated_at": "2026-08-18 08:11:36",
                }
            ],
        )

    def test_run_cache_cycle_caches_empty_snapshot_for_active_brand(self):
        redis_client = mock.Mock()
        brand = _make_brand(shop_name="ajmal-perfumes-india")
        with mock.patch.object(pipeline, "get_active_brands", return_value=[brand]):
            with mock.patch.object(
                pipeline,
                "fetch_top_products_inventory_rows",
                return_value=[],
            ):
                with mock.patch.object(
                    pipeline,
                    "cache_top_products_inventory_snapshot",
                    return_value="Inventory_cache:ajmal-perfumes-india",
                ) as cache_mock:
                    counts = pipeline.run_cache_cycle(redis_client)
        self.assertEqual(counts, {"brands": 1, "cached": 1, "failed": 0})
        payload = cache_mock.call_args.args[2]
        self.assertEqual(payload["row_count"], 0)
        self.assertEqual(payload["items"], [])

    def test_run_cache_cycle_continues_when_one_brand_fails(self):
        redis_client = mock.Mock()
        brand_a = _make_brand(brand_id=1, brand_name="A", shop_name="a")
        brand_b = _make_brand(brand_id=2, brand_name="B", shop_name="b")

        def fetch_side_effect(brand):
            if brand.brand_id == 1:
                raise RuntimeError("db failed")
            return []

        with mock.patch.object(pipeline, "get_active_brands", return_value=[brand_a, brand_b]):
            with mock.patch.object(
                pipeline,
                "fetch_top_products_inventory_rows",
                side_effect=fetch_side_effect,
            ):
                with mock.patch.object(
                    pipeline,
                    "cache_top_products_inventory_snapshot",
                    return_value="Inventory_cache:b",
                ) as cache_mock:
                    counts = pipeline.run_cache_cycle(redis_client)
        self.assertEqual(counts["brands"], 2)
        self.assertEqual(counts["cached"], 1)
        self.assertEqual(counts["failed"], 1)
        self.assertEqual(cache_mock.call_count, 1)

    def test_run_cache_cycle_continues_when_one_redis_write_fails(self):
        redis_client = mock.Mock()
        brand_a = _make_brand(brand_id=1, brand_name="A", shop_name="a")
        brand_b = _make_brand(brand_id=2, brand_name="B", shop_name="b")

        with mock.patch.object(pipeline, "get_active_brands", return_value=[brand_a, brand_b]):
            with mock.patch.object(
                pipeline,
                "fetch_top_products_inventory_rows",
                return_value=[],
            ):
                def cache_side_effect(_redis, brand, _payload):
                    if brand.brand_id == 1:
                        raise RuntimeError("redis failed")
                    return "Inventory_cache:b"

                with mock.patch.object(
                    pipeline,
                    "cache_top_products_inventory_snapshot",
                    side_effect=cache_side_effect,
                ):
                    counts = pipeline.run_cache_cycle(redis_client)
        self.assertEqual(counts["brands"], 2)
        self.assertEqual(counts["cached"], 1)
        self.assertEqual(counts["failed"], 1)

    def test_cache_worker_runs_once_on_startup(self):
        stop_event = mock.Mock()
        stop_event.is_set.side_effect = [False, True]
        # first wait = the startup slot (not stopped), second = the interval wait (stopped)
        stop_event.wait.side_effect = [False, True]
        redis_client = mock.Mock()
        with mock.patch.object(pipeline, "get_redis_client", return_value=redis_client):
            with mock.patch.object(pipeline, "run_cache_cycle") as run_mock:
                pipeline.cache_worker(stop_event)
        run_mock.assert_called_once_with(redis_client)


if __name__ == "__main__":
    unittest.main()
