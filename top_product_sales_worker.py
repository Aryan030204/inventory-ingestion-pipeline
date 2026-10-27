import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import redis
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

TOP_PRODUCT_SALES_KEY_PREFIX = "inventory_top_product_sales"
TOP_PRODUCT_SALES_CACHE_TTL_SECONDS = 48 * 60 * 60
DEFAULT_SHOPIFY_API_VERSION = "2025-10"
DEFAULT_STORE_TIMEZONE = "Asia/Kolkata"
TOP_PRODUCT_SALES_SCHEDULE_TIMEZONE = ZoneInfo("Asia/Kolkata")
TOP_PRODUCT_SALES_PAGE_SIZE = 1000
# Daily run at 00:23 IST instead of 00:00, off the ETL tick and the other midnight jobs. The cycle only needs
# the day to have rolled over, so the offset changes nothing for the cached data.
TOP_PRODUCT_SALES_RUN_OFFSET_MINUTES = 23
# The startup cycle waits this long (INVENTORY_STARTUP_DELAY_S + 120 s) so a restart does not start every
# worker at once. The cycle skips brands already cached for today, so a late start loses nothing.
TOP_PRODUCT_SALES_STARTUP_OFFSET_S = 120


@dataclass(frozen=True)
class TopProductSalesRecord:
    product_id: int
    product_title: str
    sales: Decimal
    rank: int
    source_rows: Tuple[Dict[str, Any], ...]


class TopProductSalesService:
    def __init__(
        self,
        *,
        logger,
        log_prefix: Callable[[Any], str],
        normalize_shop_domain: Callable[[str], str],
        extract_numeric_gid: Callable[[Any, str], int],
        quantize: Callable[[Decimal, str], Decimal],
        get_active_brands: Callable[[], List[Any]],
        get_redis_client: Callable[[], redis.Redis],
    ) -> None:
        self.logger = logger
        self.log_prefix = log_prefix
        self.normalize_shop_domain = normalize_shop_domain
        self.extract_numeric_gid = extract_numeric_gid
        self.quantize = quantize
        self.get_active_brands = get_active_brands
        self.get_redis_client = get_redis_client

    def make_shopifyql_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            pool_connections=5, pool_maxsize=10, max_retries=retry, pool_block=False
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def format_shopifyql_table(self, table_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not table_data:
            return []

        columns = table_data.get("columns") or []
        rows = table_data.get("rows") or []
        if not columns or not rows:
            return []

        formatted: List[Dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                obj: Dict[str, Any] = {}
                for col in columns:
                    name = col.get("name")
                    if not name:
                        continue
                    dtype = str(col.get("dataType") or "").upper()
                    value = row.get(name)
                    if dtype == "INTEGER":
                        try:
                            obj[name] = int(value)
                        except (TypeError, ValueError):
                            obj[name] = 0
                    elif dtype in {"NUMERIC", "DECIMAL"}:
                        try:
                            obj[name] = float(value)
                        except (TypeError, ValueError):
                            obj[name] = 0.0
                    else:
                        obj[name] = value
                formatted.append(obj)
                continue

            if isinstance(row, (list, tuple)):
                obj = {}
                for index, value in enumerate(row):
                    if index >= len(columns):
                        continue
                    col = columns[index]
                    name = col.get("name")
                    if not name:
                        continue
                    dtype = str(col.get("dataType") or "").upper()
                    if dtype == "INTEGER":
                        try:
                            obj[name] = int(value)
                        except (TypeError, ValueError):
                            obj[name] = 0
                    elif dtype in {"NUMERIC", "DECIMAL"}:
                        try:
                            obj[name] = float(value)
                        except (TypeError, ValueError):
                            obj[name] = 0.0
                    else:
                        obj[name] = value
                formatted.append(obj)

        return formatted

    def normalize_utc_datetime(self, now_utc: Optional[datetime] = None) -> datetime:
        now_dt = now_utc or datetime.now(ZoneInfo("UTC"))
        if now_dt.tzinfo is None:
            return now_dt.replace(tzinfo=ZoneInfo("UTC"))
        return now_dt.astimezone(ZoneInfo("UTC"))

    def get_brand_today_and_yesterday(
        self, brand: Any, now_utc: Optional[datetime] = None
    ) -> Tuple[str, str]:
        now_dt = self.normalize_utc_datetime(now_utc)
        store_now = now_dt.astimezone(ZoneInfo(brand.store_timezone))
        today = store_now.date()
        yesterday = today - timedelta(days=1)
        return today.isoformat(), yesterday.isoformat()

    def get_brand_sales_window(
        self, brand: Any, now_utc: Optional[datetime] = None
    ) -> Tuple[str, str]:
        now_dt = self.normalize_utc_datetime(now_utc)
        store_now = now_dt.astimezone(ZoneInfo(brand.store_timezone))
        end_date_exclusive = store_now.date()
        start_date_inclusive = end_date_exclusive - timedelta(days=7)
        return start_date_inclusive.isoformat(), end_date_exclusive.isoformat()

    def get_current_ist_date(self, now_utc: Optional[datetime] = None) -> str:
        now_dt = self.normalize_utc_datetime(now_utc)
        return now_dt.astimezone(TOP_PRODUCT_SALES_SCHEDULE_TIMEZONE).date().isoformat()

    def get_next_top_product_sales_run_at(
        self, now_utc: Optional[datetime] = None
    ) -> datetime:
        now_dt = self.normalize_utc_datetime(now_utc)
        ist_now = now_dt.astimezone(TOP_PRODUCT_SALES_SCHEDULE_TIMEZONE)
        run_offset = timedelta(minutes=TOP_PRODUCT_SALES_RUN_OFFSET_MINUTES)
        run_today = datetime.combine(
            ist_now.date(), datetime.min.time(), tzinfo=TOP_PRODUCT_SALES_SCHEDULE_TIMEZONE
        ) + run_offset
        next_run = run_today if ist_now < run_today else run_today + timedelta(days=1)
        return next_run.astimezone(ZoneInfo("UTC"))

    def build_top_product_sales_shopifyql(
        self,
        start_date_inclusive: str,
        end_date_exclusive: str,
        limit: int = TOP_PRODUCT_SALES_PAGE_SIZE,
        offset: int = 0,
    ) -> str:
        offset_clause = f" OFFSET {offset}" if offset > 0 else ""
        return f"""
FROM sales
  SHOW gross_sales
  GROUP BY product_id, product_title, product_variant_id, product_variant_title,
    product_variant_sku
  WITH TOTALS
  SINCE {start_date_inclusive} UNTIL {end_date_exclusive}
  ORDER BY gross_sales DESC
  LIMIT {limit}{offset_clause}
VISUALIZE gross_sales TYPE table
""".strip()

    def fetch_shopifyql_table_data(
        self, brand: Any, shopifyql_query: str
    ) -> List[Dict[str, Any]]:
        if not brand.shop_name or not brand.access_token:
            raise ValueError(
                f"Missing Shopify credentials for {brand.shop_domain}: "
                "shop_name/access_token is required."
            )

        ql_flat = shopifyql_query.replace("\n", " ").strip().replace('"', '\\"')
        graphql_query = f"""
    query {{
      shopifyqlQuery(query: "{ql_flat}") {{
        tableData {{
          rows
          columns {{
            name
            dataType
            displayName
          }}
        }}
        parseErrors
      }}
    }}
    """.strip()
        payload = {"query": graphql_query}
        api_version = brand.api_version or DEFAULT_SHOPIFY_API_VERSION
        url = f"https://{brand.shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": brand.access_token,
        }

        last_exc: Optional[Exception] = None
        session = self.make_shopifyql_session()
        try:
            for attempt in range(1, 4):
                try:
                    response = session.post(
                        url,
                        headers=headers,
                        data=json.dumps(payload),
                        timeout=60,
                    )
                    if response.status_code != 200:
                        self.logger.warning(
                            "%s ShopifyQL HTTP %s for top-product sales attempt %s/3: %s",
                            self.log_prefix(brand),
                            response.status_code,
                            attempt,
                            response.text[:500],
                        )
                        continue

                    body = response.json()
                    if body.get("errors"):
                        raise ValueError(
                            f"GraphQL errors: {json.dumps(body.get('errors'), separators=(',', ':'))}"
                        )

                    ql_result = (body.get("data") or {}).get("shopifyqlQuery") or {}
                    parse_errors = ql_result.get("parseErrors") or []
                    if parse_errors:
                        raise ValueError(f"ShopifyQL parse errors: {parse_errors}")

                    table_data = ql_result.get("tableData")
                    return self.format_shopifyql_table(table_data)
                except Exception as exc:
                    last_exc = exc
                    self.logger.warning(
                        "%s ShopifyQL top-product sales fetch failed attempt %s/3: %s",
                        self.log_prefix(brand),
                        attempt,
                        exc,
                    )
                    try:
                        session.close()
                    except Exception:
                        pass
                    session = self.make_shopifyql_session()
                    time.sleep(2**attempt)
        finally:
            try:
                session.close()
            except Exception:
                pass

        if last_exc is not None:
            raise last_exc
        return []

    def fetch_all_top_product_sales_rows(
        self,
        brand: Any,
        start_date_inclusive: str,
        end_date_exclusive: str,
        page_size: int = TOP_PRODUCT_SALES_PAGE_SIZE,
    ) -> List[Dict[str, Any]]:
        all_rows: List[Dict[str, Any]] = []
        offset = 0

        while True:
            shopifyql_query = self.build_top_product_sales_shopifyql(
                start_date_inclusive,
                end_date_exclusive,
                limit=page_size,
                offset=offset,
            )
            rows = self.fetch_shopifyql_table_data(brand, shopifyql_query)
            self.logger.info(
                "%s Shopify product sales page fetched. offset=%s rows=%s",
                self.log_prefix(brand),
                offset,
                len(rows),
            )
            if not rows:
                break

            all_rows.extend(rows)
            if len(rows) < page_size:
                break
            offset += page_size

        return all_rows

    def parse_decimal_value(self, value: Any) -> Decimal:
        text = str(value or "").strip().replace(",", "")
        if not text:
            return Decimal("0")
        return Decimal(text)

    def normalize_top_product_sales_rows(
        self, rows: Sequence[Dict[str, Any]]
    ) -> List[TopProductSalesRecord]:
        aggregated: Dict[int, Decimal] = {}
        product_titles: Dict[int, str] = {}
        source_rows_by_product: Dict[int, List[Dict[str, Any]]] = {}
        for row in rows:
            try:
                product_id = self.extract_numeric_gid(row.get("product_id"), "product_id")
            except Exception:
                continue

            try:
                sales = self.parse_decimal_value(row.get("gross_sales"))
            except Exception:
                sales = Decimal("0")

            aggregated[product_id] = aggregated.get(product_id, Decimal("0")) + sales
            product_title = str(row.get("product_title") or "").strip()
            if product_id not in product_titles and product_title:
                product_titles[product_id] = product_title
            source_rows_by_product.setdefault(product_id, []).append(
                {
                    "product_id": str(product_id),
                    "product_title": product_title,
                    "product_variant_id": str(row.get("product_variant_id") or "").strip(),
                    "product_variant_title": str(
                        row.get("product_variant_title") or ""
                    ).strip(),
                    "product_variant_sku": str(row.get("product_variant_sku") or "").strip(),
                    "gross_sales": float(self.quantize(sales)),
                }
            )

        ranked: List[TopProductSalesRecord] = []
        for index, (product_id, sales) in enumerate(
            sorted(aggregated.items(), key=lambda item: (-item[1], item[0]))[:20],
            start=1,
        ):
            source_rows = tuple(
                sorted(
                    source_rows_by_product.get(product_id, []),
                    key=lambda row: (
                        -float(row.get("gross_sales") or 0),
                        row.get("product_variant_id") or "",
                    ),
                )
            )
            ranked.append(
                TopProductSalesRecord(
                    product_id=product_id,
                    product_title=product_titles.get(product_id, ""),
                    sales=self.quantize(sales),
                    rank=index,
                    source_rows=source_rows,
                )
            )
        return ranked

    def build_top_product_sales_cache_key(self, shop_domain: str) -> str:
        return f"{TOP_PRODUCT_SALES_KEY_PREFIX}:{self.normalize_shop_domain(shop_domain)}"

    def build_top_product_sales_payload(
        self,
        window_start_date: str,
        window_end_date_exclusive: str,
        records: Sequence[TopProductSalesRecord],
        updated_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload_products = [
            {
                "product_id": str(record.product_id),
                "product_title": record.product_title,
                "sales": float(record.sales),
                "rank": record.rank,
                "source_rows": list(record.source_rows),
            }
            for record in records
        ]
        return {
            "window_start_date": window_start_date,
            "window_end_date_exclusive": window_end_date_exclusive,
            "updated_at": updated_at
            or datetime.now(ZoneInfo("UTC"))
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
            "products": payload_products,
        }

    def get_cached_top_product_sales_date(
        self, redis_client: redis.Redis, shop_domain: str
    ) -> Optional[str]:
        raw_value = redis_client.get(self.build_top_product_sales_cache_key(shop_domain))
        if not raw_value:
            return None
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            return None
        if not isinstance(payload, dict):
            return None
        cached_date = payload.get("window_end_date_exclusive")
        if not isinstance(cached_date, str) or not cached_date.strip():
            return None
        return cached_date.strip()

    def cache_top_product_sales(
        self,
        redis_client: redis.Redis,
        shop_domain: str,
        payload: Dict[str, Any],
    ) -> str:
        key = self.build_top_product_sales_cache_key(shop_domain)
        redis_client.set(
            key,
            json.dumps(payload, separators=(",", ":")),
            ex=TOP_PRODUCT_SALES_CACHE_TTL_SECONDS,
        )
        return key

    def fetch_top_product_sales_for_brand(
        self, brand: Any, now_utc: Optional[datetime] = None
    ) -> Tuple[str, str, List[TopProductSalesRecord]]:
        window_start_date, window_end_date_exclusive = self.get_brand_sales_window(
            brand, now_utc=now_utc
        )
        self.logger.info(
            "%s Fetching product sales for %s to %s (today exclusive).",
            self.log_prefix(brand),
            window_start_date,
            window_end_date_exclusive,
        )
        rows = self.fetch_all_top_product_sales_rows(
            brand,
            window_start_date,
            window_end_date_exclusive,
        )
        self.logger.info(
            "%s Shopify product sales returned %s rows.",
            self.log_prefix(brand),
            len(rows),
        )
        records = self.normalize_top_product_sales_rows(rows)
        self.logger.info(
            "%s Top %s products identified for %s to %s.",
            self.log_prefix(brand),
            len(records),
            window_start_date,
            window_end_date_exclusive,
        )
        return window_start_date, window_end_date_exclusive, records

    def run_top_product_sales_cycle(
        self, redis_client: redis.Redis, now_utc: Optional[datetime] = None
    ) -> Dict[str, int]:
        brands = self.get_active_brands()
        if not brands:
            self.logger.info(
                "%s Top-product sales worker found no active brands.",
                self.log_prefix(),
            )
            return {"brands": 0, "cached": 0, "skipped": 0, "failed": 0}

        cached_count = 0
        skipped_count = 0
        failed_count = 0

        for brand in brands:
            try:
                window_start_date, window_end_date_exclusive = self.get_brand_sales_window(
                    brand, now_utc=now_utc
                )
                cached_date = self.get_cached_top_product_sales_date(
                    redis_client, brand.shop_domain
                )
                if cached_date == window_end_date_exclusive:
                    skipped_count += 1
                    self.logger.info(
                        "%s Top-product sales already cached for %s to %s. Skipping.",
                        self.log_prefix(brand),
                        window_start_date,
                        window_end_date_exclusive,
                    )
                    continue

                records_window_start, records_window_end, records = self.fetch_top_product_sales_for_brand(
                    brand, now_utc=now_utc
                )
                payload = self.build_top_product_sales_payload(
                    records_window_start,
                    records_window_end,
                    records,
                )
                key = self.cache_top_product_sales(redis_client, brand.shop_domain, payload)
                cached_count += 1
                self.logger.info(
                    "%s Cached top product sales in Redis for %s to %s at %s.",
                    self.log_prefix(brand),
                    records_window_start,
                    records_window_end,
                    key,
                )
            except Exception as exc:
                failed_count += 1
                self.logger.exception(
                    "%s Top-product sales worker failed: %s",
                    self.log_prefix(brand),
                    exc,
                )

        self.logger.info(
            "%s Top-product sales cycle complete. brands=%s cached=%s skipped=%s failed=%s",
            self.log_prefix(),
            len(brands),
            cached_count,
            skipped_count,
            failed_count,
        )
        return {
            "brands": len(brands),
            "cached": cached_count,
            "skipped": skipped_count,
            "failed": failed_count,
        }

    def top_product_sales_worker(self, stop_event) -> None:
        redis_client = self.get_redis_client()
        self.logger.info(
            "%s topProductSalesWorker started. schedule=00:00 %s ttl_seconds=%s",
            self.log_prefix(),
            TOP_PRODUCT_SALES_SCHEDULE_TIMEZONE.key,
            TOP_PRODUCT_SALES_CACHE_TTL_SECONDS,
        )

        startup_delay = max(0, int(os.environ.get("INVENTORY_STARTUP_DELAY_S", "150") or 0))
        if stop_event.wait(startup_delay + TOP_PRODUCT_SALES_STARTUP_OFFSET_S):
            return
        try:
            self.run_top_product_sales_cycle(redis_client)
        except Exception as exc:
            self.logger.exception(
                "%s topProductSalesWorker failed: %s",
                self.log_prefix(),
                exc,
            )

        while not stop_event.is_set():
            now_utc = self.normalize_utc_datetime()
            next_run_at = self.get_next_top_product_sales_run_at(now_utc)
            sleep_for = max(1, int((next_run_at - now_utc).total_seconds()))
            self.logger.info(
                "%s topProductSalesWorker sleeping until %s.",
                self.log_prefix(),
                next_run_at.astimezone(TOP_PRODUCT_SALES_SCHEDULE_TIMEZONE).isoformat(),
            )
            if stop_event.wait(sleep_for):
                break

            try:
                self.run_top_product_sales_cycle(redis_client)
            except Exception as exc:
                self.logger.exception(
                    "%s topProductSalesWorker failed: %s",
                    self.log_prefix(),
                    exc,
                )
