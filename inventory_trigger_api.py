import base64
import logging
import os
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import certifi
import mysql.connector
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from mysql.connector import pooling

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

GET_BRANDS_API = os.environ.get("GET_BRANDS_API", "").strip()
PIPELINE_AUTH_HEADER = os.environ.get("PIPELINE_AUTH_HEADER", "").strip().strip('"')
PASSWORD_AES_KEY = os.environ.get("PASSWORD_AES_KEY", "").strip()
DB_CONNECT_TIMEOUT_S = int(os.environ.get("DB_CONNECT_TIMEOUT_S", "10"))
MAX_PARALLEL_WORKERS = int(os.environ.get("INVENTORY_PIPELINE_WORKERS", "4"))
MAX_ITEMS_PER_WORKER = int(os.environ.get("INVENTORY_PIPELINE_CHUNK_SIZE", "10"))

app = Flask(__name__)

_pool_lock = threading.Lock()
_db_pools: Dict[int, pooling.MySQLConnectionPool] = {}


@dataclass(frozen=True)
class InventoryItem:
    product_id: int
    variant_id: int
    inventory_available: int


@dataclass(frozen=True)
class BrandJob:
    brand_id: int
    items: List[InventoryItem]


@dataclass(frozen=True)
class BrandContext:
    brand_id: int
    brand_tag: str
    brand_name: str
    database: str


def log_prefix(
    brand: Optional[BrandContext] = None, request_id: Optional[str] = None
) -> str:
    if brand and brand.brand_name:
        return f"[{brand.brand_name}]"
    return "[pipeline]"


def decrypt_value(encrypted_val: str) -> str:
    if not encrypted_val or ":" not in encrypted_val:
        return encrypted_val

    if not PASSWORD_AES_KEY:
        logger.warning("PASSWORD_AES_KEY missing, returning encrypted value as-is.")
        return encrypted_val

    try:
        iv_b64, ciphertext_b64 = encrypted_val.split(":", 1)
        iv = base64.b64decode(iv_b64)
        ciphertext = base64.b64decode(ciphertext_b64)

        key_bytes = PASSWORD_AES_KEY.encode("utf-8")
        if len(key_bytes) < 32:
            key_bytes = key_bytes.ljust(32, b"\0")
        elif len(key_bytes) > 32:
            key_bytes = key_bytes[:32]

        cipher = Cipher(
            algorithms.AES(key_bytes), modes.CBC(iv), backend=default_backend()
        )
        decryptor = cipher.decryptor()
        padded_content = decryptor.update(ciphertext) + decryptor.finalize()
        padding_len = padded_content[-1]

        if padding_len < 1 or padding_len > 16:
            return padded_content.decode("utf-8", errors="ignore")

        return padded_content[:-padding_len].decode("utf-8")
    except Exception as exc:
        logger.error("Decryption failed: %s", exc)
        return encrypted_val


def _write_b64_to_tempfile(b64_str: str, prefix: str) -> str:
    data = base64.b64decode(b64_str.encode("utf-8"))
    fd, path = tempfile.mkstemp(prefix=prefix, suffix=".pem")
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
    return path


def resolve_ca_bundle_path() -> Optional[str]:
    mode = os.environ.get("DB_TLS_CA_MODE", "certifi").strip().lower()
    if mode == "certifi":
        return certifi.where()
    if mode == "rds":
        path = os.environ.get("RDS_CA_PATH")
        if path and Path(path).exists():
            return path
        bundle = os.environ.get("RDS_CA_BUNDLE_B64")
        if bundle:
            return _write_b64_to_tempfile(bundle, "rds-ca-")
        return None
    if mode == "custom":
        path = os.environ.get("CUSTOM_CA_PATH")
        if path and Path(path).exists():
            return path
        bundle = os.environ.get("CUSTOM_CA_BUNDLE_B64")
        if bundle:
            return _write_b64_to_tempfile(bundle, "custom-ca-")
        return None
    if mode == "none":
        return None
    return certifi.where()


def fetch_brand_config(brand_id: int, request_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not GET_BRANDS_API or not PIPELINE_AUTH_HEADER:
        raise RuntimeError("GET_BRANDS_API or PIPELINE_AUTH_HEADER is not configured.")

    url = f"{GET_BRANDS_API.rstrip('/')}/{brand_id}"
    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    logger.info(
        "%s Fetching brand config from tenant API: %s",
        log_prefix(request_id=request_id),
        url,
    )
    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code == 200:
        body = response.json()
        logger.info(
            "%s Brand config fetched successfully.",
            log_prefix(
                brand=BrandContext(
                    brand_id=brand_id,
                    brand_tag=(body.get("brand_tag") or str(brand_id)).lower(),
                    brand_name=body.get("brand_name", str(brand_id)),
                    database=body.get("db_database") or body.get("brand_tag") or str(brand_id),
                ),
                request_id=request_id,
            ),
        )
        return body
    logger.error(
        "%s Failed to fetch brand config: %s %s",
        log_prefix(request_id=request_id),
        response.status_code,
        response.text[:300],
    )
    return None


def build_db_config(
    brand_id: int, request_id: Optional[str] = None
) -> Tuple[BrandContext, Dict[str, Any]]:
    config = fetch_brand_config(brand_id, request_id=request_id)
    if not config:
        raise ValueError(f"Brand config not found for brand_id={brand_id}")

    return BrandContext(
        brand_id=brand_id,
        brand_tag=(config.get("brand_tag") or str(brand_id)).lower(),
        brand_name=config.get("brand_name", str(brand_id)),
        database=config.get("db_database") or config.get("brand_tag") or str(brand_id),
    ), {
        "host": config["db_host"],
        "port": int(config.get("port", 3306)),
        "user": config["db_user"],
        "password": decrypt_value(config["db_password"]),
        "database": config.get("db_database") or config.get("brand_tag") or str(brand_id),
    }


def get_or_create_pool(
    brand_id: int, request_id: Optional[str] = None
) -> Tuple[BrandContext, pooling.MySQLConnectionPool]:
    with _pool_lock:
        existing = _db_pools.get(brand_id)
        if existing:
            logger.info(
                "%s Reusing existing MySQL connection pool.",
                log_prefix(request_id=request_id),
            )
            return existing["brand"], existing["pool"]

        brand, db_config = build_db_config(brand_id, request_id=request_id)
        ca_path = resolve_ca_bundle_path()
        verify_cert = os.environ.get("DB_SSL_VERIFY_CERT", "true").strip().lower() == "true"
        verify_identity = (
            os.environ.get("DB_SSL_VERIFY_IDENTITY", "false").strip().lower() == "true"
        )

        connect_kwargs = {
            "host": db_config["host"],
            "port": db_config["port"],
            "user": db_config["user"],
            "password": db_config["password"],
            "database": db_config["database"],
            "connection_timeout": DB_CONNECT_TIMEOUT_S,
            "pool_name": f"inventory_pipeline_{brand_id}",
            "pool_size": int(os.environ.get("INVENTORY_DB_POOL_SIZE", "5")),
            "pool_reset_session": True,
        }

        logger.info(
            "%s Creating MySQL connection pool host=%s port=%s ssl=%s pool_size=%s",
            log_prefix(brand=brand, request_id=request_id),
            db_config["host"],
            db_config["port"],
            "enabled" if db_config["host"].lower() not in {"localhost", "127.0.0.1"} and ca_path else "disabled",
            connect_kwargs["pool_size"],
        )

        if db_config["host"].lower() not in {"localhost", "127.0.0.1"} and ca_path:
            connect_kwargs.update(
                {
                    "ssl_ca": ca_path,
                    "ssl_verify_cert": verify_cert,
                    "ssl_verify_identity": verify_identity,
                }
            )

        try:
            pool = pooling.MySQLConnectionPool(**connect_kwargs)
        except Exception as exc:
            if "ssl" in str(exc).lower():
                logger.warning(
                    "%s SSL connection failed, retrying without SSL options: %s",
                    log_prefix(brand=brand, request_id=request_id),
                    exc,
                )
                connect_kwargs.pop("ssl_ca", None)
                connect_kwargs.pop("ssl_verify_cert", None)
                connect_kwargs.pop("ssl_verify_identity", None)
                pool = pooling.MySQLConnectionPool(**connect_kwargs)
            else:
                raise

        _db_pools[brand_id] = {"brand": brand, "pool": pool}
        logger.info("%s MySQL connection pool initialized.", log_prefix(brand=brand, request_id=request_id))
        return brand, pool


@contextmanager
def get_db_cursor(brand_id: int, request_id: Optional[str] = None):
    brand, pool = get_or_create_pool(brand_id, request_id=request_id)
    connection = pool.get_connection()
    cursor = connection.cursor(dictionary=True)
    logger.info("%s Database cursor opened.", log_prefix(brand=brand, request_id=request_id))
    try:
        yield brand, cursor, connection
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            connection.close()
        except Exception:
            pass
        logger.info("%s Database cursor closed.", log_prefix(brand=brand, request_id=request_id))


def parse_single_brand_job(payload: Dict[str, Any], job_index: Optional[int] = None) -> BrandJob:
    if not isinstance(payload, dict):
        if job_index is None:
            raise ValueError("Request body must be a JSON object.")
        raise ValueError(f"jobs[{job_index}] must be a JSON object.")

    brand_id_raw = payload.get("brand_id")
    if brand_id_raw is None:
        if job_index is None:
            raise ValueError("Request body must include brand_id.")
        raise ValueError(f"jobs[{job_index}] must include brand_id.")

    try:
        brand_id = int(brand_id_raw)
    except (TypeError, ValueError) as exc:
        if job_index is None:
            raise ValueError("brand_id must be an integer.") from exc
        raise ValueError(f"jobs[{job_index}].brand_id must be an integer.") from exc

    raw_items = payload.get("items")
    if not isinstance(raw_items, list) or not raw_items:
        if job_index is None:
            raise ValueError("Request body must include a non-empty items array.")
        raise ValueError(f"jobs[{job_index}] must include a non-empty items array.")

    items: List[InventoryItem] = []
    for index, raw_item in enumerate(raw_items, start=1):
        if not isinstance(raw_item, dict):
            prefix = "items" if job_index is None else f"jobs[{job_index}].items"
            raise ValueError(f"{prefix}[{index}] must be an object.")

        try:
            product_id = int(raw_item["product_id"])
            variant_id = int(raw_item["variant_id"])
            inventory_available = int(raw_item["inventory_available"])
        except KeyError as exc:
            prefix = "items" if job_index is None else f"jobs[{job_index}].items"
            raise ValueError(f"{prefix}[{index}] is missing {exc.args[0]}.") from exc
        except (TypeError, ValueError) as exc:
            prefix = "items" if job_index is None else f"jobs[{job_index}].items"
            raise ValueError(
                f"{prefix}[{index}] product_id, variant_id, and inventory_available must be integers."
            ) from exc

        if product_id <= 0 or variant_id <= 0:
            prefix = "items" if job_index is None else f"jobs[{job_index}].items"
            raise ValueError(
                f"{prefix}[{index}] product_id and variant_id must be positive integers."
            )

        items.append(
            InventoryItem(
                product_id=product_id,
                variant_id=variant_id,
                inventory_available=max(inventory_available, 0),
            )
        )

    return BrandJob(brand_id=brand_id, items=items)


def parse_payload(payload: Any) -> Tuple[List[BrandJob], bool]:
    if isinstance(payload, dict):
        return [parse_single_brand_job(payload)], False

    if isinstance(payload, list):
        if not payload:
            raise ValueError("Request body array must contain at least one brand job.")
        return [
            parse_single_brand_job(job_payload, job_index=index)
            for index, job_payload in enumerate(payload, start=1)
        ], True

    raise ValueError("Request body must be either a JSON object or an array of brand jobs.")


def build_pair_filter(items: Sequence[InventoryItem]) -> Tuple[str, List[int]]:
    clauses: List[str] = []
    params: List[int] = []
    for item in items:
        clauses.append("(product_id = %s AND variant_id = %s)")
        params.extend([item.product_id, item.variant_id])
    return " OR ".join(clauses), params


def quantize(value: Decimal, places: str = "0.01") -> Decimal:
    return value.quantize(Decimal(places), rounding=ROUND_HALF_UP)


def compute_metrics(sold_units: int, inventory_available: int, days: int) -> Tuple[Decimal, Decimal]:
    sold = Decimal(sold_units)
    inventory = Decimal(inventory_available)
    drr = quantize(sold / Decimal(days))
    if drr == 0:
        return drr, Decimal("0.00")
    doh = quantize(inventory / drr)
    return drr, doh


def chunk_items(items: Sequence[InventoryItem], chunk_size: int) -> List[List[InventoryItem]]:
    return [list(items[i : i + chunk_size]) for i in range(0, len(items), chunk_size)]


def fetch_sold_units(
    cursor,
    items: Sequence[InventoryItem],
    brand: BrandContext,
    request_id: Optional[str] = None,
) -> Dict[Tuple[int, int], Dict[str, int]]:
    pair_filter, params = build_pair_filter(items)
    logger.info(
        "%s Fetching sold units from products_sold_units_daily for %s item pairs.",
        log_prefix(brand=brand, request_id=request_id),
        len(items),
    )
    cursor.execute(
        f"""
        SELECT product_id, variant_id, sold_units_7d, sold_units_30d, sold_units_90d
        FROM products_sold_units_daily
        WHERE {pair_filter}
        """,
        params,
    )

    result: Dict[Tuple[int, int], Dict[str, int]] = {}
    for row in cursor.fetchall():
        result[(int(row["product_id"]), int(row["variant_id"]))] = {
            "sold_units_7d": int(row.get("sold_units_7d") or 0),
            "sold_units_30d": int(row.get("sold_units_30d") or 0),
            "sold_units_90d": int(row.get("sold_units_90d") or 0),
        }
    logger.info(
        "%s Sold units fetched for %s/%s requested pairs.",
        log_prefix(brand=brand, request_id=request_id),
        len(result),
        len(items),
    )
    return result


def fetch_existing_inventory_rows(
    cursor, items: Sequence[InventoryItem], brand: BrandContext, request_id: Optional[str] = None
) -> Dict[Tuple[int, int], Dict[str, Optional[str]]]:
    pair_filter, params = build_pair_filter(items)
    logger.info(
        "%s Fetching existing rows from top_products_inventory.",
        log_prefix(brand=brand, request_id=request_id),
    )
    cursor.execute(
        f"""
        SELECT product_id, variant_id, product_title, variant_title, sku
        FROM top_products_inventory
        WHERE {pair_filter}
        """,
        params,
    )
    rows = {
        (int(row["product_id"]), int(row["variant_id"])): {
            "product_title": row.get("product_title"),
            "variant_title": row.get("variant_title"),
            "sku": row.get("sku"),
        }
        for row in cursor.fetchall()
    }
    logger.info(
        "%s Existing inventory rows found for %s/%s pairs.",
        log_prefix(brand=brand, request_id=request_id),
        len(rows),
        len(items),
    )
    return rows


def fetch_latest_order_metadata(
    cursor, items: Sequence[InventoryItem], brand: BrandContext, request_id: Optional[str] = None
) -> Dict[Tuple[int, int], Dict[str, Optional[str]]]:
    pair_filter, params = build_pair_filter(items)
    metadata_by_pair: Dict[Tuple[int, int], Dict[str, Optional[str]]] = {}
    logger.info(
        "%s Fetching latest product metadata from order tables.",
        log_prefix(brand=brand, request_id=request_id),
    )

    cursor.execute(
        f"""
        SELECT product_id, variant_id, line_item, variant_title, sku, updated_at, created_at
        FROM (
            SELECT product_id, variant_id, line_item, variant_title, sku, updated_at, created_at
            FROM shopify_orders_update
            WHERE {pair_filter}

            UNION ALL

            SELECT product_id, variant_id, line_item, variant_title, sku, updated_at, created_at
            FROM shopify_orders
            WHERE {pair_filter}
        ) AS combined_orders
        ORDER BY COALESCE(updated_at, created_at) DESC
        """,
        params + params,
    )

    for row in cursor.fetchall():
        try:
            pair = (int(row["product_id"]), int(row["variant_id"]))
        except (TypeError, ValueError):
            continue

        if pair in metadata_by_pair:
            continue

        metadata_by_pair[pair] = {
            "product_title": row.get("line_item"),
            "variant_title": row.get("variant_title"),
            "sku": row.get("sku"),
        }

    logger.info(
        "%s Order metadata found for %s/%s pairs.",
        log_prefix(brand=brand, request_id=request_id),
        len(metadata_by_pair),
        len(items),
    )
    return metadata_by_pair


def process_inventory_chunk(
    brand_id: int,
    items: Sequence[InventoryItem],
    request_id: Optional[str] = None,
    worker_id: Optional[int] = None,
    chunk_index: Optional[int] = None,
    total_chunks: Optional[int] = None,
) -> Dict[str, Any]:
    request_pairs = {(item.product_id, item.variant_id) for item in items}
    start_ts = time.perf_counter()

    with get_db_cursor(brand_id, request_id=request_id) as (brand, cursor, connection):
        logger.info(
            "%s Worker=%s chunk=%s/%s starting inventory upsert for %s incoming items.",
            log_prefix(brand=brand, request_id=request_id),
            worker_id if worker_id is not None else "-",
            chunk_index if chunk_index is not None else "-",
            total_chunks if total_chunks is not None else "-",
            len(items),
        )
        sold_units_map = fetch_sold_units(cursor, items, brand, request_id=request_id)
        existing_inventory_map = fetch_existing_inventory_rows(
            cursor, items, brand, request_id=request_id
        )
        order_metadata_map = fetch_latest_order_metadata(
            cursor, items, brand, request_id=request_id
        )

        missing_in_sales = sorted(request_pairs - set(sold_units_map.keys()))
        missing_in_inventory = sorted(request_pairs - set(existing_inventory_map.keys()))
        missing_metadata: List[Tuple[int, int]] = []

        upsert_rows = []
        updated_pairs: List[Tuple[int, int]] = []

        for item in items:
            pair = (item.product_id, item.variant_id)
            sold_units = sold_units_map.get(pair)
            if not sold_units:
                continue

            drr_7d, doh_7d = compute_metrics(
                sold_units["sold_units_7d"], item.inventory_available, 7
            )
            drr_30d, doh_30d = compute_metrics(
                sold_units["sold_units_30d"], item.inventory_available, 30
            )
            drr_90d, doh_90d = compute_metrics(
                sold_units["sold_units_90d"], item.inventory_available, 90
            )

            metadata = existing_inventory_map.get(pair) or order_metadata_map.get(pair) or {}
            product_title = (metadata.get("product_title") or "").strip() or f"Product {item.product_id}"
            variant_title = (metadata.get("variant_title") or "").strip() or f"Variant {item.variant_id}"
            sku = metadata.get("sku")

            if pair not in existing_inventory_map and pair not in order_metadata_map:
                missing_metadata.append(pair)

            upsert_rows.append(
                (
                    item.product_id,
                    product_title,
                    item.variant_id,
                    variant_title,
                    sku,
                    item.inventory_available,
                    sold_units["sold_units_7d"],
                    sold_units["sold_units_30d"],
                    sold_units["sold_units_90d"],
                    str(drr_7d),
                    str(drr_30d),
                    str(drr_90d),
                    str(doh_7d),
                    str(doh_30d),
                    str(doh_90d),
                )
            )
            updated_pairs.append(pair)

        logger.info(
            "%s Worker=%s chunk=%s/%s prepared upsert batch. ready=%s missing_sales=%s missing_inventory=%s metadata_fallback=%s",
            log_prefix(brand=brand, request_id=request_id),
            worker_id if worker_id is not None else "-",
            chunk_index if chunk_index is not None else "-",
            total_chunks if total_chunks is not None else "-",
            len(upsert_rows),
            len(missing_in_sales),
            len(missing_in_inventory),
            len(set(missing_metadata)),
        )

        if upsert_rows:
            cursor.executemany(
                """
                INSERT INTO top_products_inventory (
                    product_id,
                    product_title,
                    variant_id,
                    variant_title,
                    sku,
                    inventory_available,
                    sold_units_7d,
                    sold_units_30d,
                    sold_units_90d,
                    drr_7d,
                    drr_30d,
                    drr_90d,
                    doh_7d,
                    doh_30d,
                    doh_90d
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    product_id = VALUES(product_id),
                    product_title = COALESCE(NULLIF(VALUES(product_title), ''), product_title),
                    variant_title = COALESCE(NULLIF(VALUES(variant_title), ''), variant_title),
                    sku = COALESCE(VALUES(sku), sku),
                    inventory_available = VALUES(inventory_available),
                    sold_units_7d = VALUES(sold_units_7d),
                    sold_units_30d = VALUES(sold_units_30d),
                    sold_units_90d = VALUES(sold_units_90d),
                    drr_7d = VALUES(drr_7d),
                    drr_30d = VALUES(drr_30d),
                    drr_90d = VALUES(drr_90d),
                    doh_7d = VALUES(doh_7d),
                    doh_30d = VALUES(doh_30d),
                    doh_90d = VALUES(doh_90d),
                    updated_at = CURRENT_TIMESTAMP
                """,
                upsert_rows,
            )
            connection.commit()
            logger.info(
                "%s Worker=%s chunk=%s/%s upsert committed successfully for %s rows.",
                log_prefix(brand=brand, request_id=request_id),
                worker_id if worker_id is not None else "-",
                chunk_index if chunk_index is not None else "-",
                total_chunks if total_chunks is not None else "-",
                len(upsert_rows),
            )
        else:
            logger.info(
                "%s Worker=%s chunk=%s/%s no rows qualified for upsert after sold-units filtering.",
                log_prefix(brand=brand, request_id=request_id),
                worker_id if worker_id is not None else "-",
                chunk_index if chunk_index is not None else "-",
                total_chunks if total_chunks is not None else "-",
            )

    elapsed_s = round(time.perf_counter() - start_ts, 2)
    logger.info(
        "%s Worker=%s chunk=%s/%s finished in %ss.",
        log_prefix(brand=brand, request_id=request_id),
        worker_id if worker_id is not None else "-",
        chunk_index if chunk_index is not None else "-",
        total_chunks if total_chunks is not None else "-",
        elapsed_s,
    )

    return {
        "brand_id": brand_id,
        "brand_tag": brand.brand_tag,
        "brand_name": brand.brand_name,
        "received_count": len(items),
        "updated_count": len(upsert_rows),
        "duration_seconds": elapsed_s,
        "missing_in_products_sold_units_daily": [
            {"product_id": product_id, "variant_id": variant_id}
            for product_id, variant_id in missing_in_sales
        ],
        "missing_in_top_products_inventory": [
            {"product_id": product_id, "variant_id": variant_id}
            for product_id, variant_id in missing_in_inventory
        ],
        "missing_metadata_fallback_used": [
            {"product_id": product_id, "variant_id": variant_id}
            for product_id, variant_id in sorted(set(missing_metadata))
        ],
        "updated_pairs": [
            {"product_id": product_id, "variant_id": variant_id}
            for product_id, variant_id in updated_pairs
        ],
    }


def process_inventory_update(
    brand_id: int, items: Sequence[InventoryItem], request_id: Optional[str] = None
) -> Dict[str, Any]:
    start_ts = time.perf_counter()
    brand, _ = get_or_create_pool(brand_id, request_id=request_id)
    chunks = chunk_items(items, MAX_ITEMS_PER_WORKER)
    worker_count = min(MAX_PARALLEL_WORKERS, max(1, len(chunks)))

    logger.info(
        "%s Starting parallel inventory pipeline workers=%s chunks=%s chunk_size=%s total_items=%s.",
        log_prefix(brand=brand, request_id=request_id),
        worker_count,
        len(chunks),
        MAX_ITEMS_PER_WORKER,
        len(items),
    )

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_chunk = {
            executor.submit(
                process_inventory_chunk,
                brand_id,
                chunk,
                request_id,
                ((index - 1) % worker_count) + 1,
                index,
                len(chunks),
            ): index
            for index, chunk in enumerate(chunks, start=1)
        }

        for future in as_completed(future_to_chunk):
            chunk_index = future_to_chunk[future]
            chunk_result = future.result()
            results.append(chunk_result)
            logger.info(
                "%s Chunk %s/%s completed with %s updates.",
                log_prefix(brand=brand, request_id=request_id),
                chunk_index,
                len(chunks),
                chunk_result["updated_count"],
            )

    elapsed_s = round(time.perf_counter() - start_ts, 2)

    merged_result = {
        "brand_id": brand.brand_id,
        "brand_tag": brand.brand_tag,
        "brand_name": brand.brand_name,
        "received_count": len(items),
        "updated_count": sum(result["updated_count"] for result in results),
        "duration_seconds": elapsed_s,
        "worker_count": worker_count,
        "chunk_count": len(chunks),
        "chunk_size": MAX_ITEMS_PER_WORKER,
        "missing_in_products_sold_units_daily": [],
        "missing_in_top_products_inventory": [],
        "missing_metadata_fallback_used": [],
        "updated_pairs": [],
    }

    seen_keys = {
        "missing_in_products_sold_units_daily": set(),
        "missing_in_top_products_inventory": set(),
        "missing_metadata_fallback_used": set(),
        "updated_pairs": set(),
    }

    for result in results:
        for key in seen_keys:
            for row in result[key]:
                pair = (row["product_id"], row["variant_id"])
                if pair in seen_keys[key]:
                    continue
                seen_keys[key].add(pair)
                merged_result[key].append(row)

    logger.info(
        "%s Parallel inventory pipeline finished in %ss. total_updates=%s total_chunks=%s workers=%s.",
        log_prefix(brand=brand, request_id=request_id),
        elapsed_s,
        merged_result["updated_count"],
        merged_result["chunk_count"],
        merged_result["worker_count"],
    )

    return merged_result


@app.get("/health")
def healthcheck():
    return jsonify({"status": "ok"})


@app.post("/inventory-pipeline/trigger")
def trigger_inventory_pipeline():
    request_id = uuid.uuid4().hex[:8]
    incoming_key = request.headers.get("x-pipeline-key", "").strip()
    if not PIPELINE_AUTH_HEADER or incoming_key != PIPELINE_AUTH_HEADER:
        logger.warning(
            "%s Rejecting request due to missing/invalid x-pipeline-key header.",
            log_prefix(request_id=request_id),
        )
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Unauthorized: invalid or missing x-pipeline-key header.",
                }
            ),
            401,
        )

    payload = request.get_json(silent=True)
    if payload is None:
        logger.warning("%s Rejecting request due to invalid JSON body.", log_prefix(request_id=request_id))
        return jsonify({"success": False, "message": "Invalid JSON body."}), 400

    try:
        jobs, is_batch = parse_payload(payload)
        logger.info(
            "%s Accepted inventory trigger with %s brand job(s).",
            log_prefix(request_id=request_id),
            len(jobs),
        )
        results: List[Dict[str, Any]] = []
        total_items = 0
        total_updates = 0

        for job in jobs:
            logger.info(
                "%s Dispatching brand_id=%s with %s item(s).",
                log_prefix(request_id=request_id),
                job.brand_id,
                len(job.items),
            )
            result = process_inventory_update(job.brand_id, job.items, request_id=request_id)
            results.append(result)
            total_items += len(job.items)
            total_updates += result["updated_count"]
            logger.info(
                "%s Inventory trigger completed with %s updates.",
                log_prefix(
                    brand=BrandContext(
                        brand_id=result["brand_id"],
                        brand_tag=result["brand_tag"],
                        brand_name=result["brand_name"],
                        database="resolved-at-runtime",
                    ),
                    request_id=request_id,
                ),
                result["updated_count"],
            )

        if not is_batch:
            return jsonify({"success": True, "message": "Inventory pipeline completed.", "data": results[0]}), 200

        return (
            jsonify(
                {
                    "success": True,
                    "message": "Inventory batch pipeline completed.",
                    "data": {
                        "brand_job_count": len(results),
                        "received_count": total_items,
                        "updated_count": total_updates,
                        "results": results,
                    },
                }
            ),
            200,
        )
    except ValueError as exc:
        logger.warning(
            "%s Payload validation failed: %s",
            log_prefix(request_id=request_id),
            exc,
        )
        return jsonify({"success": False, "message": str(exc)}), 400
    except Exception as exc:
        logger.exception("%s Inventory pipeline failed: %s", log_prefix(request_id=request_id), exc)
        return jsonify({"success": False, "message": "Internal server error."}), 500


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8050")),
        debug=os.environ.get("FLASK_DEBUG", "false").strip().lower() == "true",
    )
