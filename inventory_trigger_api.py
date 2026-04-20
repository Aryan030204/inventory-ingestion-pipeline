import base64
import json
import logging
import os
import re
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import certifi
import mysql.connector
import redis
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from dotenv import load_dotenv
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
REDIS_URI = os.environ.get("REDIS_URI", "").strip()
CALCULATION_INTERVAL_MINUTES = int(
    os.environ.get("CALCULATION_INTERVAL_MINUTES", "10")
)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
SCHEDULED_WINDOW_MINUTES = int(os.environ.get("SCHEDULED_WINDOW", "30"))
INGESTION_POLL_SECONDS = int(os.environ.get("INGESTION_POLL_SECONDS", "5"))

SOURCE_KEY_PREFIX = os.environ.get("INVENTORY_SOURCE_PREFIX", "product_inventory")
READY_KEY_PREFIX = os.environ.get("INVENTORY_READY_PREFIX", "inventory_calc_ready")
READY_FLAG_KEY = os.environ.get("INVENTORY_READY_FLAG_KEY", "inventory_ingestion_ready")
READY_COUNT_KEY = os.environ.get("INVENTORY_READY_COUNT_KEY", "inventory_calc_ready_count")

_pool_lock = threading.Lock()
_db_pools: Dict[int, Dict[str, Any]] = {}
_brand_cache_lock = threading.Lock()
_brand_context_by_shop_domain: Dict[str, "BrandContext"] = {}
_brand_cache_loaded_at = 0.0


@dataclass(frozen=True)
class BrandContext:
    brand_id: int
    brand_tag: str
    brand_name: str
    database: str
    shop_domain: str


@dataclass(frozen=True)
class SourceInventoryItem:
    shop_domain: str
    product_id: int
    product_title: str
    variant_id: int
    variant_title: str
    sku: Optional[str]
    inventory_quantity: int


@dataclass(frozen=True)
class CalculatedInventoryItem:
    shop_domain: str
    product_id: int
    product_title: str
    variant_id: int
    variant_title: str
    sku: Optional[str]
    inventory_quantity: int
    sold_units_7d: int
    sold_units_30d: int
    sold_units_90d: int
    drr_7d: Decimal
    drr_30d: Decimal
    drr_90d: Decimal


def log_prefix(brand: Optional[BrandContext] = None) -> str:
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


def fetch_active_brands() -> Dict[str, str]:
    if not GET_BRANDS_API or not PIPELINE_AUTH_HEADER:
        raise RuntimeError("GET_BRANDS_API or PIPELINE_AUTH_HEADER is not configured.")

    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    response = requests.get(GET_BRANDS_API, headers=headers, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Brands API returned unexpected payload shape.")
    return payload


def fetch_brand_config(brand_id: int) -> Dict[str, Any]:
    if not GET_BRANDS_API or not PIPELINE_AUTH_HEADER:
        raise RuntimeError("GET_BRANDS_API or PIPELINE_AUTH_HEADER is not configured.")

    url = f"{GET_BRANDS_API.rstrip('/')}/{brand_id}"
    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError(f"Brand config payload is invalid for brand_id={brand_id}")
    return payload


def normalize_shop_domain(shop_domain: str) -> str:
    value = (shop_domain or "").strip().lower()
    value = re.sub(r"^https?://", "", value)
    value = value.strip("/")
    return value


def build_shop_domain(shop_name: str) -> str:
    normalized = normalize_shop_domain(shop_name)
    if normalized.endswith(".myshopify.com"):
        return normalized
    if normalized:
        return f"{normalized}.myshopify.com"
    return normalized


def load_brand_context_cache(force: bool = False) -> Dict[str, BrandContext]:
    global _brand_cache_loaded_at

    with _brand_cache_lock:
        if _brand_context_by_shop_domain and not force:
            return dict(_brand_context_by_shop_domain)

        brand_map = fetch_active_brands()
        refreshed: Dict[str, BrandContext] = {}

        for brand_id_raw in brand_map.keys():
            brand_id = int(brand_id_raw)
            config = fetch_brand_config(brand_id)
            brand = BrandContext(
                brand_id=brand_id,
                brand_tag=(config.get("brand_tag") or str(brand_id)).lower(),
                brand_name=config.get("brand_name", str(brand_id)),
                database=config.get("db_database") or config.get("brand_tag") or str(brand_id),
                shop_domain=build_shop_domain(config.get("shop_name", "").strip()),
            )

            refreshed[brand.shop_domain] = brand
            shop_name = normalize_shop_domain(config.get("shop_name", ""))
            if shop_name:
                refreshed[shop_name] = brand

        _brand_context_by_shop_domain.clear()
        _brand_context_by_shop_domain.update(refreshed)
        _brand_cache_loaded_at = time.time()
        logger.info(
            "%s Brand context cache loaded for %s shop domain aliases.",
            log_prefix(),
            len(refreshed),
        )
        return dict(refreshed)


def resolve_brand_for_shop_domain(shop_domain: str) -> BrandContext:
    normalized = normalize_shop_domain(shop_domain)
    cache = load_brand_context_cache()
    brand = cache.get(normalized)
    if brand:
        return brand

    cache = load_brand_context_cache(force=True)
    brand = cache.get(normalized)
    if brand:
        return brand

    raise ValueError(f"No tenant config found for shop_domain={shop_domain}")


def build_db_config(brand_id: int) -> Tuple[BrandContext, Dict[str, Any]]:
    config = fetch_brand_config(brand_id)

    return BrandContext(
        brand_id=brand_id,
        brand_tag=(config.get("brand_tag") or str(brand_id)).lower(),
        brand_name=config.get("brand_name", str(brand_id)),
        database=config.get("db_database") or config.get("brand_tag") or str(brand_id),
        shop_domain=build_shop_domain(config.get("shop_name", "").strip()),
    ), {
        "host": config["db_host"],
        "port": int(config.get("port", 3306)),
        "user": config["db_user"],
        "password": decrypt_value(config["db_password"]),
        "database": config.get("db_database") or config.get("brand_tag") or str(brand_id),
    }


def get_or_create_pool(brand_id: int) -> Tuple[BrandContext, pooling.MySQLConnectionPool]:
    with _pool_lock:
        existing = _db_pools.get(brand_id)
        if existing:
            return existing["brand"], existing["pool"]

        brand, db_config = build_db_config(brand_id)
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
                connect_kwargs.pop("ssl_ca", None)
                connect_kwargs.pop("ssl_verify_cert", None)
                connect_kwargs.pop("ssl_verify_identity", None)
                pool = pooling.MySQLConnectionPool(**connect_kwargs)
            else:
                raise

        _db_pools[brand_id] = {"brand": brand, "pool": pool}
        logger.info(
            "%s MySQL connection pool initialized for %s.",
            log_prefix(brand),
            brand.shop_domain,
        )
        return brand, pool


@contextmanager
def get_db_cursor(brand_id: int):
    brand, pool = get_or_create_pool(brand_id)
    connection = pool.get_connection()
    cursor = connection.cursor(dictionary=True)
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


def get_redis_client() -> redis.Redis:
    if not REDIS_URI:
        raise RuntimeError("REDIS_URI is not configured.")
    redis_uri = REDIS_URI
    if "://" not in redis_uri:
        redis_uri = f"redis://{redis_uri}"
    return redis.Redis.from_url(redis_uri, decode_responses=True)


def quantize(value: Decimal, places: str = "0.01") -> Decimal:
    return value.quantize(Decimal(places), rounding=ROUND_HALF_UP)


def compute_drr(sold_units: int, days: int) -> Decimal:
    return quantize(Decimal(sold_units) / Decimal(days))


def compute_doh(inventory_available: int, drr: Decimal) -> Decimal:
    if drr == 0:
        return Decimal("0.00")
    return quantize(Decimal(inventory_available) / drr)


def extract_numeric_gid(value: Any, field_name: str) -> int:
    text = str(value or "").strip()
    match = re.search(r"(\d+)$", text)
    if not match:
        raise ValueError(f"{field_name} is not a valid Shopify GID or numeric id: {value}")
    return int(match.group(1))


def parse_source_item(raw_item: Dict[str, Any], shop_domain_hint: str) -> SourceInventoryItem:
    shop_domain = normalize_shop_domain(raw_item.get("shop_domain") or shop_domain_hint)
    if not shop_domain:
        raise ValueError("shop_domain is missing.")

    inventory_quantity = int(raw_item.get("inventory_quantity") or 0)
    return SourceInventoryItem(
        shop_domain=shop_domain,
        product_id=extract_numeric_gid(raw_item.get("product_id"), "product_id"),
        product_title=str(raw_item.get("product_title") or "").strip(),
        variant_id=extract_numeric_gid(raw_item.get("variant_id"), "variant_id"),
        variant_title=str(raw_item.get("variant_title") or "").strip(),
        sku=(str(raw_item.get("sku")).strip() if raw_item.get("sku") is not None else None),
        inventory_quantity=max(inventory_quantity, 0),
    )


def _decode_json_blob(raw_value: str) -> List[Dict[str, Any]]:
    payload = json.loads(raw_value)
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().lower() in {"true", "1", "yes", "y"}


def _read_source_documents(redis_client: redis.Redis, key: str) -> Tuple[str, Any]:
    key_type = redis_client.type(key)
    if key_type == "none":
        return "none", None
    if key_type == "string":
        raw_value = redis_client.get(key)
        return key_type, (json.loads(raw_value) if raw_value else None)
    if key_type == "list":
        return key_type, [json.loads(entry) for entry in redis_client.lrange(key, 0, -1)]
    if key_type == "set":
        return key_type, [json.loads(entry) for entry in redis_client.smembers(key)]
    if key_type == "hash":
        raw_map = redis_client.hgetall(key)
        return key_type, {field: json.loads(value) for field, value in raw_map.items()}
    logger.warning("%s Unsupported Redis key type for %s: %s", log_prefix(), key, key_type)
    return key_type, None


def _write_source_documents(redis_client: redis.Redis, key: str, key_type: str, payload: Any) -> None:
    pipeline = redis_client.pipeline()

    if key_type == "string":
        if payload is None:
            pipeline.delete(key)
        else:
            pipeline.set(key, json.dumps(payload, separators=(",", ":")))
        pipeline.execute()
        return

    if key_type == "list":
        pipeline.delete(key)
        serialized = [json.dumps(entry, separators=(",", ":")) for entry in (payload or [])]
        if serialized:
            pipeline.rpush(key, *serialized)
        pipeline.execute()
        return

    if key_type == "set":
        pipeline.delete(key)
        serialized = [json.dumps(entry, separators=(",", ":")) for entry in (payload or [])]
        if serialized:
            pipeline.sadd(key, *serialized)
        pipeline.execute()
        return

    if key_type == "hash":
        pipeline.delete(key)
        if payload:
            serialized_map = {
                field: json.dumps(value, separators=(",", ":"))
                for field, value in payload.items()
            }
            pipeline.hset(key, mapping=serialized_map)
        pipeline.execute()
        return


def _collect_changed_docs_from_node(node: Any, collected: List[Dict[str, Any]]) -> Any:
    if isinstance(node, dict):
        if "product_id" in node and "variant_id" in node:
            if _is_truthy(node.get("inventory_changed")):
                updated = dict(node)
                updated["inventory_changed"] = False
                collected.append(updated)
                return updated
            return node

        updated_node = {}
        for key, value in node.items():
            updated_node[key] = _collect_changed_docs_from_node(value, collected)
        return updated_node

    if isinstance(node, list):
        return [_collect_changed_docs_from_node(item, collected) for item in node]

    return node


def pull_changed_source_items_for_domain(
    redis_client: redis.Redis, shop_domain: str
) -> List[SourceInventoryItem]:
    key = f"{SOURCE_KEY_PREFIX}:{shop_domain}"
    key_type, payload = _read_source_documents(redis_client, key)
    if key_type == "none" or payload is None:
        return []

    changed_docs: List[Dict[str, Any]] = []
    updated_payload = _collect_changed_docs_from_node(payload, changed_docs)
    _write_source_documents(redis_client, key, key_type, updated_payload)

    items: List[SourceInventoryItem] = []
    for raw_doc in changed_docs:
        try:
            items.append(parse_source_item(raw_doc, shop_domain))
        except Exception as exc:
            logger.warning(
                "%s Skipping invalid changed payload for %s: %s",
                log_prefix(),
                shop_domain,
                exc,
            )

    return items


def _pull_json_documents(redis_client: redis.Redis, key: str) -> List[Dict[str, Any]]:
    key_type = redis_client.type(key)
    docs: List[Dict[str, Any]] = []

    if key_type == "none":
        return docs

    if key_type == "list":
        raw_entries = redis_client.lrange(key, 0, -1)
        redis_client.delete(key)
        for raw_entry in raw_entries:
            docs.extend(_decode_json_blob(raw_entry))
        return docs

    if key_type == "string":
        raw_value = redis_client.get(key)
        redis_client.delete(key)
        if raw_value:
            docs.extend(_decode_json_blob(raw_value))
        return docs

    if key_type == "set":
        raw_entries = list(redis_client.smembers(key))
        redis_client.delete(key)
        for raw_entry in raw_entries:
            docs.extend(_decode_json_blob(raw_entry))
        return docs

    if key_type == "hash":
        raw_entries = list(redis_client.hvals(key))
        redis_client.delete(key)
        for raw_entry in raw_entries:
            docs.extend(_decode_json_blob(raw_entry))
        return docs

    logger.warning("%s Unsupported Redis key type for %s: %s", log_prefix(), key, key_type)
    return docs


def serialize_ready_item(item: CalculatedInventoryItem) -> str:
    return json.dumps(
        {
            "shop_domain": item.shop_domain,
            "product_id": item.product_id,
            "product_title": item.product_title,
            "variant_id": item.variant_id,
            "variant_title": item.variant_title,
            "sku": item.sku,
            "inventory_quantity": item.inventory_quantity,
            "sold_units_7d": item.sold_units_7d,
            "sold_units_30d": item.sold_units_30d,
            "sold_units_90d": item.sold_units_90d,
            "drr_7d": str(item.drr_7d),
            "drr_30d": str(item.drr_30d),
            "drr_90d": str(item.drr_90d),
        },
        separators=(",", ":"),
    )


def parse_ready_item(raw_doc: Dict[str, Any], shop_domain_hint: str) -> CalculatedInventoryItem:
    shop_domain = normalize_shop_domain(raw_doc.get("shop_domain") or shop_domain_hint)
    return CalculatedInventoryItem(
        shop_domain=shop_domain,
        product_id=int(raw_doc["product_id"]),
        product_title=str(raw_doc.get("product_title") or "").strip(),
        variant_id=int(raw_doc["variant_id"]),
        variant_title=str(raw_doc.get("variant_title") or "").strip(),
        sku=(str(raw_doc.get("sku")).strip() if raw_doc.get("sku") is not None else None),
        inventory_quantity=max(int(raw_doc.get("inventory_quantity") or 0), 0),
        sold_units_7d=max(int(raw_doc.get("sold_units_7d") or 0), 0),
        sold_units_30d=max(int(raw_doc.get("sold_units_30d") or 0), 0),
        sold_units_90d=max(int(raw_doc.get("sold_units_90d") or 0), 0),
        drr_7d=Decimal(str(raw_doc.get("drr_7d") or "0")),
        drr_30d=Decimal(str(raw_doc.get("drr_30d") or "0")),
        drr_90d=Decimal(str(raw_doc.get("drr_90d") or "0")),
    )


def pull_ready_items_for_domain(
    redis_client: redis.Redis, shop_domain: str
) -> List[CalculatedInventoryItem]:
    key = f"{READY_KEY_PREFIX}:{shop_domain}"
    raw_docs = _pull_json_documents(redis_client, key)
    items: List[CalculatedInventoryItem] = []
    for raw_doc in raw_docs:
        try:
            items.append(parse_ready_item(raw_doc, shop_domain))
        except Exception as exc:
            logger.warning(
                "%s Skipping invalid ready payload for %s: %s",
                log_prefix(),
                shop_domain,
                exc,
            )
    return items


def store_ready_items(
    redis_client: redis.Redis, shop_domain: str, items: Sequence[CalculatedInventoryItem]
) -> None:
    if not items:
        return

    key = f"{READY_KEY_PREFIX}:{shop_domain}"
    payloads = [serialize_ready_item(item) for item in items]
    pipeline = redis_client.pipeline()
    pipeline.rpush(key, *payloads)
    pipeline.incrby(READY_COUNT_KEY, len(payloads))
    pipeline.execute()
    logger.info(
        "%s Cached %s calculated records to %s.",
        log_prefix(),
        len(payloads),
        key,
    )


def scan_shop_domains(redis_client: redis.Redis, prefix: str) -> List[str]:
    domains: List[str] = []
    pattern = f"{prefix}:*"
    for key in redis_client.scan_iter(match=pattern):
        if ":" not in key:
            continue
        domains.append(key.split(":", 1)[1])
    return sorted(set(domains))


def should_trigger_ingestion(redis_client: redis.Redis) -> bool:
    ready_count = int(redis_client.get(READY_COUNT_KEY) or 0)
    if ready_count >= BATCH_SIZE:
        redis_client.set(READY_FLAG_KEY, "1")
        logger.info(
            "%s Ready count %s reached batch size %s. Ingestion flagged.",
            log_prefix(),
            ready_count,
            BATCH_SIZE,
        )
        return True
    return False


def bulk_fetch_sold_units(cursor, product_ids: Sequence[int]) -> Dict[int, Dict[str, int]]:
    if not product_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(product_ids))
    end_dt = datetime.utcnow()
    start_7d = end_dt - timedelta(days=7)
    start_30d = end_dt - timedelta(days=30)
    start_90d = end_dt - timedelta(days=90)

    cursor.execute(
        f"""
        SELECT
            product_id,
            SUM(CASE WHEN created_dt >= %s AND created_dt <= %s THEN COALESCE(line_item_quantity, 0) ELSE 0 END) AS sold_units_7d,
            SUM(CASE WHEN created_dt >= %s AND created_dt <= %s THEN COALESCE(line_item_quantity, 0) ELSE 0 END) AS sold_units_30d,
            SUM(CASE WHEN created_dt >= %s AND created_dt <= %s THEN COALESCE(line_item_quantity, 0) ELSE 0 END) AS sold_units_90d
        FROM shopify_orders
        WHERE created_dt >= %s
          AND created_dt <= %s
          AND product_id IN ({placeholders})
        GROUP BY product_id
        """,
        (
            start_7d,
            end_dt,
            start_30d,
            end_dt,
            start_90d,
            end_dt,
            start_90d,
            end_dt,
            *product_ids,
        ),
    )

    sold_units_by_product: Dict[int, Dict[str, int]] = {}
    for row in cursor.fetchall():
        sold_units_by_product[int(row["product_id"])] = {
            "sold_units_7d": int(row.get("sold_units_7d") or 0),
            "sold_units_30d": int(row.get("sold_units_30d") or 0),
            "sold_units_90d": int(row.get("sold_units_90d") or 0),
        }

    return sold_units_by_product


def calculate_domain_items(
    brand: BrandContext, source_items: Sequence[SourceInventoryItem]
) -> List[CalculatedInventoryItem]:
    product_ids = sorted({item.product_id for item in source_items})
    with get_db_cursor(brand.brand_id) as (_, cursor, _connection):
        sold_units_by_product = bulk_fetch_sold_units(cursor, product_ids)

    calculated_items: List[CalculatedInventoryItem] = []
    for item in source_items:
        sold_units = sold_units_by_product.get(
            item.product_id,
            {"sold_units_7d": 0, "sold_units_30d": 0, "sold_units_90d": 0},
        )
        calculated_items.append(
            CalculatedInventoryItem(
                shop_domain=item.shop_domain,
                product_id=item.product_id,
                product_title=item.product_title or f"Product {item.product_id}",
                variant_id=item.variant_id,
                variant_title=item.variant_title or f"Variant {item.variant_id}",
                sku=item.sku,
                inventory_quantity=item.inventory_quantity,
                sold_units_7d=sold_units["sold_units_7d"],
                sold_units_30d=sold_units["sold_units_30d"],
                sold_units_90d=sold_units["sold_units_90d"],
                drr_7d=compute_drr(sold_units["sold_units_7d"], 7),
                drr_30d=compute_drr(sold_units["sold_units_30d"], 30),
                drr_90d=compute_drr(sold_units["sold_units_90d"], 90),
            )
        )

    logger.info(
        "%s Calculation worker produced %s records for %s.",
        log_prefix(brand),
        len(calculated_items),
        brand.shop_domain,
    )
    return calculated_items


def upsert_top_products_inventory(
    brand: BrandContext, items: Sequence[CalculatedInventoryItem]
) -> int:
    if not items:
        return 0

    upsert_rows = []
    for item in items:
        doh_7d = compute_doh(item.inventory_quantity, item.drr_7d)
        doh_30d = compute_doh(item.inventory_quantity, item.drr_30d)
        doh_90d = compute_doh(item.inventory_quantity, item.drr_90d)
        upsert_rows.append(
            (
                item.product_id,
                item.product_title,
                item.variant_id,
                item.variant_title,
                item.sku,
                item.inventory_quantity,
                item.sold_units_7d,
                item.sold_units_30d,
                item.sold_units_90d,
                str(item.drr_7d),
                str(item.drr_30d),
                str(item.drr_90d),
                str(doh_7d),
                str(doh_30d),
                str(doh_90d),
            )
        )

    with get_db_cursor(brand.brand_id) as (_, cursor, connection):
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
        "%s Ingestion worker upserted %s rows into top_products_inventory.",
        log_prefix(brand),
        len(upsert_rows),
    )
    return len(upsert_rows)


def run_calculation_cycle(redis_client: redis.Redis) -> Dict[str, int]:
    domains = scan_shop_domains(redis_client, SOURCE_KEY_PREFIX)
    if not domains:
        logger.info("%s Calculation worker found no source inventory keys.", log_prefix())
        return {"domains": 0, "source_items": 0, "ready_items": 0}

    processed_domains = 0
    source_count = 0
    ready_count = 0

    for domain in domains:
        source_items = pull_changed_source_items_for_domain(redis_client, domain)
        if not source_items:
            continue

        brand = resolve_brand_for_shop_domain(domain)
        calculated_items = calculate_domain_items(brand, source_items)
        store_ready_items(redis_client, brand.shop_domain, calculated_items)
        should_trigger_ingestion(redis_client)

        processed_domains += 1
        source_count += len(source_items)
        ready_count += len(calculated_items)

    logger.info(
        "%s Calculation cycle complete. domains=%s source_items=%s ready_items=%s",
        log_prefix(),
        processed_domains,
        source_count,
        ready_count,
    )
    return {
        "domains": processed_domains,
        "source_items": source_count,
        "ready_items": ready_count,
    }


def run_ingestion_cycle(redis_client: redis.Redis, reason: str) -> Dict[str, int]:
    domains = scan_shop_domains(redis_client, READY_KEY_PREFIX)
    if not domains:
        redis_client.delete(READY_FLAG_KEY)
        redis_client.set(READY_COUNT_KEY, "0")
        logger.info("%s Ingestion worker found no ready keys (%s).", log_prefix(), reason)
        return {"domains": 0, "ready_items": 0, "upserted_items": 0}

    processed_domains = 0
    pulled_count = 0
    upserted_count = 0

    for domain in domains:
        ready_items = pull_ready_items_for_domain(redis_client, domain)
        if not ready_items:
            continue

        brand = resolve_brand_for_shop_domain(domain)
        upserted_count += upsert_top_products_inventory(brand, ready_items)
        processed_domains += 1
        pulled_count += len(ready_items)

    remaining_domains = scan_shop_domains(redis_client, READY_KEY_PREFIX)
    if remaining_domains:
        remaining_total = 0
        for domain in remaining_domains:
            remaining_total += redis_client.llen(f"{READY_KEY_PREFIX}:{domain}")
        redis_client.set(READY_COUNT_KEY, remaining_total)
        if remaining_total >= BATCH_SIZE:
            redis_client.set(READY_FLAG_KEY, "1")
        else:
            redis_client.delete(READY_FLAG_KEY)
    else:
        redis_client.delete(READY_FLAG_KEY)
        redis_client.set(READY_COUNT_KEY, "0")

    logger.info(
        "%s Ingestion cycle complete (%s). domains=%s pulled=%s upserted=%s",
        log_prefix(),
        reason,
        processed_domains,
        pulled_count,
        upserted_count,
    )
    return {
        "domains": processed_domains,
        "ready_items": pulled_count,
        "upserted_items": upserted_count,
    }


def calculation_worker(stop_event: threading.Event) -> None:
    redis_client = get_redis_client()
    logger.info(
        "%s calculationWorker started. interval=%s minutes",
        log_prefix(),
        CALCULATION_INTERVAL_MINUTES,
    )

    while not stop_event.is_set():
        cycle_started_at = time.time()
        try:
            run_calculation_cycle(redis_client)
        except Exception as exc:
            logger.exception("%s calculationWorker failed: %s", log_prefix(), exc)

        elapsed = time.time() - cycle_started_at
        sleep_for = max(1, CALCULATION_INTERVAL_MINUTES * 60 - int(elapsed))
        stop_event.wait(sleep_for)


def ingestion_worker(stop_event: threading.Event) -> None:
    redis_client = get_redis_client()
    next_scheduled_run_at = time.time() + max(1, SCHEDULED_WINDOW_MINUTES * 60)
    logger.info(
        "%s ingestionWorker started. scheduled_window=%s minutes batch_size=%s",
        log_prefix(),
        SCHEDULED_WINDOW_MINUTES,
        BATCH_SIZE,
    )

    while not stop_event.is_set():
        try:
            flagged = str(redis_client.get(READY_FLAG_KEY) or "").strip() == "1"
            now_ts = time.time()
            scheduled_due = now_ts >= next_scheduled_run_at

            if flagged:
                run_ingestion_cycle(redis_client, reason="batch-threshold")
                next_scheduled_run_at = time.time() + max(1, SCHEDULED_WINDOW_MINUTES * 60)
            elif scheduled_due:
                run_ingestion_cycle(redis_client, reason="scheduled-window")
                next_scheduled_run_at = time.time() + max(1, SCHEDULED_WINDOW_MINUTES * 60)
        except Exception as exc:
            logger.exception("%s ingestionWorker failed: %s", log_prefix(), exc)

        stop_event.wait(max(1, INGESTION_POLL_SECONDS))


def validate_environment() -> None:
    missing = []
    if not REDIS_URI:
        missing.append("REDIS_URI")
    if not GET_BRANDS_API:
        missing.append("GET_BRANDS_API")
    if not PIPELINE_AUTH_HEADER:
        missing.append("PIPELINE_AUTH_HEADER")
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


def main() -> None:
    validate_environment()
    load_brand_context_cache(force=True)

    stop_event = threading.Event()
    workers = [
        threading.Thread(target=calculation_worker, args=(stop_event,), daemon=True),
        threading.Thread(target=ingestion_worker, args=(stop_event,), daemon=True),
    ]

    for worker in workers:
        worker.start()

    logger.info("%s Inventory Redis worker service started.", log_prefix())

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("%s Shutdown signal received.", log_prefix())
        stop_event.set()
        for worker in workers:
            worker.join(timeout=5)
        logger.info("%s Inventory Redis worker service stopped.", log_prefix())


if __name__ == "__main__":
    main()
