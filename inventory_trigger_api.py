import base64
import json
import logging
import os
import re
import tempfile
import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import certifi
import mysql.connector
import redis
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from dotenv import load_dotenv
from mysql.connector import pooling
from flask import Flask, request, jsonify

from top_product_sales_worker import (
    DEFAULT_STORE_TIMEZONE,
    TOP_PRODUCT_SALES_CACHE_TTL_SECONDS,
    TOP_PRODUCT_SALES_PAGE_SIZE,
    TOP_PRODUCT_SALES_SCHEDULE_TIMEZONE,
    TopProductSalesRecord,
    TopProductSalesService,
)

load_dotenv()


def _is_truthy_env(value: Optional[str]) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


TEST_MODE = _is_truthy_env(os.environ.get("TEST_MODE"))

if TEST_MODE:
    load_dotenv(".env.local", override=False)

app = Flask(__name__)

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
CALCULATION_BATCH_WINDOW_SECONDS = int(
    os.environ.get("CALCULATION_BATCH_WINDOW_SECONDS", "30")
)
CALCULATION_BATCH_MAX_ITEMS = int(
    os.environ.get("CALCULATION_BATCH_MAX_ITEMS", "100")
)
CALCULATE_30D = _is_truthy_env(os.environ.get("30DAYS_CALC"))

SOURCE_KEY_PREFIX = os.environ.get("INVENTORY_SOURCE_PREFIX", "product_inventory")
READY_KEY_PREFIX = os.environ.get("INVENTORY_READY_PREFIX", "inventory_calc_ready")
READY_FLAG_KEY = os.environ.get("INVENTORY_READY_FLAG_KEY", "inventory_ingestion_ready")
READY_COUNT_KEY = os.environ.get("INVENTORY_READY_COUNT_KEY", "inventory_calc_ready_count")
DEFAULT_SHOPIFY_API_VERSION = "2025-10"

_pool_lock = threading.Lock()
_db_pools: Dict[int, Dict[str, Any]] = {}
_brand_cache_lock = threading.Lock()
_brand_context_by_shop_domain: Dict[str, "BrandContext"] = {}
_brand_cache_loaded_at = 0.0
_pending_batches_lock = threading.Lock()
_pending_batches_by_domain: Dict[str, "PendingCalculationBatch"] = {}


@dataclass(frozen=True)
class BrandContext:
    brand_id: int
    brand_tag: str
    brand_name: str
    database: str
    shop_domain: str
    shop_name: str
    access_token: str
    api_version: str
    store_timezone: str


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
    drr_7d: Decimal
    drr_30d: Decimal


@dataclass
class PendingCalculationBatch:
    first_added_at: float
    items: List[SourceInventoryItem]


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


def _get_local_brand_indices() -> List[int]:
    indices: List[int] = []
    total_raw = os.environ.get("TOTAL_CONFIG_COUNT", "").strip()
    if total_raw.isdigit():
        for idx in range(int(total_raw)):
            if os.environ.get(f"BRAND_ID_{idx}"):
                indices.append(idx)
        return indices

    for key in os.environ.keys():
        match = re.match(r"^BRAND_ID_(\d+)$", key)
        if match:
            indices.append(int(match.group(1)))
    return sorted(set(indices))


def _get_local_brand_index(brand_id: int) -> int:
    for idx in _get_local_brand_indices():
        raw_id = os.environ.get(f"BRAND_ID_{idx}")
        if raw_id and int(raw_id) == brand_id:
            return idx
    raise ValueError(f"Brand config not found for brand_id={brand_id} in .env.local")


def _pick_config_value(config: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = config.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _normalize_store_timezone(raw_value: Any, brand_label: str) -> str:
    candidate = str(raw_value or "").strip()
    if candidate:
        try:
            ZoneInfo(candidate)
            return candidate
        except Exception:
            logger.warning(
                "%s Invalid store timezone %r. Falling back to %s.",
                brand_label,
                candidate,
                DEFAULT_STORE_TIMEZONE,
            )
    else:
        logger.warning(
            "%s Store timezone missing. Falling back to %s.",
            brand_label,
            DEFAULT_STORE_TIMEZONE,
        )
    return DEFAULT_STORE_TIMEZONE


def fetch_active_brands() -> Dict[str, str]:
    if TEST_MODE:
        active_raw = os.environ.get("ACTIVE_BRAND_IDS", "").strip()
        active_ids = {
            value.strip()
            for value in active_raw.split(",")
            if value.strip()
        }
        brand_map: Dict[str, str] = {}
        for idx in _get_local_brand_indices():
            brand_id = (os.environ.get(f"BRAND_ID_{idx}") or "").strip()
            if not brand_id:
                continue
            if active_ids and brand_id not in active_ids:
                continue
            shop_name = (os.environ.get(f"SHOP_NAME_{idx}") or "").strip()
            brand_map[brand_id] = shop_name
        return brand_map

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
    if TEST_MODE:
        idx = _get_local_brand_index(brand_id)
        return {
            "brand_tag": (os.environ.get(f"BRAND_TAG_{idx}") or str(brand_id)).strip(),
            "brand_name": (os.environ.get(f"BRAND_NAME_{idx}") or str(brand_id)).strip(),
            "db_database": (os.environ.get(f"DB_DATABASE_{idx}") or "").strip(),
            "shop_name": (os.environ.get(f"SHOP_NAME_{idx}") or "").strip(),
            "access_token": (os.environ.get(f"ACCESS_TOKEN_{idx}") or "").strip(),
            "api_version": (os.environ.get(f"API_VERSION_{idx}") or "").strip(),
            "store_timezone": (
                os.environ.get(f"STORE_TIMEZONE_{idx}")
                or os.environ.get(f"TIMEZONE_{idx}")
                or ""
            ).strip(),
            "db_host": (os.environ.get(f"DB_HOST_{idx}") or "").strip(),
            "db_user": (os.environ.get(f"DB_USER_{idx}") or "").strip(),
            "db_password": (os.environ.get(f"DB_PASSWORD_{idx}") or "").strip(),
            "port": int((os.environ.get(f"DB_PORT_{idx}") or "3306").strip()),
        }

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


def normalize_shop_name(shop_name: str) -> str:
    normalized = normalize_shop_domain(shop_name)
    if normalized.endswith(".myshopify.com"):
        return normalized[: -len(".myshopify.com")]
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
            brand_label = f"[{config.get('brand_name', brand_id)}]"
            brand = BrandContext(
                brand_id=brand_id,
                brand_tag=(config.get("brand_tag") or str(brand_id)).lower(),
                brand_name=config.get("brand_name", str(brand_id)),
                database=config.get("db_database") or config.get("brand_tag") or str(brand_id),
                shop_domain=build_shop_domain(config.get("shop_name", "").strip()),
                shop_name=normalize_shop_name(config.get("shop_name", "").strip()),
                access_token=decrypt_value(config.get("access_token", "")),
                api_version=_pick_config_value(
                    config,
                    "api_version",
                    "shopify_api_version",
                )
                or DEFAULT_SHOPIFY_API_VERSION,
                store_timezone=_normalize_store_timezone(
                    _pick_config_value(
                        config,
                        "store_timezone",
                        "timezone",
                        "time_zone",
                        "shop_timezone",
                        "shop_time_zone",
                    ),
                    brand_label,
                ),
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
    brand_label = f"[{config.get('brand_name', brand_id)}]"

    return BrandContext(
        brand_id=brand_id,
        brand_tag=(config.get("brand_tag") or str(brand_id)).lower(),
        brand_name=config.get("brand_name", str(brand_id)),
        database=config.get("db_database") or config.get("brand_tag") or str(brand_id),
        shop_domain=build_shop_domain(config.get("shop_name", "").strip()),
        shop_name=normalize_shop_name(config.get("shop_name", "").strip()),
        access_token=decrypt_value(config.get("access_token", "")),
        api_version=_pick_config_value(
            config,
            "api_version",
            "shopify_api_version",
        )
        or DEFAULT_SHOPIFY_API_VERSION,
        store_timezone=_normalize_store_timezone(
            _pick_config_value(
                config,
                "store_timezone",
                "timezone",
                "time_zone",
                "shop_timezone",
                "shop_time_zone",
            ),
            brand_label,
        ),
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
            "drr_7d": str(item.drr_7d),
            "drr_30d": str(item.drr_30d),
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
        sold_units_7d=int(raw_doc.get("sold_units_7d") or 0),
        sold_units_30d=int(raw_doc.get("sold_units_30d") or 0),
        drr_7d=Decimal(str(raw_doc.get("drr_7d") or "0")),
        drr_30d=Decimal(str(raw_doc.get("drr_30d") or "0")),
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


def get_active_brands() -> List[BrandContext]:
    cache = load_brand_context_cache()
    unique_by_brand_id: Dict[int, BrandContext] = {}
    for brand in cache.values():
        unique_by_brand_id[brand.brand_id] = brand
    return [unique_by_brand_id[key] for key in sorted(unique_by_brand_id)]


TOP_PRODUCT_SALES_SERVICE = TopProductSalesService(
    logger=logger,
    log_prefix=log_prefix,
    normalize_shop_domain=normalize_shop_domain,
    extract_numeric_gid=extract_numeric_gid,
    quantize=quantize,
    get_active_brands=get_active_brands,
    get_redis_client=get_redis_client,
)


def _format_shopifyql_table(table_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    return TOP_PRODUCT_SALES_SERVICE.format_shopifyql_table(table_data)


def _normalize_utc_datetime(now_utc: Optional[datetime] = None) -> datetime:
    return TOP_PRODUCT_SALES_SERVICE.normalize_utc_datetime(now_utc)


def _get_brand_today_and_yesterday(
    brand: BrandContext, now_utc: Optional[datetime] = None
) -> Tuple[str, str]:
    return TOP_PRODUCT_SALES_SERVICE.get_brand_today_and_yesterday(brand, now_utc)


def get_current_ist_date(now_utc: Optional[datetime] = None) -> str:
    return TOP_PRODUCT_SALES_SERVICE.get_current_ist_date(now_utc)


def get_next_top_product_sales_run_at(now_utc: Optional[datetime] = None) -> datetime:
    return TOP_PRODUCT_SALES_SERVICE.get_next_top_product_sales_run_at(now_utc)


def build_top_product_sales_shopifyql(
    yesterday_date: str, today_date: str, limit: int = TOP_PRODUCT_SALES_PAGE_SIZE, offset: int = 0
) -> str:
    return TOP_PRODUCT_SALES_SERVICE.build_top_product_sales_shopifyql(
        yesterday_date,
        today_date,
        limit=limit,
        offset=offset,
    )


def fetch_shopifyql_table_data(
    brand: BrandContext, shopifyql_query: str
) -> List[Dict[str, Any]]:
    return TOP_PRODUCT_SALES_SERVICE.fetch_shopifyql_table_data(brand, shopifyql_query)


def fetch_all_top_product_sales_rows(
    brand: BrandContext,
    yesterday_date: str,
    today_date: str,
    page_size: int = TOP_PRODUCT_SALES_PAGE_SIZE,
) -> List[Dict[str, Any]]:
    return TOP_PRODUCT_SALES_SERVICE.fetch_all_top_product_sales_rows(
        brand,
        yesterday_date,
        today_date,
        page_size=page_size,
    )


def normalize_top_product_sales_rows(
    rows: Sequence[Dict[str, Any]],
) -> List[TopProductSalesRecord]:
    return TOP_PRODUCT_SALES_SERVICE.normalize_top_product_sales_rows(rows)


def build_top_product_sales_cache_key(shop_domain: str) -> str:
    return TOP_PRODUCT_SALES_SERVICE.build_top_product_sales_cache_key(shop_domain)


def build_top_product_sales_payload(
    sales_date: str, records: Sequence[TopProductSalesRecord], updated_at: Optional[str] = None
) -> Dict[str, Any]:
    return TOP_PRODUCT_SALES_SERVICE.build_top_product_sales_payload(
        sales_date,
        records,
        updated_at=updated_at,
    )


def get_cached_top_product_sales_date(
    redis_client: redis.Redis, shop_domain: str
) -> Optional[str]:
    return TOP_PRODUCT_SALES_SERVICE.get_cached_top_product_sales_date(
        redis_client, shop_domain
    )


def cache_top_product_sales(
    redis_client: redis.Redis,
    shop_domain: str,
    payload: Dict[str, Any],
) -> str:
    return TOP_PRODUCT_SALES_SERVICE.cache_top_product_sales(
        redis_client,
        shop_domain,
        payload,
    )


def fetch_top_product_sales_for_brand(
    brand: BrandContext, now_utc: Optional[datetime] = None
) -> Tuple[str, List[TopProductSalesRecord]]:
    return TOP_PRODUCT_SALES_SERVICE.fetch_top_product_sales_for_brand(
        brand, now_utc=now_utc
    )


def run_top_product_sales_cycle(
    redis_client: redis.Redis, now_utc: Optional[datetime] = None
) -> Dict[str, int]:
    return TOP_PRODUCT_SALES_SERVICE.run_top_product_sales_cycle(
        redis_client, now_utc=now_utc
    )


def _variant_sales_key(product_id: Any, variant_id: Any) -> Optional[Tuple[int, int]]:
    try:
        normalized_product_id = int(product_id)
        normalized_variant_id = int(variant_id)
    except (TypeError, ValueError):
        return None

    if normalized_product_id <= 0 or normalized_variant_id <= 0:
        return None

    return normalized_product_id, normalized_variant_id


def _build_sold_units_query(
    product_ids: Sequence[int], report_end_date, calculate_30d: bool
) -> Tuple[str, Tuple[Any, ...]]:
    placeholders = ", ".join(["%s"] * len(product_ids))
    end_date_str = report_end_date.isoformat()
    start_7d = (report_end_date - timedelta(days=6)).isoformat()

    if not calculate_30d:
        sql = f"""
        WITH window_orders AS (
            SELECT
                order_id,
                product_id,
                variant_id,
                COALESCE(line_item_quantity, 0) AS line_item_quantity
            FROM shopify_orders
            WHERE created_date >= %s
              AND created_date <= %s
              AND product_id IN ({placeholders})
        ),
        gross_sales AS (
            SELECT
                product_id,
                variant_id,
                SUM(line_item_quantity) AS gross_sold_units_7d
            FROM window_orders
            GROUP BY product_id, variant_id
        ),
        reversal_orders AS (
            SELECT DISTINCT
                order_id
            FROM returns_fact
            WHERE event_date >= %s
              AND event_date <= %s
              AND event_type IN ('REFUND', 'CANCEL', 'CANCEL (RTO)')
        ),
        order_products AS (
            SELECT
                order_id,
                product_id,
                variant_id,
                SUM(line_item_quantity) AS line_item_quantity
            FROM window_orders
            GROUP BY
                order_id,
                product_id,
                variant_id
        ),
        reversed_sales AS (
            SELECT
                op.product_id,
                op.variant_id,
                SUM(op.line_item_quantity) AS reversed_units_7d
            FROM order_products op
            INNER JOIN reversal_orders ro
                ON ro.order_id = op.order_id
            GROUP BY op.product_id, op.variant_id
        )
        SELECT
            gs.product_id,
            gs.variant_id,
            gs.gross_sold_units_7d - COALESCE(rs.reversed_units_7d, 0) AS sold_units_7d,
            0 AS sold_units_30d,
            gs.gross_sold_units_7d AS gross_sold_units_7d,
            COALESCE(rs.reversed_units_7d, 0) AS reversed_units_7d,
            0 AS gross_sold_units_30d,
            0 AS reversed_units_30d
        FROM gross_sales gs
        LEFT JOIN reversed_sales rs
            ON rs.product_id = gs.product_id
           AND rs.variant_id = gs.variant_id
        """
        params = (start_7d, end_date_str, *product_ids, start_7d, end_date_str)
        return sql, params

    start_30d = (report_end_date - timedelta(days=29)).isoformat()
    sql = f"""
    WITH window_orders_30d AS (
        SELECT
            order_id,
            product_id,
            variant_id,
            created_date,
            COALESCE(line_item_quantity, 0) AS line_item_quantity
        FROM shopify_orders
        WHERE created_date >= %s
          AND created_date <= %s
          AND product_id IN ({placeholders})
    ),
    gross_sales AS (
        SELECT
            product_id,
            variant_id,
            SUM(
                CASE
                    WHEN created_date >= %s
                     AND created_date <= %s
                    THEN line_item_quantity
                    ELSE 0
                END
            ) AS gross_sold_units_7d,
            SUM(
                CASE
                    WHEN created_date >= %s
                     AND created_date <= %s
                    THEN line_item_quantity
                    ELSE 0
                END
            ) AS gross_sold_units_30d
        FROM window_orders_30d
        GROUP BY product_id, variant_id
    ),
    reversal_orders_7d AS (
        SELECT DISTINCT
            order_id
        FROM returns_fact
        WHERE event_date >= %s
          AND event_date <= %s
          AND event_type IN ('REFUND', 'CANCEL', 'CANCEL (RTO)')
    ),
    reversal_orders_30d AS (
        SELECT DISTINCT
            order_id
        FROM returns_fact
        WHERE event_date >= %s
          AND event_date <= %s
          AND event_type IN ('REFUND', 'CANCEL', 'CANCEL (RTO)')
    ),
    order_products_30d AS (
        SELECT
            order_id,
            product_id,
            variant_id,
            created_date,
            SUM(line_item_quantity) AS line_item_quantity
        FROM window_orders_30d
        GROUP BY
            order_id,
            product_id,
            variant_id,
            created_date
    ),
    reversed_sales_7d AS (
        SELECT
            op.product_id,
            op.variant_id,
            SUM(op.line_item_quantity) AS reversed_units_7d
        FROM order_products_30d op
        INNER JOIN reversal_orders_7d ro
            ON ro.order_id = op.order_id
        WHERE op.created_date >= %s
          AND op.created_date <= %s
        GROUP BY op.product_id, op.variant_id
    ),
    reversed_sales_30d AS (
        SELECT
            op.product_id,
            op.variant_id,
            SUM(op.line_item_quantity) AS reversed_units_30d
        FROM order_products_30d op
        INNER JOIN reversal_orders_30d ro
            ON ro.order_id = op.order_id
        GROUP BY op.product_id, op.variant_id
    )
    SELECT
        gs.product_id,
        gs.variant_id,
        gs.gross_sold_units_7d - COALESCE(r7.reversed_units_7d, 0) AS sold_units_7d,
        gs.gross_sold_units_30d - COALESCE(r30.reversed_units_30d, 0) AS sold_units_30d,
        gs.gross_sold_units_7d AS gross_sold_units_7d,
        COALESCE(r7.reversed_units_7d, 0) AS reversed_units_7d,
        gs.gross_sold_units_30d AS gross_sold_units_30d,
        COALESCE(r30.reversed_units_30d, 0) AS reversed_units_30d
    FROM gross_sales gs
    LEFT JOIN reversed_sales_7d r7
        ON r7.product_id = gs.product_id
       AND r7.variant_id = gs.variant_id
    LEFT JOIN reversed_sales_30d r30
        ON r30.product_id = gs.product_id
       AND r30.variant_id = gs.variant_id
    """
    params = (
        start_30d,
        end_date_str,
        *product_ids,
        start_7d,
        end_date_str,
        start_30d,
        end_date_str,
        start_7d,
        end_date_str,
        start_30d,
        end_date_str,
        start_7d,
        end_date_str,
    )
    return sql, params


def bulk_fetch_sold_units(
    cursor, variant_pairs: Sequence[Tuple[int, int]]
) -> Dict[Tuple[int, int], Dict[str, int]]:
    if not variant_pairs:
        return {}

    requested_variant_keys = set(variant_pairs)
    product_ids = sorted({product_id for product_id, _variant_id in requested_variant_keys})
    end_date = datetime.utcnow().date() - timedelta(days=1)
    sql, params = _build_sold_units_query(product_ids, end_date, CALCULATE_30D)
    cursor.execute(sql, params)

    sold_units_by_variant: Dict[Tuple[int, int], Dict[str, int]] = {}
    for row in cursor.fetchall():
        variant_key = _variant_sales_key(row.get("product_id"), row.get("variant_id"))
        if variant_key is None:
            logger.warning(
                "%s Skipping malformed sold-units row product_id=%s variant_id=%s",
                log_prefix(),
                row.get("product_id"),
                row.get("variant_id"),
            )
            continue

        if variant_key not in requested_variant_keys:
            continue

        product_id, variant_id = variant_key
        sold_units_7d = int(row.get("sold_units_7d") or 0)
        sold_units_30d = int(row.get("sold_units_30d") or 0)
        gross_units_7d = int(row.get("gross_sold_units_7d") or 0)
        reversed_units_7d = int(row.get("reversed_units_7d") or 0)
        gross_units_30d = int(row.get("gross_sold_units_30d") or 0)
        reversed_units_30d = int(row.get("reversed_units_30d") or 0)

        if reversed_units_7d > gross_units_7d or sold_units_7d < 0:
            logger.warning(
                "%s Negative or inconsistent 7d sold-units for product_id=%s variant_id=%s gross=%s reversed=%s net=%s",
                log_prefix(),
                product_id,
                variant_id,
                gross_units_7d,
                reversed_units_7d,
                sold_units_7d,
            )

        if CALCULATE_30D and (reversed_units_30d > gross_units_30d or sold_units_30d < 0):
            logger.warning(
                "%s Negative or inconsistent 30d sold-units for product_id=%s variant_id=%s gross=%s reversed=%s net=%s",
                log_prefix(),
                product_id,
                variant_id,
                gross_units_30d,
                reversed_units_30d,
                sold_units_30d,
            )

        sold_units_by_variant[variant_key] = {
            "sold_units_7d": sold_units_7d,
            "sold_units_30d": sold_units_30d,
        }

    return sold_units_by_variant


def calculate_domain_items(
    brand: BrandContext, source_items: Sequence[SourceInventoryItem]
) -> List[CalculatedInventoryItem]:
    variant_pairs: List[Tuple[int, int]] = []
    for item in source_items:
        variant_key = _variant_sales_key(item.product_id, item.variant_id)
        if variant_key is None:
            logger.warning(
                "%s Invalid inventory identifiers for sales lookup product_id=%s variant_id=%s",
                log_prefix(brand),
                item.product_id,
                item.variant_id,
            )
            continue
        variant_pairs.append(variant_key)

    unique_variant_pairs = sorted(set(variant_pairs))
    with get_db_cursor(brand.brand_id) as (_, cursor, _connection):
        sold_units_by_variant = bulk_fetch_sold_units(cursor, unique_variant_pairs)

    calculated_items: List[CalculatedInventoryItem] = []
    for item in source_items:
        variant_key = _variant_sales_key(item.product_id, item.variant_id)
        if variant_key is None:
            logger.warning(
                "%s Falling back to zero sold-units for malformed inventory row product_id=%s variant_id=%s",
                log_prefix(brand),
                item.product_id,
                item.variant_id,
            )
            sold_units = {"sold_units_7d": 0, "sold_units_30d": 0}
        else:
            sold_units = sold_units_by_variant.get(
                variant_key,
                {"sold_units_7d": 0, "sold_units_30d": 0},
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
                drr_7d=compute_drr(sold_units["sold_units_7d"], 7),
                drr_30d=compute_drr(sold_units["sold_units_30d"], 30)
                if CALCULATE_30D
                else Decimal("0.00"),
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
                0,
                str(item.drr_7d),
                str(item.drr_30d),
                "0.00",
                str(doh_7d),
                str(doh_30d),
                "0.00",
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


def _append_pending_batch(shop_domain: str, items: Sequence[SourceInventoryItem]) -> None:
    if not items:
        return

    now_ts = time.time()
    with _pending_batches_lock:
        batch = _pending_batches_by_domain.get(shop_domain)
        if batch is None:
            _pending_batches_by_domain[shop_domain] = PendingCalculationBatch(
                first_added_at=now_ts,
                items=list(items),
            )
            return

        batch.items.extend(items)


def _pull_due_pending_batch(shop_domain: str) -> Optional[List[SourceInventoryItem]]:
    now_ts = time.time()
    with _pending_batches_lock:
        batch = _pending_batches_by_domain.get(shop_domain)
        if batch is None:
            return None

        batch_age_s = now_ts - batch.first_added_at
        if (
            len(batch.items) < CALCULATION_BATCH_MAX_ITEMS
            and batch_age_s < CALCULATION_BATCH_WINDOW_SECONDS
        ):
            return None

        items = list(batch.items)
        _pending_batches_by_domain.pop(shop_domain, None)
        return items


def _has_pending_batch(shop_domain: str) -> bool:
    with _pending_batches_lock:
        return shop_domain in _pending_batches_by_domain


def _has_any_pending_batches() -> bool:
    with _pending_batches_lock:
        return bool(_pending_batches_by_domain)


def _flush_domain_batch(
    redis_client: redis.Redis, shop_domain: str, source_items: Sequence[SourceInventoryItem]
) -> Dict[str, int]:
    if not source_items:
        return {"domains": 0, "source_items": 0, "ready_items": 0}

    brand = resolve_brand_for_shop_domain(shop_domain)
    calculated_items = calculate_domain_items(brand, source_items)
    store_ready_items(redis_client, brand.shop_domain, calculated_items)
    should_trigger_ingestion(redis_client)

    return {
        "domains": 1,
        "source_items": len(source_items),
        "ready_items": len(calculated_items),
    }


def run_calculation_cycle(redis_client: redis.Redis) -> Dict[str, int]:
    domains = scan_shop_domains(redis_client, SOURCE_KEY_PREFIX)
    pending_domains: List[str] = []
    with _pending_batches_lock:
        pending_domains = list(_pending_batches_by_domain.keys())

    if not domains and not pending_domains:
        logger.info("%s Calculation worker found no source inventory keys.", log_prefix())
        return {"domains": 0, "source_items": 0, "ready_items": 0}

    processed_domains = 0
    source_count = 0
    ready_count = 0

    for domain in sorted(set(domains + pending_domains)):
        source_items = pull_changed_source_items_for_domain(redis_client, domain)
        if source_items:
            _append_pending_batch(domain, source_items)

        batch_items = _pull_due_pending_batch(domain)
        if not batch_items:
            continue

        cycle_counts = _flush_domain_batch(redis_client, domain, batch_items)
        processed_domains += cycle_counts["domains"]
        source_count += cycle_counts["source_items"]
        ready_count += cycle_counts["ready_items"]

    logger.info(
        "%s Calculation cycle complete. domains=%s source_items=%s ready_items=%s pending_domains=%s",
        log_prefix(),
        processed_domains,
        source_count,
        ready_count,
        sum(1 for domain in sorted(set(domains + pending_domains)) if _has_pending_batch(domain)),
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
        if _has_any_pending_batches():
            sleep_for = max(1, CALCULATION_BATCH_WINDOW_SECONDS - int(elapsed))
        else:
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


def top_product_sales_worker(stop_event: threading.Event) -> None:
    TOP_PRODUCT_SALES_SERVICE.top_product_sales_worker(stop_event)


def send_lifecycle_event(run_id: str, status: str) -> None:
    if status == "running":
        manager_endpoint = os.environ.get("MANAGER_STARTED_ENDPOINT", "").strip()
    elif status == "completed":
        manager_endpoint = os.environ.get("MANAGER_COMPLETED_ENDPOINT", "").strip()
    else:
        logger.warning("%s Unknown lifecycle status %s.", log_prefix(), status)
        return

    pipeline_id = os.environ.get("PIPELINE_ID", "").strip()

    if not manager_endpoint or not pipeline_id:
        logger.warning("%s Endpoint or PIPELINE_ID missing. Skipping lifecycle event %s.", log_prefix(), status)
        return

    payload = {
        "runId": run_id,
        "pipelineId": pipeline_id,
        "status": status
    }
    headers = {"Content-Type": "application/json"}
    if PIPELINE_AUTH_HEADER:
        headers["x-pipeline-key"] = PIPELINE_AUTH_HEADER

    try:
        response = requests.post(manager_endpoint, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info("%s Sent lifecycle event: %s for runId: %s", log_prefix(), status, run_id)
    except Exception as exc:
        logger.error("%s Failed to send lifecycle event %s: %s", log_prefix(), status, exc)


def _derive_error_code(exc: Exception) -> str:
    name = exc.__class__.__name__
    if not name:
        return "UNKNOWN_ERROR"
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")
    return normalized.upper() if normalized else "UNKNOWN_ERROR"


def _build_error_body(log_tail: deque, exc: Exception) -> str:
    if log_tail:
        return "\n".join(log_tail)
    fallback = str(exc).strip()
    return fallback if fallback else repr(exc)


def send_failed_event(run_id: str, error_code: str, error_body: str) -> None:
    manager_endpoint = os.environ.get("MANAGER_FAILED_ENDPOINT", "").strip()
    pipeline_id = os.environ.get("PIPELINE_ID", "").strip()

    if not manager_endpoint or not pipeline_id:
        logger.warning("%s Failed endpoint or PIPELINE_ID missing. Skipping failed event.", log_prefix())
        return

    payload = {
        "runId": run_id,
        "pipelineId": pipeline_id,
        "errorCode": error_code,
        "errorBody": error_body,
    }
    headers = {"Content-Type": "application/json"}
    if PIPELINE_AUTH_HEADER:
        headers["x-pipeline-key"] = PIPELINE_AUTH_HEADER

    try:
        response = requests.post(manager_endpoint, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info("%s Sent failed event for runId: %s", log_prefix(), run_id)
    except Exception as exc:
        logger.error("%s Failed to send failed event: %s", log_prefix(), exc)


def run_manual_trigger(run_id: str):
    log_tail: deque[str] = deque(maxlen=2)
    handler = logging.Handler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    )

    def _emit(record: logging.LogRecord) -> None:
        log_tail.append(handler.format(record))

    handler.emit = _emit  # type: ignore[assignment]
    logger.addHandler(handler)

    success = False
    logger.info("%s Manual ingestion trigger started. runId=%s", log_prefix(), run_id)
    send_lifecycle_event(run_id, "running")
    try:
        redis_client = get_redis_client()
        run_ingestion_cycle(redis_client, reason="manual-trigger")
        logger.info("%s Manual ingestion trigger completed successfully.", log_prefix())
        success = True
    except Exception as exc:
        logger.exception("%s Manual ingestion trigger failed: %s", log_prefix(), exc)
        error_code = _derive_error_code(exc)
        error_body = _build_error_body(log_tail, exc)
        send_failed_event(run_id, error_code, error_body)
    finally:
        logger.removeHandler(handler)
        handler.close()

    if success:
        send_lifecycle_event(run_id, "completed")


@app.route("/trigger", methods=["POST"])
def trigger_pipeline():
    auth_key = request.headers.get("x-pipeline-key")
    if not auth_key or auth_key != PIPELINE_AUTH_HEADER:
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    run_id = str(payload.get("runId") or "").strip()
    if not run_id:
        return jsonify({"status": "error", "message": "runId is required"}), 400

    threading.Thread(target=run_manual_trigger, args=(run_id,), daemon=True).start()
    return jsonify({"status": "success", "message": "Pipeline triggered", "runId": run_id}), 202


def validate_environment() -> None:
    missing = []
    if not REDIS_URI:
        missing.append("REDIS_URI")
    if not TEST_MODE:
        if not GET_BRANDS_API:
            missing.append("GET_BRANDS_API")
        if not PIPELINE_AUTH_HEADER:
            missing.append("PIPELINE_AUTH_HEADER")
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


def main() -> None:
    validate_environment()
    load_brand_context_cache(force=True)

    self_run = os.environ.get("SELF_RUN", "true").strip().lower() == "true"
    stop_event = threading.Event()
    workers = [
        threading.Thread(target=calculation_worker, args=(stop_event,), daemon=True),
        threading.Thread(target=top_product_sales_worker, args=(stop_event,), daemon=True),
    ]

    # Calculation worker always runs. SELF_RUN controls only ingestion scheduling.
    if self_run:
        workers.append(
            threading.Thread(target=ingestion_worker, args=(stop_event,), daemon=True)
        )

    for worker in workers:
        worker.start()

    if self_run:
        logger.info(
            "%s Inventory Redis worker service started (ingestion self-run enabled).",
            log_prefix(),
        )
    else:
        logger.info(
            "%s Inventory Redis worker service started (ingestion manual-trigger only).",
            log_prefix(),
        )

    try:
        # Start Flask server
        port = int(os.environ.get("PORT", 8050))
        app.run(host="0.0.0.0", port=port)
    except (KeyboardInterrupt, SystemExit):
        logger.info("%s Shutdown signal received.", log_prefix())
        stop_event.set()
        for worker in workers:
            worker.join(timeout=5)
        logger.info("%s Inventory Redis worker service stopped.", log_prefix())


if __name__ == "__main__":
    main()
