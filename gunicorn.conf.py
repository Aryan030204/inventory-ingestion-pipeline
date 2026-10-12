import os

from inventory_trigger_api import start_background_workers

bind = f"0.0.0.0:{os.environ.get('PORT', '8050')}"

# Deliberately 1 by default: the app keeps per-process in-memory state (DB
# connection pools, the cached Redis client, the ingestion-run lock, pending
# calculation batches) that is not shared across processes. Running more than
# one worker would duplicate the background workers and DB/Redis connections
# started in post_fork below rather than coordinate across them.
workers = int(os.environ.get("GUNICORN_WORKERS", "1"))

timeout = int(os.environ.get("GUNICORN_TIMEOUT", "180"))
graceful_timeout = int(os.environ.get("GUNICORN_GRACEFUL_TIMEOUT", "30"))

# Periodically recycle the worker process so any slow memory growth within a
# single long-lived process gets reclaimed automatically instead of
# accumulating for the lifetime of the container.
max_requests = int(os.environ.get("GUNICORN_MAX_REQUESTS", "1000"))
max_requests_jitter = int(os.environ.get("GUNICORN_MAX_REQUESTS_JITTER", "100"))


def post_fork(server, worker):
    start_background_workers()
