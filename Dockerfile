FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8050 \
    GUNICORN_WORKERS=1 \
    GUNICORN_TIMEOUT=180 \
    GUNICORN_GRACEFUL_TIMEOUT=30 \
    GUNICORN_MAX_REQUESTS=1000 \
    GUNICORN_MAX_REQUESTS_JITTER=100

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY inventory_trigger_api.py .
COPY top_product_sales_worker.py .
COPY gunicorn.conf.py .

EXPOSE 8050

CMD ["gunicorn", "-c", "gunicorn.conf.py", "inventory_trigger_api:app"]
