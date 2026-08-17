FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8050 \
    GUNICORN_WORKERS=2 \
    GUNICORN_TIMEOUT=180 \
    GUNICORN_GRACEFUL_TIMEOUT=30

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY inventory_trigger_api.py .
COPY top_product_sales_worker.py .

EXPOSE 8050

CMD ["python", "inventory_trigger_api.py"]
