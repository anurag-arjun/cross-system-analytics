FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "fastapi>=0.115.0" \
    "uvicorn[standard]>=0.30.0" \
    "clickhouse-connect>=0.7.0" \
    "python-dotenv>=1.0.0"

COPY api /app/api

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
