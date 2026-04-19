FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY base/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Strip gas-station routes at build time (same pattern as standalone Dockerfile)
COPY grocery/standalone/strip_gas_station.py /tmp/strip_gas_station.py
COPY base/api/main.py /tmp/api_src.py
RUN python /tmp/strip_gas_station.py /tmp/api_src.py main.py \
 && rm /tmp/strip_gas_station.py /tmp/api_src.py

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
