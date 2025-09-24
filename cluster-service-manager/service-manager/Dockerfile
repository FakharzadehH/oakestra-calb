FROM python:3.10-slim-bookworm
LABEL org.opencontainers.image.source=https://github.com/oakestra/oakestra-net

# Python runtime quality-of-life
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System dependencies for building any missing wheels
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first for better layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app

# Default environment (can be overridden by compose/env)
ENV FLASK_ENV=development \
    FLASK_DEBUG=TRUE \
    MY_PORT=10200 \
    MQTT_BROKER_PORT=10003 \
    MQTT_BROKER_URL=localhost \
    ROOT_SERVICE_MANAGER_URL=localhost \
    ROOT_SERVICE_MANAGER_PORT=10100 \
    SYSTEM_MANAGER_URL=localhost \
    SYSTEM_MANAGER_PORT=10000 \
    CLUSTER_MONGO_URL=localhost \
    CLUSTER_MONGO_PORT=10207

EXPOSE 10200

CMD ["python", "-u", "service_manager.py"]
