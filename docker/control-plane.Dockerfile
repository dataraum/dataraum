FROM python:3.14-slim

LABEL org.opencontainers.image.source="https://github.com/dataraum/dataraum"
LABEL org.opencontainers.image.description="DataRaum control plane (platform substrate)"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# System deps: gcc/g++ for any source builds; curl for the container healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ curl && \
    rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Lock + manifest first for layer caching
COPY pyproject.toml uv.lock README.md ./
COPY src/ src/
COPY config/ config/

# Production deps + package, frozen to the locked resolution
RUN uv sync --no-dev --frozen

# DuckLake local-FS data path (mounted as a named volume from docker-compose)
RUN mkdir -p /var/lib/dataraum/lake /var/lib/dataraum/sources

EXPOSE 8000

ENTRYPOINT ["/app/.venv/bin/uvicorn", "dataraum.server.app:app", "--host", "0.0.0.0", "--port", "8000"]
