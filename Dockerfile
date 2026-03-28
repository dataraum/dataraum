FROM python:3.14-slim

LABEL org.opencontainers.image.source="https://github.com/dataraum/dataraum"
LABEL org.opencontainers.image.description="DataRaum MCP Server — rich metadata context for AI analytics"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# System deps for DuckDB and scientific stack
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy everything needed for the build
COPY pyproject.toml uv.lock README.md ./
COPY src/ src/
COPY config/ config/

# Install production deps + package in one layer
RUN uv sync --no-dev --frozen

# Note: PYTHON_GIL=0 is only for free-threaded builds (3.14t).
# The slim image uses standard CPython, so we leave it unset.

# Default workspace inside container — mount a volume to persist across runs
ENV DATARAUM_HOME=/workspace
VOLUME /workspace

# Mount your data here (read-only is fine)
# Usage: docker run -v /path/to/csvs:/sources:ro ...
# Then: add_source with path "/sources/my-file.csv"
VOLUME /sources

# MCP server uses stdio transport
ENTRYPOINT ["/app/.venv/bin/dataraum-mcp"]
