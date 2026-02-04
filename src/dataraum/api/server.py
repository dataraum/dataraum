"""API server entry point with free-threading support.

Usage:
    # Via script (recommended)
    uv run dataraum-api

    # Via uvicorn directly with free-threading
    uv run python -Xgil=0 -m uvicorn dataraum.api.main:create_app --factory --reload

    # Via this module
    uv run python -Xgil=0 -m dataraum.api.server
"""

import os
import sys
from pathlib import Path


def main() -> None:
    """Start the API server with free-threading enabled."""
    import uvicorn

    # Check if free-threading is available and enabled
    gil_disabled = hasattr(sys, "_is_gil_enabled") and not sys._is_gil_enabled()

    if not gil_disabled:
        print("Warning: GIL is enabled. For best performance, run with:")
        print("  uv run python -Xgil=0 -m dataraum.api.server")
        print()

    # Get configuration from environment
    host = os.environ.get("DATARAUM_API_HOST", "127.0.0.1")
    port = int(os.environ.get("DATARAUM_API_PORT", "8000"))
    reload = os.environ.get("DATARAUM_API_RELOAD", "false").lower() == "true"
    output_dir = os.environ.get("DATARAUM_OUTPUT_DIR", "./pipeline_output")

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    print("Starting DataRaum API server...")
    print(f"  Host: {host}:{port}")
    print(f"  Output dir: {output_dir}")
    print(f"  Free-threading: {'enabled' if gil_disabled else 'disabled'}")
    print()

    # Set output_dir for the app factory
    os.environ["DATARAUM_OUTPUT_DIR"] = output_dir

    uvicorn.run(
        "dataraum.api.main:create_app",
        factory=True,
        host=host,
        port=port,
        reload=reload,
    )


if __name__ == "__main__":
    main()
