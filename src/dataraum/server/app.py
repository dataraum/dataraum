"""Platform FastAPI app.

L1 placeholder: exposes `/health` so the container substrate is verifiable.
Real subsystems (sessions, sources, REST API) are wired in by later lanes.
"""

from fastapi import FastAPI

app = FastAPI(title="DataRaum Control Plane", version="0.2.2")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
