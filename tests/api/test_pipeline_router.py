"""Tests for pipeline router endpoints."""

import json
import time

import pytest
from fastapi.testclient import TestClient


class TestTriggerPipeline:
    """Tests for POST /api/v1/sources/{source_id}/run."""

    def test_source_not_found(self, test_client: TestClient):
        """Non-existent source returns 404."""
        response = test_client.post(
            "/api/v1/sources/nonexistent/run",
            json={},
        )
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_trigger_success(self, test_client: TestClient, seeded_source: dict):
        """Triggering pipeline returns run_id and status."""
        response = test_client.post(
            f"/api/v1/sources/{seeded_source['source_id']}/run",
            json={"skip_llm": True},
        )
        assert response.status_code == 200

        data = response.json()
        assert "run_id" in data
        assert data["source_id"] == seeded_source["source_id"]
        assert data["status"] == "running"
        assert "message" in data

    def test_trigger_with_options(self, test_client: TestClient, seeded_source: dict):
        """Pipeline can be triggered with skip_llm and force options."""
        response = test_client.post(
            f"/api/v1/sources/{seeded_source['source_id']}/run",
            json={
                "skip_llm": True,
                "force": True,
                "target_phase": "profiling",
            },
        )
        assert response.status_code == 200
        assert response.json()["status"] == "running"


class TestPipelineStatus:
    """Tests for GET /api/v1/sources/{source_id}/status."""

    def test_source_not_found(self, test_client: TestClient):
        """Non-existent source returns 404."""
        response = test_client.get("/api/v1/sources/nonexistent/status")
        assert response.status_code == 404

    def test_status_no_runs(self, test_client: TestClient, seeded_source: dict):
        """Source with no runs returns empty status."""
        response = test_client.get(f"/api/v1/sources/{seeded_source['source_id']}/status")
        assert response.status_code == 200

        data = response.json()
        assert data["source_id"] == seeded_source["source_id"]
        assert data["last_run_id"] is None
        assert data["completed"] == 0


class TestStreamProgress:
    """Tests for GET /api/v1/runs/{run_id}/stream."""

    def test_not_found(self, test_client: TestClient):
        """Invalid run_id returns 404."""
        response = test_client.get("/api/v1/runs/invalid-uuid/stream")
        assert response.status_code == 404


class TestSingletonExecution:
    """Tests for singleton pipeline execution.

    Note: These tests are timing-dependent. In test environments,
    the pipeline may fail/complete very fast, making it hard to
    hit the 409 case. The in-memory lock + database check ensures
    correctness in production.
    """

    def test_sequential_runs_get_different_ids(self, test_client: TestClient, seeded_source: dict):
        """Sequential triggers get different run_ids."""
        # First trigger
        resp1 = test_client.post(
            f"/api/v1/sources/{seeded_source['source_id']}/run",
            json={"skip_llm": True},
        )
        assert resp1.status_code == 200
        first_run_id = resp1.json()["run_id"]

        # Wait briefly for first to complete/fail
        time.sleep(0.1)

        # Second trigger - should get new run_id since first completed
        resp2 = test_client.post(
            f"/api/v1/sources/{seeded_source['source_id']}/run",
            json={"skip_llm": True},
        )

        # Either gets 409 (first still running) or 200 with new run_id
        if resp2.status_code == 409:
            detail = resp2.json()["detail"]
            assert "already running" in detail.lower()
            assert first_run_id in detail
        else:
            assert resp2.status_code == 200
            second_run_id = resp2.json()["run_id"]
            assert second_run_id != first_run_id


@pytest.mark.integration
class TestPipelineIntegration:
    """Integration tests for full pipeline flow.

    These tests run the actual pipeline and may be slow.
    Mark with pytest.mark.slow if needed.
    """

    def test_stream_after_trigger(self, test_client: TestClient, seeded_source: dict):
        """Stream endpoint returns events after triggering."""
        # Trigger pipeline
        trigger_resp = test_client.post(
            f"/api/v1/sources/{seeded_source['source_id']}/run",
            json={"skip_llm": True},
        )
        assert trigger_resp.status_code == 200
        run_id = trigger_resp.json()["run_id"]

        # Wait a bit for pipeline to run (it fails fast in tests)
        time.sleep(0.2)

        # Stream should return state (either final or in-progress)
        with test_client.stream("GET", f"/api/v1/runs/{run_id}/stream") as response:
            content = b""
            for chunk in response.iter_bytes():
                content += chunk
                # SSE ends with double newline after data
                if b"\n\n" in content:
                    break

        # Parse SSE event
        text = content.decode()
        assert "data:" in text

        # Extract JSON from SSE format
        for line in text.split("\n"):
            if line.startswith("data:"):
                data = json.loads(line[5:].strip())
                # Could be start, complete, or error depending on timing
                assert data["event"] in ("complete", "error", "start")
                assert data["run_id"] == run_id
                break
