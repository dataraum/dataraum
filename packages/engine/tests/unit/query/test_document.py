"""Tests for QueryDocument model."""

from dataraum.query.document import QueryAssumptionData, QueryDocument, SQLStep


class TestQueryDocument:
    """Tests for QueryDocument dataclass."""

    def test_to_dict(self):
        """Document converts to dictionary."""
        doc = QueryDocument(
            summary="Test summary.",
            steps=[
                SQLStep(step_id="s1", sql="SQL", description="Desc"),
            ],
            final_sql="FINAL SQL",
            column_mappings={"a": "b"},
            assumptions=[
                QueryAssumptionData(
                    dimension="d",
                    target="t",
                    assumption="a",
                    basis="inferred",
                    confidence=0.5,
                )
            ],
        )

        d = doc.to_dict()

        assert d["summary"] == "Test summary."
        assert len(d["steps"]) == 1
        assert d["steps"][0]["step_id"] == "s1"
        assert d["final_sql"] == "FINAL SQL"
        assert d["column_mappings"]["a"] == "b"
        assert len(d["assumptions"]) == 1
