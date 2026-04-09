"""Tests for the Python API (Context class and result wrappers)."""

from __future__ import annotations

from dataraum.context import (
    ContractResultWrapper,
    EntropyResultWrapper,
    RunResultWrapper,
)


class TestEntropyResultWrapper:
    def test_repr_with_data(self) -> None:
        wrapper = EntropyResultWrapper(
            {
                "source": "test",
                "overall_readiness": "ready",
                "entropy_score": 0.15,
                "dimension_scores": {"structural.types": 0.1},
                "columns": [{"table": "t", "column": "c", "explanation": "ok"}],
            }
        )
        assert "ready" in repr(wrapper).lower()
        assert "0.150" in repr(wrapper)

    def test_repr_html_with_data(self) -> None:
        wrapper = EntropyResultWrapper(
            {
                "source": "test",
                "overall_readiness": "investigate",
                "entropy_score": 0.35,
                "dimension_scores": {"semantic.role": 0.2},
                "columns": [],
            }
        )
        html = wrapper._repr_html_()
        assert "INVESTIGATE" in html
        assert "0.350" in html
        assert "semantic.role" in html

    def test_repr_html_error(self) -> None:
        wrapper = EntropyResultWrapper({"error": "No sources found"})
        html = wrapper._repr_html_()
        assert "No sources found" in html

    def test_dict_access(self) -> None:
        wrapper = EntropyResultWrapper({"source": "test", "entropy_score": 0.1})
        assert wrapper["source"] == "test"
        assert wrapper.get("missing", "default") == "default"
        assert "source" in wrapper

    def test_keys(self) -> None:
        wrapper = EntropyResultWrapper({"source": "test", "entropy_score": 0.1})
        assert "source" in wrapper.keys()


class TestContractResultWrapper:
    def test_repr_pass(self) -> None:
        wrapper = ContractResultWrapper(
            {
                "contract": "aggregation_safe",
                "is_compliant": True,
                "overall_score": 0.12,
                "dimension_scores": {},
                "violations": [],
                "warnings": [],
            }
        )
        assert "PASS" in repr(wrapper)
        assert "aggregation_safe" in repr(wrapper)

    def test_repr_html_fail_with_violations(self) -> None:
        wrapper = ContractResultWrapper(
            {
                "contract": "regulatory",
                "is_compliant": False,
                "overall_score": 0.55,
                "dimension_scores": {"structural": 0.3},
                "violations": [{"details": "Missing audit trail"}],
                "warnings": [],
            }
        )
        html = wrapper._repr_html_()
        assert "FAIL" in html
        assert "Missing audit trail" in html
        assert "structural" in html

    def test_repr_html_error(self) -> None:
        wrapper = ContractResultWrapper({"error": "Contract not found: bad"})
        assert "Contract not found" in wrapper._repr_html_()

    def test_dict_access(self) -> None:
        wrapper = ContractResultWrapper({"contract": "test", "is_compliant": True})
        assert wrapper["contract"] == "test"
        assert "contract" in wrapper


class TestRunResultWrapper:
    def test_success(self) -> None:
        wrapper = RunResultWrapper(
            {
                "success": True,
                "phases_completed": ["import", "typing"],
                "phases_failed": [],
            }
        )
        assert wrapper.success is True
        assert "success" in repr(wrapper)
        assert "2 phases" in repr(wrapper)

    def test_failure(self) -> None:
        wrapper = RunResultWrapper(
            {
                "success": False,
                "phases_completed": ["import"],
                "phases_failed": ["typing"],
                "error": "Type inference failed",
            }
        )
        assert wrapper.success is False

        html = wrapper._repr_html_()
        assert "Failed" in html
        assert "Type inference failed" in html

    def test_dict_access(self) -> None:
        wrapper = RunResultWrapper({"success": True})
        assert wrapper["success"] is True
        assert "success" in wrapper


class TestContextImport:
    def test_context_importable(self) -> None:
        from dataraum import Context

        assert Context is not None

    def test_wrappers_importable(self) -> None:
        from dataraum.context import (
            ContractResultWrapper,
            EntropyResultWrapper,
            QueryResultWrapper,
            RunResultWrapper,
            SourcesAccessor,
        )

        assert all(
            cls is not None
            for cls in [
                EntropyResultWrapper,
                ContractResultWrapper,
                RunResultWrapper,
                QueryResultWrapper,
                SourcesAccessor,
            ]
        )
