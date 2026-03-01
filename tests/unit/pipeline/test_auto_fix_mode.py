"""Tests for auto_fix gate mode."""

from dataraum.pipeline.runner import GateMode


class TestAutoFixMode:
    def test_auto_fix_is_valid_gate_mode(self):
        mode = GateMode("auto_fix")
        assert mode == GateMode.AUTO_FIX

    def test_all_gate_modes(self):
        assert GateMode.SKIP.value == "skip"
        assert GateMode.PAUSE.value == "pause"
        assert GateMode.FAIL.value == "fail"
        assert GateMode.AUTO_FIX.value == "auto_fix"

    def test_gate_mode_from_string(self):
        assert GateMode("skip") == GateMode.SKIP
        assert GateMode("pause") == GateMode.PAUSE
        assert GateMode("fail") == GateMode.FAIL
        assert GateMode("auto_fix") == GateMode.AUTO_FIX
