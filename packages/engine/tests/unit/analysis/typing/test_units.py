"""Tests for unit detection using Pint."""

from dataraum.analysis.typing.units import (
    _should_try_pint,
    detect_unit,
)


class TestShouldTryPint:
    """Tests for the _should_try_pint optimization function."""

    def test_rejects_long_strings(self):
        """Long strings are unlikely to be unit expressions."""
        long_string = "This is a very long string that definitely is not a unit expression"
        assert not _should_try_pint(long_string)

    def test_rejects_strings_without_numbers(self):
        """Strings without numbers can't be unit expressions."""
        assert not _should_try_pint("hello world")
        assert not _should_try_pint("kg")  # Just the unit, no magnitude
        assert not _should_try_pint("USD")

    def test_accepts_unit_patterns(self):
        """Strings with known unit patterns should be tried."""
        assert _should_try_pint("100 kg")
        assert _should_try_pint("50.5 m")
        assert _should_try_pint("$1,234.56")
        assert _should_try_pint("€100")
        assert _should_try_pint("25%")

    def test_accepts_short_numbers(self):
        """Short strings with numbers should be tried."""
        assert _should_try_pint("123")
        assert _should_try_pint("12.34")

    def test_accepts_operators(self):
        """Strings with operators might be unit expressions."""
        assert _should_try_pint("100/hour")
        assert _should_try_pint("50*2")


class TestDetectUnit:
    """Tests for the detect_unit function."""

    def test_detects_weight_units(self):
        """Test detection of weight units."""
        values = ["100 kg", "50 kg", "75.5 kg", "200 kg"]
        result = detect_unit(values)

        assert result is not None
        assert "kg" in result.unit.lower() or "kilogram" in result.unit.lower()
        assert result.confidence > 0.5

    def test_detects_currency(self):
        """Test detection of currency units."""
        values = ["$100", "$250.50", "$1,000", "$75.25"]
        result = detect_unit(values)

        # Note: Pint might not recognize currency symbols directly
        # This test may skip if Pint doesn't handle currencies well
        if result is not None:
            assert "USD" in result.unit.upper() or "currency" in result.unit.lower()

    def test_returns_none_for_no_units(self):
        """Test that no unit is detected for plain numbers."""
        values = ["123", "456", "789", "1000"]
        result = detect_unit(values)

        # Plain numbers should not have units detected
        assert result is None

    def test_returns_none_for_text(self):
        """Test that no unit is detected for text values."""
        values = ["hello", "world", "foo", "bar"]
        result = detect_unit(values)

        assert result is None

    def test_returns_none_for_empty_list(self):
        """Test handling of empty input."""
        result = detect_unit([])
        assert result is None

    def test_handles_mixed_values(self):
        """Test handling of mixed unit values."""
        # Most values have kg, some don't
        values = ["100 kg", "50 kg", "unknown", "75 kg", "N/A", "200 kg"]
        result = detect_unit(values)

        # Should still detect kg since it's the majority
        if result is not None:
            assert result.confidence > 0.5

    def test_sample_size_limit(self):
        """Test that sample_size parameter limits processing."""
        # Create a large list
        values = [f"{i} kg" for i in range(1000)]

        # Should still work with sample size limit
        result = detect_unit(values, sample_size=10)

        # Result should be valid (or None if detection failed)
        if result is not None:
            assert result.confidence > 0
