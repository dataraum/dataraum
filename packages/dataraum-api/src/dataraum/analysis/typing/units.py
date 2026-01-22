"""Unit detection using Pint.

This module detects units in VARCHAR string values (e.g., "100 kg", "$1,234.56").
It extracts both the numeric magnitude and the unit, which helps with type inference.

Units are detected on VARCHAR values BEFORE type conversion. If a value like
"100 kg" is detected, it tells us:
1. The column can be converted to numeric (DOUBLE)
2. The column has an associated unit (kg) for semantic context
"""

from dataclasses import dataclass

import pint

# Initialize Pint with currency support
ureg: pint.UnitRegistry[float] = pint.UnitRegistry(
    preprocessors=[
        lambda s: s.replace("$", "USD "),
        lambda s: s.replace("€", "EUR "),
        lambda s: s.replace("£", "GBP "),
        lambda s: s.replace("¥", "JPY "),
    ]
)

# Define custom currency units
ureg.define("USD = [currency]")
ureg.define("EUR = [currency]")
ureg.define("GBP = [currency]")
ureg.define("JPY = [currency]")
ureg.define("CHF = [currency]")


@dataclass
class UnitDetectionResult:
    """Result of unit detection."""

    unit: str
    confidence: float


def _should_try_pint(value_str: str) -> bool:
    """Determine if a string is worth sending to Pint for parsing.

    This optimization avoids expensive Pint calls on obvious non-unit values.

    Args:
        value_str: String value to check

    Returns:
        True if value should be parsed with Pint
    """
    val_lower = value_str.lower()

    # Skip obvious non-unit text
    if len(value_str) > 50:  # Very long strings unlikely to be units
        return False

    # Must contain numbers to be a unit expression
    has_numbers = any(c.isdigit() for c in value_str)
    if not has_numbers:
        return False

    # Check for known unit patterns or operators
    has_operators = any(op in value_str for op in ["/", "*", "+", "-"])
    has_unit_patterns = any(
        unit in val_lower
        for unit in [
            "kg",
            "lb",
            "oz",
            "g",
            "m",
            "ft",
            "in",
            "cm",
            "mm",
            "km",
            "mile",
            "mph",
            "kph",
            "l",
            "gal",
            "ml",
            "sec",
            "min",
            "hr",
            "hour",
            "day",
            "week",
            "month",
            "year",
            "usd",
            "eur",
            "gbp",
            "chf",
            "jpy",
            "%",
            "percent",
            "degree",
            "celsius",
            "fahrenheit",
            "$",
            "€",
            "£",
            "¥",
        ]
    )

    return has_operators or has_unit_patterns or len(value_str) <= 16


def detect_unit(values: list[str], sample_size: int = 100) -> UnitDetectionResult | None:
    """Detect units in a list of VARCHAR values using Pint.

    This function:
    1. Filters values that likely contain units (optimization)
    2. Attempts to parse with Pint to extract unit information
    3. Returns the most common unit with confidence score

    Note: This works on VARCHAR values BEFORE numeric conversion.
    If Pint successfully parses a value, it means:
    - The value can be converted to numeric (the magnitude)
    - The value has an associated unit (e.g., "kg", "USD")

    Args:
        values: List of string values to analyze
        sample_size: Number of values to sample (default 100)

    Returns:
        UnitDetectionResult if a unit is detected, None otherwise
    """

    if not values:
        return None

    detected_units: dict[str, int] = {}
    successfully_parsed = 0

    # Sample values to avoid processing too many
    sample = values[:sample_size] if len(values) > sample_size else values

    for value in sample:
        str_value = str(value).strip()

        # Optimization: skip values unlikely to contain units
        if not _should_try_pint(str_value):
            continue

        try:
            # Try to parse as a unit expression
            quantity = ureg.parse_expression(str_value)

            # Check if we got a Quantity (has units) vs just a number
            if isinstance(quantity, ureg.Quantity):
                # Extract unit as string
                unit_str = str(quantity.units)
                detected_units[unit_str] = detected_units.get(unit_str, 0) + 1
                successfully_parsed += 1

        except (pint.UndefinedUnitError, pint.DimensionalityError, AttributeError, ValueError):
            # Value doesn't have valid units, skip
            continue

    if not detected_units:
        return None

    # Find most common unit
    most_common_unit = max(detected_units, key=detected_units.get)  # type: ignore[arg-type]

    # Confidence is based on:
    # - How many values had this unit vs total values that had any unit
    # - How many values successfully parsed vs total sample
    unit_consistency = detected_units[most_common_unit] / successfully_parsed
    parse_rate = successfully_parsed / len(sample)
    confidence = (unit_consistency + parse_rate) / 2.0

    if confidence > 0.5:
        return UnitDetectionResult(unit=most_common_unit, confidence=confidence)

    return None
