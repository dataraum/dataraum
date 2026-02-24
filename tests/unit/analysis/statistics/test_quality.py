"""Tests for statistical quality assessment (Phase 1)."""

import numpy as np
from scipy import stats


class TestBenfordLaw:
    """Test Benford's Law compliance checking."""

    def test_benford_compliant_data(self):
        """Test with data that should follow Benford's Law."""
        # Generate Benford-compliant data (e.g., Fibonacci numbers)
        fibonacci = [1, 1]
        for _ in range(500):
            fibonacci.append(fibonacci[-1] + fibonacci[-2])

        # Extract first digits
        first_digits = np.array([int(str(x)[0]) for x in fibonacci[2:]])  # Skip first two
        observed_counts = np.bincount(first_digits, minlength=10)[1:]
        expected_freq = np.log10(1 + 1 / np.arange(1, 10))

        # Chi-square test
        chi2, p_value = stats.chisquare(observed_counts, expected_freq * len(first_digits))

        # Should pass (p_value > 0.05)
        assert p_value > 0.05, f"Fibonacci should follow Benford's Law (p={p_value})"

    def test_benford_non_compliant_data(self):
        """Test with uniformly distributed data (should not follow Benford's Law)."""
        # Uniform random numbers don't follow Benford's Law
        np.random.seed(42)
        uniform_data = np.random.randint(100, 1000, size=1000)

        first_digits = np.array([int(str(x)[0]) for x in uniform_data])
        observed_counts = np.bincount(first_digits, minlength=10)[1:]
        expected_freq = np.log10(1 + 1 / np.arange(1, 10))

        chi2, p_value = stats.chisquare(observed_counts, expected_freq * len(first_digits))

        # Should fail (p_value < 0.05)
        assert p_value < 0.05, f"Uniform data should NOT follow Benford's Law (p={p_value})"


class TestOutlierDetection:
    """Test outlier detection methods."""

    def test_iqr_outlier_detection(self):
        """Test IQR method for outlier detection."""
        # Create data with known outliers
        np.random.seed(42)
        normal_data = np.random.normal(100, 10, size=1000)
        outliers = np.array([200, 250, -50, -100])  # Clear outliers
        data = np.concatenate([normal_data, outliers])

        # Calculate IQR bounds
        q1, q3 = np.percentile(data, [25, 75])
        iqr = q3 - q1
        lower_fence = q1 - 1.5 * iqr
        upper_fence = q3 + 1.5 * iqr

        # Count outliers
        outlier_mask = (data < lower_fence) | (data > upper_fence)
        outlier_count = np.sum(outlier_mask)

        # Should detect the outliers we added
        assert outlier_count >= 4, f"Should detect at least 4 outliers, found {outlier_count}"

        # Verify the known outliers are detected
        assert 200 > upper_fence or 200 < lower_fence
        assert 250 > upper_fence or 250 < lower_fence


