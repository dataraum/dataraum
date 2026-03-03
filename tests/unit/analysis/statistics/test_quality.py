"""Tests for statistical quality assessment (Phase 1)."""

import numpy as np
from scipy import stats


class TestModifiedZScore:
    """Test Modified Z-Score outlier detection math."""

    def test_known_outliers_detected(self):
        """Modified Z-score detects known outliers in normal data."""
        np.random.seed(42)
        data = np.random.normal(100, 10, size=1000)
        outliers = np.array([300, 400, -100, -200])
        data = np.concatenate([data, outliers])

        median = np.median(data)
        mad = np.median(np.abs(data - median))
        modified_z = 0.6745 * np.abs(data - median) / mad

        # All injected outliers should exceed threshold 3.5
        outlier_mask = modified_z > 3.5
        detected = np.sum(outlier_mask)
        assert detected >= 4, f"Should detect at least 4 outliers, found {detected}"

    def test_constant_data_returns_zero_mad(self):
        """Constant data has MAD=0, no outliers possible."""
        data = np.full(100, 42.0)
        median = np.median(data)
        mad = np.median(np.abs(data - median))
        assert mad == 0.0

    def test_threshold_sensitivity(self):
        """Lower threshold detects more outliers."""
        np.random.seed(42)
        data = np.random.normal(100, 10, size=1000)
        median = np.median(data)
        mad = np.median(np.abs(data - median))
        modified_z = 0.6745 * np.abs(data - median) / mad

        count_strict = np.sum(modified_z > 3.5)
        count_loose = np.sum(modified_z > 2.5)
        assert count_loose >= count_strict

    def test_robust_to_outliers(self):
        """MAD is not inflated by outliers (unlike stddev)."""
        np.random.seed(42)
        clean = np.random.normal(100, 10, size=1000)
        dirty = np.concatenate([clean, np.array([10000, 20000, 30000])])

        mad_clean = np.median(np.abs(clean - np.median(clean)))
        mad_dirty = np.median(np.abs(dirty - np.median(dirty)))

        # MAD should barely change despite extreme outliers
        assert abs(mad_dirty - mad_clean) / mad_clean < 0.05


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
