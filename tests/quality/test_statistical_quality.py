"""Tests for statistical quality assessment (Phase 1)."""

import numpy as np
import pytest
from scipy import stats

from dataraum_context.profiling.models import BenfordAnalysis


class TestBenfordLaw:
    """Test Benford's Law compliance checking."""

    def test_benford_expected_distribution(self):
        """Test that Benford's Law expected distribution is correct."""
        expected = np.log10(1 + 1 / np.arange(1, 10))

        # Verify the first few values
        assert abs(expected[0] - 0.301) < 0.001  # P(first digit = 1)
        assert abs(expected[1] - 0.176) < 0.001  # P(first digit = 2)
        assert abs(expected[8] - 0.046) < 0.001  # P(first digit = 9)

        # Verify sum is 1.0
        assert abs(sum(expected) - 1.0) < 0.001

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


class TestKSTest:
    """Test Kolmogorov-Smirnov test for distribution stability."""

    def test_ks_same_distribution(self):
        """Test KS test with samples from same distribution."""
        np.random.seed(42)
        sample1 = np.random.normal(0, 1, size=1000)
        sample2 = np.random.normal(0, 1, size=1000)

        ks_statistic, p_value = stats.ks_2samp(sample1, sample2)

        # Should NOT reject null hypothesis (distributions are same)
        assert p_value > 0.01, f"Same distributions should have p > 0.01, got {p_value}"

    def test_ks_different_distributions(self):
        """Test KS test with samples from different distributions."""
        np.random.seed(42)
        sample1 = np.random.normal(0, 1, size=1000)
        sample2 = np.random.normal(5, 1, size=1000)  # Different mean

        ks_statistic, p_value = stats.ks_2samp(sample1, sample2)

        # Should reject null hypothesis (distributions are different)
        assert p_value < 0.01, f"Different distributions should have p < 0.01, got {p_value}"


@pytest.mark.skipif(
    not pytest.importorskip("sklearn", reason="scikit-learn not installed"),
    reason="Requires scikit-learn",
)
class TestIsolationForest:
    """Test Isolation Forest anomaly detection."""

    def test_isolation_forest_detects_outliers(self):
        """Test that Isolation Forest can detect obvious outliers."""
        from sklearn.ensemble import IsolationForest

        np.random.seed(42)
        # Normal data
        normal_data = np.random.normal(0, 1, size=(1000, 1))
        # Outliers
        outliers = np.array([[10], [15], [-10], [-15]])

        data = np.vstack([normal_data, outliers])

        iso_forest = IsolationForest(contamination=0.01, random_state=42)
        predictions = iso_forest.fit_predict(data)

        # -1 = outlier, 1 = inlier
        outlier_mask = predictions == -1
        outlier_count = np.sum(outlier_mask)

        # Should detect some outliers
        assert outlier_count > 0, "Should detect at least some outliers"
        assert outlier_count < 50, "Should not flag too many as outliers"


class TestPydanticModels:
    """Test Pydantic models for statistical quality."""

    def test_benford_analysis_model(self):
        """Test BenfordAnalysis Pydantic model."""
        result = BenfordAnalysis(
            chi_square=10.5,
            p_value=0.15,
            is_compliant=True,
            interpretation="Follows Benford's Law",
            digit_distribution={"1": 0.301, "2": 0.176, "3": 0.125},
        )

        assert result.chi_square == 10.5
        assert result.p_value == 0.15
        assert result.is_compliant is True
        assert "Benford" in result.interpretation
        assert result.digit_distribution["1"] == 0.301
