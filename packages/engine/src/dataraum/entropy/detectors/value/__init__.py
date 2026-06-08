"""Value layer entropy detectors.

Detectors for value-level uncertainty:
- Null semantics
- Outliers
- Benford's Law compliance

(temporal_drift and slice_variance were cut — DAT-442 reset. temporal_drift:
distribution drift cannot separate a shift from natural volatility on the
additive-flow columns it applied to (real drift → DAT-445's expected-variation
model). slice_variance: a between-slice k-sample test is structurally blind to
the slice-GLOBAL injections the eval creates (Δη²≈0) and saturates on the
legitimate cross-slice heterogeneity of real financial data — no grounded
statistic rescues it; proven in dataraum-eval's recorded finding. The slicing
analyzer still computes slice profiles + drift summaries for dimensional_entropy,
which reads them directly.)
"""

from dataraum.entropy.detectors.value.benford import BenfordDetector
from dataraum.entropy.detectors.value.null_semantics import NullRatioDetector
from dataraum.entropy.detectors.value.outliers import OutlierRateDetector

__all__ = [
    "BenfordDetector",
    "NullRatioDetector",
    "OutlierRateDetector",
]
