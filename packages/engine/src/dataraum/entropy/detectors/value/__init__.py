"""Value layer entropy detectors.

Detectors for value-level uncertainty:
- Null ratio
- Null-token adjudication (null_semantics — pooled witnesses)
- Slice-conditional null (DAT-473 — nulls concentrated in a slice, bias-corrected Cramér's V)
- Benford's Law compliance

(temporal_drift, slice_variance, and outlier_rate were cut — DAT-442 reset.
temporal_drift: distribution drift cannot separate a shift from natural
volatility on the additive-flow columns it applied to (real drift → DAT-445's
expected-variation model). slice_variance: a between-slice k-sample test is
structurally blind to the slice-GLOBAL injections the eval creates (Δη²≈0) and
saturates on the legitimate cross-slice heterogeneity of real financial data.
outlier_rate: absolute single-column IQR/z-score has no setting that separates
an injected outlier burst from clean financial heavy tails — log-IQR absorbs the
burst, linear IQR flags 25%+ of legitimate long-tail values; same wall, proven
in dataraum-eval's recorded finding. The profiler's outlier statistics
(iqr_outlier_ratio, outlier_detection) stay — they are consumed independently.
The slicing analyzer still computes slice profiles + drift summaries for
dimensional_entropy, which reads them directly.)
"""

from dataraum.entropy.detectors.value.benford import BenfordDetector
from dataraum.entropy.detectors.value.null_semantics import NullRatioDetector
from dataraum.entropy.detectors.value.null_token_adjudication import NullSemanticsDetector
from dataraum.entropy.detectors.value.slice_conditional_null import SliceConditionalNullDetector

__all__ = [
    "BenfordDetector",
    "NullRatioDetector",
    "NullSemanticsDetector",
    "SliceConditionalNullDetector",
]
