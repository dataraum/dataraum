# Product target

- The product is **performance analytics** — operational performance decisions
  over prepared, entropy-scored context. Financial reporting is not a target.
- Finance exists **forward-looking only**: cash-flow forecast, DSO risk, margin
  decomposition. Reporting artifacts (trial balance, balance sheet) serve as
  generator-internal consistency checks, never as graded deliverables.
- Deliverables are graded at **decision tolerance per KPI**:
  `pipeline error + model error ≤ decision tolerance`. Reporting-grade
  exactness is not a bar anywhere; a KPI without a declared tolerance defaults
  to exactness, which is a definition bug — every KPI declares its tolerance
  where it is defined.
- Delivering a wrong number inside a claimed tolerance (`wrong_delivered`) is
  the cardinal failure metric.
- Not yet defined: which verticals beyond forward-looking finance; multi-site
  comparison; wedge industries.
