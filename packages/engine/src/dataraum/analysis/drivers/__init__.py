"""Driver discovery (DAT-545) — rank dimensions by information gain for a measure.

A deterministic, on-demand engine: given a numeric measure and the catalog's
grain-safe candidate dimensions, a greedy variance-reduction tree finds which
dimensions and which slices most explain the measure's variation — gated by a
within-dataset permutation null so it surfaces real drivers, not noise.

Vertical-agnostic by construction: the ranking is ordinal (within the dataset
received) and the noise gate is built from the same data, so there is no global
threshold and nothing finance-specific. Built on the DAT-536 dimension catalog +
DAT-537 alias/hierarchy edges; validated by the DAT-544 kill-gate spike.

This package is the ENGINE only (pure, in-memory result) — persistence, caching,
and agent wiring are DAT-546+.
"""
