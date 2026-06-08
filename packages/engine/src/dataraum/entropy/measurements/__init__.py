"""Adjudication-entropy measurements built on the pooling engine (ADR-0009).

Each measurement is a small module of pure witness extractors plus a
``measure_*`` entry that pools them per claim slot. The generic engine lives in
:mod:`dataraum.entropy.pooling`; everything here is per-measurement.
"""
