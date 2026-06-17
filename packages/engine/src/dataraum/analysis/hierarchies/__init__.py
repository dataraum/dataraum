"""Dimension hierarchy / functional-dependency discovery (DAT-537).

A deterministic g3 functional-dependency pass over each fact's grain-verified
enriched view: it finds drill-down hierarchies (``zip → city → state``) and 1:1
aliases among the catalog's grain-safe slice dimensions, with no LLM and no NMI.
Built on the DAT-536 dimension catalog; consumed downstream by the answer agent
(DAT-538) and the driver-tree engine (DAT-545, which uses alias collapse to
de-confound its ranking).
"""
