"""Catalogue-semantics authoring (DAT-823).

The begin_session phase that authors the workspace catalogue's BUSINESS reading
once the structure is settled: after ``semantic_per_table`` confirmed the
relationships, ``enriched_views`` composed the fact×dimension views, and
``slicing`` resolved the dimension identities. It owns everything that needs the
composed catalogue to argue: every column's :class:`ColumnConcept` row (meaning,
unit source, derived-formula hypothesis) and each table's business reading
(``TableEntity.detected_entity_type`` + ``description``).

The per-table tier keeps the structural half (relationship confirmation,
table_role, grain, time/identity columns); this package authors the semantics at
the horizon where the evidence for them exists.
"""
