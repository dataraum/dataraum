# Relationships & aggregation

Two structural questions decide whether cross-table analysis is safe: how tables join, and
how a measure may be aggregated. Both are answered from the data, as pooled claims with
measured uncertainty — the same model as every other
[measurement](measurement.md).

## Relationships

A join between two tables is established in three steps, each recorded:

1. **Candidates from values.** Column pairs are proposed by value overlap — containment
   and Jaccard similarity, computed exactly (deterministic MinHash signatures above one
   million distinct values) — together with per-column uniqueness, which yields the
   candidate cardinality (one-to-one, one-to-many, many-to-many).
2. **Evaluation against the data.** Before any semantic step, each candidate is measured:
   referential integrity in both directions, orphan counts, whether the join verifies its
   claimed cardinality, and whether it introduces duplicate rows.
3. **Semantic confirmation.** An LLM reads the evaluated candidates in table context and
   confirms a subset. Confirmed relationships persist, alongside relationships you teach.

The standing claim — *do these columns join?* — is then adjudicated by a pooled
measurement with four witnesses: value overlap (the data witness), the LLM's judgment,
manual curation (a `relationship` teach), and keeper retention (a join you chose to keep).
A confident LLM witness over a weak data witness raises conflict, and the relationship is
flagged for investigation rather than silently kept or dropped.

### Composite keys

Some joins need more than one column: a referencing column plus a scoping column present
on both sides, typically a tenant key — `(account, business_id)` rather than `account`
alone. On its own, each half is wrong: the referencing column joins many-to-many across
scopes, and the scoping column joins everything to everything.

These are detected and cured in the same measured way. When a candidate's best
single-column join is many-to-many, a pre-pass tests whether fusing co-present columns
collapses the join out of many-to-many; the fused key is offered to the same LLM
confirmation step as a hint. A confirmed composite is then **minted as a surrogate key**:
both tables gain one deterministic hash column over the key's components
(`_sk__account__business_id`), and the catalog records one ordinary single-column
relationship on that pair — many-to-one, measured. A NULL in any component yields a NULL
hash, so unkeyed rows simply don't match. Downstream, a composite key is
indistinguishable from any other foreign key.

The cure is only applied when the data proves the key. A confirmed composite whose fused
join still measures many-to-many is refused — the single-column relationship stays,
flagged as a fan trap. Some dimensions are structurally unjoinable at row grain (two
rows sharing every attribute the referencing table carries); the flag is the honest
verdict there, and answer-time SQL falls back to set-grain semi-joins.

Confirmed relationships feed three consumers: **enriched join views** (grain-preserving
joins of a fact table with its dimensions — a view whose row count differs from the fact's
is dropped, because the join changed the grain), the **Model** graph's *relates* edges,
and the SQL composed at answer time.

## Aggregation

The claim: is a measure column a **flow** (a per-period movement, summable across periods)
or a **stock** (a carried-forward level, like a balance)? The distinction decides how the
column may be aggregated — summing a stock across periods double-counts it.

Two witnesses pool over the claim space {stock, flow}:

- **the LLM's independent read** of the column — name, table context, sample values,
- **structural reconciliation** — the data witness. Where a measure aggregates an event
  table, the engine compares the measure's period series `y[t]` with the per-period net
  movement `m[t]` aggregated independently from the events. A flow satisfies
  `y[t] ≈ m[t]`; a stock satisfies `Δy[t] ≈ m[t]`. Scale-free residuals decide between
  the two. The witness abstains when both residuals are large (which indicates a wrong
  anchor — wrong entity, wrong join, wrong period — rather than a verdict), when the
  series is shorter than four periods, or when too few entities agree.

The concept's declared behaviour is deliberately not a witness. The same concept
materializes as a flow in one table and a stock in another — a periodic movement column and
a period-end level column can both ground *account balance* — so the vocabulary has nothing
to declare here. Stock or flow is decided from the data.

The reconciliation exists because the alternative reads the name, and on an ambiguously
named column a name-reader is confidently wrong — calibration puts its accuracy there at
chance. Only a witness whose input is the data can dissent in that case, which is why it
carries the verdict when the two disagree: an LLM read landing on the opposite side of the
line from a reconciliation that fired is overruled rather than pooled against it. A reading
that agrees corroborates and lowers the remaining ignorance; where no lineage reconciled —
every add_source run, and any measure with no event table behind it — the LLM read stands
alone.

The pooled verdict is recorded on the column as `additive` or `point_in_time`, and
aggregation follows it: metrics and [drivers](operating-model.md#the-parts) sum a flow over
the period and take a stock at the period's level. Only the verdict is carried; a
disagreement between the two reads is logged where it is resolved, not propagated as a
doubt flag downstream. The same reconciliation pass also checks sum-consistency across fact
tables that share a slice dimension, so a measure reported by two sources is compared
per slice value and period rather than assumed to agree.

## Correcting them

The join claim accepts a teach: a `relationship` teach confirms, rejects, or adds one. Like
every teach, it enters the pool as a witness and the claim is re-adjudicated on the next run
— see [frame, ground, teach](frame-ground-teach.md).

Temporal behaviour has **no** teach, deliberately. It is decided from the data, so there is
no format for a person to declare — a column that resolves to the wrong behaviour is a
column that is grounded wrongly (the wrong join, the wrong anchor), and it is corrected
there. Offering a "behaviour" button off the disagreement would put a name-based override on
top of the one witness that read the data.
