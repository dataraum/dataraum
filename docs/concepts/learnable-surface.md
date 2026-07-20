# The learnable surface

The learnable surface is the set of things DataRaum can be taught about an organization: a
small, **typed, closed** vocabulary that agents and users fill in but cannot extend. This
is one of the **Goodhart firewall**'s three enforcement points; the others are the
measurement pooling ([measurement & detectors](measurement.md)) and the stage
authorization of lifecycle operations
([the operating model](operating-model.md#the-lifecycle)).

## Closed for use

An agent in the loop cannot invent a new *kind* of claim. It can assert that two columns
join, or that a token means *missing*; it cannot create a claim type outside the registered
set — a teach of an unregistered type fails. Users reviewing an agent's proposals see the
same fixed set of types.

Free text does not move a score. When an agent attaches context to a high-uncertainty
result ("this column is sparse because the field is only collected for a subset"), the
context is recorded alongside the measurement; the measured value is unchanged.
Corrections happen by **teaching**, which enters the next analysis run as a witness. (The
disagreement model is in [measurement](measurement.md); the principle is
[ADR-0009](../adr/0009-entropy-as-disagreement.md).)

Adding a new teach *type* is a change to the platform, made by the people building it —
not an in-loop action.

## The teach types

A **teach** is a typed correction. Each writes to a known place and re-enters the next run as
a witness. The live set:

| Group | Teach | What it asserts |
|---|---|---|
| **Typing & values** | `type_pattern` | A pattern that maps raw values to a typed column |
| | `null_value` | A token that means *missing* in this data (e.g. a placeholder string) |
| | `unit` | The unit a named column is measured in |
| **Structure** | `relationship` | A join between two columns — confirm one that was detected, reject it, or add one the detection missed |
| | `hierarchy` | A drill-down chain over a fact's enriched view (finest to coarsest), or a set of columns that are 1:1 aliases of each other |
| **Model** | `validation` | A rule the data must satisfy |
| | `cycle` | A business process with ordered stages |
| | `metric` | A measure, expressed as a calculation graph |

A teach is applied by writing one row to the workspace's overlay, after which the affected
phase re-runs and the scores are recomputed. Teaches persist across every future run.

Which types an agent is *offered* depends on where it is working, because a teach is only
useful where a re-run can realize it. The typing-grain teaches — `type_pattern`,
`null_value`, `unit` — are offered in **Connect**, where an import replay applies them; the
catalogue-grain topology teaches — `relationship`, `hierarchy` — in **Stage**, where a
session re-run does. `validation`, `cycle`, and `metric` are not on the general teach tool
at all: each has its own tool that validates the full specification first and then writes
through the same path.

## Verticals — reusable starting points

A **vertical** is a bundle of concepts, rules, processes, and measures for a domain,
expressed in the same typed vocabulary you'd produce by hand, and read through the same
path: its concepts are seeded as concept rows, and its rules, processes, and measures are
merged with whatever the workspace teaches on top. It is a starting bundle; the platform
does not depend on it:

- You create one simply by [framing](the-journey.md#frame) your concepts — the concepts you
  declare *are* your workspace's vertical.
- A vertical can be shipped and shared, so a domain's accumulated knowledge can seed a new
  workspace. (A finance vertical exists today as one such bundle; it is an example of the
  surface, not a special case in the code.)

Verticals are loosely coupled to industry verticals in the business sense — the long-horizon
aim is for shared ones to align with standard domain ontologies, but nothing depends on it.

## Concepts and columns

The concept vocabulary is **not** a teach type. Framing writes concepts as rows in the
workspace's own `concepts` table, and every later stage reads them from there.

Those concepts meet the data at two grains, in two places:

- **Per column, in add_source.** `semantic_per_column` reads each column against the
  declared vocabulary and records what it is — role, entity, business term, description,
  and an independent stock-or-flow claim. The vocabulary is context for that reading; the
  phase writes no single-slot concept binding, because one column commonly carries several
  facets at once.
- **Per concept, in operating_model.** Grounding a declared artifact produces, for each
  concept it needs, the relation and the enumerated columns that compute it — an
  enumeration checked against that relation's schema when it is saved. That is where
  *which columns ground this concept* is written down.

Between the two, `semantic_per_table` (in begin_session) adds the catalogue-grain reading of
a column: what it means once every source is composed into one picture, which column defines
its unit, and its resolved temporal behaviour. It is recorded per run under the catalogue
head.

## From taught to executed

What you teach and frame doesn't stay declared: model artifacts move through an explicit
lifecycle — **declared → grounded → executed**, recorded per run — as the engine binds
them to real data. The lifecycle lives with
[the operating model](operating-model.md#the-lifecycle); the loop that drives it —
declare, ground, correct — is [frame, ground, teach](frame-ground-teach.md).
