# The learnable surface

The learnable surface is the set of things DataRaum can be taught about an organization: a
small, **typed, closed** vocabulary that agents and users fill in but cannot extend. This
is one of the **Goodhart firewall**'s three enforcement points; the others are the
measurement pooling ([measurement & detectors](measurement.md)) and the stage
authorization of lifecycle operations
([the operating model](operating-model.md#the-lifecycle)).

## Closed for use

An agent in the loop cannot invent a new *kind* of claim. It can declare that a column
grounds a concept, or that a token means *missing*; it cannot create a claim type outside
the registered set — a teach of an unregistered type fails. Users reviewing an agent's
proposals see the same fixed set of types.

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
| | `unit` | The unit a column is measured in |
| **Meaning** | `concept` | A business concept and the column-name patterns that suggest it |
| | `concept_property` | A patch to one field of a concept's annotation |
| | `rebind` | An added indicator that re-grounds a column's behaviour |
| **Structure** | `relationship` | A confirmed join between two columns |
| | `expected_dependency` | A documented dependency between dimensions |
| **Model** | `validation` | A rule the data must satisfy |
| | `cycle` | A business process with ordered stages |
| | `metric` | A measure, expressed as a calculation graph |

A teach is applied by writing one row to the workspace's overlay, after which the affected
phase re-runs and the scores are recomputed. Teaches persist across every future run.

## Verticals — reusable starting points

A **vertical** is a bundle of concepts, rules, processes, and measures for a domain,
expressed in the same teach types you'd produce by hand. It is a starting bundle; the
platform does not depend on it:

- You create one simply by [framing](the-journey.md#frame) your concepts — the concepts you
  declare *are* your workspace's vertical.
- A vertical can be shipped and shared, so a domain's accumulated knowledge can seed a new
  workspace. (A finance vertical exists today as one such bundle; it is an example of the
  surface, not a special case in the code.)

Verticals are loosely coupled to industry verticals in the business sense — the long-horizon
aim is for shared ones to align with standard domain ontologies, but nothing depends on it.

## Concepts grounded in columns

A declared **concept** becomes real when columns are bound to it. The catalogue-grain binding
— which column grounds which concept, what unit it carries, what behaviour it has — is owned
by a single phase (`semantic_per_table`, in begin_session) and recorded per run, so there is
one authoritative place a concept's grounding lives, not copies drifting across stages.

## From taught to executed

What you teach and frame doesn't stay declared: model artifacts move through an explicit
lifecycle — **declared → grounded → executed**, recorded per run — as the engine binds
them to real data. The lifecycle lives with
[the operating model](operating-model.md#the-lifecycle); the loop that drives it —
declare, ground, correct — is [frame, ground, teach](frame-ground-teach.md).
