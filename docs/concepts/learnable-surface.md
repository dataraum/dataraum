# The learnable surface

The learnable surface is the set of things DataRaum can be taught about an organization. It
is what lets the LLM contribute *and* keeps it from running off the rails: a small, **typed,
closed** vocabulary that the agent — and you — can only **fill in**, never extend. This is
the closed-vocabulary half of the **Goodhart firewall** (the measurement half is in
[measurement & detectors](measurement.md)).

## Closed for use, the firewall

An agent in the loop cannot invent a new *kind* of thing to assert. It can declare that a
column grounds a concept, or that a token means *missing*, but it cannot create a new species
of claim that escapes measurement. You, reviewing its proposals, see the same fixed set of
types. The system, the agent, and the user share one vocabulary.

This is the **firewall**: the agent can only optimize against signals the closed set was
designed to surface, and — critically — **its words never move a score**. When an agent adds
context to a high-uncertainty result ("this column is sparse because the field is only
collected for a subset"), that context is recorded *alongside* the measurement; the
uncertainty stays exactly as measured. Legitimate corrections happen by **teaching**, which
re-enters analysis as evidence (a *witness*), not as an override. (This is the same
disagreement model as [measurement](measurement.md); the principle is
[ADR-0009](../adr/0009-entropy-as-disagreement.md).)

Adding a new teach *type* is a deliberate change to the platform, made out-of-band by the
people building it — never an in-loop action.

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
phase re-runs and the scores re-measure. You teach; the engine reapplies; the readiness
moves — and your teach persists across every future run.

## Verticals — reusable starting points

A **vertical** is a bundle of concepts, rules, processes, and measures for a domain,
expressed in the same teach types you'd produce by hand. It is a *head start*, not a
configuration the platform depends on:

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
