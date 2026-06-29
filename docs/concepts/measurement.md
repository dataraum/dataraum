# Measurement & detectors

DataRaum's deliverable is **understood data** — and "understood" is not a figure of speech,
it is a measured quantity. After every phase, detectors quantify *how much the system does
not yet know* about each column, relationship, table, and view, and report it as a signal
you can act on. This page explains what that quantity is, why it's expressed as **entropy**,
and why the way it's computed makes it hard to game.

## Why entropy

Pick a single claim worth resolving — *"is this column a date?"*, *"does `N/A` mean missing
here?"*, *"is this measure a stock or a flow?"*. The claim has a small, fixed **claim space**
(the handful of mutually exclusive answers). The system holds a belief over that space: a
probability distribution `q`. The natural measure of *how unsure that belief is* is its
**Shannon entropy**, `H(q) = −Σ qᵢ log qᵢ` — zero when the belief is certain, maximal when
it's spread evenly. That is the primitive. Everything — type errors, missingness, ambiguous
joins, undeclared units, contested meanings — is a claim whose belief carries entropy. There
is no separate "data quality" score beside it.

A belief gets uncertain in two distinct ways, and DataRaum keeps them apart because they
call for different actions:

- **Conflict** — informed sources *disagree* with each other. → *investigate / teach.*
- **Ignorance** — no informed source has weighed in at all. → *collect more evidence.*

## Witnesses and the claim space

The system never asks one oracle for the answer. For each claim, several **witnesses** each
emit a distribution over the claim space, and each carries a **reliability** `r ∈ [0,1]`:

- a witness reading the **field name**,
- a witness reading the **data** (at least one per claim *must* — see the firewall below),
- a witness reading what you've **taught**.

These are pooled. The actionable belief is the **log-linear opinion pool**
`q ∝ Π pᵢ^{rᵢ}` — each witness's distribution raised to the power of its reliability, so a
trusted, sharp witness pulls `q` toward its claim and an unreliable one barely moves it.

This is the **Dawid–Skene** model of truth recovery from multiple noisy annotators —
witnesses are the annotators, reliability is each annotator's trustworthiness — with one
deliberate change. Classic Dawid–Skene *estimates* annotator reliability by EM over the
unlabelled data, which lets a confidently-wrong annotator drag the consensus. DataRaum
instead **measures** each witness's reliability offline against known ground truth (the
calibration rig: `r` = the witness's agreement accuracy on injected cases, Beta-smoothed),
ships those as a versioned artifact, and treats them as fixed at run time.

## Conflict, ignorance, and the decomposition

The two outputs fall straight out of the entropy of the pooled belief. For the *linear*
mixture `m = Σ rᵢpᵢ` there is an exact identity:

```
H(m) = Σ rᵢ·H(pᵢ)   +   JSD(p₁…pₙ)
       └ within-witness ┘   └ between-witness ┘
        (each one's own       (how much they
         uncertainty)          disagree)
```

- **Conflict `C`** is that between-witness term — the generalized **Jensen–Shannon
  divergence** — normalized by the entropy of the weights so it reads as *the fraction of the
  maximum disagreement these witnesses could express*. Two confident witnesses pointing at
  different answers give `C → 1`.
- **Ignorance `U` = κ / (κ + m)**, where the informative mass `m = Σ rᵢ·certaintyᵢ` discounts
  each witness by both its reliability *and* how sharp its distribution is — a uniform
  witness, however reliable, adds no certainty. `U → 1` when nobody informative has spoken.

So the name is literal: the signal is the Shannon entropy of the system's belief, split into
its disagreement and its emptiness. (A second, simpler flavour exists for the purely
statistical detectors — *surprise entropy*, the KL divergence `D_KL(observed ‖ reference)`
of a distribution from its expectation, used by `null_ratio` and Benford. Those never enter
the pool.)

## The Goodhart firewall

Goodhart's law: *when a measure becomes a target, it stops being a good measure.* An agent
optimizing against entropy — or a user impatient to turn something green — is a direct
Goodhart threat. The pooling shape is what neutralizes it:

- **Corrections enter as witnesses, never as overrides.** A teach is one more reliability-
  weighted opinion in the pool, not a write to the score. You cannot *assert* a column into
  being understood.
- **Reliability is resolution-only.** Conflict is provably weight-robust: two one-hot
  witnesses disagreeing give `C = 1` for *any* reliabilities. So a wrong or self-serving
  reliability can mis-*resolve* which answer wins — it can never *hide a disagreement*. The
  flag stands even if the weighting is off.
- **At least one witness must read the data.** If every witness read the column *name*, they
  would agree confidently and be wrong together — name-correlated witnesses fail in lockstep
  (this is measured, not assumed). A data-reading witness is what makes disagreement
  detectable.
- **Words don't enter the pool.** Free-text context an agent attaches to a result is recorded
  *alongside* the measurement; it has no distribution and no reliability, so it cannot move
  `q`, `C`, or `U`.

The result is the property the whole system leans on: **it cannot make false progress by
hiding what it doesn't understand.** (Framework: [ADR-0009](../adr/0009-entropy-as-disagreement.md).)

## The detectors

Detectors are the things that build claim spaces, gather witnesses, and run the pool. They're
organized into four layers — kinds of uncertainty — each scoring at the granularity that fits
what it measures.

| Layer | What it catches | Detectors |
|---|---|---|
| **Structural** | schema and joins | type fidelity · join-path determinism · relationship quality · relationship discovery |
| **Value** | the data's values | null ratio · null semantics · *(slice-conditional nulls, Benford — informative)* |
| **Semantic** | business meaning | business meaning · unit · time-role · dimension coverage · *(cross-column dependence — informative)* |
| **Computational** | derived & cross-table | derived-value (formula) · stock/flow behaviour · cross-table consistency |

About sixteen run today, across four **granularities** — column, relationship, table, view —
that readiness rolls up over. A few are kept as **informative** signals: surfaced for context
but deliberately *not* fed into readiness, because calibration showed they don't predict real
problems well enough to act on. Earning a place on the readiness path is something a detector
has to prove.

## From entropy to readiness

Raw entropy isn't what you act on — **readiness** is. Each target's `(conflict, ignorance)`
is run through per-**intent** loss weights (a column that's fine for exploration may be unsafe
for a reported total) into a single risk, then banded:

| Risk | Readiness | Meaning |
|---|---|---|
| low (≤ 0.3) | **ready** | understood well enough to rely on |
| medium (≤ 0.6) | **investigate** | usable, with open questions worth a look |
| high (> 0.6) | **blocked** | too uncertain to safely build on |

The weights and thresholds live in configuration, not code, so the *how-much-it-matters*
judgement is tunable from data without touching detector logic. (An earlier Bayesian-network
rollup was removed in favour of these explicit per-intent loss tables.)

## Calibration is what makes the numbers mean something

A readiness score is only worth trusting if low scores track usable data and high scores
track real problems. DataRaum establishes that empirically: detectors run against datasets
with **deliberately injected issues and a recorded answer key**, and each must show both
**recall** (catches the injection) and **precision** (doesn't cry wolf on clean data). The
witness reliabilities are measured by the same rig.

To keep it honest, every measurement ships in one fixed shape — claim space, witness
extractors, detector shell, config rows, write-back, teach applier, eval entry — with **no
tuned numbers in detector code at all**: every threshold, weight, and reliability lives in
config with provenance. Implementation and calibration are separate activities on separate
artifacts, so nobody nudges a constant to make a test pass.
([ADR-0011](../adr/0011-measurement-pack.md).)

When a score is elevated, the **why** view surfaces the witnesses, the contested claim, and a
ranked list of teaches that would resolve it. You teach; the affected phase re-runs; the
score moves — or it doesn't, and you've learned the problem is real.
