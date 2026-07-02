# Measurement & detectors

After every phase, detectors quantify *how much the system does not yet know* about each
column, relationship, table, and view, and report it as a signal you can act on. This page
explains what that quantity is, why it is expressed as **entropy**, and which properties of
the computation keep it independent of the parties being measured.

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

No single source answers a claim. For each claim, several **witnesses** each emit a
distribution over the claim space, and each carries a **reliability** `r ∈ [0,1]`:

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
optimizing against entropy, or a user trying to turn something green, are both cases of it.
Four properties of the pooling address it:

- **Corrections enter as witnesses.** A teach is one more reliability-weighted opinion in
  the pool, not a write to the score.
- **Reliability is resolution-only.** Conflict is weight-robust: two one-hot witnesses
  disagreeing give `C = 1` for *any* reliabilities. A wrong or self-serving reliability can
  mis-resolve which answer wins; it cannot hide the disagreement.
- **At least one witness per claim reads the data.** Witnesses that all read the column
  *name* agree confidently and fail together (measured in calibration, not assumed). A
  data-reading witness is what makes disagreement detectable.
- **Free text does not enter the pool.** Context an agent attaches to a result is recorded
  alongside the measurement; it has no distribution and no reliability, so it cannot move
  `q`, `C`, or `U`.

(Framework: [ADR-0009](../adr/0009-entropy-as-disagreement.md).)

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
that readiness rolls up over. A few are kept as **informative** signals: surfaced for
context but not fed into readiness, because calibration showed they don't predict real
problems well enough to act on.

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

## Calibration

A readiness score is useful if low scores track usable data and high scores track real
problems. That is established empirically: detectors run against datasets with **injected
issues and a recorded answer key**, and each must show both **recall** (catches the
injection) and **precision** (no false alarms on clean data). The witness reliabilities are
measured by the same rig.

Every measurement ships in one fixed shape — claim space, witness extractors, detector
shell, config rows, write-back, teach applier, eval entry — with no tuned numbers in
detector code: every threshold, weight, and reliability lives in config with provenance.
Implementation and calibration are separate activities on separate artifacts.
([ADR-0011](../adr/0011-measurement-pack.md).)

When a score is elevated, the **why** view surfaces the witnesses, the contested claim, and
a ranked list of teaches that would resolve it. After a teach, the affected phase re-runs
and the score is recomputed; a score that does not move indicates the problem is in the
data itself.
