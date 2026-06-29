# DataRaum

**Ground your organization's operating model in its own data.** Every organization runs on
an operating model — the entities it deals in, the processes it runs, the rules that must
hold, the measures it watches. That model usually lives scattered across tools, documents,
and people, disconnected from the data underneath it. DataRaum brings the two together: it
turns the structured data an organization already has into an **executable operating
model**, with an LLM held to a closed vocabulary and to measurements it cannot fake.

A semantic layer tells BI tools what columns are *called*. DataRaum learns what they *mean*
— the concepts, relationships, rules, and measures of the organization — and grounds each
one in the actual data, so a definition stops being words in a document and becomes
something computed directly from your sources, with a measured confidence behind it.

## The idea

A modern LLM already knows the general shape of how organizations work. What it *doesn't*
know is how **yours** works: which fields carry which meaning, how your sources relate, what
a given value actually represents, which tables describe the same thing. That knowledge is
latent in the data and the organization. It has to be recovered, then bound to the data —
not assumed.

DataRaum does the recovering and the binding, and produces the operating model as the
durable artifact at the end: the concepts, processes, rules, and measures that used to live
in scattered tools and tribal knowledge, now expressed as something you can run, measure,
and ask questions against.

What it produces is **data understanding** in a literal sense — and that is why we call it
that. The deliverable is not a cleaned table or a dashboard; it is a *measured account of how
well the system comprehends each part of your data* — solid here, shaky there — carried
alongside the model so you always know how far to trust it.

## Why you can trust the LLM here

An LLM is what makes grounding an organization this way possible — and an LLM left to its own
devices is exactly what you can't put in charge of decisions that matter. Two mechanisms keep
it honest:

- **A closed vocabulary it can't escape.** The LLM can't invent new *kinds* of claim. It
  works against a small, typed surface — concepts, measures, rules, processes, and a handful
  of *teaches* — and can only fill those in. Even a correction enters as *evidence to be
  weighed*, never as a direct edit to a result: it cannot make a problem disappear by
  describing it. (This is the **Goodhart firewall** — see
  [the learnable surface](concepts/learnable-surface.md).)
- **Measurement it can't game.** The system continuously measures its own uncertainty — as
  **entropy**, the disagreement between independent witnesses — and reports it as a plain
  readiness signal: *ready*, *investigate*, *blocked*. Those numbers aren't vibes; the
  detectors behind them are **calibrated against known ground truth**, so a low score really
  does track usable data and a high one really does mean *look here*. See
  [measurement & detectors](concepts/measurement.md).

The effect is a system that can't make false progress by hiding what it doesn't know.

## How it gets there

DataRaum doesn't index schemas, and it doesn't hand everything to the LLM. It runs the data
through a **pipeline of analysis phases**, each using the right method for the job, and
blends three kinds of evidence:

- **Deterministic** — exact structure: types, keys, the joins between tables.
- **Statistical** — what the shape of the data reveals: distributions, outliers, drift.
- **LLM** — meaning: what a field *is*, which concept it grounds, how a measure is composed.

No single method is trusted on its own. Where they **disagree** — the field's name claims
one thing, the data shows another — that disagreement is the signal the **detectors**
measure, and it's what turns into the readiness you can act on.

```mermaid
flowchart TB
    SRC["<b>structured sources</b><br/>databases · APIs · spreadsheets · files"]

    subgraph PIPE["analysis pipeline — phases"]
        direction LR
        D["<b>deterministic</b><br/>typing · keys · joins"]
        S["<b>statistical</b><br/>profiles · outliers · drift"]
        L["<b>LLM</b><br/>meaning · concepts · measures"]
    end

    DET["<b>detectors</b><br/>entropy = disagreement between witnesses"]
    READY["<b>readiness</b><br/>ready · investigate · blocked"]
    OM["<b>operating model</b><br/>concepts · processes · rules · measures"]

    SRC --> PIPE
    PIPE --> DET --> READY
    PIPE --> OM
    READY -. "what to fix next" .-> OM

    style SRC fill:#e8f5e9
    style OM fill:#e3f2fd
```

## Many sources, one model

Real questions span sources — different systems, exports, and spreadsheets that were never
designed to fit together. DataRaum brings typed sources into one analytical workspace: it
works out how they relate, builds enriched join views, finds the dimensions you can slice
by, and reconciles measures across tables. The operating model is built over that combined
picture, not over one source at a time.

## How you use it

You work in a **workspace** through a web cockpit. Nothing about your domain is baked in:
you **describe what you care about in plain language**, and DataRaum builds the model with
you. The journey is short — describe your domain (**frame**), **connect** your sources,
**stage** an analytical session, build the **operating model**, then **ask** questions in
plain language and get answers grounded in that model, with the SQL and the confidence
shown. See the [Overview](getting-started/overview.md) for the whole arc.

## Where to go next

- **The concept, in depth** — [The approach](concepts/approach.md) (how the methods
  combine), [the journey](concepts/the-journey.md), the
  [pipeline & phases](concepts/pipeline.md), the
  [learnable surface](concepts/learnable-surface.md) (the closed vocabulary), and
  [measurement & detectors](concepts/measurement.md) (entropy, witnesses, calibration).
- **Using it** — [Overview](getting-started/overview.md),
  [Running the stack](getting-started/running-the-stack.md), and
  [the Cockpit](cockpit/cockpit.md).
- **Under the hood** — the [platform architecture](platform/architecture.md), the
  [substrate](platform/substrate.md), and [orchestration](platform/orchestration.md).
- **The design intent** (north-star, not current state) —
  [Architecture (vision)](vision/architecture-future.md).
