# PII Pseudonymization for GDPR (BYOC)

> Status: DESIGN DRAFT (ideation, 2026-06-17)
> Scope: engineering only. DPA/legal paperwork explicitly out of scope.

## Problem

DataRaum processes customers' real tabular data and runs LLM analysis over it. Two distinct exposure surfaces today carry **unmasked personal data**, and current coverage is partial and naive:

1. **LLM-disclosure surface.** Several pipeline stages send raw cell values to Claude. Coverage is two-of-four:
   - ✅ `semantic_per_column` and `semantic_per_table` redact via `llm/privacy.py` (`DataSampler`) — but **only by matching the column *name*** against regexes in `dataraum-config/llm/config.yaml` (`.*email.*`, `.*ssn.*`, …). A column named `user_attr` holding emails is not caught; content is never inspected.
   - ❌ `graphs/agent.py` (operating_model) — `_describe_table()` ships up to 5 raw distinct values per column straight to the prompt (`graphs/agent.py:642`), bypassing `DataSampler` entirely.
   - ❌ cockpit `run_sql` — the agent can issue `SELECT email FROM users` and the rows land in its context with no redaction.

2. **At-rest / management-plane surface.** Raw values persist unmasked in DuckDB (`lake.raw/typed/quarantine`) and **partially in Postgres** (`statistical_profile.top_values` JSONB stores actual sample values). Crucially, raw values also leak into **logs, error traces, and telemetry** (the same `sample_values`/`top_values` the engine logs) — and in the BYOC model that telemetry is exactly what flows back to DataRaum through the observation/management plane.

The unused `pii: bool` flag in `typing/patterns.py` is a seam that was anticipated for this and never wired.

### Why BYOC changes the framing (and the stakes)

DataRaum ships into the **customer's own cloud** (Northflank-managed; DataRaum only monitors + updates). This flips the GDPR role allocation in our favor:

- **Customer = controller** and operator of the processing — the software runs in their account, under their credentials.
- **Anthropic (via the customer's own Bedrock/Vertex, EU region, under the customer's cloud DPA) = the customer's sub-processor.** DataRaum is not in that chain.
- **DataRaum = software supplier**, not a processor of the analytics data — *because it never accesses that data.*

That clean "supplier, no access" position holds on **one engineered condition: the management/observation plane provably never carries personal data.** The moment a stack trace with raw cell values, or telemetry with row samples, flows back to DataRaum/Northflank, we have *accessed* the customer's personal data and fall back to processor status.

So pseudonymization is not paperwork-avoidance — it is **the mechanism that keeps the BYOC no-access guarantee true**. If identifiers are tokenized at ingestion, a value that leaks into a log line that reaches our observation stack is a token, not personal data.

**The observation stack is TBD** — this is an advantage. We design it personal-data-free from day one rather than retrofitting redaction onto an existing telemetry pipeline.

## Design

### Decisions locked (from ideation)

| Decision | Choice | Rationale |
|---|---|---|
| Privacy target | **Pseudonymization**, not anonymization | It's an analytics platform; quasi-identifiers must survive for analysis. Honest claim: still personal data, strong Art. 32 measure. |
| Reversibility | **Reversible via the source** (back-reference), not a vault store | We keep the S3 source-of-record, so re-identification = re-derive value↔token from the source. No ciphertext mapping table needed — the vault collapses to a single secret. |
| Identifier handling | **Deterministic salted hash with a per-workspace random secret** (HMAC *not* required) | `token = hash(workspace_secret ‖ normalized_value)`; `workspace_secret` = random bytes in the secret store. Same input → same token (joins survive), scoped per workspace. The **secret** is the legally meaningful part — Art. 4(5)'s "additional information kept separately" — and the only defense against a dictionary attack on a token that leaks externally. HMAC vs. `hash(secret ‖ value)` is an engineering detail, legally equivalent. |
| Free-text PII | **Presidio span-redaction** per cell | Tokenization doesn't fit prose; PII hides mid-sentence. |
| Detection | **Presidio, self-hosted in-engine, column-grain hybrid** | The detection step must see raw values; self-hosted keeps that step *inside the trust boundary*. A cloud PII service would re-introduce the disclosure it's meant to remove. |
| Access control | **ACL (separate concern)** — the per-workspace secret gets the strictest | Pseudonymization = disclosure control; ACL = access control. Orthogonal. The secret is the crown jewel. |
| LLM transport | **Bedrock/Vertex EU region, pinned by safe default** | Only AWS Bedrock and GCP Vertex keep Claude in-region today; Azure Foundry routes back to Anthropic's servers (EU "coming 2026"). Ship EU-region pinning the customer can't accidentally override. |
| Legal paperwork | **Out of scope** | BYOC narrows DataRaum's DPA to operational metadata; lawyer owns the final role mapping. |

### Architecture: detection → classification → tokenize-at-typing → masked `lake.typed`

This maps onto DataRaum's existing **detector → applier → run-versioned-metadata** pattern. PII classification is just another column-level detector output; masking is just another applier.

**1. Detection at ingestion (typing phase), content-based.**
Presidio runs **locally, in-process** in the engine. Classify at **column grain**, not per cell: sample N values + the column name → label the column:

| `pii_class` | Treatment | Examples |
|---|---|---|
| `direct_id` | Tokenize (salted hash, reversible via source) | name, email, phone, SSN, account no. |
| `quasi_id` | **Pass through raw** + flag for ACL/awareness; **coarsen fine-grained birth-dates** (DOB → month or year) to break the combination | zip, gender, city; DOB→coarsened |
| `free_text_pii` | Per-cell Presidio span-redaction | notes, descriptions, comments |
| `clean` | Pass through | measures, codes, timestamps |

The name-regex prior stays as a cheap first pass; Presidio content-detection is the authority. Custom recognizers live in `dataraum-config` **per vertical** (finance vs. health have different identifier sets) — fits config-as-data.

**2. Salted-hash tokenization — one secret, no mapping table.**
- `token = hash(workspace_secret ‖ normalized_value)`. Deterministic → joinable; same value → same token within a workspace. Secret = random bytes per workspace (HMAC not required; it's one valid construction among equals).
- **No vault / ciphertext store.** Because the S3 source-of-record is retained, reversal is back-reference: re-derive value↔token from the source. The only thing kept is the **per-workspace secret** in the secret store (KMS/env), not a `token→value` table.
- The secret is the single highest-value asset → strictest ACL. Keep it stable per workspace (joins depend on it); use a dedicated random secret, **not** the workspace ID (don't rely on an identifier's obscurity).
- **Erasure (Art. 17) = customer deletes from the source + re-import** (they're the controller; the source is the only raw at rest). Bulk crypto-shred is available for free by destroying the workspace secret.
- *Why a secret at all:* not reversibility (the source handles that) — it's the legally meaningful "additional information kept separately" (Art. 4(5)) and the only defense against a dictionary attack on a token that leaks to an external surface. A non-secret salt (e.g. workspace ID) gives scoping but no brute-force resistance.

**3. Masking materializes at the typing boundary — `lake.typed` *is* the masked surface.**
There is **no separate `masked.` schema and no read-time downstream redaction.** The tokenization/redaction/coarsening applier runs once, at typing, and `lake.typed` holds the pseudonymized values directly: salted-hash tokens for `direct_id`, Presidio redaction for `free_text_pii`, coarsened DOB, raw passthrough for `quasi_id`/`clean`. Spike A proved every downstream consumer works on this surface — so typed is the single, already-safe analysis surface. Materialize-once beats redact-on-every-read: it can't be bypassed, it's cheaper, and the guarantee becomes *structural* ("raw isn't at rest") not *enforced-per-read* ("remember to mask").

Raw is needed only by type inference, PII detection, and tokenization — all at/inside the typing phase, **nowhere downstream**. So raw exists only:
   - **transiently**, in the typing phase's working memory (the one window it's needed);
   - via **back-reference to the source** for `direct_id` reversal (lawful access) — no separate ciphertext store;
   - in the **S3 source-of-record** — the true raw-of-record (object-store seam, re-importable via DAT-420). `lake.raw`/`lake.quarantine` collapse to transient staging within the import/typing transaction, not persisted PII tiers.

If detection misses a column (baseline recall isn't perfect), the fix is simply **re-import** — the source already sits in the customer's cloud. No retained raw tier, no GC policy: the S3 source *is* the raw of record.

**4. The guarantee is structural, not a per-read chokepoint.**
Because raw is not at rest in any lake schema readers can reach, there is **nothing to redact downstream and nothing to enforce per-read** — the LLM surface (semantic agents, graphs agent, cockpit `answer`/`run_sql`) and the observation stack all read `lake.typed`, which is already pseudonymized. The remaining requirement is narrow:
   - **Observation/management surface (BYOC-critical):** scrub raw from logs and error traces emitted **during the typing window** — the only time raw is in memory. The per-workspace secret sits in a strict-ACL secret store, never exposed to analysis/LLM/observation readers.
   - **Engine-internal raw-readers** (typing, column-eligibility) operate inside that window by design — they produce the masked typed surface.
   - **The existing LLM leaks (`graphs/agent.py`, `run_sql`) are not separate bugs** — those consumers correctly read `lake.typed`; they leak only because typed isn't pseudonymized *yet*. Masking typed (P3) closes them as a consequence, with **no change to the consumers and no interim patch** (no monkey-patch-then-revert).

**5. EU-region pinning as safe default.**
Bedrock/Vertex EU endpoints pinned in the deployment config the customer can't accidentally override. DataRaum ships the safe default; the customer holds the cloud DPA.

### What explicitly does NOT change

- The analysis engine's logic (relationships, correlations, slicing). Tokenization preserves joins; quasi-ids pass through; measures untouched. Fidelity is preserved by design.
- ACL / access-control design — separate workstream, only the **per-workspace secret** gets a hard requirement here.
- Legal contracts / DPA negotiation — out of scope.
- DuckDB/SQLAlchemy persistence seam — masking is an applier in the typing write-path; the schema/engine seam is unchanged. (Note: `lake.typed` content *does* change — it becomes pseudonymized — but the persistence machinery does not.)

## Phasing (sketch — for /decompose)

*(No interim/stopgap phase: the two existing LLM leaks aren't separate bugs — they read `lake.typed` correctly and close automatically once P3 masks it. The customer ask is met by building P1→P4 properly, not by a read-time patch we'd revert.)*

- **P1 — Detection layer:** Presidio in-engine, column-grain classifier (baseline recall; birthdate-like → `quasi_id`), `pii_class` persisted as a detector output; wire the dormant `patterns.py:pii` seam; per-vertical recognizers in `dataraum-config`. Recall fine-tuning happens at design-partner onboarding, not here.
- **P2 — Salted-hash tokenization:** per-workspace random secret in the secret store (no vault/mapping table), deterministic tokenization applier; reversal via source back-reference; erasure = source-delete + re-import.
- **P3 — Mask the typed surface (keystone):** flip the typing write-path so `lake.typed` emits pseudonymized values (tokens / redaction / DOB-coarsening / passthrough); confine raw to transient + the S3 source; collapse `lake.raw`/`quarantine` to transient staging; stop persisting raw `statistical_profile.top_values` for PII-classed columns; handle the `masked-partial` edge (Validation/Metrics/Quality rules fail-loud on tokenized columns).
- **P4 — Region pinning + observation-stack-clean:** Bedrock/Vertex EU safe defaults; define the TBD observation stack as personal-data-free by construction (only the typing-window logs/traces can ever carry raw).

## Open Questions

1. ~~**What reads raw vs. masked?**~~ **RESOLVED (Spike A, 2026-06-17).** Consumer audit of all ~21 readers across engine + cockpit. Only the **typing phase** and **column-eligibility** require raw — and both are *pre-tokenization* (they produce the masked `lake.typed`), never external. **Relationship/FK detection is `tokens-ok`** (Jaccard/orphan checks need value *equality*; `token(a)==token(b) ⟺ a==b`) — the load-bearing confirmation. Everything else is `masked-ok`. Two edges for P3: (a) a **`masked-partial`** class — Validation / Metrics / Quality run user/LLM-authored rules that may text-pattern-match a tokenized column (aggregates/numeric fine); these should **fail-loud** on tokenized columns, not silently mismatch; (b) Slicing on a direct-PII dimension. Net: *"everything downstream of typing reads masked"* holds, with those two known exceptions handled in P3.
2. ~~**Keyed vs. plain hash?**~~ **RESOLVED.** Baseline = **deterministic salted hash with a per-workspace random secret** (`hash(secret ‖ value)`); **HMAC not legally required** — GDPR is technology-neutral, and the secret kept separately is what satisfies Art. 4(5). A non-secret salt (workspace ID) gives scoping but not brute-force resistance, so use a dedicated random secret. Remaining detail (small): secret is kept **stable** per workspace (joins depend on it); rotation is exceptional and terminal for already-tokenized data (re-derive from source if ever needed); lives in KMS-per-workspace vs. static env — engineering choice for P2, not a spike.
3. ~~**Quasi-identifier residual risk.**~~ **RESOLVED.** Quasi-ids pass through raw + flagged, with ONE targeted coarsening: **fine-grained birth-dates → month or year.** Re-identification power lives in the *combination*, and DOB is its highest-entropy member; coarsening it collapses the joint distinguishing power, so no general k-anonymity framework is needed. Accepted residual: very-small-population locales (tiny villages) — acceptable for an internal tool; adversarial re-identification against public records is data theft (a crime) regardless. **No extra phase:** birthdate-like classification in P1, date-coarsening applier in P3.
4. **Detection recall — baseline vs. tuned.** Separate *"yes, we detect PII"* (table-stakes capability — ship baseline Presidio in P1) from *"this is tuned perfectly for your data"* (recall fine-tuning). The latter is a **design-partner onboarding loop with the first few customers**, not a pre-decompose blocker. Presidio NER has false negatives and is language-dependent; column-grain sampling bounds cost. → NOT a spike; baseline ships in P1, recall is fine-tuned per-customer at onboarding.
5. **Observation stack shape.** Since it's TBD: what's the minimal telemetry that's genuinely personal-data-free yet still useful for monitoring/debugging in someone else's cloud? → design alongside P4; this is where BYOC's "clean slate" advantage is spent.
6. **`statistical_profile.top_values` in Postgres.** Stop persisting raw top-values for PII-classed columns (they'd otherwise be a raw PII surface at rest, defeating masked-typed) — store tokenized/redacted top-values instead. → P3.

## Alternatives Considered

- **Faker synthetic replacement.** Rejected: destroys joins, distributions, and relationships — kills the core product. Faker is a test-fixture tool. (Presidio *can* use Faker as an operator, but tokenization is the right operator for identifiers.)
- **Anonymization (irreversible).** Rejected: breaks joins and subject-access, and is not honestly achievable for an analytical lake with quasi-identifiers. We'd over-claim "GDPR-exempt." Pseudonymization is the truthful target.
- **Cloud PII-detection-as-a-service (GCP DLP / AWS Comprehend / Azure PII).** Rejected: detection must see raw values; sending raw data to another service to *find* PII re-introduces the disclosure we're removing and adds a sub-processor. Self-hosted Presidio keeps the riskiest step in-process.
- **Geo-fencing LLM calls as the primary control.** Rejected as *primary*: addresses Chapter V transfers only, not minimization. Kept as a *layer* (EU-region pinning) under BYOC, where it's the customer's cloud anyway.
- **Keep raw in `lake.typed` + redact on every read (the earlier hedge).** Rejected: leak-prone (every reader, log line, and agent query must be routed through a chokepoint correctly, forever — miss one and PII leaks) and complex. Masking *once* at the typing boundary makes the guarantee structural — there is no raw at rest to leak. It stays non-destructive because raw is recoverable from the S3 source.
- **Destructive masking (no recoverable raw anywhere).** Rejected by the reversibility decision: forecloses lawful reversal and subject-access. Raw is recoverable via the S3 source-of-record.
- **Vault / `token→value` ciphertext store.** Rejected: redundant with the retained source. Reversal is back-reference via the source; we keep only the per-workspace secret, not a mapping table.
- **HMAC specifically (over a salted hash with a secret).** Not required: GDPR is technology-neutral; the secret kept separately satisfies Art. 4(5). A salted hash with a per-workspace random secret is the legally sufficient baseline.
- **Full per-vertical k-anonymity / generalization framework for quasi-ids.** Rejected: overkill. Coarsening the single highest-entropy quasi-identifier (DOB) defangs the combination without a generalization engine.
- **System-prompt "prefer masked" for the cockpit agent.** Rejected (Spike A): relying on LLM discretion to pick the safe schema is the same leaky pattern. Moot under masked-typed — the agent reads `lake.typed`, which is already pseudonymized; raw isn't reachable to begin with.
- **Name-regex masking only (status quo).** Rejected as sufficient: misses content, covers only 2 of 4 surfaces. Kept as a cheap prior in front of content detection.
