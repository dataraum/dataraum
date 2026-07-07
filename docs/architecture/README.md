# Architecture — living facts

These documents state **what is true of the system now**: the overarching ideas,
requirements, invariants, and cross-package structure. They are not decision
records, not a changelog, and not an archive.

The rules:

- **Present tense only.** A sentence that describes the past, a decision-event,
  or a dead idea does not belong here — delete it. Git history is the only
  archive; there is no status field, no "superseded", no dates, no tickets.
- **Updated in place, in the same PR that changes the fact.** A document that
  contradicts the code is a bug in whichever of the two is wrong.
- **A prohibition is a fact** ("the engine has no HTTP surface — do not add
  one"). That is the only form in which a rejected alternative earns a line:
  as a standing constraint, never as a story about how it was rejected.
- **Cross-cutting facts only.** What one module does belongs in that module's
  code comments and tests; what one package needs belongs in its CLAUDE.md.
  A fact goes here when it spans packages or constrains future work broadly.
- **Requirements are facts about the target**, stated so a stranger can check
  the system against them. "Not yet defined: X" is a valid fact.

One file per concern; the directory listing is the index.
