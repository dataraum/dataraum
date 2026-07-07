# Durable execution, intelligence at the edge

Two ideas organize how work runs.

**All long-running work is durable execution.** Any step can die — process,
container, host — and the run resumes where it stopped. Retry, backoff, and
resumption are the platform's job; analysis code never hand-rolls them, and a
completed step is never silently re-done.

**Intelligence sits at the edge.** The interactive, streaming, agentic layer
lives in the user-facing tier; the engine computes durably and
deterministically. Neither blocks the other: an interactive session dying
never loses durable work, and durable work never waits on a user being
present.

- The integration surface between the tiers is deliberately narrow — shared
  state plus durable signals. A bespoke API between them would grow to mirror
  one side or the other; the narrowness is the requirement.
- Where durable replay demands determinism, determinism is a hard constraint
  on that code, enforced structurally rather than by convention.
