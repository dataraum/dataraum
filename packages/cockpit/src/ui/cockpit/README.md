# Cockpit — the three-region agentic view (DAT-347, C1)

The inner surface that fills the C0 shell's `/workspace/$wsId/cockpit` route. It
renders **strictly inside** that route component — the outer shell, theme,
routing, and ⌘K belong to C0 (`src/ui/{app-shell,sections,theme}.ts[x]`).

## Layout

```
CockpitHome                  the /cockpit index route (DAT-528): "tell or click"
                             landing — composer + type chips + recent history
CockpitView                  a specific chat (/cockpit/$conversationId): the
│                            working split (mounts only inside a real conversation)
├── ChatRail                 messages + collapsible tool-call cards (left)
│   └── Composer             input bubble; bottom row = the ChatSwitcher type
│                            drop-up (fed via provider typeNav) + send-on-text/stop
└── FocusCanvas              renders the active CanvasState via the widget registry
```

State lives in `cockpit-state.tsx`, which also OWNS the agent chat (`useChat`).
The focus canvas is **derived** from the message stream during render — not
stored — so there are no effects mirroring it (see "Chat transport" below). The
context is split in two: a reactive **state** context (`messages`, `canvas`, …)
and a stable **actions** context (`sendMessage`, `pinCanvas`, …).
Components that only dispatch read `useCockpitActions()` and never re-render
while a turn streams; components that render streaming state read `useCockpit()`.

## The contract: register, don't replace

**The focus canvas is a registry, not a switch.** Adding a new canvas
visualization (the C2-C6 columns) is four additive edits — and it never touches
`FocusCanvas`, `CockpitView`, the chat stream, or the shell:

1. **Add one `CanvasState` member** in `canvas-state.ts`
   (e.g. `| { kind: "table-preview"; rows: Row[] }`).
2. **Add one widget file** in `widgets/` whose props are
   `{ state: Extract<CanvasState, { kind: "table-preview" }> }`.
3. **Add one `register()` line** in `canvas-registry.ts`
   (`.register({ kind: "table-preview", component: TablePreviewWidget })`).
4. **Add one mapper case** in `tool-result-to-canvas.ts`
   (map your tool's result to the new member).

That's it. `FocusCanvas` resolves the widget by `canvasState.kind`; an
unregistered kind degrades to the error widget, so a partially-landed column
never crashes the view.

## Chat transport (DAT-353)

`CockpitProvider` (`cockpit-state.tsx`) owns the TanStack AI SDK's `useChat({
connection: fetchServerSentEvents("/api/chat") })`. The SDK owns the whole loop:
the conversation state, the agentic tool-loop (it executes server tools directly
— there is no approval gate; the user's instruction is the consent — feeds
results back, iterates), and the SSE transport.
We no longer hand-roll the wire. Streaming fires **only on user submit**
(`sendMessage`) — never on mount — so the view is SSR-safe. The provider also
threads an `AbortController` so the Stop button (and a disconnect) cancels the
stream → the server aborts the Anthropic call (see `routes/api/chat.ts`).

The focus canvas is **derived during render**, not synced through effects:

```
canvas = pinned ?? live ?? (isLoading ? loading : empty)
```

where `live = canvasFromMessages(messages)` finds the latest completed tool
result and maps it via `toolResultToCanvas`, and `pinned = canvasFromCallId(...)`
re-resolves a clicked history chip. (The imperative `showCanvas` override slot
was retired by DAT-436: its one user — the add_source progress widget seeded by
the REST trigger button — became chat-derivable when approving `select` started
the import and its result began carrying the run ids.)

`useStableValue` returns the previous reference when
the derived canvas is value-equal, so streaming text doesn't churn the canvas
subtree (`FocusCanvas` is `memo`'d). This replaced an effect chain that mirrored
the canvas into state and was the source of a recurring stuck-spinner /
duplicate-chip / re-issued-stream bug class — every bug was a symptom of
deriving state by side effect.
