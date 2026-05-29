# Cockpit — the three-region agentic view (DAT-347, C1)

The inner surface that fills the C0 shell's `/workspace/$wsId/cockpit` route. It
renders **strictly inside** that route component — the outer shell, theme,
routing, and ⌘K belong to C0 (`src/ui/{app-shell,sections,theme}.ts[x]`).

## Layout

```
CockpitView                  three-region grid, independent scroll, scoped hotkeys
├── ChatRail                 messages + collapsible tool-call cards + input (left)
└── work region (right)
    ├── StageNavigator       horizontal strip of the 7 JOURNEY stages
    └── FocusCanvas          renders the active CanvasState via the widget registry
```

State lives in one small reducer (`cockpit-state.tsx`): `{ activeStage,
canvasState }`. The chat rail dispatches canvas updates; the stage navigator
dispatches stage changes.

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

`ChatRail` uses the TanStack AI SDK's `useChat({ connection:
fetchServerSentEvents("/api/chat") })`. The SDK owns the whole loop: the
conversation state, the agentic tool-loop (it executes server tools, pauses on
`needsApproval` tools for the user to Approve/Deny via `addToolApprovalResponse`,
feeds results back, iterates), and the SSE transport. We no longer hand-roll the
wire — the old `use-chat-stream.ts` probe is gone. Streaming fires **only on
user submit** (`sendMessage`) — never on mount — so the view is SSR-safe.

`canvasFromMessages(messages)` (in `tool-result-to-canvas.ts`) adapts the SDK
message list to the canvas: it finds the latest completed tool result and maps
it via `toolResultToCanvas`. `ChatRail` projects that onto the focus canvas in a
`useEffect([messages, setCanvasState])`. **`setCanvasState` MUST have stable
identity** (`cockpit-state.tsx` pins the dispatchers with `useCallback`) — a
per-dispatch identity would re-fire the effect, which re-dispatches a fresh
canvas object → an infinite render loop.
