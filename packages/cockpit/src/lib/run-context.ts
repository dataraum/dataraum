// Request-scoped conversation context (DAT-528) — carries the originating
// conversationId from the chat handler down to recordRun WITHOUT threading it
// through every Temporal driver.
//
// recordRun is called two hops deep inside the tool drivers (select →
// triggerAddSource → recordRun), and a TanStack AI tool `.server()` handler gets
// only `{ abortSignal }` — there is no per-request channel for the conversationId.
// AsyncLocalStorage supplies it ambiently: the chat handler runs the whole turn
// inside `runWithConversation(threadId, …)`, and any recordRun firing under that
// turn's async call tree reads it back. The chat() agent loop dispatches tools via
// a plain `await tool.execute(args, ctx)` inside the async generator the handler
// consumes, so the store propagates straight through (verified by run-context's
// dispatch-mirror test).
//
// Per-request + concurrency-safe by construction: each `.run()` is isolated to its
// own async context, so concurrent turns (and future multi-user, DAT-357) never
// see each other's id — this is NOT a shared global.

import { AsyncLocalStorage } from "node:async_hooks";

interface ConversationContext {
	conversationId: string;
}

// Lazy singleton — instantiated on first SERVER call, never at module load. This
// module is server-only, but it is reachable from a CLIENT graph by a non-route
// path the `.functions.ts` peel can't cover: `probe.tsx` (client) → the
// `importSources` server fn's FILE (`server/import-sources.ts`) → `trigger-add-
// source` → here. The build replaces the server fn with a stub but keeps the
// FILE's transitively-imported helpers if they aren't tree-shakeable — and a
// top-level `new AsyncLocalStorage()` is a module side effect prod DCE cannot
// drop, so it leaked into the client bundle (shimmed-empty `new` → crash). With
// the `new` deferred into `store()`, this module is side-effect-free and DCE
// removes it from every client chunk (like `mappers` after the createHash split).
// See [[feedback_cockpit_isomorphic_import_side_effects]].
let storage: AsyncLocalStorage<ConversationContext> | null = null;

function store(): AsyncLocalStorage<ConversationContext> {
	storage ??= new AsyncLocalStorage<ConversationContext>();
	return storage;
}

/**
 * Run `fn` with `conversationId` bound for its ENTIRE async call tree — every
 * `currentConversationId()` reached from within (including tool handlers the
 * chat() loop awaits) reads it. Returns `fn`'s result so an async turn can be
 * awaited by the caller.
 */
export function runWithConversation<T>(conversationId: string, fn: () => T): T {
	return store().run({ conversationId }, fn);
}

/**
 * The conversationId of the in-flight turn, or `null` outside any
 * `runWithConversation` scope — a run with no originating chat (a legacy run, or
 * a future auto-orchestrated one). A null-conversation run simply doesn't narrate
 * (the completion-watcher filters on a matching conversationId).
 */
export function currentConversationId(): string | null {
	return store().getStore()?.conversationId ?? null;
}
