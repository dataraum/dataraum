# Tech Design — Streaming `run_sql` results to the cockpit grid

Status: **Draft for review** · Owner: Philipp · Branch: `worktree-arrow-runsql-design` (off `main`)
Scope: cockpit (`packages/cockpit`) · No engine changes.

> Outcome of this doc is a **design**, not an implementation. Code blocks are
> illustrative sketches of the seams, not the patch.

---

## 1. Problem

`run_sql` (DAT-367) runs read-only DuckDB SQL over the DuckLake lake, cockpit-side,
and returns `{columns, rows, rowCount}` as **fully-materialized JSON-safe row
objects** (`src/duckdb/query-result.ts` → `getRowObjectsJson()`).

That shape is correct for its current consumer — the **chat agent**, which reasons
over a small, `LIMIT`-bounded text sample in-context. It is the wrong shape for the
*human* consumer we're now adding: a **canvas data grid** that should browse results
that can be far larger than what belongs in an LLM context. Shipping tens of
thousands of row-objects as one JSON blob is naive — it's row-oriented (repeats every
key on every row), it materializes the entire result before the first byte ships, and
it has no batching or backpressure.

This design adds a **separate, streaming, human/grid-facing result path** without
disturbing the agent's JSON tool.

## 2. Key constraint (verified) — the neo driver cannot emit Arrow

The idea that kicked this off was "stream Apache Arrow instead of JSON." We verified
against the pinned driver, `@duckdb/node-api@1.5.3-r.2`:

- **No Arrow surface in the neo API.** `grep -ri arrow` over `@duckdb/node-api/lib`
  → nothing. No `arrowIPCStream`, `toArrow`, `RecordBatch`, IPC.
- **The C-API Arrow functions are not bound.** In `@duckdb/node-bindings/duckdb.d.ts`
  every `duckdb_*_arrow*` line (`query_arrow`, `data_chunk_to_arrow`, `arrow_scan`, …)
  is **commented out**; the bindings only `export function` what they actually bind.
  `duckdb.js` (runtime) has zero `arrow` references.
- `arrowIPCStream()` exists only on the **deprecated** `duckdb` (node) package, which
  the team deliberately moved off of. Not an option.

What the neo reader/chunk getters actually return is **plain materialized JS heap**,
not a view over native memory:

```ts
reader.getRows()        // DuckDBValue[][]                — row-major JS arrays
reader.getRowObjects()  // Record<string, DuckDBValue>[]  — keyed JS objects
reader.getColumns()     // DuckDBValue[][]                — column-major JS arrays
// *Json variants additionally coerce to JSON-safe (bigint→string, dates→ISO)
```

The DuckDB native vector inside a `DuckDBDataChunk` is **already transcoded into JS
values** by the binding before TS ever touches it.

**Consequence:** zero-copy Arrow end-to-end is impossible with this driver — the copy
into the JS heap has already happened. Arrow on the wire would require a *second*
transcode (JS values → `apache-arrow` `RecordBatch` → IPC). Arrow remains a nice
pattern, **not a requirement**.

### Decision

Stream **columnar NDJSON** over a dedicated endpoint:

- one DuckDB chunk (~2048 rows) → one NDJSON line of **column arrays** → one flush;
- columnar (keys written once per batch, not per row) → most of the wire savings;
- no new dependency on either end; reuses the JSON-safe coercion we already trust;
- leaves a clean upgrade path to Arrow IPC later (§11) behind the same protocol shape.

**Non-goals:** changing the agent-facing `run_sql` tool (its JSON contract stays —
see §9, scope deferred); browser-side DuckDB-WASM; engine/HTTP changes.

## 3. Why a separate channel, not the chat SSE

These are two transports for two payloads and must not be conflated:

| | `/api/chat` | new grid endpoint |
|---|---|---|
| Content-Type | `text/event-stream` (SSE) | `application/x-ndjson` |
| Payload | agent text + tool-call lifecycle | columnar result batches |
| Framing | `event:`/`data:` UTF-8 frames | one JSON object per `\n` |
| Consumer | chat rail | canvas grid widget |

Multiplexing result data into the chat SSE would mean encoding bulk tabular data into
UTF-8 event frames riding alongside token deltas — wrong tool, head-of-line blocking
against the agent's text, and no independent lifecycle (cancel the grid without
cancelling the turn). The SSE carries only a lightweight **handle**; the grid fetches
the payload on its own channel.

```
chat SSE     ── text + tool_result{ handle: queryId, columns, rowCountHint } ─→ chat rail
grid HTTP    ── application/x-ndjson  (header · batch · batch · … · footer) ───→ canvas grid
```

## 4. Wire protocol — columnar NDJSON

One JSON object per line. Three frame kinds, discriminated by `t`:

```jsonc
// 1. header — first line, always
{"t":"h","columns":["id","name","amount","created_at"],
 "types":["INTEGER","VARCHAR","DECIMAL(18,2)","TIMESTAMP"],
 "queryId":"q_7f3a"}

// 2. batch — one per DuckDB chunk; arrays are column-major, JSON-safe, equal length
{"t":"b","n":2048,
 "cols":[[1,2,…],["ada","ben",…],["10.00","9.99",…],["2026-05-01T…","…"]]}

// 3. footer — last line, always (even on cap/error)
{"t":"f","rows":50000,"truncated":true,"cap":50000}
// or, on mid-stream failure:
{"t":"f","rows":4096,"error":"… DuckDB message …"}
```

- **Columnar** `cols[colIndex][rowIndex]` mirrors `getColumns*()` ordering — the client
  reconstructs a column store directly, no per-row key parsing.
- **Types** carried in the header (DuckDB type strings) drive client cell formatting
  (right-align numbers, render timestamps, etc.) without guessing from values.
- **Footer is mandatory** so the client can always distinguish "stream finished
  cleanly", "hit the cap" (`truncated`), and "failed mid-stream" (`error`). A
  truncated/errored stream still returns HTTP 200 — the body is the source of truth,
  because we can't change status codes after the first byte has flushed.
- JSON-safe values come from the same coercion `query-result.ts` already relies on
  (bigint→string, dates→ISO, nested→plain JSON), so the grid handles every DuckDB type
  the agent path already handles.

## 5. Server design

### 5.1 Route

A TanStack Start file route, same `createFileRoute(...).server.handlers.POST` +
`new Response(ReadableStream)` shape as `routes/api/chat.ts`:

```
src/routes/api/run-sql.ts        POST { sql, params?, cap? } → application/x-ndjson
```

### 5.2 Streaming loop (sketch)

Uses neo's lazy `stream()` (NOT `runAndReadAll`, which materializes everything) and
iterates `DuckDBDataChunk`s, converting each to JSON-safe column arrays:

```ts
// illustrative — not the patch
const conn = await getLakeConnection();
const wrapped = `SELECT * FROM (${sql}) AS _run_sql`;        // cap applied below, not LIMIT-in-SQL
const result = await conn.stream(wrapped, params);            // DuckDBResult, lazy

const columns = result.columnNames();
const types   = result.columnTypesJson();

const body = new ReadableStream({
  async start(controller) {
    const line = (o: unknown) => controller.enqueue(enc.encode(JSON.stringify(o) + "\n"));
    line({ t: "h", columns, types, queryId });

    let rows = 0, truncated = false;
    try {
      for await (const chunk of result) {                     // async-iterates chunks
        const remaining = cap - rows;
        if (remaining <= 0) { truncated = true; break; }
        const cols = chunk.convertColumns(jsonConverter);      // JSON-safe column arrays
        const n = Math.min(chunk.rowCount, remaining);
        line({ t: "b", n, cols: n < chunk.rowCount ? sliceCols(cols, n) : cols });
        rows += n;
        if (rows >= cap) { truncated = true; break; }
      }
      line({ t: "f", rows, truncated, ...(truncated && { cap }) });
    } catch (err) {
      line({ t: "f", rows, error: msg(err) });                 // fail in-band; body already 200
    } finally {
      controller.close();
    }
  },
  cancel() { /* see 5.4 */ },
});
return new Response(body, { headers: { "Content-Type": "application/x-ndjson",
                                       "Cache-Control": "no-cache, no-transform" } });
```

Per-chunk flush gives **natural backpressure**: `controller.enqueue` respects the
stream's desired size, so a slow client throttles `fetchChunk` rather than buffering
the whole result in server RAM. Peak server memory ≈ one chunk.

### 5.3 Connection model

- **Streaming reads** reuse the existing memoized, READ_ONLY lake connection
  (`getLakeConnection()`, `src/duckdb/lake.ts`). DuckDB connections serialize
  statements; concurrent grid queries on the one shared connection queue. For the
  expected interactive concurrency that's fine. If it bites, open a **small pool** of
  reader connections off the same `DuckDBInstance` (the instance is the expensive part;
  `instance.connect()` is cheap) — call out as a tuning knob, not MVP.
- **Materialized paging** (§6.2) needs its own short-lived connection so its temp table
  doesn't leak onto the shared reader — see §6.2.

### 5.4 Cancellation

User closes the grid / navigates away → `fetch` aborts → the `ReadableStream`'s
`cancel()` fires. We must stop pulling chunks. neo doesn't expose a hard interrupt on
the streaming result, so the loop checks an `aborted` flag set by `cancel()` and breaks
at the next chunk boundary (≤ one chunk of wasted work). Document this bound.

### 5.5 Validation / safety

- READ_ONLY ATTACH already blocks writes at the engine level (defense in depth — keep).
- `cap` clamped server-side to a hard ceiling (e.g. `min(cap ?? 50_000, 200_000)`) so a
  client can't ask for an unbounded materialization.
- `params` stay parameterized (neo positional binds) — same rule as today's tool.

## 6. Pagination & batching — two distinct axes

"Batching" and "pagination" are different concerns; the design handles both.

### 6.1 Intra-query batching (always on)

The streaming loop above **is** the batching: DuckDB hands ~2048-row chunks, each
becomes one NDJSON batch line, flushed immediately. The grid renders the first batch
before the last one is computed. This is the default and needs no client paging.

### 6.2 Result that exceeds the streaming cap — two options

**Option 1 — Stream-with-cap (MVP).** Stream up to `cap` rows, set `truncated:true` in
the footer, surface a "showing first N of many — refine your query" banner in the grid.
Simple, honest, covers the overwhelming majority of interactive use. No cursor state.

**Option 2 — Materialize + keyset paging (scale path, when needed).** For "let me page
through the whole thing" without re-running expensive SQL per page:

1. First request **materializes once** into a temp table on a dedicated connection:
   `CREATE TEMP TABLE _q_<id> AS <sql>`. Temp tables land in the writable in-memory
   `temp` catalog — allowed even though the lake is ATTACHed READ_ONLY.
2. Subsequent pages use **keyset on `rowid`** (stable, O(1) seek, no OFFSET re-scan):
   `SELECT * FROM _q_<id> WHERE rowid > $cursor ORDER BY rowid LIMIT $page`.
3. The footer of each page carries the next `cursor` (last `rowid`) or `done:true`.
4. TTL/cleanup: drop `_q_<id>` on grid close, on a sweep timer, and on process exit.

Why keyset over `LIMIT/OFFSET`: arbitrary user SQL has no guaranteed stable order, so
`OFFSET` can skip/duplicate rows between pages and costs an O(offset) re-scan. `rowid`
on the materialized snapshot is stable and cheap.

**Recommendation:** ship Option 1; add Option 2 only when a real "browse everything"
need appears. Keep the protocol forward-compatible: the header already carries
`queryId`, and the footer's `truncated` is the hook that later becomes "request more
via `?queryId=…&cursor=…`".

## 7. Client design

### 7.1 NDJSON reader (parallels `readSseStream`)

A sibling to `use-chat-stream.ts`'s `readSseStream` — same buffer-partial-reads
discipline, split on `\n` instead of `\n\n`:

```ts
// illustrative
export async function readNdjsonStream(
  body: ReadableStream<Uint8Array>,
  onFrame: (f: ResultFrame) => void,
): Promise<void> {
  const reader = body.getReader();
  const dec = new TextDecoder();
  let buf = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (value) buf += dec.decode(value, { stream: true });
    let nl = buf.indexOf("\n");
    while (nl !== -1) {
      const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
      if (line) onFrame(JSON.parse(line) as ResultFrame);
      nl = buf.indexOf("\n");
    }
    if (done) break;
  }
  if (buf.trim()) onFrame(JSON.parse(buf) as ResultFrame);
}
```

### 7.2 Columnar store

Accumulate batches into per-column arrays — never rebuild row-objects:

```ts
interface ColumnStore {
  columns: string[];
  types: string[];
  cols: unknown[][];   // cols[colIndex] grows as batches arrive
  rowCount: number;
  status: "streaming" | "done" | "truncated" | "error";
}
// on "b": for each c, store.cols[c].push(...frame.cols[c]); rowCount += frame.n
```

### 7.3 TanStack Table — index rows + accessor (Method 2 from the prompt)

TanStack Table wants an array; we give it **row indices**, and accessors read out of the
column store. No row-object rematerialization, integrates with virtualization:

```ts
const data = useMemo(() => Array.from({ length: store.rowCount }, (_, i) => i), [store.rowCount]);
const columns = useMemo(() =>
  store.columns.map((name, c) => ({
    id: name,
    header: name,
    accessorFn: (rowIndex: number) => store.cols[c][rowIndex],   // O(1) columnar read
    meta: { type: store.types[c] },                              // drives alignment/format
  })), [store.columns, store.types]);

const table = useReactTable({ data, columns, getCoreRowModel: getCoreRowModel() });
```

Sorting/filtering note: with index rows, default client sort/filter need custom
`sortingFn`/`filterFn` that resolve through the column store, **or** push sort/filter to
the server (re-issue SQL with `ORDER BY`/`WHERE`). Server-side is the natural fit here —
DuckDB is right there and the result is already capped — so the grid's sort/filter
controls re-run the query rather than sorting in JS. (MVP can ship read-only, no sort.)

### 7.4 Virtualization

For large caps, wrap the table body in `@tanstack/react-virtual` so only visible rows
hit the DOM. The columnar store + index rows make this cheap (no per-row object
allocation). Pull `@tanstack/react-virtual` in when the grid lands.

## 8. UI integration — the existing canvas seams

The DAT-347 canvas is "register-don't-replace" — a grid is purely additive:

1. **`canvas-state.ts`** — add a member:
   `| { kind: "result-grid"; queryId: string; columns: string[]; types: string[] }`
2. **`widgets/result-grid.tsx`** — new widget: receives the narrowed state, opens the
   NDJSON stream for `queryId`, owns the column store + TanStack table + virtualizer.
3. **`canvas-registry.ts`** — one `.register({ kind: "result-grid", component: ResultGrid })`.
4. **`tool-result-to-canvas.ts`** — one case mapping a `run_sql` tool result (carrying
   the handle) → `{ kind: "result-grid", queryId, columns, types }`.

No edits to `FocusCanvas`, the chat stream, or the shell — matches the contract in
`ui/cockpit/README.md`.

### 8.1 How the handle reaches the grid

The agent runs `run_sql`; the chat route emits `tool_result` over SSE carrying a
**handle** (`queryId` + column/type metadata + `rowCountHint`), not rows.
`tool-result-to-canvas.ts` maps that to the `result-grid` canvas state; the grid widget
then independently `fetch`es `/api/run-sql` (by `queryId`, or by re-sending the SQL —
see §9) and streams. This keeps bulk data off the SSE entirely.

## 9. Relationship to the agent-facing `run_sql` tool (scope: decide later)

Two consumers, two needs:

- **Agent**: small, `LIMIT`-bounded, JSON row-objects in-context. Today's
  `src/duckdb/run-sql.ts` + `tools/run_sql.ts`. **Unchanged in this design.**
- **Human grid**: streamed columnar NDJSON over the new endpoint.

Open decision (deferred per review): do both share one execution that derives the
agent's small sample from the first streamed batch (one query, two renderings), or stay
fully separate (agent re-runs with its own `LIMIT`)? The protocol supports either —
`queryId` is the join point if we unify later. **No code commitment until decided.**
The cleanest interim: the `run_sql` tool result includes both its small JSON sample
(for the agent) **and** a `queryId` handle (for the grid to stream the fuller result).

## 10. Dependencies

- MVP (Option 1 + read-only grid): **`@tanstack/react-table`** only. NDJSON reader is
  hand-rolled (mirrors existing `readSseStream`); no `apache-arrow`.
- When the grid grows: **`@tanstack/react-virtual`**.
- Arrow stays **out** unless/until §11 is triggered.

## 11. Future — Arrow upgrade path (not now)

If a future driver binds the Arrow C-API (or we add a server-side `apache-arrow`
encoder), the **same protocol shape** absorbs it: add a header field
`encoding: "arrow"`, and batch frames become length-prefixed Arrow IPC `RecordBatch`
bytes on a binary `application/vnd.apache.arrow.stream` response; the client swaps its
column store for an Arrow `Table` and keeps the identical index-row + `accessorFn`
TanStack wiring. The grid component above is the only thing that changes. Documented so
we don't paint ourselves in.

## 12. Testing

- **Unit**: `readNdjsonStream` with split/partial mock reads (mirror
  `use-chat-stream.test.ts`); frame parser; column-store accumulation; cap/truncation
  footer logic.
- **Integration** (`*.integration.test.ts`, like `run-sql.integration.test.ts`): stream
  a known lake table, assert header/batch/footer sequence, row count, type fidelity
  (bigint, DECIMAL, TIMESTAMP, LIST/STRUCT), `truncated` at cap, in-band error footer.
- **Cancellation**: abort the fetch mid-stream, assert the server loop stops within one
  chunk and the connection is reusable afterward.

## 13. Phasing

1. **P1 — Server stream.** `/api/run-sql` route, streaming loop over `conn.stream`,
   columnar NDJSON, cap+truncation, in-band errors, cancellation. Integration tests.
2. **P2 — Client grid.** `readNdjsonStream`, column store, `result-grid` widget +
   canvas-state member + registry + tool→canvas mapper. `@tanstack/react-table`.
   Read-only, no sort.
3. **P3 — Scale/UX.** Virtualization; server-side sort/filter (re-issue SQL); truncation
   banner.
4. **P4 (deferred, on demand).** Materialize + keyset paging (§6.2); agent/grid scope
   unification (§9).

## 14. Open questions

- Cap default + hard ceiling (50k / 200k?) — needs a real-data feel.
- Sort/filter: server-side re-query (recommended) vs client custom fns — confirm in P3.
- §9 scope (shared execution vs separate) — explicitly deferred.
- Do we want `queryId`-addressable re-fetch (server remembers SQL by id, enables P4
  paging) or stateless re-send-the-SQL? P1 can be stateless; P4 needs the registry.
