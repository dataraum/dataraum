// Probe widget → STAGING HUB (DAT-576 + DAT-592 + DAT-594) — the unified Connect
// surface: assemble a HETEROGENEOUS import set (uploaded FILES and probed SQL
// QUERIES), declare a business model over it (FRAME a new vertical or adopt a
// builtin), then click START to import the whole set in ONE batched
// addSourceWorkflow run.
//
// Runs (probing) stream through /api/probe-sql into the SAME virtualized result
// grid the lake uses (StreamingGrid), so a large external result never floods the
// DOM. The agent only SEEDS this surface: a `probe` tool call projects its source +
// sql into the editor (out of CHIP_ONLY) for the user to edit and re-run. The run
// itself is a direct fetch — no agent round-trip.
//
// Staging set (DAT-594): "Add to import set" stages a query, the 📤 Upload modal
// stages files; both live behind a COUNT SYMBOL in the panel header (always
// visible, never pushed below the tall result grid). The 🎯 Frame/Vertical modal
// declares the model (seeded from the staged set's schemas, or pick a builtin). The
// frame-before-import gate moved from "can't Add" to "can't START": staging is FREE,
// Start is gated on a non-empty set AND a framed workspace. Import calls the
// `importSources` server fn — deterministic, no LLM round-trip — which persists one
// source per item and starts ONE batched run. The run's progress renders INLINE at
// the top here (the canvas is message-derived, so a direct action can't project a
// canvas member), while the background completion-watcher narrates completion into
// the chat (the run is recorded against the conversationId we pass).

import {
	ActionIcon,
	Alert,
	Badge,
	Button,
	Code,
	Group,
	Loader,
	Modal,
	Select,
	Stack,
	Text,
	TextInput,
	Tooltip,
} from "@mantine/core";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useParams } from "@tanstack/react-router";
import {
	Download as DownloadIcon,
	FileText,
	Layers,
	Target,
	Upload as UploadIcon,
	X,
} from "lucide-react";
import { useMemo, useState } from "react";
import { progressQueryKey } from "#/lib/workflow-progress-event";
import { getActiveVerticalStatus } from "#/server/active-vertical";
import { getConfiguredDatabases } from "#/server/configured-databases";
import { importSources } from "#/server/import-sources";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { UploadDropzone } from "#/ui/cockpit/upload-dropzone";
import { MeasureProgressWidget } from "#/ui/cockpit/widgets/measure-progress";
import { ModelModal } from "#/ui/cockpit/widgets/model-modal";
import { StreamingGrid } from "#/ui/cockpit/widgets/result-grid";
import { SqlEditor } from "#/ui/cockpit/widgets/sql-editor";

/** One staged QUERY — becomes one single-statement `db_recipe` source on import.
 * `credential_source` is the configured connection the query reads through (the
 * probed source) — distinct from `source_name`, the new source's own name. */
type QueryItem = {
	kind: "query";
	source_name: string;
	credential_source: string;
	backend: string;
	sql: string;
};

/** One staged FILE — becomes one content-keyed `src_<digest>` source on import.
 * `file_uri` is the staged `s3://…/<digest>/<filename>` upload handle; `filename`
 * is its display leaf (the set lists files by original name, keyed by digest). */
type FileItem = {
	kind: "file";
	file_uri: string;
	filename: string;
	source_type: string;
};

/** One staged item in the heterogeneous import set. */
type ImportItem = QueryItem | FileItem;

/** The probe-sql request a Run submits — mirrors the /api/probe-sql body. */
type ProbeRun = {
	source_name: string;
	backend: string;
	sql: string;
};

// Client mirror of select/mappers SOURCE_NAME_PATTERN — the AUTHORITY lives there
// and the server re-validates loud; inlined to keep that crypto-bearing module out
// of the client bundle (the upload/policy split precedent). Gates the "Add" button
// for fast feedback only.
const SOURCE_NAME_RE = /^[a-z][a-z0-9_]{1,48}$/;

/** The display leaf (filename) of a staged `s3://…/<digest>/<filename>` URI. */
function uploadFilename(uri: string): string {
	return uri.split("/").filter(Boolean).at(-1) ?? uri;
}

/** The engine `source_type` from a URI suffix — a client-side mirror (the server
 * re-derives via select/mappers; this is just for the set's display badge). */
function sourceTypeForUriClient(uri: string): string {
	const lower = uri.toLowerCase();
	if (/\.(parquet|pq)$/.test(lower)) return "parquet";
	if (/\.(json|ndjson|jsonl)$/.test(lower)) return "json";
	return "csv";
}

export function ProbeWidget({
	state,
}: {
	state: Extract<CanvasState, { kind: "probe" }>;
}) {
	// Remount the EXPLORE panel whenever the agent SEED (source + sql) changes, so a
	// repeated probe / open_probe re-seeds the editor + picker via fresh state init
	// (idiom rule 5). The import set + an in-flight run's progress live ABOVE this
	// key so a re-seed never wipes a half-built set or a running import.
	const seedKey = `${state.source?.name ?? ""}|${state.sql ?? ""}`;

	// The originating chat (route param) — threaded to the import so the run is
	// recorded against it (the completion-watcher routes progress + narration by
	// conversationId). Absent off-route (the unit tests) → a null-conversation run.
	const params = useParams({ strict: false }) as { conversationId?: string };
	const conversationId = params.conversationId ?? null;

	const [importSet, setImportSet] = useState<ImportItem[]>([]);
	// Re-adding a query name re-points its SQL (an edit), never a silent duplicate;
	// re-adding a file URI (same digest) is a no-op (dedup by digest, like the
	// server). Queries key on source_name, files on file_uri.
	const addQuery = (item: QueryItem) =>
		setImportSet((s) => [
			...s.filter(
				(x) => !(x.kind === "query" && x.source_name === item.source_name),
			),
			item,
		]);
	const addFiles = (uris: string[]) =>
		setImportSet((s) => {
			const have = new Set(
				s
					.filter((x): x is FileItem => x.kind === "file")
					.map((x) => x.file_uri),
			);
			const fresh: FileItem[] = uris
				.filter((uri) => !have.has(uri))
				.map((uri) => ({
					kind: "file",
					file_uri: uri,
					filename: uploadFilename(uri),
					source_type: sourceTypeForUriClient(uri),
				}));
			return [...s, ...fresh];
		});
	// Remove by stable identity (query → source_name, file → file_uri).
	const removeItem = (id: string) =>
		setImportSet((s) =>
			s.filter((x) =>
				x.kind === "query" ? x.source_name !== id : x.file_uri !== id,
			),
		);

	const queryCount = importSet.filter((x) => x.kind === "query").length;

	const queryClient = useQueryClient();
	// Bumped per import so a SECOND import in this chat REMOUNTS the progress widget.
	// The run's workflow id is the reused per-workspace `addsource-<ws>` (DAT-562), so
	// without a per-import key the widget would keep the prior import's node (DAT-595).
	const [importEpoch, setImportEpoch] = useState(0);

	const importMutation = useMutation({
		mutationFn: (items: ImportItem[]) =>
			importSources({
				data: {
					queries: items
						.filter((x): x is QueryItem => x.kind === "query")
						.map(({ source_name, credential_source, backend, sql }) => ({
							source_name,
							credential_source,
							backend,
							sql,
						})),
					files: items
						.filter((x): x is FileItem => x.kind === "file")
						.map((x) => ({ file_uri: x.file_uri })),
					conversationId,
				},
			}),
		onSuccess: (data) => {
			// Clear the staged set — the run is now live (its progress renders at the
			// top); the set is free to build the next batch.
			setImportSet([]);
			setImportEpoch((e) => e + 1);
			// Reset the PRIOR import's cached (terminal) progress under the reused
			// `addsource-<ws>` key so the widget re-seeds to THIS run's latest execution
			// — the poll is `refetchInterval: false`, so a stale done-snapshot would
			// otherwise linger and show the previous import's tables (DAT-595).
			queryClient.resetQueries({
				queryKey: progressQueryKey(data.workflow_id),
			});
		},
	});

	const run = importMutation.data;

	return (
		<Stack gap="md" data-testid="canvas-probe">
			{/* Run status sits at the TOP, always visible — never below the tall grid. */}
			{importMutation.error && (
				<Alert color="red" data-testid="probe-import-error">
					{(importMutation.error as Error).message}
				</Alert>
			)}
			{importMutation.isPending && !run && (
				<Group gap="xs" data-testid="probe-import-starting">
					<Loader size="sm" />
					<Text size="sm" c="dimmed">
						Starting import…
					</Text>
				</Group>
			)}
			{run && (
				<Stack gap="xs" data-testid="probe-import-progress">
					<Text size="sm" c="dimmed">
						Importing {run.source_names.length} source
						{run.source_names.length === 1 ? "" : "s"}:{" "}
						{run.source_names.join(", ")}.
						{conversationId
							? " You'll be told in the chat when it's done."
							: ""}
					</Text>
					{/* Keyed per IMPORT (workflow id + epoch), not just the reused
					    per-workspace workflow id, so a second import remounts a fresh
					    progress node instead of reusing the prior import's (DAT-595). */}
					<MeasureProgressWidget
						key={`${run.workflow_id}:${importEpoch}`}
						state={{
							kind: "add-source-progress",
							workflowId: run.workflow_id,
							runId: run.run_id,
						}}
					/>
				</Stack>
			)}

			<ProbePanel
				key={seedKey}
				state={state}
				importSet={importSet}
				queryCount={queryCount}
				onAddQuery={addQuery}
				onAddFiles={addFiles}
				onRemove={removeItem}
				onImport={() => importMutation.mutate(importSet)}
				pending={importMutation.isPending}
			/>
		</Stack>
	);
}

function ProbePanel({
	state,
	importSet,
	queryCount,
	onAddQuery,
	onAddFiles,
	onRemove,
	onImport,
	pending,
}: {
	state: Extract<CanvasState, { kind: "probe" }>;
	importSet: ImportItem[];
	queryCount: number;
	onAddQuery: (item: QueryItem) => void;
	onAddFiles: (uris: string[]) => void;
	onRemove: (id: string) => void;
	onImport: () => void;
	pending: boolean;
}) {
	const queryClient = useQueryClient();
	const sources = useQuery({
		queryKey: ["configured-databases"],
		queryFn: () => getConfiguredDatabases(),
	});
	const list = useMemo(() => sources.data ?? [], [sources.data]);

	// Is the workspace framed? Importing grounds against the vertical's concepts and
	// fails loud if there are none, so the import START is gated on this (DAT-594:
	// the gate moved from Add to Start). Probing (Run) and staging stay open —
	// read-only exploration + set-building is how you decide the frame. `framed`
	// only when CONFIRMED true (loading/error → gated, safe).
	const frameStatus = useQuery({
		queryKey: ["active-vertical-status"],
		queryFn: () => getActiveVerticalStatus(),
		// Session-stable — only the `frame`/`use_vertical` flow changes it — so don't
		// re-hit the server on every window focus. The frame/vertical modal
		// invalidates this key on success so the Start gate flips immediately.
		staleTime: 5 * 60 * 1000,
	});
	const framed = frameStatus.data?.framed === true;
	const unframed = frameStatus.isSuccess && !framed;

	// Seed from the canvas state (agent-generate): a projected source + sql preload
	// the picker + editor as INITIAL values; the user then edits freely.
	const [selected, setSelected] = useState<string | null>(
		state.source?.name ?? null,
	);
	const [sqlText, setSqlText] = useState<string>(state.sql ?? "");
	const [importAs, setImportAs] = useState<string>("");
	const [submitted, setSubmitted] = useState<ProbeRun | null>(null);
	// Bumped per Run so an identical re-run still remounts StreamingGrid (fresh
	// stream + sort reset), not just a different query.
	const [runId, setRunId] = useState(0);
	// View state for the three modals — pure UI, fine to reset on re-seed (the SET
	// itself lives in ProbeWidget and survives).
	const [setOpen, setSetOpen] = useState(false);
	const [uploadOpen, setUploadOpen] = useState(false);
	const [modelOpen, setModelOpen] = useState(false);

	const selectedSource = useMemo(
		() => list.find((s) => s.name === selected) ?? null,
		[list, selected],
	);
	const hasSql = sqlText.trim().length > 0;
	const canRun = selectedSource !== null && hasSql;

	const run = () => {
		if (!selectedSource || !hasSql) return;
		setSubmitted({
			source_name: selectedSource.name,
			backend: selectedSource.backend,
			sql: sqlText,
		});
		setRunId((r) => r + 1);
	};

	const nameValid = SOURCE_NAME_RE.test(importAs);
	// Re-using a staged name UPDATES that query's SQL (addQuery replaces by name) —
	// a valid edit, not a duplicate. So the gate ALLOWS it; only the button label
	// flips to "Update" so the action is unambiguous.
	const nameStaged = importSet.some(
		(s) => s.kind === "query" && s.source_name === importAs,
	);
	// Staging is FREE now (DAT-594): no `framed` gate on Add — the frame-before-
	// import gate moved to START. Add only needs a source, SQL, and a valid name.
	const canAdd = selectedSource !== null && hasSql && nameValid;

	const add = () => {
		if (!selectedSource || !hasSql || !nameValid) return;
		onAddQuery({
			kind: "query",
			source_name: importAs,
			// The picked source IS the connection the query reads through; the engine
			// resolves credentials from it, so the new source can be named freely.
			credential_source: selectedSource.name,
			backend: selectedSource.backend,
			sql: sqlText,
		});
		setImportAs("");
	};

	const noSources = !sources.isLoading && list.length === 0;
	// The Start gate (DAT-594): a non-empty set AND a framed workspace. Disabled
	// Start tells the user WHICH precondition is missing.
	const canStart = importSet.length > 0 && framed;
	const startBlockedReason =
		importSet.length === 0
			? "Stage a query or upload a file first."
			: !framed
				? "Declare a business model (Frame / Vertical) before importing."
				: undefined;

	return (
		<Stack gap="sm" data-testid="probe-explore">
			<Group justify="space-between" wrap="nowrap">
				<Text size="sm" fw={600}>
					Assemble an import set
				</Text>
				<Group gap="xs" wrap="nowrap">
					{selectedSource && (
						<Badge variant="light" size="sm" tt="none">
							{selectedSource.backend}
						</Badge>
					)}
					{/* Header toolbar (DAT-594): Upload files, declare the model, and the
					    set count symbol — always here (not pushed below the result grid). */}
					<Tooltip label="Upload files">
						<Button
							size="compact-xs"
							variant="light"
							leftSection={<UploadIcon size={14} />}
							onClick={() => setUploadOpen(true)}
							data-testid="probe-upload-open"
						>
							Upload
						</Button>
					</Tooltip>
					<Tooltip label="Frame a model or pick a vertical">
						<Button
							size="compact-xs"
							variant="light"
							leftSection={<Target size={14} />}
							onClick={() => setModelOpen(true)}
							data-testid="probe-model-open"
						>
							{framed ? "Model ✓" : "Frame / Vertical"}
						</Button>
					</Tooltip>
					{importSet.length > 0 && (
						<Tooltip label="View import set">
							<Button
								size="compact-xs"
								variant="light"
								leftSection={<Layers size={14} />}
								onClick={() => setSetOpen(true)}
								data-testid="probe-import-indicator"
							>
								{importSet.length}
							</Button>
						</Tooltip>
					)}
					{/* The primary action lives here, last in the toolbar — gated on a
					    non-empty set AND a framed workspace. The reason it's disabled
					    shows on hover (the <span> wrapper is the tooltip target so it
					    still fires while the Button has pointer-events:none). */}
					<Tooltip
						label={
							startBlockedReason ??
							`Import ${importSet.length} source${importSet.length === 1 ? "" : "s"} in one run`
						}
					>
						<span>
							<Button
								size="compact-xs"
								variant="filled"
								leftSection={<DownloadIcon size={14} />}
								onClick={onImport}
								disabled={!canStart}
								loading={pending}
								data-testid="probe-start"
							>
								Import
							</Button>
						</span>
					</Tooltip>
				</Group>
			</Group>
			<Text size="xs" c="dimmed">
				Probe a database with read-only DuckDB SQL (use LIMIT, not TOP) and
				stage queries, or upload files — then declare a model and Start the
				import. ⌘/Ctrl+Enter to run.
			</Text>

			{unframed && (
				<Alert color="yellow" data-testid="probe-unframed">
					No business model yet —{" "}
					{frameStatus.data?.vertical === "_adhoc" ? (
						"this workspace hasn't been framed"
					) : (
						<>
							the vertical{" "}
							<Text span ff="monospace" size="xs">
								{frameStatus.data?.vertical}
							</Text>{" "}
							has no concepts
						</>
					)}
					. Stage your set, then declare a model (Frame / Vertical) before you
					Start — an imported source has nothing to ground against without one.
				</Alert>
			)}

			<Select
				data-testid="probe-source-select"
				placeholder={
					sources.isLoading
						? "Loading sources…"
						: list.length
							? "Pick a source"
							: "No configured sources"
				}
				data={list.map((s) => ({
					value: s.name,
					label: `${s.name} (${s.backend})`,
				}))}
				value={selected}
				onChange={setSelected}
				disabled={sources.isLoading || list.length === 0}
				searchable
			/>

			{noSources && (
				<Alert color="gray" data-testid="probe-no-sources">
					No configured database sources. Set a{" "}
					<Text span ff="monospace" size="xs">
						DATARAUM_&lt;NAME&gt;_URL
					</Text>{" "}
					and bring the source up (see{" "}
					<Text span ff="monospace" size="xs">
						docker-compose.sources.yml
					</Text>
					), or upload files instead.
				</Alert>
			)}

			<SqlEditor
				value={sqlText}
				onChange={setSqlText}
				onRun={run}
				placeholder="SELECT * FROM my_schema.my_table LIMIT 100"
			/>

			<Group align="flex-end" gap="sm" wrap="nowrap">
				<Button
					size="xs"
					variant="default"
					onClick={run}
					disabled={!canRun}
					data-testid="probe-run"
				>
					Run
				</Button>
				<TextInput
					size="xs"
					flex={1}
					label="Import as"
					placeholder="source_name (lowercase, e.g. wwi_open_orders)"
					value={importAs}
					onChange={(e) => setImportAs(e.currentTarget.value)}
					error={
						importAs.length > 0 && !nameValid
							? "lowercase, start with a letter, [a-z0-9_], 2–49 chars"
							: undefined
					}
					data-testid="probe-import-name"
				/>
				<Button
					size="xs"
					onClick={add}
					disabled={!canAdd}
					data-testid="probe-add-to-set"
				>
					{nameStaged ? "Update query" : "Add to import set"}
				</Button>
			</Group>

			{submitted && (
				<StreamingGrid key={runId} endpoint="/api/probe-sql" body={submitted} />
			)}

			{/* Upload modal — reuses the standalone dropzone; staged files join the set. */}
			<Modal
				opened={uploadOpen}
				onClose={() => setUploadOpen(false)}
				centered
				size="lg"
				title="Upload files to the import set"
			>
				<UploadDropzone
					onUploaded={(uris) => {
						onAddFiles(uris);
						setUploadOpen(false);
					}}
				/>
			</Modal>

			{/* Frame / Vertical modal — declares the model from the staged set's schemas
			    (frame a new vertical) or adopts a builtin; invalidates the status query
			    on success so the Start gate flips. */}
			<ModelModal
				opened={modelOpen}
				onClose={() => setModelOpen(false)}
				importSet={importSet}
				onModelDeclared={() => {
					queryClient.invalidateQueries({
						queryKey: ["active-vertical-status"],
					});
					setModelOpen(false);
				}}
			/>

			<Modal
				opened={setOpen}
				onClose={() => setSetOpen(false)}
				centered
				size="lg"
				title={`Import set (${importSet.length})`}
			>
				<Stack gap="sm">
					<Text size="xs" c="dimmed">
						{queryCount} quer{queryCount === 1 ? "y" : "ies"} and{" "}
						{importSet.length - queryCount} file
						{importSet.length - queryCount === 1 ? "" : "s"}. Each imports as
						its own source; they import together as one run.
					</Text>
					{importSet.length === 0 ? (
						<Text size="sm" c="dimmed">
							No queries or files staged yet.
						</Text>
					) : (
						<Stack gap="xs" data-testid="probe-import-set">
							{importSet.map((item) =>
								item.kind === "query" ? (
									<Group
										key={`q:${item.source_name}`}
										justify="space-between"
										wrap="nowrap"
										align="flex-start"
									>
										<Stack gap={2} style={{ minWidth: 0, flex: 1 }}>
											<Badge variant="light" size="sm" tt="none">
												{item.source_name}
											</Badge>
											<Code block>{item.sql}</Code>
										</Stack>
										<ActionIcon
											variant="subtle"
											color="gray"
											size="sm"
											aria-label={`Remove ${item.source_name}`}
											onClick={() => onRemove(item.source_name)}
										>
											<X size={14} />
										</ActionIcon>
									</Group>
								) : (
									<Group
										key={`f:${item.file_uri}`}
										justify="space-between"
										wrap="nowrap"
										align="center"
									>
										<Group
											gap="xs"
											style={{ minWidth: 0, flex: 1 }}
											wrap="nowrap"
										>
											<FileText size={14} aria-hidden />
											<Text size="sm" truncate>
												{item.filename}
											</Text>
											<Badge variant="light" size="xs" tt="none">
												{item.source_type}
											</Badge>
										</Group>
										<ActionIcon
											variant="subtle"
											color="gray"
											size="sm"
											aria-label={`Remove ${item.filename}`}
											onClick={() => onRemove(item.file_uri)}
										>
											<X size={14} />
										</ActionIcon>
									</Group>
								),
							)}
						</Stack>
					)}
					{/* Review/remove only — Start funnels through the single panel-level
					    button (probe-start) so the framed-gate lives in one place. */}
				</Stack>
			</Modal>
		</Stack>
	);
}
