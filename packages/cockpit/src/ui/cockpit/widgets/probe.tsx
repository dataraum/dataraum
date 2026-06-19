// Probe widget (DAT-576 + DAT-592) — the editable Connect-phase surface: pick a
// configured DB source, write/edit read-only SQL, run it against the external DB
// BEFORE ingest, and (DAT-592) stage queries into an IMPORT SET that imports each
// as its own single-statement `db_recipe` source.
//
// Runs stream through /api/probe-sql into the SAME virtualized result grid the lake
// uses (StreamingGrid), so a large external result never floods the DOM. The agent
// only SEEDS this surface: a `probe` tool call projects its source + sql into the
// editor (out of CHIP_ONLY) for the user to edit and re-run. The run itself is a
// direct fetch — no agent round-trip.
//
// Import set (DAT-592): "Add to import set" stages {import-as name, backend, sql}.
// The set lives behind a COUNT SYMBOL in the panel header (always visible, never
// pushed below the tall result grid); clicking it opens a centered MODAL listing
// the staged queries with remove + "Import N sources". Import calls the
// `importSources` server fn — deterministic, no LLM round-trip — which persists one
// source per query and starts ONE batched addSourceWorkflow run. The run's progress
// renders INLINE at the top here (the canvas is message-derived, so a direct action
// can't project a canvas member), while the background completion-watcher narrates
// completion into the chat (the run is recorded against the conversationId we pass).

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
import { useMutation, useQuery } from "@tanstack/react-query";
import { useParams } from "@tanstack/react-router";
import { Layers, X } from "lucide-react";
import { useMemo, useState } from "react";
import { getConfiguredDatabases } from "#/server/configured-databases";
import { importSources } from "#/server/import-sources";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { MeasureProgressWidget } from "#/ui/cockpit/widgets/measure-progress";
import { StreamingGrid } from "#/ui/cockpit/widgets/result-grid";
import { SqlEditor } from "#/ui/cockpit/widgets/sql-editor";

/** One staged query — becomes one single-statement `db_recipe` source on import.
 * A `type` (not `interface`) so it's assignable to the server fn's input shape.
 * `credential_source` is the configured connection the query reads through (the
 * probed source) — distinct from `source_name`, the new source's own name. */
type ImportSpec = {
	source_name: string;
	credential_source: string;
	backend: string;
	sql: string;
};

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

	const [importSet, setImportSet] = useState<ImportSpec[]>([]);
	// Re-adding a name re-points its SQL (an edit), never a silent duplicate.
	const addToSet = (spec: ImportSpec) =>
		setImportSet((s) => [
			...s.filter((x) => x.source_name !== spec.source_name),
			spec,
		]);
	const removeFromSet = (name: string) =>
		setImportSet((s) => s.filter((x) => x.source_name !== name));

	const importMutation = useMutation({
		mutationFn: (specs: ImportSpec[]) =>
			importSources({ data: { sources: specs, conversationId } }),
		// Clear the staged set on a successful start — the run is now live (its
		// progress renders at the top); the set is free to build the next batch.
		onSuccess: () => setImportSet([]),
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
					{/* Keyed by workflow id so a second import (a new run id under the same
					    per-workspace workflow id) remounts cleanly rather than reusing a
					    stale node — forward-compat if the id ever becomes per-run. */}
					<MeasureProgressWidget
						key={run.workflow_id}
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
				onAdd={addToSet}
				onRemove={removeFromSet}
				onImport={() => importMutation.mutate(importSet)}
				pending={importMutation.isPending}
			/>
		</Stack>
	);
}

function ProbePanel({
	state,
	importSet,
	onAdd,
	onRemove,
	onImport,
	pending,
}: {
	state: Extract<CanvasState, { kind: "probe" }>;
	importSet: ImportSpec[];
	onAdd: (spec: ImportSpec) => void;
	onRemove: (name: string) => void;
	onImport: () => void;
	pending: boolean;
}) {
	const sources = useQuery({
		queryKey: ["configured-databases"],
		queryFn: () => getConfiguredDatabases(),
	});
	const list = useMemo(() => sources.data ?? [], [sources.data]);

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
	// The import-set modal — pure view state, fine to reset on re-seed (the SET
	// itself lives in ProbeWidget and survives).
	const [setOpen, setSetOpen] = useState(false);

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
	// Re-using a staged name UPDATES that query's SQL (addToSet replaces by name) —
	// a valid edit, not a duplicate. So the gate ALLOWS it; only the button label
	// flips to "Update" so the action is unambiguous.
	const nameStaged = importSet.some((s) => s.source_name === importAs);
	const canAdd = selectedSource !== null && hasSql && nameValid;

	const add = () => {
		if (!selectedSource || !hasSql || !nameValid) return;
		onAdd({
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

	return (
		<Stack gap="sm" data-testid="probe-explore">
			<Group justify="space-between" wrap="nowrap">
				<Text size="sm" fw={600}>
					Probe a database source
				</Text>
				<Group gap="xs" wrap="nowrap">
					{selectedSource && (
						<Badge variant="light" size="sm" tt="none">
							{selectedSource.backend}
						</Badge>
					)}
					{/* The import-set count symbol — always here in the header (not pushed
					    below the result grid), opens the staged-queries modal. */}
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
				</Group>
			</Group>
			<Text size="xs" c="dimmed">
				Read-only DuckDB SQL against a configured source (use LIMIT, not TOP) —
				no data is imported until you add a query to the import set.
				⌘/Ctrl+Enter to run.
			</Text>

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
					).
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

			<Modal
				opened={setOpen}
				onClose={() => setSetOpen(false)}
				centered
				size="lg"
				title={`Import set (${importSet.length})`}
			>
				<Stack gap="sm">
					<Text size="xs" c="dimmed">
						Each query imports as its own source. They import together as one
						run.
					</Text>
					{importSet.length === 0 ? (
						<Text size="sm" c="dimmed">
							No queries staged yet.
						</Text>
					) : (
						<Stack gap="xs" data-testid="probe-import-set">
							{importSet.map((spec) => (
								<Group
									key={spec.source_name}
									justify="space-between"
									wrap="nowrap"
									align="flex-start"
								>
									<Stack gap={2} style={{ minWidth: 0, flex: 1 }}>
										<Badge variant="light" size="sm" tt="none">
											{spec.source_name}
										</Badge>
										<Code block>{spec.sql}</Code>
									</Stack>
									<ActionIcon
										variant="subtle"
										color="gray"
										size="sm"
										aria-label={`Remove ${spec.source_name}`}
										onClick={() => onRemove(spec.source_name)}
									>
										<X size={14} />
									</ActionIcon>
								</Group>
							))}
						</Stack>
					)}
					<Group justify="flex-end">
						<Button
							size="xs"
							disabled={importSet.length === 0}
							loading={pending}
							onClick={() => {
								onImport();
								setSetOpen(false);
							}}
							data-testid="probe-import-run"
						>
							Import {importSet.length} source
							{importSet.length === 1 ? "" : "s"}
						</Button>
					</Group>
				</Stack>
			</Modal>
		</Stack>
	);
}
