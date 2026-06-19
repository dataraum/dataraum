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
// Import set (DAT-592): "Add to import set" stages {import-as name, backend, sql};
// "Import N sources" calls the `importSources` server fn — deterministic, no LLM
// round-trip — which persists one source per query and starts ONE batched
// addSourceWorkflow run. The run's progress renders INLINE here (the canvas is
// message-derived, so a direct action can't project a canvas member), while the
// background completion-watcher narrates completion into the chat (the run is
// recorded against the conversationId we pass through).

import {
	ActionIcon,
	Alert,
	Badge,
	Button,
	Code,
	Group,
	Paper,
	Select,
	Stack,
	Text,
	TextInput,
} from "@mantine/core";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useParams } from "@tanstack/react-router";
import { X } from "lucide-react";
import { useMemo, useState } from "react";
import { getConfiguredDatabases } from "#/server/configured-databases";
import { importSources } from "#/server/import-sources";
import type { CanvasState } from "#/ui/cockpit/canvas-state";
import { MeasureProgressWidget } from "#/ui/cockpit/widgets/measure-progress";
import { StreamingGrid } from "#/ui/cockpit/widgets/result-grid";
import { SqlEditor } from "#/ui/cockpit/widgets/sql-editor";

/** One staged query — becomes one single-statement `db_recipe` source on import.
 * A `type` (not `interface`) so it's assignable to the server fn's input shape. */
type ImportSpec = {
	source_name: string;
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
		// progress renders below); the set is free to build the next batch.
		onSuccess: () => setImportSet([]),
	});

	const run = importMutation.data;

	return (
		<Stack gap="md" data-testid="canvas-probe">
			<ProbePanel
				key={seedKey}
				state={state}
				stagedNames={importSet.map((s) => s.source_name)}
				onAdd={addToSet}
			/>

			{importSet.length > 0 && (
				<ImportSetPanel
					set={importSet}
					onRemove={removeFromSet}
					onImport={() => importMutation.mutate(importSet)}
					pending={importMutation.isPending}
				/>
			)}

			{importMutation.error && (
				<Alert color="red" data-testid="probe-import-error">
					{(importMutation.error as Error).message}
				</Alert>
			)}

			{run && (
				<Stack gap="xs" data-testid="probe-import-progress">
					<Text size="sm" c="dimmed">
						Importing {run.source_names.length} source
						{run.source_names.length === 1 ? "" : "s"}:{" "}
						{run.source_names.join(", ")}. You'll be told in the chat when it's
						done.
					</Text>
					<MeasureProgressWidget
						state={{
							kind: "add-source-progress",
							workflowId: run.workflow_id,
							runId: run.run_id,
						}}
					/>
				</Stack>
			)}
		</Stack>
	);
}

function ProbePanel({
	state,
	stagedNames,
	onAdd,
}: {
	state: Extract<CanvasState, { kind: "probe" }>;
	stagedNames: string[];
	onAdd: (spec: ImportSpec) => void;
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
	const nameStaged = stagedNames.includes(importAs);
	const canAdd = selectedSource !== null && hasSql && nameValid && !nameStaged;

	const add = () => {
		if (!selectedSource || !hasSql || !nameValid) return;
		onAdd({
			source_name: importAs,
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
				{selectedSource && (
					<Badge variant="light" size="sm" tt="none">
						{selectedSource.backend}
					</Badge>
				)}
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
							: nameStaged
								? "already in the import set"
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
					Add to import set
				</Button>
			</Group>

			{submitted && (
				<StreamingGrid key={runId} endpoint="/api/probe-sql" body={submitted} />
			)}
		</Stack>
	);
}

function ImportSetPanel({
	set,
	onRemove,
	onImport,
	pending,
}: {
	set: ImportSpec[];
	onRemove: (name: string) => void;
	onImport: () => void;
	pending: boolean;
}) {
	return (
		<Paper withBorder p="sm" radius="sm" data-testid="probe-import-set">
			<Stack gap="xs">
				<Text size="sm" fw={600}>
					Import set ({set.length})
				</Text>
				<Text size="xs" c="dimmed">
					Each query imports as its own source. They import together as one run.
				</Text>
				<Stack gap={4}>
					{set.map((spec) => (
						<Group key={spec.source_name} justify="space-between" wrap="nowrap">
							<Group gap="xs" wrap="nowrap" style={{ minWidth: 0 }}>
								<Badge variant="light" size="sm" tt="none">
									{spec.source_name}
								</Badge>
								<Code
									style={{
										overflow: "hidden",
										textOverflow: "ellipsis",
										whiteSpace: "nowrap",
									}}
								>
									{spec.sql}
								</Code>
							</Group>
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
				<Group>
					<Button
						size="xs"
						onClick={onImport}
						loading={pending}
						data-testid="probe-import-run"
					>
						Import {set.length} source{set.length === 1 ? "" : "s"}
					</Button>
				</Group>
			</Stack>
		</Paper>
	);
}
