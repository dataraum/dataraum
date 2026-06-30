// The chart authoring modal (DAT-626 / ADR-0015).
//
// Opens to an EMPTY state — no auto/default chart (a type-sniffing heuristic isn't
// worth the code; ADR-0015). Describing the chart is the PRIMARY path: a typed
// instruction → the forced-tool chart agent → a config that seeds the draft. The
// per-encoding controls are the same draft, surfaced behind an "Edit" disclosure
// that opens to a one-line readout of the current mapping (the deterministic
// escape hatch + the way to fine-tune a generated config). Either path produces a
// draft → validated config → LIVE preview. Accept freezes the config; the caller
// decides what "freeze" means (mint it with a report, etc.).
//
// The preview data is ONE capped page from the grid stream (`/api/run-sql`, max
// GRID_MAX_PAGE rows): charts are for aggregated results, so the cap doubles as the
// nudge — a `truncated` page shows the "charting the first N rows" warning. The
// Vega renderer is client-only (vega measures the DOM), mounted under <ClientOnly>.
//
// Split shell/content: the shell owns the (cached) data fetch; the content owns the
// authoring DRAFT and is remounted per open via a `key` (React rule 5), so each
// open starts fresh from the existing config — never a stale draft.

import {
	Alert,
	Button,
	Center,
	Collapse,
	Group,
	Loader,
	Modal,
	Paper,
	Select,
	Stack,
	Text,
	Textarea,
	TextInput,
} from "@mantine/core";
import { ClientOnly } from "@tanstack/react-router";
import { ChevronDown, ChevronUp, Sparkles, TriangleAlert } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import {
	AGGREGATES,
	CHART_MARKS,
	type ChartConfig,
	ChartConfigSchema,
	FIELD_TYPES,
	type FieldEncoding,
} from "#/charts/chart-config";
import { columnOptions } from "#/charts/chart-data";
import {
	type ChartDraft,
	draftToConfig,
	type EncodingDraft,
	emptyDraft,
	summarizeDraft,
} from "#/charts/manual-mapping";
import { type ChartData, useChartData } from "#/charts/use-chart-data";
import { validateChartConfig } from "#/charts/validate";
import { ChartView } from "#/ui/cockpit/widgets/chart-view";

export function ChartModal({
	opened,
	onClose,
	sql,
	params,
	value,
	onAccept,
}: {
	opened: boolean;
	onClose: () => void;
	sql: string;
	params?: (string | number | boolean | null)[];
	/** An existing frozen config when re-opening an already-charted surface. */
	value?: ChartConfig | null;
	/** Accept the authored config (or null to clear the chart) and close. */
	onAccept: (config: ChartConfig | null) => void;
}) {
	// Fetch one capped page for the preview — server data via the shared chart-data
	// query (React rule 3), only while the modal is open. Plain rows + columns/types,
	// bounded by GRID_MAX_PAGE.
	const { data, isLoading, error } = useChartData(sql, params, opened);

	return (
		<Modal
			opened={opened}
			onClose={onClose}
			title="Chart"
			size="xl"
			data-testid="chart-modal"
		>
			{isLoading ? (
				<Center h={240}>
					<Loader size="sm" />
				</Center>
			) : error ? (
				<Alert color="red" data-testid="chart-modal-error">
					Couldn’t load the result to chart: {String(error)}
				</Alert>
			) : !data || data.columns.length === 0 ? (
				<Text c="dimmed" size="sm" data-testid="chart-modal-empty-result">
					This result has no columns to chart.
				</Text>
			) : (
				// Remount on each open so the draft re-seeds from `value` (React rule 5).
				<ChartModalContent
					key={String(opened)}
					data={data}
					value={value}
					onAccept={onAccept}
					onClose={onClose}
				/>
			)}
		</Modal>
	);
}

/** One encoding row (x / y / color): column, measurement type, aggregate. */
function EncodingControls({
	label,
	optional,
	draft,
	columns,
	suggestFor,
	onChange,
}: {
	label: string;
	optional?: boolean;
	draft: EncodingDraft;
	columns: { value: string; label: string }[];
	/** Suggested measurement type for a column (applied when the column changes). */
	suggestFor: (column: string) => EncodingDraft["type"];
	onChange: (next: EncodingDraft) => void;
}) {
	const id = label.toLowerCase();
	return (
		<Group gap="xs" wrap="nowrap" align="flex-end">
			<Select
				label={label}
				placeholder={optional ? "(none)" : "Pick a column"}
				data={columns}
				value={draft.field}
				clearable={optional}
				searchable
				size="xs"
				style={{ flex: 2 }}
				data-testid={`chart-enc-${id}-field`}
				onChange={(field) =>
					onChange({
						...draft,
						field,
						// Default the measurement type to the column's suggestion on pick.
						type: field ? suggestFor(field) : draft.type,
					})
				}
			/>
			<Select
				label="Type"
				data={FIELD_TYPES.map((t) => ({ value: t, label: t }))}
				value={draft.type}
				size="xs"
				allowDeselect={false}
				style={{ flex: 1 }}
				data-testid={`chart-enc-${id}-type`}
				onChange={(type) =>
					type && onChange({ ...draft, type: type as EncodingDraft["type"] })
				}
			/>
			<Select
				label="Aggregate"
				placeholder="(none)"
				data={AGGREGATES.map((a) => ({ value: a, label: a }))}
				value={draft.aggregate ?? null}
				clearable
				size="xs"
				style={{ flex: 1 }}
				data-testid={`chart-enc-${id}-agg`}
				onChange={(aggregate) =>
					onChange({
						...draft,
						aggregate: (aggregate as EncodingDraft["aggregate"]) ?? null,
					})
				}
			/>
		</Group>
	);
}

function ChartModalContent({
	data,
	value,
	onAccept,
	onClose,
}: {
	data: ChartData;
	value: ChartConfig | null | undefined;
	onAccept: (config: ChartConfig | null) => void;
	onClose: () => void;
}) {
	const [draft, setDraft] = useState<ChartDraft>(() => fromConfig(value));
	// Text-to-chart (the primary path): a typed instruction → the forced-tool author
	// → a config that seeds the draft below (which the user can still tweak/finalize).
	const [instruction, setInstruction] = useState("");
	const [authoring, setAuthoring] = useState(false);
	const [authorError, setAuthorError] = useState<string | null>(null);
	// The per-encoding controls are secondary — collapsed behind a readout of the
	// current mapping, opened on demand to fine-tune or to map from scratch.
	const [editing, setEditing] = useState(false);
	// Abort an in-flight author when the user cancels/closes or fires a new one —
	// otherwise the server's forced-tool loop keeps billing Anthropic calls after the
	// modal is gone (the fetch unmount doesn't close the connection).
	const authorAbortRef = useRef<AbortController | null>(null);
	useEffect(() => () => authorAbortRef.current?.abort(), []);

	const options = useMemo(
		() => columnOptions(data.columns, data.types),
		[data.columns, data.types],
	);
	const columnData = useMemo(
		() => options.map((o) => ({ value: o.name, label: o.name })),
		[options],
	);
	const suggestFor = useMemo(() => {
		const byName = new Map(options.map((o) => [o.name, o.suggestedType]));
		return (column: string) => byName.get(column) ?? "nominal";
	}, [options]);

	// Ask the agent for a chart from the typed instruction (React rule 4: a user-
	// event mutation in a handler). On success the returned config seeds the draft;
	// the circuit-breaker error (after ≤3 attempts) is surfaced inline.
	const authorFromInstruction = async () => {
		const text = instruction.trim();
		if (!text) return;
		authorAbortRef.current?.abort();
		const ac = new AbortController();
		authorAbortRef.current = ac;
		setAuthoring(true);
		setAuthorError(null);
		try {
			const res = await fetch("/api/charts/author", {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({
					columns: options.map((o) => ({
						name: o.name,
						type: o.suggestedType,
					})),
					instruction: text,
				}),
				signal: ac.signal,
			});
			// Tool/LLM output is `unknown` at the boundary — narrow the wire shape AND
			// re-validate the config against the schema before trusting it (rule 11).
			const body = (await res.json()) as { config?: unknown; error?: unknown };
			if (!res.ok || body.config === undefined) {
				setAuthorError(
					typeof body.error === "string"
						? body.error
						: "Couldn’t author a chart — try again.",
				);
				return;
			}
			const parsed = ChartConfigSchema.safeParse(body.config);
			if (!parsed.success) {
				setAuthorError("Received an unexpected chart shape — try again.");
				return;
			}
			setDraft(fromConfig(parsed.data));
		} catch (err) {
			if (ac.signal.aborted) return; // user cancelled — no error to show
			console.error("[charts] author failed:", err);
			setAuthorError("Couldn’t reach the chart agent — try again.");
		} finally {
			setAuthoring(false);
		}
	};

	// Draft → config → validate, every render: the preview shows only a config that
	// passes the gate; the message explains an in-progress/invalid mapping.
	const candidate = draftToConfig(draft);
	const validation = candidate
		? validateChartConfig(candidate, data.columns)
		: null;
	const validConfig = validation?.ok ? validation.config : null;

	return (
		<Stack gap="md">
			{data.truncated && (
				<Alert
					color="yellow"
					icon={<TriangleAlert size={16} />}
					data-testid="chart-modal-truncated"
				>
					Charting the first {data.rowCount.toLocaleString()} rows — the result
					has more. Aggregate the query (GROUP BY / summary) to chart the whole
					result.
				</Alert>
			)}

			{/* Primary path: describe the chart and let the agent author it. The
			    result seeds the manual mapping below, which the user can fine-tune. */}
			<Stack gap={6}>
				<Group gap="xs" align="flex-end" wrap="nowrap">
					<Textarea
						label="Describe the chart"
						placeholder="e.g. revenue by month as a line, colored by region"
						value={instruction}
						size="xs"
						autosize
						minRows={2}
						maxRows={4}
						style={{ flex: 1 }}
						data-testid="chart-instruction"
						onChange={(e) => setInstruction(e.currentTarget.value)}
						onKeyDown={(e) => {
							// Enter submits; Shift+Enter keeps the newline (it's a textarea).
							if (e.key === "Enter" && !e.shiftKey) {
								e.preventDefault();
								if (!authoring) authorFromInstruction();
							}
						}}
					/>
					<Button
						size="compact-sm"
						loading={authoring}
						disabled={!instruction.trim()}
						leftSection={<Sparkles size={14} />}
						data-testid="chart-generate"
						onClick={authorFromInstruction}
					>
						Generate
					</Button>
				</Group>
				{authorError && (
					<Text size="xs" c="red" data-testid="chart-author-error">
						{authorError}
					</Text>
				)}
			</Stack>

			{/* Live preview / empty state, set apart on a tinted, rounded panel so the
			    chart reads as a distinct surface from the controls. A chart shows only
			    once both axes resolve to a valid config; otherwise prompt the user. */}
			<Paper
				bg="white"
				radius="md"
				p="md"
				style={{ border: "1px dotted var(--mantine-color-gray-4)" }}
			>
				{validConfig ? (
					<ClientOnly>
						<ChartView
							config={validConfig}
							rows={data.rows}
							testId="chart-preview"
						/>
					</ClientOnly>
				) : (
					<Center h={200} data-testid="chart-modal-empty">
						<Text c="dimmed" size="sm" ta="center" maw={420}>
							{candidate
								? validation && !validation.ok
									? validation.error
									: "Adjust the mapping to preview a chart."
								: "Describe the chart above, or map the columns yourself."}
						</Text>
					</Center>
				)}
			</Paper>

			{/* The encoding controls — secondary, collapsed behind a one-line readout
			    of the current mapping. A generated config seeds this; opening lets the
			    user fine-tune it (or map from scratch when there's nothing yet). */}
			<Stack gap="xs">
				<Group justify="space-between" wrap="nowrap" gap="xs">
					<Text
						size="xs"
						c="dimmed"
						truncate
						style={{ flex: 1, minWidth: 0 }}
						data-testid="chart-mapping-summary"
					>
						{candidate ? summarizeDraft(draft) : "No columns mapped yet"}
					</Text>
					<Button
						variant="subtle"
						color="gray"
						size="compact-xs"
						rightSection={
							editing ? <ChevronUp size={14} /> : <ChevronDown size={14} />
						}
						data-testid="chart-edit-toggle"
						onClick={() => setEditing((e) => !e)}
					>
						{editing ? "Done" : "Edit"}
					</Button>
				</Group>

				<Collapse expanded={editing}>
					<Stack gap="md" pt="xs">
						<TextInput
							label="Title"
							placeholder="Optional chart title"
							value={draft.title ?? ""}
							size="xs"
							data-testid="chart-title"
							onChange={(e) =>
								setDraft((d) => ({ ...d, title: e.currentTarget.value }))
							}
						/>

						<Select
							label="Mark"
							data={CHART_MARKS.map((m) => ({ value: m, label: m }))}
							value={draft.mark}
							allowDeselect={false}
							size="xs"
							maw={200}
							data-testid="chart-mark"
							onChange={(mark) =>
								mark &&
								setDraft((d) => ({ ...d, mark: mark as ChartDraft["mark"] }))
							}
						/>

						<EncodingControls
							label="X"
							draft={draft.x}
							columns={columnData}
							suggestFor={suggestFor}
							onChange={(x) => setDraft((d) => ({ ...d, x }))}
						/>
						<EncodingControls
							label="Y"
							draft={draft.y}
							columns={columnData}
							suggestFor={suggestFor}
							onChange={(y) => setDraft((d) => ({ ...d, y }))}
						/>
						<EncodingControls
							label="Color"
							optional
							draft={draft.color}
							columns={columnData}
							suggestFor={suggestFor}
							onChange={(color) => setDraft((d) => ({ ...d, color }))}
						/>
					</Stack>
				</Collapse>
			</Stack>

			<Group justify="space-between">
				<Button
					variant="subtle"
					color="gray"
					size="compact-sm"
					disabled={!value}
					data-testid="chart-remove"
					onClick={() => {
						onAccept(null);
						onClose();
					}}
				>
					Remove chart
				</Button>
				<Group gap="xs">
					<Button variant="default" size="compact-sm" onClick={onClose}>
						Cancel
					</Button>
					<Button
						size="compact-sm"
						disabled={!validConfig}
						data-testid="chart-accept"
						onClick={() => {
							if (validConfig) {
								onAccept(validConfig);
								onClose();
							}
						}}
					>
						Use chart
					</Button>
				</Group>
			</Group>
		</Stack>
	);
}

/** Seed a draft from an existing config (re-open) or the empty state (fresh). */
function fromConfig(config: ChartConfig | null | undefined): ChartDraft {
	if (!config) return emptyDraft();
	// Called for x/y (always present) and color (optional) — hence the `| undefined`.
	const enc = (e: FieldEncoding | undefined): EncodingDraft =>
		e
			? { field: e.field, type: e.type, aggregate: e.aggregate ?? null }
			: { field: null, type: "nominal" };
	return {
		mark: config.mark,
		x: enc(config.encoding.x),
		y: enc(config.encoding.y),
		color: enc(config.encoding.color),
		title: config.title,
	};
}
