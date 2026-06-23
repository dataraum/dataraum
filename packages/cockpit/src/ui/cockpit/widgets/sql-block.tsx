// Shared read-only SQL display (DAT-577). The codebase rendered literal SQL the
// same way in several widgets — `<ScrollArea.Autosize><Code block>{sql}</Code>`
// — so this promotes that pattern to one component (cockpit idiom rule 13), now
// reused by metric-why, validation-why, and the result-grid SQL disclosure.
//
// This is the READ-ONLY viewer. The editable probe surface is a different
// interaction (CodeMirror `SqlEditor`); only the literal-SQL DISPLAY is shared.
// Plain monospace, no syntax highlighting (deferred — the precedents are plain
// `<Code>` too); the value is the engine's persisted/agent SQL, shown verbatim.

import { Code, Group, ScrollArea, Stack, Text } from "@mantine/core";

/** A bound scalar bind-parameter value, as carried on the result-grid state. */
type BindParam = string | number | boolean | null;

// Bound the SQL surface — a generated fragment is normally short, but the widget
// must stay usable if a long one arrives (rule 15). Callers may tighten it.
const DEFAULT_MAX_HEIGHT = 240;

/**
 * Read-only literal SQL with an optional label and bind params. With neither a
 * label nor params it renders the bare scroll+code block (the metric-why step
 * case); with a label it wraps in a titled stack (the validation-why case). A
 * `data-testid` is applied to the outermost element when given.
 */
export function SqlBlock({
	sql,
	label,
	params,
	maxHeight = DEFAULT_MAX_HEIGHT,
	"data-testid": testId,
}: {
	sql: string;
	label?: string;
	params?: ReadonlyArray<BindParam>;
	maxHeight?: number;
	"data-testid"?: string;
}) {
	const body = (
		<ScrollArea.Autosize mah={maxHeight}>
			<Code block>{sql}</Code>
		</ScrollArea.Autosize>
	);

	const hasParams = params !== undefined && params.length > 0;
	if (!label && !hasParams) {
		return testId ? <div data-testid={testId}>{body}</div> : body;
	}

	return (
		<Stack gap={4} data-testid={testId}>
			{label && (
				<Text size="xs" fw={500}>
					{label}
				</Text>
			)}
			{body}
			{hasParams && (
				<Group gap={6} wrap="wrap" data-testid="sql-block-params">
					<Text size="xs" c="dimmed">
						Params:
					</Text>
					{params.map((p, i) => (
						<Code
							// biome-ignore lint/suspicious/noArrayIndexKey: positional bind params, never reordered
							key={i}
						>
							{p === null ? "null" : String(p)}
						</Code>
					))}
				</Group>
			)}
		</Stack>
	);
}
