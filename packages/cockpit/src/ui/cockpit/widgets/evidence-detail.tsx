// Evidence-detail renderer (DAT-437) — a detector's evidence `detail` as a
// hierarchical key→value list instead of a pretty-printed JSON block.
//
// `detail` is the sanitized JSON string the why_* tool projections emit
// (`renderEvidenceDetail`, DAT-433 — digests stripped, `_`-keys dropped), so
// this component only FORMATS; it never re-sanitizes. Shared by the why_column
// widget today and the upcoming why_table / why_relationship widgets (DAT-434).
//
// Rendering rules:
//   - objects   → one row per key (key dimmed, value beside it); a nested
//                 object/array indents under its key
//   - arrays    → repeated groups, one per element (objects get a hairline
//                 left border so consecutive groups read as units)
//   - leaves    → text, truncated past MAX_VALUE_CHARS with the full value in
//                 the `title` attr (hover reveals it)
//   - non-JSON  → detectors are free to emit a plain string; render it as-is —
//                 a parse failure must never blank the cell
//   - ""        → a muted dash, so an empty detail never renders a hollow cell

import { Box, Stack, Text } from "@mantine/core";

const MAX_VALUE_CHARS = 80;

/** A leaf value as display text. */
function leafText(value: unknown): string {
	if (value === null) return "—";
	if (typeof value === "string") return value;
	return String(value);
}

/** Truncated leaf text; the full value rides in `title` when cut. */
function LeafValue({ value }: { value: unknown }) {
	const text = leafText(value);
	const truncated =
		text.length > MAX_VALUE_CHARS
			? `${text.slice(0, MAX_VALUE_CHARS - 1)}…`
			: text;
	return (
		<Text
			span
			size="xs"
			title={truncated === text ? undefined : text}
			style={{ wordBreak: "break-word" }}
		>
			{truncated}
		</Text>
	);
}

function isLeaf(value: unknown): boolean {
	return value === null || typeof value !== "object";
}

/** One node of the parsed detail: a leaf, an array (repeated groups), or an
 * object (key→value rows). Nesting indents via the markup, not a depth prop. */
function DetailNode({ value }: { value: unknown }) {
	if (isLeaf(value)) return <LeafValue value={value} />;

	if (Array.isArray(value)) {
		if (value.length === 0) return <LeafValue value="—" />;
		return (
			<Stack gap={4}>
				{value.map((item, i) => (
					<Box
						// biome-ignore lint/suspicious/noArrayIndexKey: static parsed JSON
						key={i}
						pl={isLeaf(item) ? 0 : 6}
						style={
							isLeaf(item)
								? undefined
								: { borderLeft: "2px solid var(--mantine-color-gray-3)" }
						}
					>
						<DetailNode value={item} />
					</Box>
				))}
			</Stack>
		);
	}

	const entries = Object.entries(value as Record<string, unknown>);
	if (entries.length === 0) return <LeafValue value="—" />;
	return (
		<Stack gap={2}>
			{entries.map(([key, v]) =>
				isLeaf(v) ? (
					<Text span size="xs" key={key} style={{ wordBreak: "break-word" }}>
						<Text span size="xs" c="dimmed">
							{key}:{" "}
						</Text>
						<LeafValue value={v} />
					</Text>
				) : (
					<Box key={key}>
						<Text span size="xs" c="dimmed">
							{key}:
						</Text>
						<Box pl="sm">
							<DetailNode value={v} />
						</Box>
					</Box>
				),
			)}
		</Stack>
	);
}

/**
 * The shared evidence-detail cell: parses the sanitized JSON string and renders
 * the key→value hierarchy, scroll-bounded so a large blob can't blow up the
 * evidence table's row height.
 */
export function EvidenceDetail({ detail }: { detail: string }) {
	if (!detail) {
		return (
			<Text span size="xs" c="dimmed" data-testid="evidence-detail">
				—
			</Text>
		);
	}

	let parsed: unknown;
	try {
		parsed = JSON.parse(detail);
	} catch {
		// Not JSON — a detector emitted a plain string; show it untouched.
		return (
			<Text
				size="xs"
				style={{ whiteSpace: "pre-wrap", wordBreak: "break-word" }}
				data-testid="evidence-detail"
			>
				{detail}
			</Text>
		);
	}

	return (
		<Box
			style={{ maxWidth: 360, maxHeight: 200, overflowY: "auto" }}
			data-testid="evidence-detail"
		>
			<DetailNode value={parsed} />
		</Box>
	);
}
