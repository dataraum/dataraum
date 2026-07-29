// Concept graph view (DAT-737) — the vertical VOCABULARY as a faithful lens on
// the engine's own concept-neighbourhood traversal (`graphs/context_format.py`):
// each concept's `part_of` ancestry, `disjoint_with` peers, `reconciles_with`
// assertions, and its groundings (a concept can be measured on SEVERAL
// relations — "multi-groundings"). A pure render of already-fetched values
// (React idiom 12) — no recomputation, the `concept-graph.ts` builder already
// did that server-side, INCLUDING the sort order (`graph.nodes` arrives
// already sorted by name — this view must not re-sort it).
//
// Deliberately NOT the xyflow canvas the Metrics view uses: that canvas's node
// model (`OMNodeKind`/`layout.ts`'s dagre auto-layout) is purpose-built for the
// 4-kind metric DAG, and the concept graph is a different shape (a vocabulary
// with mereological + disjointness + reconciliation edges, not a composition
// DAG) — an accordion list keeps every concept reachable and scannable without
// forcing a second node-kind through machinery that was never designed for it.

import {
	Accordion,
	Alert,
	Badge,
	Group,
	ScrollArea,
	Stack,
	Text,
	Title,
} from "@mantine/core";
import type { ConceptGraph, ConceptGraphNode } from "#/tools/concept-graph";
import {
	isReusableGrounding,
	reconciliationState,
} from "#/tools/concept-graph";

// Cap the CONCEPTS rendered into the DOM (rule 15) — a vertical ships a
// bounded vocabulary (tens, not thousands of concepts), but the cap keeps
// this consistent with every other list surface in the cockpit.
const MAX_VISIBLE_CONCEPTS = 200;
// Separately cap the GROUNDINGS shown per concept — this bound is per-row,
// not "vocabulary scale": a single concept accruing hundreds of groundings
// (a runaway re-grounding loop, say) must not blow up one accordion panel
// even though the concept count itself stays small.
const MAX_VISIBLE_GROUNDINGS = 20;

function GroundingRow({
	grounding,
}: {
	grounding: ConceptGraphNode["groundings"][number];
}) {
	const label =
		grounding.statement && grounding.relation
			? `${grounding.statement} @ ${grounding.relation}`
			: (grounding.relation ?? grounding.statement ?? "(unresolved relation)");
	// Three states, not two: failed / reusable-grounded / healthy-but-not-
	// reusable (no relation — the engine's own grounding_relation_missing skip,
	// see `isReusableGrounding`). Collapsing the third into "grounded" would
	// tell a practitioner they can reuse a query the answer agent's prompt
	// block has already excluded.
	const state = grounding.failed
		? { color: "orange", text: "failed" }
		: isReusableGrounding(grounding)
			? { color: "cyan", text: "grounded" }
			: { color: "gray", text: "not reusable — no relation" };
	return (
		<Group gap="xs" wrap="nowrap" align="flex-start">
			<Badge
				color={state.color}
				variant="light"
				size="sm"
				data-testid="concept-grounding-badge"
			>
				{state.text}
			</Badge>
			<Stack gap={0}>
				<Text size="sm">{label}</Text>
				{grounding.selectExpr && (
					<Text size="xs" c="dimmed" ff="monospace">
						{grounding.selectExpr}
						{grounding.wherePredicates.length > 0
							? ` WHERE ${grounding.wherePredicates.join(" AND ")}`
							: ""}
					</Text>
				)}
				{grounding.failed && (
					<Text size="xs" c="orange">
						[{grounding.failureMode ?? "failed"}]{" "}
						{grounding.failureReason ?? "(no reason recorded)"}
					</Text>
				)}
			</Stack>
		</Group>
	);
}

function ConceptDetail({ concept }: { concept: ConceptGraphNode }) {
	const visibleGroundings = concept.groundings.slice(0, MAX_VISIBLE_GROUNDINGS);
	const groundingOverflow =
		concept.groundings.length - visibleGroundings.length;
	return (
		<Stack gap="xs">
			{concept.description && (
				<Text size="sm" c="dimmed">
					{concept.description}
				</Text>
			)}
			{concept.indicators.length > 0 && (
				<Text size="xs">
					<Text span fw={600}>
						Indicators:{" "}
					</Text>
					{concept.indicators.join(", ")}
				</Text>
			)}
			{concept.excludePatterns.length > 0 && (
				<Text size="xs">
					<Text span fw={600}>
						Excludes:{" "}
					</Text>
					{concept.excludePatterns.join(", ")}
				</Text>
			)}
			{concept.partOfParents.length > 0 && (
				<Text size="xs">
					<Text span fw={600}>
						Part of:{" "}
					</Text>
					{concept.partOfParents.join(", ")}
					{concept.partOfAncestry.length > 0
						? ` (→ ${concept.partOfAncestry.join(" → ")})`
						: ""}
				</Text>
			)}
			{concept.partOfChildren.length > 0 && (
				<Text size="xs">
					<Text span fw={600}>
						Subconcepts:{" "}
					</Text>
					{concept.partOfChildren.join(", ")}
				</Text>
			)}
			{concept.disjointWith.length > 0 && (
				<Text size="xs">
					<Text span fw={600}>
						Disjoint with:{" "}
					</Text>
					{concept.disjointWith.join(", ")}
				</Text>
			)}
			{concept.reconcilesWith.map((rec) => (
				<Text size="xs" key={`${rec.partner}:${rec.tolerance ?? ""}`}>
					<Text span fw={600}>
						Reconciles:{" "}
					</Text>
					{rec.partner === concept.name
						? "across its own groundings"
						: `with ${rec.partner}`}
					{rec.tolerance !== null ? ` (tolerance ${rec.tolerance})` : ""} —{" "}
					{reconciliationState(rec)}
				</Text>
			))}
			{concept.groundings.length === 0 && (
				<Text size="xs" c="dimmed" data-testid="concept-ungrounded">
					Not grounded — this concept has no committed extract yet.
				</Text>
			)}
			{visibleGroundings.map((g) => (
				<GroundingRow key={g.snippetId} grounding={g} />
			))}
			{groundingOverflow > 0 && (
				<Text size="xs" c="dimmed">
					…and {groundingOverflow} more groundings not shown.
				</Text>
			)}
		</Stack>
	);
}

export function ConceptGraphView({ graph }: { graph: ConceptGraph }) {
	if (graph.nodes.length === 0) {
		return (
			<Alert color="gray" data-testid="concept-graph-empty">
				No business concepts framed for this workspace yet — frame a vertical to
				populate the vocabulary.
			</Alert>
		);
	}

	// graph.nodes is ALREADY sorted by name (buildConceptGraph's contract) —
	// no re-sort here (the one-source-of-truth-for-order rule this lane's spec
	// review asked for).
	const visible = graph.nodes.slice(0, MAX_VISIBLE_CONCEPTS);
	const overflow = graph.nodes.length - visible.length;

	return (
		<Stack gap="sm" h="100%" data-testid="concept-graph-view">
			<Title order={3} size="sm">
				Business Concepts{" "}
				<Text span c="dimmed" size="xs" fw={400}>
					{graph.nodes.length} in this vertical
				</Text>
			</Title>
			<ScrollArea.Autosize mah="100%" style={{ flex: 1 }}>
				<Accordion multiple variant="separated" data-testid="concept-accordion">
					{visible.map((c) => (
						<Accordion.Item
							key={c.id}
							value={c.id}
							data-testid={`concept-item-${c.name}`}
						>
							<Accordion.Control>
								<Group gap="xs" wrap="nowrap">
									<Text fw={500}>{c.name}</Text>
									{c.kind && (
										<Badge size="xs" variant="outline" color="gray">
											{c.kind}
										</Badge>
									)}
									<Badge
										size="xs"
										variant="light"
										color={c.groundings.length > 0 ? "cyan" : "orange"}
										data-testid={`concept-grounding-count-${c.name}`}
									>
										{c.groundings.length === 0
											? "ungrounded"
											: c.groundings.length === 1
												? "1 grounding"
												: `${c.groundings.length} groundings`}
									</Badge>
								</Group>
							</Accordion.Control>
							<Accordion.Panel>
								<ConceptDetail concept={c} />
							</Accordion.Panel>
						</Accordion.Item>
					))}
				</Accordion>
			</ScrollArea.Autosize>
			{overflow > 0 && (
				<Text size="xs" c="dimmed" data-testid="concept-graph-overflow">
					…and {overflow} more concepts not shown.
				</Text>
			)}
		</Stack>
	);
}
