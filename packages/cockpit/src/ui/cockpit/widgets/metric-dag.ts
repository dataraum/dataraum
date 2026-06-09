// Shared metric-DAG vocabulary (DAT-482) — the shipped computation-graph shape +
// its narrow, used by BOTH the ModelFrame review row (summary) and the
// teach-override shadow widget (full step render). The DAG arrives UNTRUSTED:
// `model-frame` carries it off the `frame` tool result, and `metric-shadow`
// fetches it off the shipped YAML — both as `unknown` (rule 11), so the narrow
// lives here, once, render-oriented (flattened, tolerant) rather than the strict
// teach-input `GraphStepSchema` (which is the AGENT's contract, not a render view).

/** One DAG step, narrowed for rendering: the salient fields a human reads. */
export interface DagStep {
	id: string;
	type: string | null;
	level: number | null;
	/** EXTRACT steps: the framed concept this pulls (source.standard_field). */
	standardField: string | null;
	/** EXTRACT steps: the statement the field lives in. */
	statement: string | null;
	/** EXTRACT steps: how it aggregates (sum/avg/…). */
	aggregation: string | null;
	/** FORMULA steps: the arithmetic over earlier step ids. */
	expression: string | null;
	/** FORMULA steps: the step ids consumed. */
	dependsOn: string[];
	/** True on the single step whose result IS the metric's output. */
	outputStep: boolean;
}

/** A metric's output node, narrowed for rendering. */
export interface MetricOutputView {
	type: string | null;
	metricId: string | null;
	unit: string | null;
}

function str(v: unknown): string | null {
	return typeof v === "string" ? v : null;
}

function num(v: unknown): number | null {
	return typeof v === "number" ? v : null;
}

function strArray(v: unknown): string[] {
	return Array.isArray(v)
		? v.filter((x): x is string => typeof x === "string")
		: [];
}

function narrowOutput(output: unknown): MetricOutputView | null {
	if (!output || typeof output !== "object") return null;
	const o = output as Record<string, unknown>;
	return { type: str(o.type), metricId: str(o.metric_id), unit: str(o.unit) };
}

function narrowSteps(dependencies: unknown): DagStep[] {
	if (!dependencies || typeof dependencies !== "object") return [];
	const steps: DagStep[] = [];
	for (const [id, raw] of Object.entries(
		dependencies as Record<string, unknown>,
	)) {
		if (!raw || typeof raw !== "object") continue;
		const s = raw as Record<string, unknown>;
		const source = (
			s.source && typeof s.source === "object" ? s.source : {}
		) as Record<string, unknown>;
		steps.push({
			id,
			type: str(s.type),
			level: num(s.level),
			standardField: str(source.standard_field),
			statement: str(source.statement),
			aggregation: str(s.aggregation),
			expression: str(s.expression),
			dependsOn: strArray(s.depends_on),
			outputStep: s.output_step === true,
		});
	}
	// Deterministic render order: dependency level ascending (leaves first,
	// output last), ties broken by id. Level-less steps sort to the end.
	steps.sort((a, b) => {
		const la = a.level ?? Number.MAX_SAFE_INTEGER;
		const lb = b.level ?? Number.MAX_SAFE_INTEGER;
		return la - lb || a.id.localeCompare(b.id);
	});
	return steps;
}

/** Narrow a metric's untrusted `output` + `dependencies` into the render view. */
export function narrowDag(
	output: unknown,
	dependencies: unknown,
): { output: MetricOutputView | null; steps: DagStep[] } {
	return { output: narrowOutput(output), steps: narrowSteps(dependencies) };
}

/** Summarize a narrowed DAG for a one-row review (ModelFrame): step count + the
 * leaf CONCEPTS its extract steps pull (what the user commits to ground). */
export function summarizeDag(steps: DagStep[]): {
	stepCount: number;
	leafConcepts: string[];
} {
	const leafConcepts: string[] = [];
	for (const step of steps) {
		if (step.type === "extract" && step.standardField) {
			leafConcepts.push(step.standardField);
		}
	}
	return { stepCount: steps.length, leafConcepts };
}
