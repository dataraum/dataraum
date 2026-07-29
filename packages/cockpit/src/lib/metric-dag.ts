// Shared metric-DAG vocabulary (DAT-482) — the shipped computation-graph shape +
// its narrow, used by BOTH the ModelFrame review row (summary) and the
// teach-override shadow widget (full step render). The DAG arrives UNTRUSTED:
// `model-frame` carries it off the `frame` tool result, and `metric-shadow`
// fetches it off the shipped YAML — both as `unknown` (rule 11), so the narrow
// lives here, once, render-oriented (flattened, tolerant) rather than the strict
// teach-input `GraphStepSchema` (which is the AGENT's contract, not a render view).

/** A step's declared post-execution check (the engine's `GraphStep.validations`
 *  / the YAML's `validation:` block — THE KEY IS SINGULAR, graphs/loader.py
 *  reads `data.get("validation")`). Enforced by `graphs/verifier.py` against
 *  the executed value — execution-pass is not validation. Mirrors
 *  `tools/operating-model-graph.ts`'s `StepValidation` (the two DAG-narrowing
 *  paths stay independent per this module's header — a render view, not the
 *  agent's strict `GraphStepSchema`). */
export interface StepValidation {
	condition: string;
	severity: string | null;
	message: string | null;
}

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
	/** Declared post-execution checks on this step's value (DAT-616/840) —
	 *  usually only present on the output step. Empty when none declared. */
	validation: StepValidation[];
}

/** A metric's output node, narrowed for rendering. */
export interface MetricOutputView {
	type: string | null;
	metricId: string | null;
	unit: string | null;
}

/** The shipped metric a teach override replaces — identity + the narrowed DAG.
 * The contract of the `/api/shipped-metric-dag` route (DAT-482): server-narrowed
 * (concrete, serializable) so the widget is a pure render and the model never
 * sees the DAG. `null` when no shipped metric carries that graph_id. */
export interface ShippedMetricDag {
	graph_id: string;
	name: string | null;
	category: string | null;
	output: MetricOutputView | null;
	steps: DagStep[];
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

/** Narrow a step's `validation` array (untrusted — rule 11). Skips entries
 *  missing `condition` (the one required field); a non-array/absent key
 *  yields []. */
function narrowValidation(v: unknown): StepValidation[] {
	if (!Array.isArray(v)) return [];
	const out: StepValidation[] = [];
	for (const item of v) {
		if (!item || typeof item !== "object") continue;
		const rec = item as Record<string, unknown>;
		const condition = str(rec.condition);
		if (!condition) continue;
		out.push({
			condition,
			severity: str(rec.severity),
			message: str(rec.message),
		});
	}
	return out;
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
			validation: narrowValidation(s.validation),
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
