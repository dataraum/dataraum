// TS mirror of the engine's deterministic formula composer (DAT-702).
//
// `graphs/formula_composer.py` is the SOLE authoring path for a metric's
// formula and constant SQL: a CLOSED grammar — identifiers (dependency step
// ids), numeric literals, `+ - * /`, unary minus, parentheses — rendered over
// step CTEs that each return a single scalar `value`. This module mirrors that
// rendering exactly so the cockpit can recompose a metric's persisted parts
// per node (DD/43417601 § "Metric drill — the per-node re-cut") without an
// engine round-trip:
//   - identifier → `(SELECT value FROM <step_id>)`, validated against the
//     step's DECLARED dependencies (an unknown operand refuses, never guesses);
//   - numeric literal → float-forced (`100` → `100.0`) so a literal can never
//     make a surrounding division integer-typed and silently truncate;
//   - every division denominator is `NULLIF`-guarded (zero divisor → NULL);
//   - anything outside the grammar is a refusal, born-loud — the mirror must
//     never compose what the engine would have refused.
//
// The parsed identifier list doubles as the walk's REACHABILITY signal:
// declared `depends_on` over-declares (the retired tier-C output-reachability
// gate existed because of it, DAT-672 post-merge); what the expression
// actually references is the truth.
//
// Neo-free and pure: no connection, no IO — unit-testable and importable
// anywhere.

export type FormulaExpr =
	| { kind: "ref"; name: string }
	| { kind: "num"; value: number }
	| {
			kind: "bin";
			op: "+" | "-" | "*" | "/";
			left: FormulaExpr;
			right: FormulaExpr;
	  }
	| { kind: "neg"; operand: FormulaExpr };

type Token =
	| { t: "ident"; v: string }
	| { t: "num"; v: number }
	| { t: "op"; v: "+" | "-" | "*" | "/" | "(" | ")" };

const IDENT_RE = /^[A-Za-z_][A-Za-z0-9_]*/;
const NUM_RE = /^(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?/;

function tokenize(expression: string): Token[] | null {
	const out: Token[] = [];
	let rest = expression;
	while (rest.length > 0) {
		const ws = /^\s+/.exec(rest);
		if (ws) {
			rest = rest.slice(ws[0].length);
			continue;
		}
		const ident = IDENT_RE.exec(rest);
		if (ident) {
			out.push({ t: "ident", v: ident[0] });
			rest = rest.slice(ident[0].length);
			continue;
		}
		const num = NUM_RE.exec(rest);
		if (num) {
			out.push({ t: "num", v: Number(num[0]) });
			rest = rest.slice(num[0].length);
			continue;
		}
		const c = rest[0];
		if (
			c === "+" ||
			c === "-" ||
			c === "*" ||
			c === "/" ||
			c === "(" ||
			c === ")"
		) {
			out.push({ t: "op", v: c });
			rest = rest.slice(1);
			continue;
		}
		return null; // character outside the closed grammar
	}
	return out;
}

/**
 * Parse a formula expression against the closed grammar. Mirrors what Python's
 * `ast.parse` + the composer's node whitelist accept: left-associative `+ - * /`
 * over identifiers, numbers, parentheses, and unary minus. Everything else —
 * including `**`, calls, comparisons — fails the parse and refuses.
 */
export function parseFormulaExpression(
	expression: string,
): { expr: FormulaExpr } | { refusal: string } {
	const tokens = tokenize(expression);
	if (!tokens || tokens.length === 0) {
		return { refusal: `unparseable formula expression '${expression}'` };
	}
	let pos = 0;
	const peek = (): Token | undefined => tokens[pos];
	const fail = (): { refusal: string } => ({
		refusal: `unparseable formula expression '${expression}'`,
	});

	// expr := term (('+' | '-') term)*   — left-associative, Python parity
	// term := factor (('*' | '/') factor)*
	// factor := NUMBER | IDENT | '(' expr ')' | '-' factor
	function parseExpr(): FormulaExpr | null {
		let left = parseTerm();
		if (!left) return null;
		for (;;) {
			const tk = peek();
			if (tk?.t !== "op" || (tk.v !== "+" && tk.v !== "-")) return left;
			pos++;
			const right = parseTerm();
			if (!right) return null;
			left = { kind: "bin", op: tk.v, left, right };
		}
	}
	function parseTerm(): FormulaExpr | null {
		let left = parseFactor();
		if (!left) return null;
		for (;;) {
			const tk = peek();
			if (tk?.t !== "op" || (tk.v !== "*" && tk.v !== "/")) return left;
			pos++;
			const right = parseFactor();
			if (!right) return null;
			left = { kind: "bin", op: tk.v, left, right };
		}
	}
	function parseFactor(): FormulaExpr | null {
		const tk = peek();
		if (!tk) return null;
		if (tk.t === "num") {
			pos++;
			return { kind: "num", value: tk.v };
		}
		if (tk.t === "ident") {
			pos++;
			return { kind: "ref", name: tk.v };
		}
		if (tk.t === "op" && tk.v === "-") {
			pos++;
			const operand = parseFactor();
			return operand ? { kind: "neg", operand } : null;
		}
		if (tk.t === "op" && tk.v === "(") {
			pos++;
			const inner = parseExpr();
			const close = peek();
			if (!inner || close?.t !== "op" || close.v !== ")") return null;
			pos++;
			return inner;
		}
		return null;
	}

	const expr = parseExpr();
	if (!expr || pos !== tokens.length) return fail();
	return { expr };
}

/** The identifiers a parsed expression references, in first-appearance order —
 *  the walk's reachability signal (what the formula ACTUALLY uses). */
export function formulaRefs(expr: FormulaExpr): string[] {
	const out: string[] = [];
	const visit = (e: FormulaExpr): void => {
		if (e.kind === "ref") {
			if (!out.includes(e.name)) out.push(e.name);
		} else if (e.kind === "bin") {
			visit(e.left);
			visit(e.right);
		} else if (e.kind === "neg") {
			visit(e.operand);
		}
	};
	visit(expr);
	return out;
}

/** Python-`repr(float(x))` parity for the float-forced literal: integers gain
 *  a trailing `.0` (`100` → `100.0`), non-integers print as-is. */
const floatLiteral = (value: number): string =>
	Number.isInteger(value) ? `${value}.0` : String(value);

function renderExpr(
	expr: FormulaExpr,
	depStepIds: ReadonlySet<string>,
	expression: string,
): { sql: string } | { refusal: string } {
	if (expr.kind === "ref") {
		if (!depStepIds.has(expr.name)) {
			return {
				refusal:
					`formula '${expression}' references '${expr.name}', which is not a ` +
					`declared dependency — refusing to compose a fabricated operand`,
			};
		}
		return { sql: `(SELECT value FROM ${expr.name})` };
	}
	if (expr.kind === "num") return { sql: floatLiteral(expr.value) };
	if (expr.kind === "neg") {
		const inner = renderExpr(expr.operand, depStepIds, expression);
		if ("refusal" in inner) return inner;
		return { sql: `-${inner.sql}` };
	}
	const left = renderExpr(expr.left, depStepIds, expression);
	if ("refusal" in left) return left;
	const right = renderExpr(expr.right, depStepIds, expression);
	if ("refusal" in right) return right;
	const rhs = expr.op === "/" ? `NULLIF(${right.sql}, 0)` : right.sql;
	return { sql: `(${left.sql} ${expr.op} ${rhs})` };
}

/**
 * Compose a FORMULA step's scalar SQL from its expression — the mirror of
 * `compose_formula_sql`. Identifiers must be declared dependencies (the CTE
 * namespace); the result selects the rendered arithmetic `AS value`.
 */
export function composeFormulaSql(
	expression: string,
	depStepIds: ReadonlySet<string>,
): { sql: string } | { refusal: string } {
	const parsed = parseFormulaExpression(expression);
	if ("refusal" in parsed) return parsed;
	const rendered = renderExpr(parsed.expr, depStepIds, expression);
	if ("refusal" in rendered) return rendered;
	return { sql: `SELECT ${rendered.sql} AS value` };
}

/**
 * Compose a CONSTANT step's scalar SQL — the mirror of `compose_constant_sql`.
 * The value arrives stringified from the parsed DAG (`value ?? default`); an
 * integer stays integer (`30` → `SELECT 30 AS value` — a constant is never a
 * division denominator, so integer typing is safe), non-numeric refuses.
 */
export function composeConstantSql(
	value: string | null,
): { sql: string } | { refusal: string } {
	const numeric = value === null || value.trim() === "" ? NaN : Number(value);
	if (!Number.isFinite(numeric)) {
		return { refusal: `constant value '${String(value)}' is not numeric` };
	}
	return { sql: `SELECT ${String(numeric)} AS value` };
}
