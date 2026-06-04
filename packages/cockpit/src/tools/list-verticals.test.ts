// Unit tests for list_verticals (DAT-411). Mocks the three seams the tool
// reads: the config-tree fs scan (`node:fs/promises`), Bun's YAML parser, and
// the grouped concept-overlay count (`#/db/metadata/client`). Asserts the
// builtin ∪ framed union, the concept-count = ontology + overlay rule, and the
// capability flags.

import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	// Directory entries under <config>/verticals; each looks like a Dirent.
	dirEntries: [] as { name: string; isDirectory: () => boolean }[],
	readdirThrows: false,
	// ontology.yaml content by absolute path (JSON — a valid YAML subset).
	files: {} as Record<string, string>,
	// Paths that exist (for `stat` → capability flags).
	existing: new Set<string>(),
	// Grouped active concept-overlay rows: payload.vertical → count.
	overlayRows: [] as { vertical: string; n: number }[],
	// Single-vertical overlay count (verticalConceptCount path).
	overlayCountForOne: 0,
}));

vi.mock("#/config", () => ({
	config: { dataraumConfigPath: "/cfg" },
}));

// Bun's YAML — the tool parses ontology.yaml with it; JSON is a YAML subset so
// the test feeds JSON strings and parses them as JSON.
vi.mock("bun", () => ({
	YAML: { parse: (text: string) => JSON.parse(text) },
}));

vi.mock("node:fs/promises", () => ({
	readdir: vi.fn(async () => {
		if (h.readdirThrows) throw new Error("ENOENT: config tree not mounted");
		return h.dirEntries;
	}),
	readFile: vi.fn(async (path: string) => {
		const content = h.files[path];
		if (content === undefined) throw new Error(`ENOENT: ${path}`);
		return content;
	}),
	stat: vi.fn(async (path: string) => {
		if (!h.existing.has(path)) throw new Error(`ENOENT: ${path}`);
		return {};
	}),
}));

// The grouped overlay count: select().from().where().groupBy() → rows. Referenced
// lazily inside the returned objects so the factory doesn't touch consts early.
const groupByMock = vi.fn(async () => h.overlayRows);
vi.mock("#/db/metadata/client", () => ({
	metadataDb: {
		select: vi.fn(() => ({
			from: vi.fn(() => ({ where: vi.fn(() => ({ groupBy: groupByMock })) })),
		})),
	},
}));
vi.mock("#/db/metadata/schema", () => ({
	configOverlay: {
		payload: "payload",
		supersededAt: "superseded_at",
		type: "type",
	},
}));
// verticalConceptCount() counts a single vertical's overlay rows through this
// helper (a different query path than listVerticals' grouped read).
vi.mock("#/db/metadata/concept-overlays", () => ({
	countActiveConcepts: vi.fn(async () => h.overlayCountForOne),
}));
// Stub the drizzle helpers the tool feeds to the (mocked) query builder, so they
// don't choke on the stubbed string "columns"; the builder ignores them.
vi.mock("drizzle-orm", () => ({
	sql: (..._a: unknown[]) => "sql-expr",
	count: () => "count-expr",
	and: () => "and-expr",
	eq: () => "eq-expr",
	isNull: () => "isnull-expr",
}));

import { listVerticals, verticalConceptCount } from "./list-verticals";

function dir(name: string) {
	return { name, isDirectory: () => true };
}

beforeEach(() => {
	h.dirEntries = [];
	h.readdirThrows = false;
	h.files = {};
	h.existing = new Set();
	h.overlayRows = [];
	h.overlayCountForOne = 0;
	groupByMock.mockClear();
});

describe("listVerticals (DAT-411)", () => {
	it("lists builtin verticals with description, concept count, and capability flags", async () => {
		h.dirEntries = [dir("finance")];
		h.files["/cfg/verticals/finance/ontology.yaml"] = JSON.stringify({
			description: "Financial analysis and reporting.\n",
			concepts: [{ name: "revenue" }, { name: "cogs" }, { name: "cash" }],
		});
		h.existing = new Set([
			"/cfg/verticals/finance/cycles.yaml",
			"/cfg/verticals/finance/validations",
			"/cfg/verticals/finance/metrics",
		]);

		const result = await listVerticals();
		expect(result).toEqual([
			{
				name: "finance",
				kind: "builtin",
				description: "Financial analysis and reporting.",
				concept_count: 3,
				has_cycles: true,
				has_validations: true,
				has_metrics: true,
			},
		]);
	});

	it("counts a builtin's concepts as ontology + overlay", async () => {
		h.dirEntries = [dir("finance")];
		h.files["/cfg/verticals/finance/ontology.yaml"] = JSON.stringify({
			description: "Finance",
			concepts: [{ name: "revenue" }, { name: "cogs" }],
		});
		// Overlay rows naming the vertical add on top of its on-disk concepts.
		h.overlayRows = [{ vertical: "finance", n: 3 }];

		const [finance] = await listVerticals();
		expect(finance).toMatchObject({
			name: "finance",
			kind: "builtin",
			concept_count: 5, // 2 ontology + 3 overlay
		});
	});

	it("hides underscore-prefixed seeds (_adhoc) from the listing — builtin scan and framed fallback alike", async () => {
		h.dirEntries = [dir("finance"), dir("_adhoc")];
		h.files["/cfg/verticals/finance/ontology.yaml"] = JSON.stringify({
			description: "Finance",
			concepts: [{ name: "revenue" }],
		});
		h.files["/cfg/verticals/_adhoc/ontology.yaml"] = JSON.stringify({
			concepts: [],
		});
		// _adhoc carries overlay concepts too — assert it's filtered from BOTH the
		// builtin scan and the framed fallback (never re-added as "framed").
		h.overlayRows = [{ vertical: "_adhoc", n: 5 }];

		const result = await listVerticals();
		expect(result.map((v) => v.name)).toEqual(["finance"]);
	});

	it("includes framed verticals (overlay names with no directory), sorted builtins-first", async () => {
		h.dirEntries = [dir("finance"), dir("_adhoc")];
		h.files["/cfg/verticals/finance/ontology.yaml"] = JSON.stringify({
			description: "Finance",
			concepts: [{ name: "revenue" }],
		});
		h.files["/cfg/verticals/_adhoc/ontology.yaml"] = JSON.stringify({
			concepts: [],
		});
		// "sales" is framed (in the overlay, no directory); _adhoc is a builtin
		// that also has overlay concepts — it stays hidden in both roles.
		h.overlayRows = [
			{ vertical: "sales", n: 4 },
			{ vertical: "_adhoc", n: 2 },
		];

		const result = await listVerticals();
		expect(result.map((v) => [v.name, v.kind, v.concept_count])).toEqual([
			["finance", "builtin", 1],
			["sales", "framed", 4],
		]);
	});

	it("returns framed-only when the config tree is unreadable", async () => {
		h.readdirThrows = true;
		h.overlayRows = [{ vertical: "sales", n: 3 }];
		const result = await listVerticals();
		expect(result).toEqual([
			{
				name: "sales",
				kind: "framed",
				description: null,
				concept_count: 3,
				has_cycles: false,
				has_validations: false,
				has_metrics: false,
			},
		]);
	});

	it("ignores a null overlay vertical (rows whose payload has no vertical)", async () => {
		h.dirEntries = [];
		h.overlayRows = [
			{ vertical: null as unknown as string, n: 9 },
			{ vertical: "sales", n: 1 },
		];
		const result = await listVerticals();
		expect(result.map((v) => v.name)).toEqual(["sales"]);
	});
});

describe("verticalConceptCount (add_source pre-flight)", () => {
	it("sums a builtin's ontology concepts and its overlay rows", async () => {
		h.files["/cfg/verticals/finance/ontology.yaml"] = JSON.stringify({
			concepts: [{ name: "revenue" }, { name: "cogs" }],
		});
		h.overlayCountForOne = 1; // a taught extension concept
		expect(await verticalConceptCount("finance")).toBe(3);
	});

	it("counts a framed (directory-less) vertical from overlay alone", async () => {
		// No on-disk file → readOntology yields null → overlay-only.
		h.overlayCountForOne = 4;
		expect(await verticalConceptCount("sales")).toBe(4);
	});

	it("is zero for an unframed / empty vertical (the guard refuses on this)", async () => {
		h.files["/cfg/verticals/_adhoc/ontology.yaml"] = JSON.stringify({
			concepts: [],
		});
		h.overlayCountForOne = 0;
		expect(await verticalConceptCount("_adhoc")).toBe(0);
	});
});
