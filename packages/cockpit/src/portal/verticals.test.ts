// listBuiltinVerticals (DAT-821): the portal-safe fs scan — directories only,
// `_`-prefix hidden, description from ontology.yaml (null when absent or
// unparseable), name-sorted, loud when the config mount is missing.
//
// Real files in a tmp tree; ontology.yaml content is written as JSON (a YAML
// subset) with Bun's YAML.parse stubbed to JSON.parse, so the test exercises
// OUR scan rules without re-testing Bun's YAML under a node worker (where
// `import("bun")` cannot resolve).

import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { mockConfig } = vi.hoisted(() => ({
	mockConfig: { configPath: "" },
}));
vi.mock("#/portal/provisioner-config", () => ({
	provisionerConfig: () => mockConfig,
}));
vi.mock("bun", () => ({ YAML: { parse: JSON.parse } }));

import { listBuiltinVerticals } from "#/portal/verticals";

describe("listBuiltinVerticals", () => {
	let root: string;

	beforeEach(async () => {
		root = await mkdtemp(join(tmpdir(), "dat821-verticals-"));
		mockConfig.configPath = root;
	});

	afterEach(async () => {
		await rm(root, { recursive: true, force: true });
	});

	async function seedVertical(name: string, ontology?: object): Promise<void> {
		const dir = join(root, "verticals", name);
		await mkdir(dir, { recursive: true });
		if (ontology) {
			await writeFile(join(dir, "ontology.yaml"), JSON.stringify(ontology));
		}
	}

	it("lists directories name-sorted with ontology descriptions", async () => {
		await seedVertical("retail", { description: "  Retail ops  " });
		await seedVertical("finance", { description: "Corporate finance" });
		expect(await listBuiltinVerticals()).toEqual([
			{ name: "finance", description: "Corporate finance" },
			{ name: "retail", description: "Retail ops" },
		]);
	});

	it("hides underscore-prefixed internal seeds and non-directories", async () => {
		await seedVertical("finance", { description: "Corporate finance" });
		await seedVertical("_adhoc", { description: "internal" });
		await writeFile(join(root, "verticals", "README.md"), "not a vertical");
		expect(
			(await listBuiltinVerticals()).map((vertical) => vertical.name),
		).toEqual(["finance"]);
	});

	it("keeps a vertical whose ontology is missing or unparseable", async () => {
		await seedVertical("bare");
		await seedVertical("broken");
		await writeFile(
			join(root, "verticals", "broken", "ontology.yaml"),
			"{not json",
		);
		expect(await listBuiltinVerticals()).toEqual([
			{ name: "bare", description: null },
			{ name: "broken", description: null },
		]);
	});

	it("throws when the verticals tree is not readable (missing mount)", async () => {
		mockConfig.configPath = join(root, "nowhere");
		await expect(listBuiltinVerticals()).rejects.toThrow();
	});
});
