// @vitest-environment jsdom

// Unit tests for BandBadge (DAT-451) — the shared readiness-band render, now
// hardened (fold-in, both reviewers) against untrusted persisted `band` text.
// `confidence.band` is validated at MINT (mint.ts's MintBodySchema) but not on
// every read, and a direct DB edit or a future writer could put anything in
// the column. A garbage `band` must never crash the badge (a plain lookup
// resolving through Object.prototype to a function throws deep inside
// Mantine's color parser) — it degrades to gray + the raw string, same as any
// other unrecognized-but-real band.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { theme } from "#/ui/theme";
import { BandBadge } from "./band-badge";

afterEach(cleanup);

function renderBadge(band: string | null, coverage?: string | null) {
	return render(
		<MantineProvider theme={theme} env="test">
			<BandBadge band={band} coverage={coverage} />
		</MantineProvider>,
	);
}

describe("BandBadge", () => {
	it("renders the known bands with their color", () => {
		renderBadge("ready");
		expect(screen.getByText("Ready")).toBeTruthy();
	});

	it("renders a muted dash for an absent band", () => {
		renderBadge(null);
		expect(screen.getByText("—")).toBeTruthy();
	});

	it("renders 'Not measured' for an unmeasured rollup, never a green ready badge", () => {
		renderBadge("ready", "unmeasured");
		expect(screen.getByText("Not measured")).toBeTruthy();
		expect(screen.queryByText("Ready")).toBeNull();
	});

	// DAT-627 hardening: an inherited Object.prototype key must never crash
	// the badge — this is the exact class of bug the reviewers live-verified
	// (a malformed/tampered confidence.band reaching the gallery card).
	it("never throws on an inherited Object.prototype key, degrading to gray + the raw string", () => {
		expect(() => renderBadge("constructor")).not.toThrow();
		expect(screen.getByText("constructor")).toBeTruthy();
	});

	it("never throws on other unmapped band strings either", () => {
		expect(() => renderBadge("toString")).not.toThrow();
		expect(screen.getByText("toString")).toBeTruthy();
	});
});
