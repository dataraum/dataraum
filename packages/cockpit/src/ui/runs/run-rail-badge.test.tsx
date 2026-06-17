// @vitest-environment jsdom

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

// Mock the queries at the seam, keyed by queryKey so the two polls (running vs
// needs-you) return independent counts — the badge's real logic is the PRIORITY
// (needs-you over running); the poll/fetch is exercised by /smoke.
const h = vi.hoisted(() => ({
	running: 0 as number | undefined,
	awaiting: 0 as number | undefined,
}));
vi.mock("@tanstack/react-query", () => ({
	useQuery: ({ queryKey }: { queryKey: string[] }) => ({
		data: queryKey[0] === "workspace-awaiting-input" ? h.awaiting : h.running,
	}),
}));

import { RunRailBadge } from "#/ui/runs/run-rail-badge";

function renderBadge() {
	render(
		<MantineProvider env="test">
			<RunRailBadge>
				<span data-testid="icon" />
			</RunRailBadge>
		</MantineProvider>,
	);
	return screen.getByTestId("run-liveness");
}

afterEach(() => cleanup());

describe("RunRailBadge (DAT-550 liveness + DAT-553 needs-you)", () => {
	it("is inactive when nothing is running and nothing needs you", () => {
		h.running = 0;
		h.awaiting = 0;
		const b = renderBadge();
		expect(b.getAttribute("data-running")).toBe("false");
		expect(b.getAttribute("data-needs-you")).toBe("false");
	});

	it("shows the running dot when runs are in flight (no needs-you)", () => {
		h.running = 3;
		h.awaiting = 0;
		const b = renderBadge();
		expect(b.getAttribute("data-running")).toBe("true");
		expect(b.getAttribute("data-needs-you")).toBe("false");
	});

	it("shows the Needs-you count and it takes priority over the running dot", () => {
		h.running = 2;
		h.awaiting = 4;
		const b = renderBadge();
		expect(b.getAttribute("data-needs-you")).toBe("true");
		// The numbered label renders the count.
		expect(b.textContent).toContain("4");
		// Running is still recorded (data-attr) even though needs-you renders.
		expect(b.getAttribute("data-running")).toBe("true");
	});

	it("treats no-data-yet (undefined) as inactive", () => {
		h.running = undefined;
		h.awaiting = undefined;
		const b = renderBadge();
		expect(b.getAttribute("data-running")).toBe("false");
		expect(b.getAttribute("data-needs-you")).toBe("false");
	});
});
