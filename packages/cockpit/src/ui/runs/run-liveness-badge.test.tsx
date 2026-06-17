// @vitest-environment jsdom

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

// Mock the query at the seam — the badge's real logic is count → active dot; the
// poll/fetch is exercised by /smoke. (The badge imports no server-only modules,
// so this needs no config/db mocks.)
const h = vi.hoisted(() => ({ data: undefined as number | undefined }));
vi.mock("@tanstack/react-query", () => ({
	useQuery: () => ({ data: h.data }),
}));

import { RunLivenessBadge } from "#/ui/runs/run-liveness-badge";

function renderBadge() {
	render(
		<MantineProvider env="test">
			<RunLivenessBadge>
				<span data-testid="icon" />
			</RunLivenessBadge>
		</MantineProvider>,
	);
	return screen.getByTestId("run-liveness").getAttribute("data-running");
}

afterEach(() => cleanup());

describe("RunLivenessBadge (DAT-550)", () => {
	it("is inactive when no runs are in flight", () => {
		h.data = 0;
		expect(renderBadge()).toBe("false");
	});

	it("activates when runs are in flight", () => {
		h.data = 3;
		expect(renderBadge()).toBe("true");
	});

	it("treats no-data-yet (undefined) as inactive", () => {
		h.data = undefined;
		expect(renderBadge()).toBe("false");
	});
});
