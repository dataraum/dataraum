// @vitest-environment jsdom
//
// The create flow's CLIENT contract (DAT-821, senior-review finding): a
// rejected server fn must surface — the form renders the parsed lifecycle
// message in the error Alert, never a silent no-op navigation — and the
// progress panel renders each terminal state (failed-with-retry,
// restart-interrupted) off the polled shape. Server fns are mocked at the
// module seam (switcher-test idiom); their rejection shape mirrors
// serverFnError's JSON-envelope Error.

import { MantineProvider } from "@mantine/core";
import {
	createMemoryHistory,
	createRootRoute,
	createRoute,
	createRouter,
	RouterProvider,
} from "@tanstack/react-router";
import {
	cleanup,
	fireEvent,
	render,
	screen,
	waitFor,
} from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
	start: vi.fn(),
	retry: vi.fn(),
	progress: vi.fn(),
}));
vi.mock("#/routes/create.functions", () => ({
	getCreateContext: vi.fn(),
	startWorkspaceCreate: h.start,
	retryWorkspaceCreate: h.retry,
	getCreateProgress: h.progress,
}));

import { CreateForm, CreateProgressPanel } from "#/routes/create";
import type { CreateContext, CreateProgress } from "#/routes/create.functions";
import { TestQueryProvider } from "#/ui/cockpit/test-query-provider";

const FORM_CONTEXT: Extract<CreateContext, { mode: "form" }> = {
	mode: "form",
	verticals: [{ name: "finance", description: "Financial analysis" }],
	portalOrigin: "http://dataraum.localhost:8000",
};

/** serverFnError's client-side face: a rethrown Error whose message is the
 * JSON envelope. */
function fnRejection(error: string, message?: string): Error {
	return new Error(JSON.stringify(message ? { error, message } : { error }));
}

/** Mount `element` under a minimal memory router (the form uses useNavigate
 * + Link, which need a live router). */
function renderAtCreate(element: ReactNode) {
	const rootRoute = createRootRoute();
	const createRouteDef = createRoute({
		getParentRoute: () => rootRoute,
		path: "create",
		component: () => <>{element}</>,
		validateSearch: (search: Record<string, unknown>) => ({
			ws: typeof search.ws === "string" ? search.ws : undefined,
		}),
	});
	const router = createRouter({
		routeTree: rootRoute.addChildren([createRouteDef]),
		history: createMemoryHistory({ initialEntries: ["/create"] }),
	});
	render(
		<TestQueryProvider>
			<MantineProvider env="test">
				{/* biome-ignore lint/suspicious/noExplicitAny: minimal ad-hoc tree, not the generated Register */}
				<RouterProvider router={router as any} />
			</MantineProvider>
		</TestQueryProvider>,
	);
	return router;
}

afterEach(() => {
	cleanup();
	vi.clearAllMocks();
});

describe("CreateForm (DAT-821)", () => {
	it("derives the subdomain from the name and submits it", async () => {
		h.start.mockResolvedValue({ workspaceId: "ws-new" });
		renderAtCreate(<CreateForm context={FORM_CONTEXT} />);
		await screen.findByTestId("create-name");
		fireEvent.change(screen.getByTestId("create-name"), {
			target: { value: "Dept 3" },
		});
		expect(
			(screen.getByTestId("create-subdomain") as HTMLInputElement).value,
		).toBe("dept-3");
		fireEvent.click(screen.getByTestId("create-submit"));
		await waitFor(() =>
			expect(h.start).toHaveBeenCalledWith({
				data: { name: "Dept 3", vertical: "finance", subdomain: "dept-3" },
			}),
		);
	});

	it("renders a rejected start's lifecycle message in the error alert", async () => {
		h.start.mockRejectedValue(
			fnRejection(
				"subdomain_taken",
				"subdomain 'dept-3' is already claimed by a live workspace — pick another label",
			),
		);
		renderAtCreate(<CreateForm context={FORM_CONTEXT} />);
		await screen.findByTestId("create-name");
		fireEvent.change(screen.getByTestId("create-name"), {
			target: { value: "Dept 3" },
		});
		fireEvent.click(screen.getByTestId("create-submit"));
		const alert = await screen.findByTestId("create-error");
		expect(alert.textContent).toContain(
			"subdomain 'dept-3' is already claimed",
		);
	});
});

describe("CreateProgressPanel (DAT-821)", () => {
	const base: CreateProgress = {
		state: "creating",
		name: "Dept 3",
		subdomain: "ws3",
		url: null,
		inFlight: false,
		error: null,
	};

	it("renders a failed run's error verbatim with a Retry", async () => {
		h.progress.mockResolvedValue({
			...base,
			error: "workspace did not come up within 300000ms",
		});
		renderAtCreate(<CreateProgressPanel workspaceId="ws-x" />);
		const alert = await screen.findByTestId("create-error");
		expect(alert.textContent).toContain("did not come up within");
		expect(screen.getByTestId("create-retry")).toBeTruthy();
	});

	it("renders the restart-interrupted state with a Retry that re-runs the id", async () => {
		h.progress.mockResolvedValue(base);
		h.retry.mockResolvedValue({ workspaceId: "ws-x" });
		renderAtCreate(<CreateProgressPanel workspaceId="ws-x" />);
		expect(
			(await screen.findByText(/Creation interrupted/)).textContent,
		).toBeTruthy();
		fireEvent.click(screen.getByTestId("create-retry"));
		await waitFor(() =>
			expect(h.retry).toHaveBeenCalledWith({ data: { workspaceId: "ws-x" } }),
		);
	});

	it("shows the provisioning spinner while the run is in flight", async () => {
		h.progress.mockResolvedValue({ ...base, inFlight: true });
		renderAtCreate(<CreateProgressPanel workspaceId="ws-x" />);
		expect(await screen.findByText(/Provisioning/)).toBeTruthy();
		expect(screen.queryByTestId("create-retry")).toBeNull();
	});
});
