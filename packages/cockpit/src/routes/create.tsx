// `/create` — the portal's create-workspace flow (DAT-821): name + vertical
// (+ a derived, editable subdomain) → provisioner → live progress until
// `ready` → redirect to the new workspace's subdomain.
//
// PORTAL-ONLY: on a per-workspace cockpit (or signed out) the route bounces
// to `/` — but that redirect is UX; the authz boundary is inside the server
// fns (create.functions.ts). Same visual shell as the portal home: one
// centered Paper card, house tokens, widths via Mantine props (the Tailwind
// scale is remapped — see `/`).
//
// The two faces share the route via the `ws` search param: without it, the
// form; with it, the progress panel polling the registry until the workspace
// is ready (reload-safe — the id lives in the URL, and a dead run resumes
// with the same-id retry the lifecycle's convergence contract guarantees).

import {
	Alert,
	Anchor,
	Button,
	Group,
	Loader,
	Paper,
	Radio,
	Stack,
	Text,
	TextInput,
	Title,
} from "@mantine/core";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
	createFileRoute,
	Link,
	redirect,
	useNavigate,
} from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { workspaceUrlFor } from "#/auth/workspace-url";
import {
	isValidSubdomainLabel,
	subdomainLabelFrom,
} from "#/lib/subdomain-label";
import {
	type CreateContext,
	getCreateContext,
	getCreateProgress,
	retryWorkspaceCreate,
	startWorkspaceCreate,
} from "./create.functions";

export const Route = createFileRoute("/create")({
	validateSearch: (search: Record<string, unknown>) => ({
		// The in-flight workspace id — present = progress panel, absent = form.
		ws: typeof search.ws === "string" ? search.ws : undefined,
	}),
	beforeLoad: async () => {
		const context = await getCreateContext();
		if (context.mode !== "form") {
			// Workspace cockpit → its `/` goes to the flat UI; portal signed-out →
			// the login screen. Either way, `/` knows.
			throw redirect({ to: "/", search: { denied: undefined } });
		}
		return { createContext: context };
	},
	loader: ({ context }) => context.createContext,
	component: CreatePage,
});

function CreatePage() {
	const context = Route.useLoaderData();
	const { ws } = Route.useSearch();
	return (
		<div className="flex min-h-screen items-center justify-center bg-surface-muted p-lg">
			<Paper w="100%" maw={440} withBorder shadow="sm" radius="md" p="xl">
				{ws ? (
					<CreateProgressPanel workspaceId={ws} />
				) : (
					<CreateForm context={context} />
				)}
			</Paper>
		</div>
	);
}

// ── Form ────────────────────────────────────────────────────────────────────

function CreateForm({
	context,
}: {
	context: Extract<CreateContext, { mode: "form" }>;
}) {
	const navigate = useNavigate();
	const [name, setName] = useState("");
	// The subdomain follows the name (subdomainLabelFrom) until the user takes
	// it over by typing in its field — then it is theirs.
	const [manualSubdomain, setManualSubdomain] = useState<string | null>(null);
	const [vertical, setVertical] = useState(
		// A single builtin (the common installation) needs no choice ceremony.
		context.verticals.length === 1 ? (context.verticals[0]?.name ?? "") : "",
	);

	const subdomain = manualSubdomain ?? subdomainLabelFrom(name);
	const subdomainValid = isValidSubdomainLabel(subdomain);
	const submittable =
		name.trim().length > 0 && vertical !== "" && subdomainValid;

	const start = useMutation({
		mutationFn: () =>
			startWorkspaceCreate({ data: { name, vertical, subdomain } }),
		onSuccess: ({ workspaceId }) =>
			navigate({ to: "/create", search: { ws: workspaceId } }),
	});

	return (
		<form
			onSubmit={(event) => {
				event.preventDefault();
				if (submittable) {
					start.mutate();
				}
			}}
		>
			<Stack gap="lg">
				<Stack gap={4}>
					<Title order={2}>New workspace</Title>
					<Text c="dimmed" size="sm">
						A workspace gets its own engine, cockpit, and subdomain.
					</Text>
				</Stack>

				<TextInput
					label="Name"
					required
					data-testid="create-name"
					value={name}
					onChange={(event) => setName(event.currentTarget.value)}
					placeholder="e.g. Controlling"
				/>

				<Radio.Group
					label="Vertical"
					description="The domain ontology the workspace's analysis grounds against."
					required
					value={vertical}
					onChange={setVertical}
				>
					<Stack gap="xs" mt="xs">
						{context.verticals.length === 0 ? (
							<Alert color="red" variant="light" title="No verticals available">
								The portal cannot read any vertical from its config tree — the
								installation's config mount is missing or empty.
							</Alert>
						) : (
							context.verticals.map((option) => (
								<Radio
									key={option.name}
									value={option.name}
									label={option.name}
									description={option.description ?? undefined}
									data-testid={`create-vertical-${option.name}`}
								/>
							))
						)}
					</Stack>
				</Radio.Group>

				<TextInput
					label="Subdomain"
					required
					data-testid="create-subdomain"
					value={subdomain}
					onChange={(event) =>
						setManualSubdomain(event.currentTarget.value.toLowerCase())
					}
					error={
						subdomain && !subdomainValid
							? "Lowercase letters, digits, and inner dashes only"
							: undefined
					}
					description={
						subdomainValid
							? `Will live at ${workspaceUrlFor(subdomain, context.portalOrigin)}`
							: "Derived from the name — edit to pick your own"
					}
				/>

				{start.isError ? (
					<Alert
						color="red"
						variant="light"
						title="Could not start the workspace"
						data-testid="create-error"
					>
						{rpcErrorMessage(start.error)}
					</Alert>
				) : null}

				<Group justify="space-between">
					<Anchor component={Link} to="/" size="sm" c="dimmed">
						Back to workspaces
					</Anchor>
					<Button
						type="submit"
						data-testid="create-submit"
						disabled={!submittable}
						loading={start.isPending}
					>
						Create workspace
					</Button>
				</Group>
			</Stack>
		</form>
	);
}

/** The server fns reject with status-carrying JSON Responses; the RPC client
 * surfaces a non-ok response as `new Error(<body text>)` (start-client-core
 * serverFnFetcher), so the human `message` — or the `error` code — is parsed
 * back out of the error's message string. */
function rpcErrorMessage(error: unknown): string {
	if (error instanceof Error && error.message) {
		try {
			const body = JSON.parse(error.message) as {
				message?: unknown;
				error?: unknown;
			};
			if (typeof body.message === "string" && body.message) {
				return body.message;
			}
			if (typeof body.error === "string" && body.error) {
				return body.error;
			}
		} catch {
			return error.message;
		}
	}
	return "Something failed before provisioning started — try again.";
}

// ── Progress ────────────────────────────────────────────────────────────────

function CreateProgressPanel({ workspaceId }: { workspaceId: string }) {
	const queryClient = useQueryClient();
	const progress = useQuery({
		queryKey: ["create-progress", workspaceId],
		queryFn: () => getCreateProgress({ data: { workspaceId } }),
		// House polling idiom: interval callback, false once terminal (ready, or
		// failed-and-idle — a retry restarts it via invalidate below).
		refetchInterval: (query) => {
			const data = query.state.data;
			if (!data) {
				return 2000;
			}
			return data.url || (!data.inFlight && data.error) ? false : 2000;
		},
	});

	const retry = useMutation({
		mutationFn: () => retryWorkspaceCreate({ data: { workspaceId } }),
		onSuccess: () =>
			queryClient.invalidateQueries({
				queryKey: ["create-progress", workspaceId],
			}),
	});

	const url = progress.data?.url ?? null;
	// Effect justification: navigating the browser to the new subdomain is an
	// external-system side effect (full document navigation off the portal
	// origin — a router Link cannot express it).
	useEffect(() => {
		if (url) {
			window.location.assign(url);
		}
	}, [url]);

	if (!progress.data) {
		return (
			<Group gap="sm" data-testid="create-progress">
				<Loader size="sm" />
				<Text size="sm">Checking progress…</Text>
			</Group>
		);
	}

	const { state, name, subdomain, inFlight, error } = progress.data;

	return (
		<Stack gap="lg" data-testid="create-progress">
			<Stack gap={4}>
				<Title order={2}>{name ?? "New workspace"}</Title>
				{subdomain ? (
					<Text c="dimmed" size="sm">
						{subdomain}
					</Text>
				) : null}
			</Stack>

			{url ? (
				<Group gap="sm">
					<Loader size="sm" />
					<Text size="sm" data-testid="create-ready">
						Ready — taking you there…
					</Text>
				</Group>
			) : error && !inFlight ? (
				<Stack gap="sm">
					<Alert
						color="red"
						variant="light"
						title="Provisioning failed"
						data-testid="create-error"
					>
						{error}
					</Alert>
					<Group justify="space-between">
						<Anchor
							size="sm"
							c="dimmed"
							// renderRoot, not component={Link}: Mantine's polymorphic prop
							// erases the router generics, so `search` would not typecheck.
							renderRoot={(props) => (
								<Link to="/create" search={{ ws: undefined }} {...props} />
							)}
						>
							Back to the form
						</Anchor>
						{state === "creating" ? (
							<Button
								data-testid="create-retry"
								onClick={() => retry.mutate()}
								loading={retry.isPending}
							>
								Retry
							</Button>
						) : null}
					</Group>
				</Stack>
			) : state === "creating" && !inFlight ? (
				<Stack gap="sm">
					<Alert color="yellow" variant="light" title="Creation interrupted">
						This workspace is still marked as creating, but no run is in flight
						— the portal likely restarted mid-provision. Retrying resumes where
						it stopped.
					</Alert>
					<Group justify="flex-end">
						<Button
							data-testid="create-retry"
							onClick={() => retry.mutate()}
							loading={retry.isPending}
						>
							Retry
						</Button>
					</Group>
				</Stack>
			) : (
				<Stack gap="sm">
					<Group gap="sm">
						<Loader size="sm" />
						<Text size="sm">
							Provisioning — engine and cockpit are starting. This takes a
							minute or two.
						</Text>
					</Group>
					<Text size="xs" c="dimmed">
						Safe to leave open; this page follows the registry until the
						workspace is ready.
					</Text>
				</Stack>
			)}
		</Stack>
	);
}
