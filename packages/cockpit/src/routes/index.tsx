// `/` — modal on the image's role (DAT-819, DD/51740673).
//
// Workspace cockpit: redirect straight into the workspace UI (unchanged
// behavior; the membership gate already vetted the request).
//
// Portal: the login + membership-routing surface. Signed out → better-auth
// email/password form; signed in → the user's workspaces from `memberships`,
// each a plain link to its subdomain (switching workspaces IS navigation —
// no client-side workspace state). `?denied=<ws>` renders the bounce notice
// a workspace cockpit's gate redirects here with.

import {
	Alert,
	Badge,
	Button,
	Group,
	Paper,
	PasswordInput,
	Stack,
	Text,
	TextInput,
	Title,
} from "@mantine/core";
import {
	createFileRoute,
	Link,
	redirect,
	useRouter,
} from "@tanstack/react-router";
import { useState } from "react";
import { authClient } from "#/auth/auth-client";
import { getPortalHome, type PortalHome } from "./index.functions";

export const Route = createFileRoute("/")({
	validateSearch: (search: Record<string, unknown>) => ({
		// The gate's bounce marker: the workspace id the user was denied on.
		denied: typeof search.denied === "string" ? search.denied : undefined,
	}),
	beforeLoad: async () => {
		const home = await getPortalHome();
		if (home.mode === "workspace") {
			// One cockpit per workspace (DD/51740673): the subdomain IS the
			// workspace, so `/` goes straight to the flat cockpit URL.
			throw redirect({ to: "/cockpit" });
		}
		return { home };
	},
	loader: ({ context }) => context.home,
	component: PortalPage,
});

function PortalPage() {
	const home = Route.useLoaderData();
	const { denied } = Route.useSearch();
	return (
		// House tokens only (styles.css @theme mirrors src/ui/theme.ts): the
		// stock Tailwind scale is REMAPPED here — `max-w-md` resolves to
		// --spacing-md (1rem) and collapses the card — so widths go through
		// Mantine props (`maw`), the cockpit's idiom (chart-modal precedent).
		<div className="flex min-h-screen items-center justify-center bg-surface-muted p-lg">
			<Paper w="100%" maw={420} withBorder shadow="sm" radius="md" p="xl">
				<Stack gap="lg">
					<Stack gap={4}>
						<Title order={2}>DataRaum</Title>
						<Text c="dimmed" size="sm">
							{home.mode === "signin"
								? "Sign in to reach your workspaces."
								: "Your workspaces."}
						</Text>
					</Stack>
					{denied ? (
						<Alert
							color="yellow"
							variant="light"
							title="No access to that workspace"
						>
							You are signed in, but not a member of workspace {denied}.
							{home.mode === "signin"
								? " Sign in with an account that has access."
								: " Pick one of your workspaces below."}
						</Alert>
					) : null}
					{home.mode === "signin" ? (
						<SignInForm />
					) : (
						<WorkspaceList home={home} />
					)}
				</Stack>
			</Paper>
		</div>
	);
}

function SignInForm() {
	const router = useRouter();
	const [email, setEmail] = useState("");
	const [password, setPassword] = useState("");
	const [error, setError] = useState<string | null>(null);
	const [submitting, setSubmitting] = useState(false);

	const signIn = async () => {
		setSubmitting(true);
		setError(null);
		const { error: signInError } = await authClient.signIn.email({
			email,
			password,
		});
		setSubmitting(false);
		if (signInError) {
			setError(signInError.message ?? "Sign-in failed");
			return;
		}
		// The session cookie is set — re-run the portal home resolution.
		await router.invalidate();
	};

	return (
		<form
			onSubmit={(event) => {
				event.preventDefault();
				void signIn();
			}}
		>
			<Stack gap="sm">
				<TextInput
					label="Email"
					type="email"
					required
					value={email}
					onChange={(event) => setEmail(event.currentTarget.value)}
					autoComplete="email"
				/>
				<PasswordInput
					label="Password"
					required
					value={password}
					onChange={(event) => setPassword(event.currentTarget.value)}
					autoComplete="current-password"
				/>
				{error ? (
					<Alert color="red" variant="light" title="Sign-in failed">
						{error}
					</Alert>
				) : null}
				<Button type="submit" fullWidth mt="xs" loading={submitting}>
					Sign in
				</Button>
			</Stack>
		</form>
	);
}

function WorkspaceList({
	home,
}: {
	home: Extract<PortalHome, { mode: "list" }>;
}) {
	const router = useRouter();
	const signOut = async () => {
		await authClient.signOut();
		await router.invalidate();
	};

	return (
		<Stack gap="sm">
			{home.workspaces.length === 0 ? (
				// Any signed-in user of the installation may create (create-gate.server.ts:
				// one installation = one tenant), so the zero-state points at the button
				// below rather than telling people to ask someone else.
				<Text size="sm" c="dimmed">
					No workspaces yet — create one to get started.
				</Text>
			) : (
				home.workspaces.map((ws) => (
					<Paper key={ws.id} withBorder radius="md" p="sm">
						<Group justify="space-between" wrap="nowrap" gap="sm">
							<Stack gap={0} className="min-w-0">
								<Text size="sm" fw={500} truncate>
									{ws.name}
								</Text>
								<Text size="xs" c="dimmed" truncate>
									{ws.id}
								</Text>
							</Stack>
							{ws.url ? (
								// shrink-0: the nowrap Group must squeeze the truncating
								// text column, never the action.
								<Button
									component="a"
									href={ws.url}
									size="xs"
									className="shrink-0"
								>
									Open
								</Button>
							) : ws.state !== "ready" ? (
								// Mid-lifecycle (DAT-821): visible with its state, not
								// enterable — the provisioner flips it to `ready`.
								<Badge
									variant="light"
									color={ws.state === "creating" ? "yellow" : "gray"}
									className="shrink-0"
								>
									{ws.state}
								</Badge>
							) : (
								<Text size="xs" c="dimmed" className="shrink-0">
									no subdomain
								</Text>
							)}
						</Group>
					</Paper>
				))
			)}
			<Button
				component={Link}
				to="/create"
				variant="light"
				fullWidth
				mt="xs"
				data-testid="portal-new-workspace"
			>
				New workspace
			</Button>
			<Group justify="space-between" mt="xs">
				<Text size="xs" c="dimmed">
					{home.email}
				</Text>
				<Button variant="subtle" size="xs" onClick={() => void signOut()}>
					Sign out
				</Button>
			</Group>
		</Stack>
	);
}
