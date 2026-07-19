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
	Button,
	Group,
	Paper,
	PasswordInput,
	Stack,
	Text,
	TextInput,
	Title,
} from "@mantine/core";
import { createFileRoute, redirect, useRouter } from "@tanstack/react-router";
import { useState } from "react";
import { authClient } from "#/auth/auth-client";
import {
	getActiveWorkspaceId,
	getPortalHome,
	type PortalHome,
} from "./index.functions";

export const Route = createFileRoute("/")({
	validateSearch: (search: Record<string, unknown>) => ({
		// The gate's bounce marker: the workspace id the user was denied on.
		denied: typeof search.denied === "string" ? search.denied : undefined,
	}),
	beforeLoad: async () => {
		const home = await getPortalHome();
		if (home.mode === "workspace") {
			const wsId = await getActiveWorkspaceId();
			throw redirect({
				to: "/workspace/$wsId/cockpit",
				params: { wsId },
			});
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
		<div className="flex min-h-screen items-center justify-center bg-neutral-50 dark:bg-neutral-900">
			<Paper withBorder shadow="sm" p="xl" className="w-full max-w-md">
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
						<Alert color="orange" title="No access to that workspace">
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
					<Alert color="red" title="Sign-in failed">
						{error}
					</Alert>
				) : null}
				<Button type="submit" loading={submitting}>
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
				<Text size="sm" c="dimmed">
					You are not a member of any workspace yet. Ask an administrator to add
					you.
				</Text>
			) : (
				home.workspaces.map((ws) => (
					<Paper key={ws.id} withBorder p="sm">
						<Group justify="space-between" wrap="nowrap">
							<Stack gap={0} className="min-w-0">
								<Text fw={500} truncate>
									{ws.name}
								</Text>
								<Text size="xs" c="dimmed" truncate>
									{ws.id}
								</Text>
							</Stack>
							{ws.url ? (
								<Button component="a" href={ws.url} size="xs">
									Open
								</Button>
							) : (
								<Text size="xs" c="dimmed">
									no subdomain
								</Text>
							)}
						</Group>
					</Paper>
				))
			)}
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
