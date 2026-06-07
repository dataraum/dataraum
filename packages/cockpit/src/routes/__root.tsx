import {
	ColorSchemeScript,
	MantineProvider,
	mantineHtmlProps,
} from "@mantine/core";
import { TanStackDevtools } from "@tanstack/react-devtools";
import type { QueryClient } from "@tanstack/react-query";
import {
	createRootRouteWithContext,
	HeadContent,
	Scripts,
} from "@tanstack/react-router";
import { TanStackRouterDevtoolsPanel } from "@tanstack/react-router-devtools";
import type { ReactNode } from "react";

import "@mantine/core/styles.css";
import "../styles.css";
import { theme } from "#/ui/theme";

interface RouterContext {
	queryClient: QueryClient;
}

export const Route = createRootRouteWithContext<RouterContext>()({
	head: () => ({
		meta: [
			{
				charSet: "utf-8",
			},
			{
				name: "viewport",
				content: "width=device-width, initial-scale=1",
			},
			{
				title: "DataRaum Cockpit",
			},
		],
	}),
	shellComponent: RootDocument,
});

// The shell receives the ENTIRE root match chain as `children` (the installed
// contract: `shellComponent?: ({ children }) => ReactNode`) — the root match
// context provider, Suspense/CatchBoundary/CatchNotFound, and the matched
// routes. Rendering `children` (NOT a bare <Outlet/>) is what makes a future
// root errorComponent/notFoundComponent/pendingComponent actually render
// (DAT-451; the old Outlet worked only by an implementation coincidence).
function RootDocument({ children }: { children: ReactNode }) {
	return (
		<html lang="en" {...mantineHtmlProps}>
			<head>
				<ColorSchemeScript />
				<HeadContent />
			</head>
			<body>
				<MantineProvider theme={theme}>{children}</MantineProvider>
				<TanStackDevtools
					config={{
						position: "bottom-right",
					}}
					plugins={[
						{
							name: "Tanstack Router",
							render: <TanStackRouterDevtoolsPanel />,
						},
					]}
				/>
				<Scripts />
			</body>
		</html>
	);
}
