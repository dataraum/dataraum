import {
	ColorSchemeScript,
	Group,
	MantineProvider,
	mantineHtmlProps,
} from "@mantine/core";
import { TanStackDevtools } from "@tanstack/react-devtools";
import type { QueryClient } from "@tanstack/react-query";
import {
	createRootRouteWithContext,
	HeadContent,
	Link,
	Scripts,
} from "@tanstack/react-router";
import { TanStackRouterDevtoolsPanel } from "@tanstack/react-router-devtools";

import "@mantine/core/styles.css";
import appCss from "../styles.css?url";

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
		links: [
			{
				rel: "stylesheet",
				href: appCss,
			},
		],
	}),
	shellComponent: RootDocument,
});

function RootDocument({ children }: { children: React.ReactNode }) {
	return (
		<html lang="en" {...mantineHtmlProps}>
			<head>
				<ColorSchemeScript />
				<HeadContent />
			</head>
			<body>
				<MantineProvider>
					<Group
						gap="md"
						p="md"
						style={{
							borderBottom: "1px solid var(--mantine-color-gray-3)",
						}}
					>
						<Link to="/" activeProps={{ style: { fontWeight: 600 } }}>
							Home
						</Link>
						<Link to="/sources" activeProps={{ style: { fontWeight: 600 } }}>
							Sources
						</Link>
						<Link to="/chat" activeProps={{ style: { fontWeight: 600 } }}>
							Chat
						</Link>
					</Group>
					{children}
				</MantineProvider>
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
