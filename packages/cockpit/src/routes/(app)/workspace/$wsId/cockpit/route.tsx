import { createFileRoute, Outlet } from "@tanstack/react-router";

// Cockpit layout (DAT-528) — the shared chrome wrapping BOTH the history/landing
// index (/cockpit) and a specific chat (/cockpit/$conversationId). Splitting the
// cockpit into a layout + child routes is what makes a chat deep-linkable and the
// history its own surface (the DD/36667393 route shape). Thin for now (a
// pass-through Outlet); the 3-icon chat-type switcher lands here in S3 (DAT-533).
export const Route = createFileRoute("/(app)/workspace/$wsId/cockpit")({
	component: Outlet,
});
