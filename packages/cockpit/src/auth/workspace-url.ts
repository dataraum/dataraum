// Workspace-subdomain URL construction (DAT-819). Pure — no config import —
// so the portal server fn and tests share one derivation.

/**
 * The origin a workspace is served on: the registry `subdomain` label hung
 * off the portal origin's host — `http://dataraum.localhost` + `ws1` →
 * `http://ws1.dataraum.localhost`. Scheme and port ride along verbatim
 * (Caddy terminates the same listener for the whole parent-domain family).
 */
export function workspaceUrlFor(
	subdomain: string,
	portalOrigin: string,
): string {
	const origin = new URL(portalOrigin);
	return `${origin.protocol}//${subdomain}.${origin.host}`;
}
