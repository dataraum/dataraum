// Outbound HTTP proxy shim for proxied deployments (corporate networks).
//
// SERVER-ONLY, side-effect on import: importing this module installs a global
// `fetch` wrapper. It is a NO-OP unless `OUTBOUND_PROXY` is set, so default and
// air-gapped deploys are unaffected.
//
// WHY THIS EXISTS — `Bun.S3Client` honors `HTTP_PROXY` but IGNORES `NO_PROXY`.
// In a proxied environment the usual `HTTP_PROXY` env therefore silently routes
// the cockpit's INTERNAL object-store traffic (the `seaweedfs` S3 gateway)
// through the corporate proxy, which returns 503. So we run the cockpit with NO
// global proxy — `Bun.S3Client`, Postgres and Temporal all connect directly —
// and apply the proxy ONLY to public outbound `fetch` calls (the Anthropic API)
// via Bun's per-request `proxy` option. `Bun.S3Client` is native and never uses
// global `fetch`, so it always stays direct regardless of this shim.

const PROXY = process.env.OUTBOUND_PROXY;

// Public host suffixes that must traverse the proxy. Everything else — docker
// service names (`seaweedfs`, `postgres`, `temporal`), localhost — goes direct.
const PROXIED_HOST_SUFFIXES = [".anthropic.com"];

function urlOf(input: RequestInfo | URL): string {
	if (typeof input === "string") return input;
	if (input instanceof URL) return input.href;
	return input.url;
}

function hostNeedsProxy(rawUrl: string): boolean {
	try {
		const { hostname } = new URL(rawUrl);
		return PROXIED_HOST_SUFFIXES.some((suffix) => hostname.endsWith(suffix));
	} catch {
		return false;
	}
}

if (PROXY) {
	const base = globalThis.fetch;
	globalThis.fetch = ((input: RequestInfo | URL, init?: RequestInit) =>
		hostNeedsProxy(urlOf(input))
			? base(input, { ...init, proxy: PROXY })
			: base(input, init)) as typeof fetch;
}
