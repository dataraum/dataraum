// Workspace subdomain labels (DAT-821). Pure + isomorphic: the create form
// derives a label from the workspace name as the user types, and the server
// fn validates the submitted label with the same pattern — one definition,
// mirroring lifecycle.ts's validateSubdomain (DNS label: lowercase
// alphanumerics + inner dashes, ≤63 chars).

export const SUBDOMAIN_LABEL_PATTERN = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/;
export const SUBDOMAIN_LABEL_MAX = 63;

export function isValidSubdomainLabel(label: string): boolean {
	return (
		label.length <= SUBDOMAIN_LABEL_MAX && SUBDOMAIN_LABEL_PATTERN.test(label)
	);
}

/**
 * Derive a DNS label from a human workspace name: strip diacritics, lowercase,
 * collapse every non-alphanumeric run into one dash, trim edge dashes, cap at
 * 63. May return "" (e.g. an all-symbol name) — the form treats that as
 * "no derivable label; type one".
 */
export function subdomainLabelFrom(name: string): string {
	return name
		.normalize("NFKD")
		.replace(/\p{M}/gu, "")
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/^-+|-+$/g, "")
		.slice(0, SUBDOMAIN_LABEL_MAX)
		.replace(/-+$/g, "");
}
