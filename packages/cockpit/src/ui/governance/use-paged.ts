// Client-side pagination over an already-loaded list (DAT-633). The Governance
// lists are fully fetched in the loader, so paging is pure UI state — never a
// cap: every row is reachable, just one page at a time.

import { useState } from "react";

export interface PageView<T> {
	/** The clamped current page (1-based). */
	current: number;
	totalPages: number;
	pageItems: T[];
}

/** Pure paging math — a clamped window over the full list. Extracted from the
 * hook so it's unit-testable without a React harness. NEVER drops rows: every
 * item is on exactly one page. */
export function paginate<T>(
	items: readonly T[],
	page: number,
	pageSize: number,
): PageView<T> {
	const totalPages = Math.max(1, Math.ceil(items.length / pageSize));
	// Clamp so a stale/over-large page (e.g. a shrunk list) can't strand the view.
	const current = Math.min(Math.max(1, page), totalPages);
	const pageItems = items.slice((current - 1) * pageSize, current * pageSize);
	return { current, totalPages, pageItems };
}

export interface Paged<T> extends PageView<T> {
	page: number;
	setPage: (p: number) => void;
}

export function usePaged<T>(items: readonly T[], pageSize: number): Paged<T> {
	const [page, setPage] = useState(1);
	const view = paginate(items, page, pageSize);
	return { ...view, page: view.current, setPage };
}
