// jsdom polyfills — the cockpit's single DOM test environment (happy-dom removed).
//
// jsdom is the faithful DOM, but it omits a few browser APIs that Mantine and the
// autosize Textarea call on mount. Stub them so component tests render real
// components instead of forcing the components to dumb down for the test env.
// Guarded on `window` so this is a no-op under the `node` environment (the
// server-route + SSR-regression tests).

if (typeof window !== "undefined") {
	const noop = () => {};

	// Mantine's color-scheme manager + responsive hooks call matchMedia on mount;
	// jsdom doesn't implement it.
	if (!window.matchMedia) {
		window.matchMedia = (query: string) =>
			({
				matches: false,
				media: query,
				onchange: null,
				addListener: noop,
				removeListener: noop,
				addEventListener: noop,
				removeEventListener: noop,
				dispatchEvent: () => false,
			}) as unknown as MediaQueryList;
	}

	// The autosize Textarea subscribes to font-loading (Mantine Autosize.mjs:118,
	// `document.fonts.addEventListener("loadingdone", …)`); jsdom has no
	// FontFaceSet. A no-op event target is enough for mount.
	if (!document.fonts) {
		Object.defineProperty(document, "fonts", {
			configurable: true,
			value: {
				ready: Promise.resolve(),
				addEventListener: noop,
				removeEventListener: noop,
			},
		});
	}

	// Some Mantine widgets construct a ResizeObserver; jsdom omits it. Assign
	// through a loose cast — lib.dom declares ResizeObserver, so an `in`/typeof
	// guard would narrow `window` to `never` (the property is "known" to exist).
	const g = window as unknown as { ResizeObserver?: unknown };
	if (!g.ResizeObserver) {
		g.ResizeObserver = class {
			observe = noop;
			unobserve = noop;
			disconnect = noop;
		};
	}

	// jsdom doesn't implement scroll methods (ScrollArea / scroll-to-bottom call
	// them); stub to silence the "Not implemented" noise without affecting assertions.
	window.scrollTo = noop;
	Element.prototype.scrollIntoView = noop;
}
