// Markdown rendering for assistant chat text (DAT-353 Slice 2).
//
// marked → HTML string → DOMPurify → dangerouslySetInnerHTML. marked dropped its
// built-in sanitizer years ago; we render LLM-generated markdown, so sanitizing
// is MANDATORY — a model can emit `<img onerror=…>` / `<script>`. Syntax
// highlighting is marked-highlight + highlight.js, registering ONLY the languages
// the agent realistically emits (off /lib/core) so unused grammars don't ship.
// Memoized per `content`: while a turn streams, only the in-flight message
// re-parses; settled messages are skipped.

import DOMPurify from "dompurify";
import hljs from "highlight.js/lib/core";
import bash from "highlight.js/lib/languages/bash";
import json from "highlight.js/lib/languages/json";
import plaintext from "highlight.js/lib/languages/plaintext";
import python from "highlight.js/lib/languages/python";
import sql from "highlight.js/lib/languages/sql";
import typescript from "highlight.js/lib/languages/typescript";
import { Marked } from "marked";
import { markedHighlight } from "marked-highlight";
import { memo, useMemo } from "react";
import "highlight.js/styles/github.css";
import "./markdown.css";

// sql is the product's lingua franca; the rest cover the snippets the agent
// realistically writes. `plaintext` is the fallback — it MUST be registered, or
// hljs.highlight(code, {language: "plaintext"}) throws "Unknown language" and
// crashes the bubble for any unregistered fence (```rust, ```yaml, …). Registered
// off /lib/core so the grammar bundle stays small (the full hljs is ~190 langs).
const LANGUAGES = { sql, json, python, typescript, bash, plaintext } as const;
for (const [name, language] of Object.entries(LANGUAGES)) {
	hljs.registerLanguage(name, language);
}

const marked = new Marked(
	markedHighlight({
		emptyLangClass: "hljs",
		langPrefix: "hljs language-",
		highlight(code, lang) {
			// Unregistered fence → plaintext (no highlighting, just escaped).
			const language = hljs.getLanguage(lang) ? lang : "plaintext";
			return hljs.highlight(code, { language }).value;
		},
	}),
	// gfm tables/strikethrough; `breaks` turns a single newline into <br>, which
	// matches how the model lays out chat prose.
	{ gfm: true, breaks: true },
);

// A SCOPED DOMPurify instance bound to the browser window — NOT the shared
// default singleton — so our hook below can't leak onto any other module that
// sanitizes. dompurify v3's default export IS the factory: calling it with a
// window (`DOMPurify(window)`) returns a fresh instance (there is no named
// `createDOMPurify` export). `null` on the server: this component is client-only,
// and without a DOM DOMPurify is a no-op stub (no `addHook`/no real sanitize) —
// referencing it at module-eval time on the SSR server would otherwise throw.
const purifier = typeof window === "undefined" ? null : DOMPurify(window);

// Open links in a new tab with reverse-tabnabbing protection. The hook runs AFTER
// attributes are stripped, so the model can't override rel/target — and
// `javascript:`/other dangerous hrefs are already gone (default URI allowlist).
purifier?.addHook("afterSanitizeAttributes", (node) => {
	if (node.tagName === "A") {
		node.setAttribute("target", "_blank");
		node.setAttribute("rel", "noopener noreferrer");
	}
});

/**
 * Render assistant markdown safely. `content` is LLM output, so it is sanitized
 * with DOMPurify before it ever touches the DOM. Pure render of a string →
 * memoized, so streaming only re-parses the one growing message.
 */
export const MarkdownMessage = memo(function MarkdownMessage({
	content,
}: {
	content: string;
}) {
	const html = useMemo(() => {
		// Client-only: no purifier on the server (no DOM). Returning "" guarantees
		// raw LLM HTML can never exit the server even if the chat ever SSRs content.
		if (!purifier) return "";
		return purifier.sanitize(marked.parse(content, { async: false }));
	}, [content]);

	return (
		<div
			className="cockpit-md"
			data-testid="markdown-message"
			// biome-ignore lint/security/noDangerouslySetInnerHtml: DOMPurify-sanitized just above
			dangerouslySetInnerHTML={{ __html: html }}
		/>
	);
});
