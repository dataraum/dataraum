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
import python from "highlight.js/lib/languages/python";
import sql from "highlight.js/lib/languages/sql";
import typescript from "highlight.js/lib/languages/typescript";
import { Marked } from "marked";
import { markedHighlight } from "marked-highlight";
import { memo, useMemo } from "react";
import "highlight.js/styles/github.css";
import "./markdown.css";

// sql is the product's lingua franca; the rest cover the snippets the agent
// realistically writes. Registered off /lib/core so the grammar bundle stays
// small (the full highlight.js bundles ~190 languages).
const LANGUAGES = { sql, json, python, typescript, bash } as const;
for (const [name, language] of Object.entries(LANGUAGES)) {
	hljs.registerLanguage(name, language);
}

const marked = new Marked(
	markedHighlight({
		emptyLangClass: "hljs",
		langPrefix: "hljs language-",
		highlight(code, lang) {
			const language = hljs.getLanguage(lang) ? lang : "plaintext";
			return hljs.highlight(code, { language }).value;
		},
	}),
);
// gfm tables/strikethrough; `breaks` turns a single newline into <br>, which
// matches how the model lays out chat prose.
marked.setOptions({ gfm: true, breaks: true });

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
		const raw = marked.parse(content, { async: false });
		return DOMPurify.sanitize(raw);
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
