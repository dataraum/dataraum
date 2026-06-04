// @vitest-environment jsdom
//
// jsdom, not the repo-default happy-dom: DOMPurify is the browser sanitizer and
// strips correctly in real browsers + jsdom, but happy-dom's DOM is incomplete
// enough that DOMPurify silently bails on some payloads (it re-serializes
// without stripping). The XSS-guard test below is meaningless under happy-dom.

import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { MarkdownMessage } from "#/ui/cockpit/markdown";

describe("MarkdownMessage", () => {
	afterEach(() => cleanup());

	it("renders markdown formatting (bold + lists), not raw syntax", () => {
		render(<MarkdownMessage content={"**bold** and\n\n- one\n- two"} />);
		const el = screen.getByTestId("markdown-message");
		expect(el.querySelector("strong")?.textContent).toBe("bold");
		expect(el.querySelectorAll("li")).toHaveLength(2);
		// The raw `**` markers must not survive as text.
		expect(el.textContent).not.toContain("**");
	});

	it("highlights a fenced SQL code block", () => {
		render(<MarkdownMessage content={"```sql\nSELECT 1 FROM t\n```"} />);
		const code = screen.getByTestId("markdown-message").querySelector("code");
		expect(code?.className).toContain("language-sql");
		// highlight.js wraps keywords (SELECT/FROM) in token spans.
		expect(code?.querySelector("span.hljs-keyword")).toBeTruthy();
	});

	it("SANITIZES dangerous HTML from LLM output (XSS guard)", () => {
		render(
			<MarkdownMessage
				content={'<img src=x onerror="alert(1)"> <script>alert(2)</script> ok'}
			/>,
		);
		const el = screen.getByTestId("markdown-message");
		expect(el.innerHTML).not.toContain("onerror");
		expect(el.innerHTML).not.toContain("<script");
		// Benign text still renders.
		expect(el.textContent).toContain("ok");
	});
});
