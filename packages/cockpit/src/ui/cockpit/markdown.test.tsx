// @vitest-environment jsdom
//
// jsdom is the cockpit's DOM test env: DOMPurify is the browser sanitizer and
// strips correctly under jsdom (and real browsers). This file was the reason we
// dropped happy-dom — its DOM was incomplete enough that DOMPurify silently
// bailed on some payloads (re-serializing without stripping), making the
// XSS-guard tests below meaningless.

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

	it("strips a javascript: link href", () => {
		render(<MarkdownMessage content={"[click](javascript:alert(1))"} />);
		const link = screen.getByTestId("markdown-message").querySelector("a");
		expect(link?.getAttribute("href") ?? "").not.toContain("javascript:");
	});

	it("opens links in a new tab with reverse-tabnabbing protection", () => {
		render(<MarkdownMessage content={"[docs](https://example.com)"} />);
		const link = screen.getByTestId("markdown-message").querySelector("a");
		expect(link?.getAttribute("target")).toBe("_blank");
		expect(link?.getAttribute("rel")).toContain("noopener");
	});

	it("does not throw on an unregistered code fence (falls back to plaintext)", () => {
		expect(() =>
			render(<MarkdownMessage content={"```rust\nfn main() {}\n```"} />),
		).not.toThrow();
		expect(screen.getByTestId("markdown-message").textContent).toContain(
			"fn main",
		);
	});
});
