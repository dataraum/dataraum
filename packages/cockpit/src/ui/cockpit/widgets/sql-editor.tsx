// A minimal SQL editor over CodeMirror 6 PRIMARIES — no react wrapper (we depend
// on @codemirror/* directly + integrate it here; wrappers are dependency hell).
//
// The EditorView is an external system (React idiom rule 2): created ONCE in an
// effect, destroyed on cleanup. Callbacks are read through refs so a changing
// onChange/onRun never tears the editor down and rebuilds it. SSR-safe — the view
// is only ever constructed client-side inside the effect; the host renders an
// empty div on the server (no module-level DOM access in @codemirror/*).

import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
import { sql } from "@codemirror/lang-sql";
import {
	defaultHighlightStyle,
	syntaxHighlighting,
} from "@codemirror/language";
import { EditorState } from "@codemirror/state";
import {
	placeholder as cmPlaceholder,
	EditorView,
	keymap,
	lineNumbers,
} from "@codemirror/view";
import { useEffect, useRef } from "react";

export function SqlEditor({
	value,
	onChange,
	onRun,
	placeholder,
}: {
	/** Current SQL text. External changes (e.g. an agent-seeded query) are synced
	 * into the editor; the editor's own edits flow out through `onChange`. */
	value: string;
	onChange: (next: string) => void;
	/** Invoked on Mod-Enter (⌘/Ctrl+Enter) — the run shortcut. */
	onRun?: () => void;
	placeholder?: string;
}) {
	const hostRef = useRef<HTMLDivElement>(null);
	const viewRef = useRef<EditorView | null>(null);
	// Latest callbacks, read by the editor's (construct-once) extensions. Synced in
	// an effect, never written during render (React idiom rule 8).
	const onChangeRef = useRef(onChange);
	const onRunRef = useRef(onRun);
	useEffect(() => {
		onChangeRef.current = onChange;
		onRunRef.current = onRun;
	});

	// Construct the EditorView once. `value` seeds the initial doc only; later
	// external changes are applied by the sync effect below.
	// biome-ignore lint/correctness/useExhaustiveDependencies: construct once — value is the initial doc (synced below), placeholder is static.
	useEffect(() => {
		const host = hostRef.current;
		if (!host) return;
		const view = new EditorView({
			parent: host,
			state: EditorState.create({
				doc: value,
				extensions: [
					lineNumbers(),
					history(),
					keymap.of([
						{
							key: "Mod-Enter",
							run: () => {
								onRunRef.current?.();
								return true;
							},
						},
						...defaultKeymap,
						...historyKeymap,
					]),
					sql(),
					syntaxHighlighting(defaultHighlightStyle),
					...(placeholder ? [cmPlaceholder(placeholder)] : []),
					EditorView.updateListener.of((update) => {
						if (update.docChanged) {
							onChangeRef.current(update.state.doc.toString());
						}
					}),
					EditorView.theme({
						"&": { fontSize: "13px", maxHeight: "260px" },
						"&.cm-focused": { outline: "none" },
						".cm-scroller": {
							overflow: "auto",
							fontFamily:
								"var(--mantine-font-family-monospace, ui-monospace, monospace)",
						},
					}),
				],
			}),
		});
		viewRef.current = view;
		return () => {
			view.destroy();
			viewRef.current = null;
		};
	}, []);

	// Sync external `value` changes into the editor (e.g. agent-seeded SQL). When
	// the change originated from typing, `value` already equals the doc, so this is
	// a no-op — it never fights the user's cursor.
	useEffect(() => {
		const view = viewRef.current;
		if (!view) return;
		const current = view.state.doc.toString();
		if (value !== current) {
			view.dispatch({
				changes: { from: 0, to: current.length, insert: value },
			});
		}
	}, [value]);

	return (
		<div
			ref={hostRef}
			data-testid="sql-editor"
			style={{
				border: "1px solid var(--mantine-color-default-border)",
				borderRadius: "var(--mantine-radius-sm)",
				overflow: "hidden",
			}}
		/>
	);
}
