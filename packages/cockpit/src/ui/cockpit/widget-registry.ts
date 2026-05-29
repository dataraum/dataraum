// Widget registry (DAT-347, C1).
//
// The focus canvas is a REGISTRY, not a switch. Each CanvasState `kind` maps to
// one widget component. A C2-C6 column adds a member to CanvasState, a widget
// file, and ONE register() call here (via canvas-registry.ts) — it never edits
// the canvas, stream, or shell. See README.md for the register-don't-replace
// contract.

import type { ComponentType } from "react";
import type { CanvasState } from "#/ui/cockpit/canvas-state";

/**
 * Binds a CanvasState member (selected by its `kind`) to the component that
 * renders it. The component receives exactly the narrowed state for that kind,
 * so widgets never re-discriminate the union.
 */
export interface WidgetContract<K extends CanvasState["kind"]> {
	kind: K;
	component: ComponentType<{ state: Extract<CanvasState, { kind: K }> }>;
}

/**
 * Type-erased contract shape used inside the registry's store. The per-kind
 * binding only holds at `register()`; once stored we no longer know which kind a
 * given entry was for, so the component is widened to accept any CanvasState
 * member. The focus canvas re-narrows by `kind` before rendering, which is sound
 * because we resolve an entry by the same kind it was registered under.
 */
type ErasedWidget = {
	kind: CanvasState["kind"];
	component: ComponentType<{ state: CanvasState }>;
};

/**
 * A `kind`-keyed map of widget contracts. `register` is generic so each call
 * keeps its precise `kind ↔ state` binding at the boundary; the store erases it
 * (see ErasedWidget). `resolve` looks a widget up by the kind on a live
 * CanvasState — the concrete kind is only known at runtime.
 */
export class WidgetRegistry {
	#widgets = new Map<CanvasState["kind"], ErasedWidget>();

	register<K extends CanvasState["kind"]>(contract: WidgetContract<K>): this {
		// The map is keyed by kind, so re-registering the same kind replaces it —
		// useful when a column wants to override a baseline widget.
		this.#widgets.set(contract.kind, contract as unknown as ErasedWidget);
		return this;
	}

	resolve(kind: CanvasState["kind"]): ErasedWidget | undefined {
		return this.#widgets.get(kind);
	}

	has(kind: CanvasState["kind"]): boolean {
		return this.#widgets.has(kind);
	}
}
