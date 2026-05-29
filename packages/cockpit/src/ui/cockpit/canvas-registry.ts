// The one shared widget registry (DAT-347, C1).
//
// Single instance the focus canvas resolves against. C1 registers the three
// baseline widgets. A C2-C6 column lands its widget by adding ONE register()
// line here — it does not touch FocusCanvas, the stream, or the shell. See
// README.md for the register-don't-replace contract.

import { WidgetRegistry } from "#/ui/cockpit/widget-registry";
import { EmptyWidget } from "#/ui/cockpit/widgets/empty";
import { ErrorWidget } from "#/ui/cockpit/widgets/error";
import { LoadingWidget } from "#/ui/cockpit/widgets/loading";

export const canvasRegistry = new WidgetRegistry()
	.register({ kind: "empty", component: EmptyWidget })
	.register({ kind: "loading", component: LoadingWidget })
	.register({ kind: "error", component: ErrorWidget });
