// The one shared widget registry (DAT-347, C1).
//
// Single instance the focus canvas resolves against. C1 registers the three
// baseline widgets. A C2-C6 column lands its widget by adding ONE register()
// line here — it does not touch FocusCanvas, the stream, or the shell. See
// README.md for the register-don't-replace contract.

import { WidgetRegistry } from "#/ui/cockpit/widget-registry";
import { ColumnWhyWidget } from "#/ui/cockpit/widgets/column-why";
import { ConceptFrameWidget } from "#/ui/cockpit/widgets/concept-frame";
import { EmptyWidget } from "#/ui/cockpit/widgets/empty";
import { ErrorWidget } from "#/ui/cockpit/widgets/error";
import { LoadingWidget } from "#/ui/cockpit/widgets/loading";
import { MeasureProgressWidget } from "#/ui/cockpit/widgets/measure-progress";
import { ResultGridWidget } from "#/ui/cockpit/widgets/result-grid";
import { SchemaPreviewWidget } from "#/ui/cockpit/widgets/schema-preview";
import { SourceListWidget } from "#/ui/cockpit/widgets/source-list";
import { TableReadinessWidget } from "#/ui/cockpit/widgets/table-readiness";
import { UploadAreaWidget } from "#/ui/cockpit/widgets/upload-area";
import { WorkspaceInventoryWidget } from "#/ui/cockpit/widgets/workspace-inventory";

export const canvasRegistry = new WidgetRegistry()
	.register({ kind: "empty", component: EmptyWidget })
	.register({ kind: "loading", component: LoadingWidget })
	.register({ kind: "error", component: ErrorWidget })
	.register({ kind: "source-list", component: SourceListWidget })
	.register({
		kind: "workspace-inventory",
		component: WorkspaceInventoryWidget,
	})
	.register({ kind: "schema-preview", component: SchemaPreviewWidget })
	.register({ kind: "concept-frame", component: ConceptFrameWidget })
	.register({ kind: "result-grid", component: ResultGridWidget })
	.register({ kind: "table-readiness", component: TableReadinessWidget })
	.register({ kind: "column-why", component: ColumnWhyWidget })
	.register({ kind: "add-source-progress", component: MeasureProgressWidget })
	.register({ kind: "upload-area", component: UploadAreaWidget });
