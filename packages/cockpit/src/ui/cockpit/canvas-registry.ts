// The one shared widget registry (DAT-347, C1).
//
// Single instance the focus canvas resolves against. C1 registers the three
// baseline widgets. A C2-C6 column lands its widget by adding ONE register()
// line here — it does not touch FocusCanvas, the stream, or the shell. See
// README.md for the register-don't-replace contract.

import { WidgetRegistry } from "#/ui/cockpit/widget-registry";
import { AnswerResultWidget } from "#/ui/cockpit/widgets/answer-result";
import { ColumnProfileWidget } from "#/ui/cockpit/widgets/column-profile";
import { ColumnWhyWidget } from "#/ui/cockpit/widgets/column-why";
import { CycleListWidget } from "#/ui/cockpit/widgets/cycle-list";
import { CycleWhyWidget } from "#/ui/cockpit/widgets/cycle-why";
import { EmptyWidget } from "#/ui/cockpit/widgets/empty";
import { ErrorWidget } from "#/ui/cockpit/widgets/error";
import { LoadingWidget } from "#/ui/cockpit/widgets/loading";
import { MeasureProgressWidget } from "#/ui/cockpit/widgets/measure-progress";
import { MetricListWidget } from "#/ui/cockpit/widgets/metric-list";
import { MetricShadowWidget } from "#/ui/cockpit/widgets/metric-shadow";
import { MetricWhyWidget } from "#/ui/cockpit/widgets/metric-why";
import { OperatingModelProgressWidget } from "#/ui/cockpit/widgets/operating-model-progress";
import { ProbeWidget } from "#/ui/cockpit/widgets/probe";
import { RelationshipListWidget } from "#/ui/cockpit/widgets/relationship-list";
import { RelationshipWhyWidget } from "#/ui/cockpit/widgets/relationship-why";
import { ResultGridWidget } from "#/ui/cockpit/widgets/result-grid";
import { SessionProgressWidget } from "#/ui/cockpit/widgets/session-progress";
import { SourceListWidget } from "#/ui/cockpit/widgets/source-list";
import { TableReadinessWidget } from "#/ui/cockpit/widgets/table-readiness";
import { TableWhyWidget } from "#/ui/cockpit/widgets/table-why";
import { ValidationListWidget } from "#/ui/cockpit/widgets/validation-list";
import { ValidationWhyWidget } from "#/ui/cockpit/widgets/validation-why";
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
	.register({ kind: "result-grid", component: ResultGridWidget })
	.register({ kind: "answer-result", component: AnswerResultWidget })
	.register({ kind: "table-readiness", component: TableReadinessWidget })
	.register({ kind: "column-why", component: ColumnWhyWidget })
	.register({ kind: "column-profile", component: ColumnProfileWidget })
	.register({ kind: "table-why", component: TableWhyWidget })
	.register({ kind: "relationship-why", component: RelationshipWhyWidget })
	.register({ kind: "relationship-list", component: RelationshipListWidget })
	.register({ kind: "validation-list", component: ValidationListWidget })
	.register({ kind: "validation-why", component: ValidationWhyWidget })
	.register({ kind: "cycle-list", component: CycleListWidget })
	.register({ kind: "cycle-why", component: CycleWhyWidget })
	.register({ kind: "metric-list", component: MetricListWidget })
	.register({ kind: "metric-why", component: MetricWhyWidget })
	.register({ kind: "metric-shadow", component: MetricShadowWidget })
	.register({ kind: "add-source-progress", component: MeasureProgressWidget })
	.register({ kind: "session-progress", component: SessionProgressWidget })
	.register({
		kind: "operating-model-progress",
		component: OperatingModelProgressWidget,
	})
	.register({ kind: "probe", component: ProbeWidget });
