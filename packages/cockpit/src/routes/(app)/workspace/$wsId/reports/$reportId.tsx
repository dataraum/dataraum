import {
	ActionIcon,
	Button,
	Group,
	Stack,
	Text,
	TextInput,
	Title,
} from "@mantine/core";
import {
	createFileRoute,
	notFound,
	useNavigate,
	useRouter,
} from "@tanstack/react-router";
import { createServerFn, useServerFn } from "@tanstack/react-start";
import { Check, Pencil, Trash2, X } from "lucide-react";
import { useState } from "react";
import {
	getReport,
	renameReport,
	softDeleteReport,
} from "#/db/cockpit/reports";
import { ConfidenceStrip } from "#/ui/cockpit/widgets/answer-result";
import { ResultGridWidget } from "#/ui/cockpit/widgets/result-grid";

// Report detail (DAT-624) — the frozen artifact rendered over LIVE data: the SQL is
// re-run on every open through the same result-grid stream, so numbers stay current.
// The title is the one editable field (inline); the SQL / summary / confidence are
// immutable. Delete is soft (the row stays; children keep their lineage).
//
// The loader + action server fns are defined inline (the cockpit route convention)
// so the plugin strips their cockpit_db handlers from the client bundle.

const loadReport = createServerFn({ method: "GET" })
	.inputValidator((reportId: string) => reportId)
	.handler(async ({ data: reportId }) => getReport(reportId));

const renameReportFn = createServerFn({ method: "POST" })
	.inputValidator((data: { id: string; title: string }) => data)
	.handler(async ({ data }) => {
		await renameReport(data.id, data.title);
	});

const deleteReportFn = createServerFn({ method: "POST" })
	.inputValidator((reportId: string) => reportId)
	.handler(async ({ data: reportId }) => {
		await softDeleteReport(reportId);
	});

export const Route = createFileRoute(
	"/(app)/workspace/$wsId/reports/$reportId",
)({
	loader: async ({ params }) => {
		const report = await loadReport({ data: params.reportId });
		if (!report) throw notFound();
		return report;
	},
	component: ReportDetail,
});

function ReportDetail() {
	const report = Route.useLoaderData();
	const { wsId } = Route.useParams();
	const router = useRouter();
	const navigate = useNavigate();
	const rename = useServerFn(renameReportFn);
	const remove = useServerFn(deleteReportFn);

	const [editing, setEditing] = useState(false);
	// Seeded when the editor opens (below), NOT from useState(report.title): after a
	// rename, router.invalidate() refreshes `report` WITHOUT remounting, so a
	// once-initialized draft would show the stale pre-rename title on the next open.
	const [draft, setDraft] = useState("");
	const [busy, setBusy] = useState(false);

	// Mutations fired by user events live in handlers, not effects (React conv. 4).
	const saveTitle = async () => {
		const title = draft.trim();
		if (!title || title === report.title) {
			setEditing(false);
			return;
		}
		setBusy(true);
		try {
			await rename({ data: { id: report.id, title } });
			setEditing(false);
			await router.invalidate();
		} finally {
			setBusy(false);
		}
	};

	const deleteReport = async () => {
		setBusy(true);
		try {
			await remove({ data: report.id });
			navigate({ to: "/workspace/$wsId/reports", params: { wsId } });
		} finally {
			setBusy(false);
		}
	};

	return (
		<Stack p="md" gap="md" data-testid="report-detail">
			<Group justify="space-between" wrap="nowrap">
				{editing ? (
					<Group gap="xs" style={{ flex: 1 }}>
						<TextInput
							value={draft}
							onChange={(e) => setDraft(e.currentTarget.value)}
							onKeyDown={(e) => {
								if (e.key === "Enter") saveTitle();
								if (e.key === "Escape") {
									setDraft(report.title);
									setEditing(false);
								}
							}}
							style={{ flex: 1 }}
							data-autofocus
							data-testid="report-title-input"
						/>
						<ActionIcon
							variant="light"
							onClick={saveTitle}
							loading={busy}
							aria-label="Save title"
						>
							<Check size={16} />
						</ActionIcon>
						<ActionIcon
							variant="subtle"
							onClick={() => {
								setDraft(report.title);
								setEditing(false);
							}}
							aria-label="Cancel"
						>
							<X size={16} />
						</ActionIcon>
					</Group>
				) : (
					<Group gap="xs">
						<Title order={3}>{report.title}</Title>
						<ActionIcon
							variant="subtle"
							onClick={() => {
								setDraft(report.title);
								setEditing(true);
							}}
							aria-label="Rename report"
						>
							<Pencil size={16} />
						</ActionIcon>
					</Group>
				)}
				<Button
					color="red"
					variant="light"
					leftSection={<Trash2 size={14} />}
					onClick={deleteReport}
					loading={busy}
				>
					Delete
				</Button>
			</Group>

			{report.summary && <Text>{report.summary}</Text>}
			<ConfidenceStrip confidence={report.confidence} />
			<ResultGridWidget state={{ kind: "result-grid", sql: report.sql }} />
		</Stack>
	);
}
