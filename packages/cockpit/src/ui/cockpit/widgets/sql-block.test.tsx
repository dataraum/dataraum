// @vitest-environment jsdom

// Render tests for the shared read-only SqlBlock (DAT-577) — the literal-SQL
// viewer reused by metric-why, validation-why, and the result-grid disclosure.

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";

import { SqlBlock } from "#/ui/cockpit/widgets/sql-block";
import { theme } from "#/ui/theme";

function renderBlock(ui: React.ReactNode) {
	render(
		<MantineProvider theme={theme} env="test">
			{ui}
		</MantineProvider>,
	);
}

describe("SqlBlock (DAT-577)", () => {
	afterEach(() => cleanup());

	it("renders the SQL text verbatim", () => {
		renderBlock(<SqlBlock sql="SELECT 1 FROM t" />);
		expect(screen.getByText("SELECT 1 FROM t")).toBeTruthy();
	});

	it("renders a label above the SQL when given", () => {
		renderBlock(<SqlBlock sql="SELECT 1" label="SQL executed" />);
		expect(screen.getByText("SQL executed")).toBeTruthy();
		expect(screen.getByText("SELECT 1")).toBeTruthy();
	});

	it("omits the label block when no label is given", () => {
		renderBlock(<SqlBlock sql="SELECT 1" data-testid="bare" />);
		expect(screen.queryByText("SQL executed")).toBeNull();
		expect(screen.getByTestId("bare")).toBeTruthy();
	});

	it("renders bind params when present", () => {
		renderBlock(
			<SqlBlock
				sql="SELECT * FROM t WHERE a = $1 AND b = $2"
				params={[42, null]}
			/>,
		);
		expect(screen.getByTestId("sql-block-params")).toBeTruthy();
		expect(screen.getByText("42")).toBeTruthy();
		expect(screen.getByText("null")).toBeTruthy();
	});

	it("renders no params block for an empty params array", () => {
		renderBlock(<SqlBlock sql="SELECT 1" params={[]} data-testid="noparams" />);
		expect(screen.queryByTestId("sql-block-params")).toBeNull();
	});

	it("applies the data-testid to the outer element", () => {
		renderBlock(<SqlBlock sql="SELECT 1" label="X" data-testid="outer" />);
		expect(screen.getByTestId("outer")).toBeTruthy();
	});
});
