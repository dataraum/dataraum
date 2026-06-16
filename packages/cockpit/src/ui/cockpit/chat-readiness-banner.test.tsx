// @vitest-environment jsdom

import { MantineProvider } from "@mantine/core";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { ChatReadinessBanner } from "#/ui/cockpit/chat-readiness-banner";

afterEach(() => cleanup());

describe("ChatReadinessBanner (DAT-534)", () => {
	it("renders the message with its tone", () => {
		render(
			<MantineProvider env="test">
				<ChatReadinessBanner
					readiness={{ tone: "blocked", message: "Import data first." }}
				/>
			</MantineProvider>,
		);
		const banner = screen.getByTestId("chat-readiness");
		expect(banner.dataset.tone).toBe("blocked");
		expect(banner.textContent).toContain("Import data first.");
	});
});
