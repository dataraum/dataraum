// CENTRAL UI DEFINITION — single source of truth for the cockpit's visual
// shell (DAT-380, C0).
//
// Palette values are placeholders; visual identity tuned later. Structure is
// the contract — change design in this one file. Every region size / color /
// radius in the app reads from here; there are NO hardcoded hex/px values in
// JSX anywhere.
//
// The same numbers are mirrored into Tailwind v4's `@theme` block in
// styles.css. When you change a token here, change it there too (and vice
// versa) — they are one palette expressed in two systems (Mantine for
// components, Tailwind for layout utilities).

import { createTheme, type MantineColorsTuple } from "@mantine/core";

// --- Brand ramp -----------------------------------------------------------
// A neutral, professional blue-grey. Light, document-feel. Index 6 is the
// Mantine primaryShade (the default filled tone).
const brand: MantineColorsTuple = [
	"#eef2f7",
	"#dbe2ec",
	"#b6c4d8",
	"#8fa4c2",
	"#6f89b0",
	"#5b78a6",
	"#4f6e9f", // primary
	"#41598a",
	"#384f7c",
	"#2c416b",
];

// --- Per-stage colors ------------------------------------------------------
// One color per engine pipeline stage. Used to tint stage chrome (badges,
// progress, section accents) so a stage is recognizable at a glance. Keys
// match the engine stage names exactly.
export const stageColors = {
	connect: "#3b82a6",
	frame: "#6b6fc9",
	select: "#7a5ec2",
	add_source: "#2f9e7e",
	begin_session: "#c2913b",
	operating_model: "#b5573f",
	answer: "#3f7a52",
} as const;

export type Stage = keyof typeof stageColors;

// --- Design tokens ---------------------------------------------------------
// Plain object so non-Mantine code (Tailwind mirror, tests, layout math) can
// read the same values. Sizes are CSS length strings; the few raw numbers
// (rail/topbar) are pixel scalars AppShell consumes directly.
export const tokens = {
	colors: {
		brand,
		stage: stageColors,
		// Document-feel surfaces.
		surface: "#ffffff",
		surfaceMuted: "#f6f8fa",
		border: "#e3e8ef",
		text: "#1f2933",
		textMuted: "#647084",
	},
	spacing: {
		xs: "0.5rem",
		sm: "0.75rem",
		md: "1rem",
		lg: "1.5rem",
		xl: "2rem",
	},
	radii: {
		xs: "0.25rem",
		sm: "0.375rem",
		md: "0.5rem",
		lg: "0.75rem",
	},
	typography: {
		fontFamily:
			'-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif',
		fontFamilyMonospace:
			'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace',
		// A real scale, not just sm/md/lg. xs = meta/caption, md = body; xl/2xl give
		// section headings and the landing display genuine hierarchy (Refactoring UI:
		// size is one of the four hierarchy levers — give it room to work).
		fontSizeXs: "0.75rem",
		fontSizeSm: "0.8125rem",
		fontSizeMd: "0.9375rem",
		fontSizeLg: "1.125rem",
		fontSizeXl: "1.5rem",
		fontSize2xl: "2rem",
	},
	// Shell chrome dimensions (pixels — AppShell wants scalars).
	shell: {
		railWidth: 60,
		topBarHeight: 48,
	},
} as const;

// --- Mantine theme ---------------------------------------------------------
export const theme = createTheme({
	primaryColor: "brand",
	primaryShade: 6,
	colors: {
		brand,
	},
	fontFamily: tokens.typography.fontFamily,
	fontFamilyMonospace: tokens.typography.fontFamilyMonospace,
	// Off-black body text (#1f2933). Mantine's default text color resolves from
	// `black` in light mode; pure #000 on white is harsh and "cheap"-looking
	// (Refactoring UI). One change here recolors every default Text/Title.
	black: tokens.colors.text,
	// Heading hierarchy: distinct sizes + a single weight so a Title reads as a
	// heading by size+weight, not by shouting. h1 = section/landing display.
	headings: {
		fontWeight: "600",
		sizes: {
			h1: { fontSize: tokens.typography.fontSize2xl, lineHeight: "1.2" },
			h2: { fontSize: tokens.typography.fontSizeXl, lineHeight: "1.3" },
			h3: { fontSize: tokens.typography.fontSizeLg, lineHeight: "1.4" },
		},
	},
	defaultRadius: "md",
	radius: {
		xs: tokens.radii.xs,
		sm: tokens.radii.sm,
		md: tokens.radii.md,
		lg: tokens.radii.lg,
	},
	spacing: {
		xs: tokens.spacing.xs,
		sm: tokens.spacing.sm,
		md: tokens.spacing.md,
		lg: tokens.spacing.lg,
		xl: tokens.spacing.xl,
	},
	fontSizes: {
		xs: tokens.typography.fontSizeXs,
		sm: tokens.typography.fontSizeSm,
		md: tokens.typography.fontSizeMd,
		lg: tokens.typography.fontSizeLg,
		xl: tokens.typography.fontSizeXl,
	},
});
