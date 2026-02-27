# Project: Release & Productization

*Turn the codebase into a distributable product with CI, docs, and proper release management.*

---

## Problem

The codebase works but isn't shippable. There's no CI, no release process, no published package, no public docs, and no way for someone outside the project to install and use it. The MCP server and plugin exist but live inside the source tree with no distribution path.

### Current state (updated 2026-02-27)

| Item | Status |
|---|---|
| CI/CD | **Done** — `.github/workflows/ci.yml` (ruff check + format + pytest on push/PR to main) |
| PyPI package | Not published. `pyproject.toml` defines `dataraum` v0.1.0 but no build/publish pipeline |
| MCP server | Works locally via `dataraum-mcp` console script. 9 tools (6 core + 3 source mgmt). Not listed in any registry |
| Plugin | **Done** — Separate repo `dataraum/dataraum-plugin` with 9 skills, `.mcp.json`, `plugin.json` |
| Onboarding | **Done** — Source discovery, add/remove, credential chain, database backends (postgres/mysql/sqlite) |
| Documentation | Hand-written Markdown in `docs/` + `docs_new/` (rewrite in progress). No generated site |
| README | Needs update — header is correct but content may be stale |
| LICENSE | Apache 2.0 text present, but copyright placeholders not filled in |
| Version management | `"0.1.0"` in pyproject.toml and plugin.json — synced but not tag-driven |
| Branch protection | None — direct push to main |
| Dependencies | `anthropic` and `sentence-transformers` (pulls PyTorch) in core deps, not optional |
| Pre-commit | **Done** — `.pre-commit-config.yaml` exists |

---

## Workstreams

### 1. Fix the basics

Before anything public-facing, clean up what's broken.

**README.md:**
- Rewrite to match actual state: package name is `dataraum`, CLI is `dataraum` and `dataraum-mcp`
- Quick start for MCP server setup (the most common entry point)
- Remove references to FastAPI, Apache Hamilton, `dataraum-context` name

**LICENSE:**
- Fill in copyright year and holder

**Version sync:**
- Single source of truth for version. Use `hatch-vcs` (git tag → version) or keep hardcoded but sync `pyproject.toml` and `plugin.json`

**Dependency cleanup:**
- Keep `anthropic` and `sentence-transformers` in core — the product requires an LLM, and vector search is a core capability
- Review remaining optional extras for redundancy (several duplicate core deps)
- Keep `typer`, `rich`, `textual` in core (CLI is the primary interface)

**Effort:** Low. Do first.

---

### 2. GitHub Actions

Three workflows, added incrementally.

#### a) CI — test + lint (add first)

```yaml
# .github/workflows/ci.yml
# Triggers: push to main, PR to main
# Steps:
#   - uv sync --extra dev
#   - ruff check + ruff format --check
#   - pytest tests/ -v --timeout=120
```

Start with just `ruff` + `pytest`. Add `mypy` later once the existing type errors are cleaned up (strict mode is configured but likely has many violations).

#### b) Release — build + publish to PyPI

```yaml
# .github/workflows/release.yml
# Triggers: push tag matching v*
# Steps:
#   - uv build
#   - twine upload (or uv publish) to PyPI
```

Tag-triggered. `v0.2.0` tag → build wheel → publish to PyPI. Needs a PyPI API token in GitHub secrets.

#### c) Docs — build + deploy

```yaml
# .github/workflows/docs.yml
# Triggers: push to main (docs/ or zensical config changed)
# Steps:
#   - zensical build
#   - deploy to GitHub Pages
```

Added after Zensical is set up (workstream 5).

**Effort:** Medium. CI workflow is straightforward; release workflow needs PyPI account setup.

---

### 3. Lock main, feature branches

**Branch protection on `main`:**
- Require PR for all changes (no direct push)
- Require CI checks to pass before merge
- Require at least 1 approval (even if it's self-approval for solo dev — enforces the PR habit)

**Branch naming:**
- `feature/*` — new functionality
- `fix/*` — bug fixes
- `refactor/*` — restructuring (already using this: `refactor/streamline`)
- `docs/*` — documentation changes
- `release/*` — release prep if needed

**Implementation:** GitHub repo settings → Branch protection rules. Can be done via `gh api` or the GitHub UI. No code changes needed.

**Effort:** Low. Do alongside CI setup — protection rules referencing CI checks need the checks to exist first.

---

### 4. Release the MCP server

The MCP server is the primary distribution target — it's how users interact with the product.

**Step 1: Publish `dataraum` to PyPI**

Once CI and release workflows exist, tag `v0.2.0` → publish. Users install with:
```bash
pip install dataraum
# or for full LLM support
pip install dataraum[llm]
```

The `dataraum-mcp` console script is the entry point. Claude Desktop config:
```json
{
  "mcpServers": {
    "dataraum": {
      "command": "dataraum-mcp",
      "env": { "DATARAUM_OUTPUT_DIR": "./pipeline_output" }
    }
  }
}
```

**Step 2: List in MCP registries**

- [MCP Servers directory](https://github.com/modelcontextprotocol/servers) — submit a PR to add `dataraum` to the community servers list
- [Smithery](https://smithery.ai/) — MCP server registry, submit listing
- [mcp.run](https://mcp.run/) — another registry if it accepts submissions

Each registry has its own submission format, but they all just need: name, description, install command, tool list.

**Effort:** Medium. PyPI publish is mechanical once CI exists. Registry submissions are documentation work.

---

### 5. Separate plugin GitHub project

The Claude Desktop plugin (skills + `.mcp.json` + `plugin.json`) should be a separate repo: `dataraum/dataraum-plugin`.

**Why separate:**
- The plugin is a Claude-specific integration layer; the core library is LLM-agnostic
- Plugin updates (skill prompt changes, new skills) shouldn't require a core library release
- Users clone/install the plugin repo into Claude Desktop; they `pip install` the core library
- Keeps the core repo clean for other integrations (OpenAI, custom agents, etc.)

**What moves to `dataraum-plugin`:**
```
dataraum-plugin/
├── .claude-plugin/
│   └── plugin.json          # from src/dataraum/plugin/
├── .mcp.json                # from src/dataraum/plugin/
├── skills/
│   ├── analyze/SKILL.md
│   ├── context/SKILL.md
│   ├── entropy/SKILL.md
│   ├── contracts/SKILL.md
│   ├── query/SKILL.md
│   └── actions/SKILL.md
├── README.md                # installation + usage guide
└── CONNECTORS.md            # setup guide
```

**What stays in `dataraum-context`:**
- `src/dataraum/mcp/server.py` — the MCP server implementation
- All core library code

**Effort:** Low. It's mostly moving files and writing a README.

---

### 6. Documentation with Zensical

Set up [Zensical](https://zensical.org/) for a proper documentation site, deployed to GitHub Pages.

**Structure:**
```
docs-site/                    # or root-level zensical config
├── zensical.yml              # site config
├── docs/
│   ├── index.md              # landing page
│   ├── getting-started/
│   │   ├── installation.md   # pip install, MCP setup
│   │   ├── first-analysis.md # run pipeline on sample data
│   │   └── plugin-setup.md   # Claude Desktop integration
│   ├── concepts/
│   │   ├── entropy.md        # the core innovation explained
│   │   ├── contracts.md      # data readiness contracts
│   │   ├── pipeline.md       # 18-phase pipeline overview
│   │   └── fixes.md          # fix ledger concept
│   ├── tools/
│   │   ├── analyze.md        # MCP tool reference
│   │   ├── get-context.md
│   │   ├── get-entropy.md
│   │   ├── evaluate-contract.md
│   │   ├── query.md
│   │   └── get-actions.md
│   ├── configuration/
│   │   ├── ontologies.md     # domain ontology YAML
│   │   ├── patterns.md       # pattern detection config
│   │   └── llm.md            # LLM provider setup
│   └── development/
│       ├── architecture.md   # pulled from existing docs/ARCHITECTURE.md
│       └── contributing.md
```

Most content already exists in `docs/` — it needs to be reorganized and edited for a public audience (remove internal references, add context for newcomers).

**Effort:** Medium-High. Zensical setup is quick; content editing is the bulk of the work.

---

### 7. Move roadmap to GitHub Issues

Replace `docs/BACKLOG.md` and `docs/projects/*.md` with GitHub Issues and Projects for public tracking.

**Implementation:**
- Create a GitHub Project board (table view) with columns: Backlog, In Progress, Done
- Convert each project (Fixes, Onboarding, Incremental Imports, Quality UI, Release) into a GitHub Issue with the `project` label
- Convert individual work items into sub-issues or task lists within the project issue
- Use GitHub milestones for grouping (v0.2, v0.3, etc.)

Can be done via `gh` CLI:
```bash
gh issue create --title "Data Fixes" --body "$(cat docs/projects/fixes.md)" --label "project"
```

Keep `docs/projects/*.md` as the detailed design docs (linked from the issues). GitHub Issues are for tracking status; the markdown docs are for design detail.

**Effort:** Low. Mechanical migration.

---

### 8. Apply to Claude Desktop tool directory

Anthropic curates tools shown in Claude Desktop. Submission requires:
- A published, installable MCP server
- Documentation
- A working plugin with skills
- A clear value proposition

**Prerequisites:** Steps 1–6 above. The application is the last step — everything else makes the product presentable.

**What to highlight in the application:**
- Pre-computed metadata context (unique approach — most tools discover at runtime)
- Entropy-based uncertainty quantification for data quality
- 6 tools covering the full data quality lifecycle
- Works with any CSV/Parquet data, extensible to database backends
- Domain ontologies for specialized contexts (financial, marketing, etc.)

**Effort:** Low (the application itself). High prerequisite investment.

---

## Sequencing & Status

```
1. Fix basics (README, LICENSE, deps)     ─┐
2. GitHub Actions CI                ✅     ─┤── Foundation
3. Lock main, feature branches             ─┘
        │
        ▼
4. Release MCP server to PyPI             ─┐
5. Separate plugin repo             ✅     ─┤── Distribution
6. List in MCP registries                  ─┘
        │
        ▼
7. Docs site                              ─┐
8. Migrate roadmap to GitHub/Linear ✅     ─┤── Public presence
9. Apply to Claude Desktop directory       ─┘
```

**Done:**
- CI workflow (ruff + pytest on push/PR to main)
- Plugin separated to `dataraum/dataraum-plugin` repo
- Roadmap tracked in Linear (private) + GitHub Issues (public)
- Pre-commit config exists
- Onboarding tools (discover_sources, add_source, remove_source, credential chain, database backends)

**Next:**
- Fix basics (README, LICENSE, version sync, dependency cleanup)
- Branch protection on main
- PyPI release pipeline + first publish
- MCP registry listings
- Docs site

## Open Questions

- Package name: is `dataraum` the final PyPI name, or should it be `dataraum-context` (as the old README suggested)?
- Do we need a separate `dataraum-mcp` package, or is the console script from the main `dataraum` package sufficient?
- PyPI organization account: does `dataraum` on PyPI need to be reserved?
- GitHub Pages domain: `dataraum.github.io/dataraum-context` vs custom domain?
