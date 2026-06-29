---
name: test-mock-coverage-gap
description: Graph agent and validation agent tests mock renderer.render_split — they verify context dict keys but never exercise the real template render path
metadata:
  type: feedback
---

`test_prompt_selection.py` and the validation integration conftest both mock `renderer.render_split.return_value = (...)`. This means the assertions prove the context dict is built correctly but never exercise `_prepare_context` or `_render_text`. A mismatched `{placeholder}` vs `inputs:` entry is invisible to these tests.

**Why:** DAT-645 introduced `{vertical_conventions}` in `graph_sql_generation.yaml` but omitted the `inputs:` entry — tests stayed green because the renderer was mocked.

**How to apply:** Add at least one real-render smoke test per prompt template that exercises the actual `PromptRenderer.render_split` path (not mocked), or add a lint test that loads all YAML templates and checks that every `{placeholder}` in `user_prompt`/`system_prompt` is listed in `inputs:`.
