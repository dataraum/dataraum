"""Fix command — review data quality actions and document domain knowledge."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from rich.table import Table as RichTable

from dataraum.cli.common import OutputDirArg, console

if TYPE_CHECKING:
    from dataraum.documentation.agent import DocumentAgent


def fix(
    output_dir: OutputDirArg = Path("./pipeline_output"),
) -> None:
    """Review data quality actions and document domain knowledge.

    Shows document_* actions from the pipeline and runs an interactive
    agent to elicit domain knowledge from the user. Fixes are stored
    in the fix_ledger and used as context on semantic re-runs.

    Examples:

        dataraum fix ./pipeline_output
    """
    import typer
    from sqlalchemy import select

    from dataraum.cli.common import get_manager
    from dataraum.documentation.ledger import get_active_fixes, log_fix
    from dataraum.entropy.actions import load_actions
    from dataraum.storage import Source

    manager = get_manager(output_dir)

    try:
        with manager.session_scope() as session:
            # Get source
            sources = session.execute(select(Source)).scalars().all()
            if not sources:
                console.print("[red]No sources found. Run the pipeline first.[/red]")
                raise typer.Exit(1)

            source = sources[0]

            # Load actions and filter to document_* only
            all_actions = load_actions(session, source)
            doc_actions = [a for a in all_actions if a["action"].startswith("document_")]

            if not doc_actions:
                console.print("[green]No document actions pending.[/green]")
                return

            while True:
                # Filter out actions already fully covered by fixes
                active_fixes = get_active_fixes(session, source.source_id)
                fixed_keys = {
                    (f.action_name, f.table_name, f.column_name) for f in active_fixes
                }
                pending_actions = []
                for action in doc_actions:
                    cols = action.get("affected_columns", [])
                    unfixed = [
                        c for c in cols
                        if _col_ref_to_key(action["action"], c) not in fixed_keys
                    ]
                    if unfixed:
                        pending_actions.append(action)

                if not pending_actions:
                    console.print("[green]All document actions have been addressed.[/green]")
                    break

                # Paginate: show up to 10 at a time
                page = pending_actions[:10]
                remaining = len(pending_actions) - len(page)

                # Display actions table
                title = "Document Actions"
                if remaining > 0:
                    title += f" (showing {len(page)} of {len(pending_actions)})"
                table = RichTable(title=title, show_lines=True)
                table.add_column("#", style="bold", width=3)
                table.add_column("Action", style="cyan")
                table.add_column("Description")
                table.add_column("Scope", style="dim")
                table.add_column("Priority", style="bold")

                for i, action in enumerate(page, 1):
                    scope = _format_scope(action.get("affected_columns", []))
                    table.add_row(
                        str(i),
                        action["action"],
                        action.get("description", ""),
                        scope,
                        action.get("priority", ""),
                    )

                console.print()
                console.print(table)
                console.print()

                # User selects action
                choice = console.input("[bold]Select action number (q to quit): [/bold]").strip()
                if choice.lower() == "q":
                    break

                try:
                    idx = int(choice) - 1
                    if not 0 <= idx < len(page):
                        console.print("[red]Invalid number.[/red]")
                        continue
                except ValueError:
                    console.print("[red]Enter a number or 'q'.[/red]")
                    continue

                selected = page[idx]

                # Build comprehensive context from DB metadata
                column_context = _build_agent_context(session, selected, source.source_id)

                # Initialize LLM and run agent
                try:
                    agent = _create_document_agent()
                except Exception as e:
                    console.print(f"[red]Failed to initialize LLM: {e}[/red]")
                    continue

                # Generate questions
                console.print("\n[dim]Generating questions...[/dim]")
                q_result = agent.generate_questions(column_context)
                if not q_result.success:
                    console.print(f"[red]Error: {q_result.error}[/red]")
                    continue

                questions = q_result.unwrap()
                console.print(f"\n[dim]{questions.context_summary}[/dim]\n")

                # Collect answers (with clarify option)
                answers_parts: list[str] = []
                for q in questions.questions:
                    console.print(f"[bold]{q.question}[/bold]")
                    if q.question_type == "multiple_choice" and q.choices:
                        for j, choice_text in enumerate(q.choices, 1):
                            console.print(f"  {j}. {choice_text}")
                    console.print("[dim]  (type 'c' to clarify this question)[/dim]")
                    answer = console.input("[green]> [/green]").strip()

                    # Clarify loop
                    while answer.lower() == "c":
                        console.print("[dim]Clarifying...[/dim]")
                        clarify_result = agent.generate_questions(
                            column_context
                            + f"\n\nThe user found this question unclear: {q.question!r}"
                            + "\nPlease rephrase with simpler, more concrete language."
                            + " Reference actual column names, sample values, or statistics.",
                        )
                        if clarify_result.success:
                            rephrased = clarify_result.unwrap()
                            if rephrased.questions:
                                q = rephrased.questions[0]
                                console.print(f"\n[bold]{q.question}[/bold]")
                                if q.question_type == "multiple_choice" and q.choices:
                                    for j, choice_text in enumerate(q.choices, 1):
                                        console.print(f"  {j}. {choice_text}")
                                console.print("[dim]  (type 'c' to clarify again)[/dim]")
                        else:
                            console.print("[yellow]Could not clarify. Please answer as best you can.[/yellow]")
                        answer = console.input("[green]> [/green]").strip()

                    # Resolve multiple choice number to text
                    if q.question_type == "multiple_choice" and q.choices:
                        try:
                            ci = int(answer) - 1
                            if 0 <= ci < len(q.choices):
                                answer = q.choices[ci]
                        except ValueError:
                            pass

                    answers_parts.append(f"Q: {q.question}\nA: {answer}")
                    console.print()

                user_answers = "\n\n".join(answers_parts)

                # Interpret answers
                console.print("[dim]Interpreting answers...[/dim]")
                interp_result = agent.interpret_answers(column_context, user_answers)
                if not interp_result.success:
                    console.print(f"[red]Error: {interp_result.error}[/red]")
                    continue

                interp = interp_result.unwrap()

                console.print(f"\n[bold]Interpretation:[/bold] {interp.summary}")
                console.print(f"[dim]{interp.interpretation}[/dim]")
                console.print(f"[dim]Confidence: {interp.confidence}[/dim]\n")

                confirm = console.input("[bold]Confirm? (y/n): [/bold]").strip().lower()
                if confirm != "y":
                    console.print("[yellow]Skipped.[/yellow]")
                    continue

                # Log fix for each affected column
                for col_ref in selected.get("affected_columns", []):
                    parts = col_ref.split(".", 1)
                    tbl = parts[0]
                    col = parts[1] if len(parts) > 1 else None

                    log_fix(
                        session=session,
                        source_id=source.source_id,
                        action_name=selected["action"],
                        table_name=tbl,
                        column_name=col,
                        user_input=user_answers,
                        interpretation=interp.interpretation,
                    )

                session.commit()
                console.print("[green]Fix logged successfully.[/green]")

    finally:
        manager.close()


def _col_ref_to_key(action_name: str, col_ref: str) -> tuple[str, str, str | None]:
    """Convert a col_ref like 'table.col' or 'table' to a fix_ledger lookup key."""
    parts = col_ref.split(".", 1)
    table = parts[0]
    column = parts[1] if len(parts) > 1 else None
    return (action_name, table, column)


def _format_scope(affected_columns: list[str]) -> str:
    """Format affected columns as a scope string, distinguishing table vs column level."""
    if not affected_columns:
        return ""

    # Separate table-level (no dot) from column-level entries
    tables = [c for c in affected_columns if "." not in c]
    columns = [c for c in affected_columns if "." in c]

    parts: list[str] = []
    if tables:
        parts.append("Table: " + ", ".join(tables[:3]))
        if len(tables) > 3:
            parts[-1] += f" (+{len(tables) - 3})"
    if columns:
        parts.append("Columns: " + ", ".join(columns[:3]))
        if len(columns) > 3:
            parts[-1] += f" (+{len(columns) - 3})"

    return "\n".join(parts)


def _build_agent_context(session: Any, action: dict[str, Any], source_id: str) -> str:
    """Build comprehensive context for the document agent.

    Assembles everything the agent needs to ask grounded questions:
    - The action itself (why it was proposed, what it expects)
    - Entropy interpretation (the reasoning chain that led to the action)
    - Column/table statistics and semantic annotations
    - Existing fixes on the same table (to avoid redundant questions)

    Args:
        session: Database session
        action: Action dict from load_actions()
        source_id: Source ID for querying existing fixes

    Returns:
        Structured context string for the LLM prompt
    """
    from sqlalchemy import select

    from dataraum.analysis.semantic.db_models import SemanticAnnotation
    from dataraum.analysis.statistics.db_models import StatisticalProfile
    from dataraum.documentation.ledger import get_active_fixes
    from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord
    from dataraum.storage import Column, Table

    sections: list[str] = []

    # === Section 1: Action details ===
    action_lines = [
        "<action_details>",
        f"Action: {action.get('action', '')}",
        f"Description: {action.get('description', '')}",
        f"Priority: {action.get('priority', '')} (score: {action.get('priority_score', 0):.2f})",
        f"Effort: {action.get('effort', '')}",
    ]
    if action.get("expected_impact"):
        action_lines.append(f"Expected impact: {action['expected_impact']}")
    params = action.get("parameters", {})
    if params:
        action_lines.append("Suggested parameters:")
        for k, v in params.items():
            action_lines.append(f"  {k}: {v}")
    if action.get("fixes_violations"):
        action_lines.append(f"Fixes contract violations: {', '.join(action['fixes_violations'])}")
    action_lines.append("</action_details>")
    sections.append("\n".join(action_lines))

    # === Section 2: Entropy interpretations (the reasoning chain) ===
    affected = action.get("affected_columns", [])
    interp_lines = ["<entropy_analysis>"]
    interp_lines.append("Why this action was proposed (from entropy analysis):")

    for col_ref in affected:
        parts = col_ref.split(".", 1)
        table_name = parts[0]
        column_name = parts[1] if len(parts) > 1 else None

        interp_stmt = select(EntropyInterpretationRecord).where(
            EntropyInterpretationRecord.source_id == source_id,
            EntropyInterpretationRecord.table_name == table_name,
        )
        if column_name:
            interp_stmt = interp_stmt.where(
                EntropyInterpretationRecord.column_name == column_name
            )
        else:
            interp_stmt = interp_stmt.where(
                EntropyInterpretationRecord.column_name.is_(None)
            )

        interp_rec = session.execute(interp_stmt).scalars().first()
        if interp_rec:
            interp_lines.append(f"\n  [{col_ref}]")
            interp_lines.append(f"  Explanation: {interp_rec.explanation}")
            if interp_rec.assumptions_json:
                assumptions = interp_rec.assumptions_json
                if isinstance(assumptions, list) and assumptions:
                    interp_lines.append("  Assumptions made (may be wrong — ask the user):")
                    for a in assumptions[:5]:
                        dim = a.get("dimension", "")
                        text = a.get("assumption_text", "")
                        conf = a.get("confidence", "")
                        interp_lines.append(f"    - [{dim}] {text} (confidence: {conf})")

    interp_lines.append("</entropy_analysis>")
    sections.append("\n".join(interp_lines))

    # === Section 3: Data profile for each affected column/table ===
    data_lines = ["<data_profile>"]

    for col_ref in affected:
        parts = col_ref.split(".", 1)
        table_name = parts[0]
        column_name = parts[1] if len(parts) > 1 else None

        if column_name:
            stmt = (
                select(Column, Table, StatisticalProfile)
                .join(Table, Column.table_id == Table.table_id)
                .outerjoin(StatisticalProfile, Column.column_id == StatisticalProfile.column_id)
                .where(Table.table_name == table_name, Column.column_name == column_name)
                .limit(1)
            )
            row = session.execute(stmt).first()
            if row:
                col, tbl, profile = row
                data_lines.append(f"\n  Column: {table_name}.{column_name}")
                data_lines.append(f"    Table rows: {tbl.row_count or 'unknown'}")
                if profile:
                    data_lines.append(f"    Distinct values: {profile.distinct_count or 'unknown'}")
                    data_lines.append(f"    Null ratio: {profile.null_ratio or 0:.1%}")
                    pdata = profile.profile_data or {}
                    top_vals = pdata.get("top_values", [])
                    if top_vals:
                        samples = [str(v.get("value", "")) for v in top_vals[:5]]
                        data_lines.append(f"    Sample values: {', '.join(samples)}")
                    numeric = pdata.get("numeric_stats")
                    if numeric:
                        data_lines.append(
                            f"    Range: {numeric.get('min', '?')} to {numeric.get('max', '?')}"
                            f", mean: {numeric.get('mean', '?')}"
                        )

                sem_stmt = select(SemanticAnnotation).where(
                    SemanticAnnotation.column_id == col.column_id
                )
                sem = session.execute(sem_stmt).scalars().first()
                if sem:
                    data_lines.append(f"    Semantic role: {sem.semantic_role or 'unknown'}")
                    if sem.business_name:
                        data_lines.append(f"    Business name: {sem.business_name}")
                    if sem.business_concept:
                        data_lines.append(f"    Business concept: {sem.business_concept}")
                    if sem.unit_source_column:
                        data_lines.append(f"    Unit source: {sem.unit_source_column}")
        else:
            tbl_stmt = (
                select(Column, Table)
                .join(Table, Column.table_id == Table.table_id)
                .where(Table.table_name == table_name, Table.layer == "typed")
            )
            rows = session.execute(tbl_stmt).all()
            if rows:
                tbl = rows[0][1]
                data_lines.append(f"\n  Table: {table_name} ({tbl.row_count or '?'} rows)")
                data_lines.append("    Columns:")
                for col, _ in rows:
                    sem_stmt = select(SemanticAnnotation).where(
                        SemanticAnnotation.column_id == col.column_id
                    )
                    sem = session.execute(sem_stmt).scalars().first()
                    role = sem.semantic_role if sem else "unknown"
                    concept = f" ({sem.business_concept})" if sem and sem.business_concept else ""
                    data_lines.append(f"    - {col.column_name}: {role}{concept}")

    data_lines.append("</data_profile>")
    sections.append("\n".join(data_lines))

    # === Section 4: Related actions on the same columns ===
    # (helps agent understand the broader picture)
    related_lines = ["<related_actions>"]
    related_lines.append("Other actions affecting the same columns:")
    has_related = False
    for col_ref in affected:
        interp_parts = col_ref.split(".", 1)
        t_name = interp_parts[0]
        c_name = interp_parts[1] if len(interp_parts) > 1 else None

        if c_name:
            interp_stmt = select(EntropyInterpretationRecord).where(
                EntropyInterpretationRecord.source_id == source_id,
                EntropyInterpretationRecord.table_name == t_name,
                EntropyInterpretationRecord.column_name == c_name,
            )
            interp_rec = session.execute(interp_stmt).scalars().first()
            if interp_rec and interp_rec.resolution_actions_json:
                raw_actions = interp_rec.resolution_actions_json
                if isinstance(raw_actions, list):
                    for ra in raw_actions:
                        ra_name = ra.get("action", "")
                        if ra_name and ra_name != action.get("action"):
                            related_lines.append(
                                f"  - {ra_name} on {col_ref}: {ra.get('description', '')[:120]}"
                            )
                            has_related = True

    if not has_related:
        related_lines.append("  (none)")
    related_lines.append("</related_actions>")
    sections.append("\n".join(related_lines))

    # === Section 5: Existing fixes on the same table ===
    existing_fixes = get_active_fixes(session, source_id)
    table_names = {col_ref.split(".", 1)[0] for col_ref in affected}
    related_fixes = [f for f in existing_fixes if f.table_name in table_names]

    fix_lines = ["<existing_fixes>"]
    if related_fixes:
        fix_lines.append("Domain knowledge already documented for this table:")
        for f in related_fixes:
            scope = f"{f.table_name}.{f.column_name}" if f.column_name else f.table_name
            fix_lines.append(f"  - {f.action_name} on {scope}: {f.interpretation}")
    else:
        fix_lines.append("No existing domain fixes for this table.")
    fix_lines.append("</existing_fixes>")
    sections.append("\n".join(fix_lines))

    # === Section 6: Network impact (if available) ===
    if action.get("network_impact", 0) > 0:
        net_lines = ["<network_impact>"]
        net_lines.append(
            f"Bayesian network causal impact: {action['network_impact']:.3f} "
            f"across {action.get('network_columns', 0)} columns"
        )
        col_deltas = action.get("column_deltas", {})
        if col_deltas:
            net_lines.append("Per-column impact (how much entropy decreases if resolved):")
            for col_key, delta in sorted(col_deltas.items(), key=lambda x: -x[1]):
                net_lines.append(f"  {col_key}: -{delta:.3f}")
        net_lines.append("</network_impact>")
        sections.append("\n".join(net_lines))

    return "\n\n".join(sections)


def _create_document_agent() -> DocumentAgent:
    """Create a DocumentAgent with the configured LLM provider."""
    from dataraum.documentation.agent import DocumentAgent
    from dataraum.llm import PromptRenderer, create_provider, load_llm_config

    config = load_llm_config()
    provider_config = config.providers.get(config.active_provider)
    if not provider_config:
        raise ValueError(f"Provider '{config.active_provider}' not configured")

    provider = create_provider(config.active_provider, provider_config.model_dump())
    renderer = PromptRenderer()
    model = provider.get_model_for_tier("capable")

    return DocumentAgent(
        provider=provider,
        renderer=renderer,
        model=model,
    )
