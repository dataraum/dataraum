"""Interactive CLI gate handler using Rich.

Presents gates to the user in the terminal with numbered options
and an escape hatch for free-text LLM questions.
"""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.text import Text

from dataraum.pipeline.gates import (
    Gate,
    GateActionType,
    GateResolution,
)


class InteractiveCLIHandler:
    """Gate handler for interactive CLI sessions.

    Renders gates as Rich panels with numbered options.
    When a FIX action is selected and pipeline context is available,
    executes the fix via FixExecutor and displays before/after scores.
    Free-text input is forwarded to an LLM for gate-contextual answers.
    """

    def __init__(self, console: Console | None = None):
        self.console = console or Console()
        self._manager: Any | None = None
        self._source_id: str = ""
        self._live: Any | None = None

    def set_context(self, manager: Any, source_id: str) -> None:
        """Inject pipeline context for fix execution.

        Called by the runner after the ConnectionManager is created.
        """
        self._manager = manager
        self._source_id = source_id

    def set_live(self, live: Any) -> None:
        """Inject the Live display so we can pause/resume it."""
        self._live = live

    def resolve(self, gate: Gate) -> GateResolution:
        """Present gate to user and collect their choice."""
        try:
            if self._live:
                self._live.stop()
            self._render_gate(gate)
            return self._prompt_user(gate)
        except (KeyboardInterrupt, EOFError):
            self.console.print("\n  [dim]Interrupted — skipping gate[/dim]")
            return GateResolution(action_taken=GateActionType.SKIP)
        finally:
            if self._live:
                self._live.start()

    def notify(self, message: str) -> None:
        """Display a notification message."""
        self.console.print(f"  [dim]{message}[/dim]")

    def _render_gate(self, gate: Gate) -> None:
        """Render the gate as a Rich panel."""
        # Build violation table
        violation_table = Table(show_header=False, box=None, padding=(0, 2))
        violation_table.add_column("Dimension", style="bold")
        violation_table.add_column("Score", justify="right")
        violation_table.add_column("Threshold", justify="right", style="dim")
        violation_table.add_column("Status")

        for v in gate.violations:
            score_style = "red" if v.score > v.threshold else "green"
            violation_table.add_row(
                v.dimension,
                f"{v.score:.2f}",
                f"(max {v.threshold:.2f})",
                Text("BLOCKED", style=score_style),
            )
            if v.evidence_summary:
                violation_table.add_row("", "", "", f"  {v.evidence_summary}")
            for target in v.affected_targets[:3]:  # Show at most 3
                violation_table.add_row("", "", "", f"  -> {target}")

        # Build action list
        actions_text = Text()
        for action in gate.suggested_actions:
            actions_text.append(f"\n  [{action.index}] ", style="bold cyan")
            actions_text.append(action.label)
            if action.confidence > 0:
                actions_text.append(f" ({action.confidence:.0%} confidence)", style="dim")

        actions_text.append("\n\n  Or type a question about this gate: ", style="italic dim")

        # Compose panel content
        content = Text()
        content.append_text(Text.from_ansi(str(violation_table)))
        content.append_text(actions_text)

        title = f"GATE: {gate.gate_type}"
        self.console.print()
        self.console.print(
            Panel(
                content,
                title=title,
                title_align="left",
                border_style="yellow",
                padding=(1, 2),
            )
        )

    def _prompt_user(self, gate: Gate) -> GateResolution:
        """Prompt user for gate resolution."""
        valid_indices = {str(a.index) for a in gate.suggested_actions}

        while True:
            choice = Prompt.ask("  Choice", console=self.console)
            choice = choice.strip()

            if not choice:
                continue

            # Check for numbered action
            if choice in valid_indices:
                idx = int(choice)
                action = next(a for a in gate.suggested_actions if a.index == idx)

                if action.action_type == GateActionType.SKIP:
                    return GateResolution(
                        action_taken=GateActionType.SKIP,
                        action_index=idx,
                    )

                if action.action_type == GateActionType.FIX:
                    fix_result = self._execute_fix(action, gate)
                    if fix_result is not None:
                        self._display_fix_result(fix_result)
                    return GateResolution(
                        action_taken=GateActionType.FIX,
                        action_index=idx,
                        parameters=action.parameters,
                    )

                return GateResolution(
                    action_taken=action.action_type,
                    action_index=idx,
                    parameters=action.parameters,
                )

            # Free-text question
            return GateResolution(
                action_taken=GateActionType.QUESTION,
                user_input=choice,
            )

    def _execute_fix(self, action: Any, gate: Gate) -> Any:
        """Execute a fix action via FixExecutor.

        Returns FixResult on success, None if context not available.
        """
        if not self._manager:
            self.console.print("  [yellow]Cannot execute fix: no pipeline context available[/yellow]")
            return None

        from dataraum.entropy.fix_executor import (
            FixExecutor,
            FixRequest,
            get_default_action_registry,
        )

        action_type = action.parameters.get("action_type", "")
        target = action.parameters.get("target", "")

        registry = get_default_action_registry()
        definition = registry.get(action_type)
        if not definition:
            self.console.print(f"  [red]Unknown action: {action_type}[/red]")
            return None

        # Prompt for any required parameters not already in action.parameters
        params = dict(action.parameters)
        for param_name, param_desc in definition.parameters_schema.items():
            if param_name not in params or not params[param_name]:
                value = Prompt.ask(f"  {param_desc}", console=self.console)
                params[param_name] = value

        request = FixRequest(
            action_type=action_type,
            target=target,
            parameters=params,
            actor="user",
            gate_type=gate.gate_type,
            blocked_phase=gate.blocked_phase,
            source_id=self._source_id,
        )

        executor = FixExecutor(registry)
        try:
            with self._manager.session_scope() as session:
                return executor.execute(request, session)
        except Exception as e:
            self.console.print(f"  [red]Fix execution error: {e}[/red]")
            return None

    def _display_fix_result(self, result: Any) -> None:
        """Display fix execution result with before/after scores."""
        if result.success:
            self.console.print("  [green]Fix applied successfully[/green]")

            # Show before/after scores
            if result.before_scores and result.after_scores:
                self.console.print()
                for dim in sorted(result.before_scores):
                    before = result.before_scores[dim]
                    after = result.after_scores.get(dim, before)
                    delta = after - before
                    if delta < 0:
                        style = "green"
                        arrow = "improved"
                    elif delta > 0:
                        style = "red"
                        arrow = "worsened"
                    else:
                        style = "dim"
                        arrow = "unchanged"
                    self.console.print(
                        f"    {dim}: {before:.3f} -> {after:.3f} ({arrow})",
                        style=style,
                    )

            if result.improved:
                self.console.print("  [green]Overall improvement detected[/green]")
            else:
                self.console.print("  [yellow]No improvement detected[/yellow]")
        else:
            self.console.print(f"  [red]Fix failed: {result.error}[/red]")
