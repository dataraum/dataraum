"""Interactive CLI gate handler using Rich.

Presents gates to the user in the terminal with numbered options
and an escape hatch for free-text LLM questions.
"""

from __future__ import annotations

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
    Free-text input is forwarded to an LLM for gate-contextual answers.
    """

    def __init__(self, console: Console | None = None):
        self.console = console or Console()

    async def resolve(self, gate: Gate) -> GateResolution:
        """Present gate to user and collect their choice."""
        self._render_gate(gate)
        return self._prompt_user(gate)

    async def notify(self, message: str) -> None:
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
