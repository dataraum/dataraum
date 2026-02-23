"""Verify Bayesian Entropy Network against real pipeline output.

Usage:
    uv run python scripts/verify_network.py /path/to/pipeline_output

Loads EntropyObjects from the metadata.db, feeds them through the
network bridge, runs all 4 inference operations, and computes priorities.
"""

from __future__ import annotations

import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table as RichTable
from sqlalchemy import select

from dataraum.core.connections import get_manager_for_directory
from dataraum.entropy.core.storage import EntropyRepository
from dataraum.entropy.models import EntropyObject
from dataraum.entropy.network.bridge import entropy_objects_to_evidence
from dataraum.entropy.network.inference import (
    backward_diagnose,
    forward_propagate,
    most_probable_explanation,
    what_if_analysis,
)
from dataraum.entropy.network.model import EntropyNetwork
from dataraum.entropy.network.priority import compute_network_priorities
from dataraum.storage import Source, Table

console = Console()


def main(output_dir: Path) -> None:
    console.print("\n[bold]Bayesian Entropy Network Verification[/bold]")
    console.print(f"Pipeline output: {output_dir}\n")

    # 1. Load entropy objects from pipeline output
    manager = get_manager_for_directory(output_dir)
    try:
        with manager.session_scope() as session:
            source = session.execute(select(Source)).scalar_one()
            tables = session.execute(
                select(Table).where(
                    Table.source_id == source.source_id,
                    Table.layer == "typed",
                )
            ).scalars().all()
            table_ids = [t.table_id for t in tables]

            console.print(f"Source: [cyan]{source.name}[/cyan]")
            console.print(f"Typed tables: {len(tables)}")

            repo = EntropyRepository(session)
            all_objects = repo.load_for_tables(table_ids, enforce_typed=True)
            console.print(f"Total EntropyObjects: {len(all_objects)}\n")

            # Group objects by target for per-column analysis
            by_target: dict[str, list[EntropyObject]] = {}
            for obj in all_objects:
                by_target.setdefault(obj.target, []).append(obj)

            # 2. Build network
            network = EntropyNetwork()
            console.print(f"Network: {len(network.node_names)} nodes, "
                          f"{len(network.model.edges())} edges\n")

            # 3. Show dimension path coverage
            all_paths = {obj.dimension_path for obj in all_objects}
            from dataraum.entropy.network.bridge import build_dimension_path_to_node_map
            path_map = build_dimension_path_to_node_map(network)
            mapped = all_paths & set(path_map.keys())
            unmapped = all_paths - set(path_map.keys())

            coverage_table = RichTable(title="Dimension Path Coverage")
            coverage_table.add_column("Status")
            coverage_table.add_column("Count")
            coverage_table.add_column("Paths")
            coverage_table.add_row("Mapped", str(len(mapped)),
                                   ", ".join(sorted(mapped)[:5]) + ("..." if len(mapped) > 5 else ""))
            coverage_table.add_row("Unmapped", str(len(unmapped)),
                                   ", ".join(sorted(unmapped)[:5]) + ("..." if len(unmapped) > 5 else ""))
            console.print(coverage_table)
            console.print()

            # 4. Run per-target analysis (pick first 3 columns)
            targets = list(by_target.keys())[:3]
            for target in targets:
                objects = by_target[target]
                console.rule(f"[bold]{target}[/bold]")

                # Bridge: convert to evidence
                evidence = entropy_objects_to_evidence(objects, network)
                if not evidence:
                    console.print("[yellow]No evidence mapped for this target[/yellow]\n")
                    continue

                # Show evidence
                ev_table = RichTable(title="Evidence (from detectors)")
                ev_table.add_column("Node")
                ev_table.add_column("State")
                ev_table.add_column("Raw Score")
                score_by_node = {}
                for obj in objects:
                    node_name = path_map.get(obj.dimension_path)
                    if node_name:
                        score_by_node[node_name] = obj.score
                for node, state in sorted(evidence.items()):
                    raw = score_by_node.get(node, "?")
                    ev_table.add_row(node, state, f"{raw:.3f}" if isinstance(raw, float) else str(raw))
                console.print(ev_table)

                # Forward propagation
                posteriors = forward_propagate(network, evidence)
                intent_table = RichTable(title="Intent Node Posteriors")
                intent_table.add_column("Intent")
                intent_table.add_column("P(low)")
                intent_table.add_column("P(medium)")
                intent_table.add_column("P(high)")
                for intent in network.get_intent_nodes():
                    if intent in posteriors:
                        d = posteriors[intent]
                        intent_table.add_row(
                            intent,
                            f"{d['low']:.3f}",
                            f"{d['medium']:.3f}",
                            f"{d['high']:.3f}",
                        )
                console.print(intent_table)

                # MAP explanation
                mpe = most_probable_explanation(network, evidence)
                mpe_table = RichTable(title="Most Probable Explanation (non-evidence)")
                mpe_table.add_column("Node")
                mpe_table.add_column("MAP State")
                for node in sorted(mpe.keys()):
                    if node not in evidence:
                        mpe_table.add_row(node, mpe[node])
                console.print(mpe_table)

                # Priority analysis
                priorities = compute_network_priorities(network, evidence)
                if priorities:
                    pri_table = RichTable(title="Intervention Priorities")
                    pri_table.add_column("Node")
                    pri_table.add_column("Current")
                    pri_table.add_column("Impact Delta")
                    pri_table.add_column("Affected Intents")
                    for p in priorities[:5]:
                        intents_str = ", ".join(
                            f"{k}: {v:.3f}" for k, v in p.affected_intents.items()
                        )
                        pri_table.add_row(
                            p.node, p.current_state,
                            f"{p.impact_delta:.4f}", intents_str,
                        )
                    console.print(pri_table)
                else:
                    console.print("[green]All evidence is low — no interventions needed[/green]")

                console.print()

            # 5. Global analysis (all objects combined)
            console.rule("[bold]Global Analysis (all columns)[/bold]")
            global_evidence = entropy_objects_to_evidence(all_objects, network)
            console.print(f"Global evidence nodes: {len(global_evidence)}")
            console.print(f"States: {dict(sorted(global_evidence.items()))}\n")

            # Backward diagnosis: what causes high aggregation risk?
            if "aggregation_intent" not in global_evidence:
                diag = backward_diagnose(
                    network, {"aggregation_intent": "high"},
                )
                diag_table = RichTable(title="Root Cause Diagnosis (if aggregation_intent were high)")
                diag_table.add_column("Root Node")
                diag_table.add_column("P(low)")
                diag_table.add_column("P(medium)")
                diag_table.add_column("P(high)")
                for node in sorted(diag.keys()):
                    d = diag[node]
                    diag_table.add_row(node, f"{d['low']:.3f}", f"{d['medium']:.3f}", f"{d['high']:.3f}")
                console.print(diag_table)

            # What-if: fix the highest-priority node globally
            global_priorities = compute_network_priorities(network, global_evidence)
            if global_priorities:
                top = global_priorities[0]
                console.print(f"\n[bold]What-if: Fix [cyan]{top.node}[/cyan] from {top.current_state} → low[/bold]")
                remaining = {k: v for k, v in global_evidence.items() if k != top.node}
                wi = what_if_analysis(
                    network, remaining,
                    intervention={top.node: "low"},
                )
                for intent, dist in sorted(wi.items()):
                    console.print(f"  {intent}: P(high) = {dist['high']:.3f}")

            # 6. Network context assembler (per-column)
            console.rule("[bold]Network Context Assembler (per-column)[/bold]")
            from dataraum.entropy.views.network_context import (
                _assemble_network_context,
                format_network_context,
            )

            net_ctx = _assemble_network_context(all_objects, network)
            console.print(f"Total columns: {net_ctx.total_columns}")
            console.print(f"  Blocked: {net_ctx.columns_blocked}")
            console.print(f"  Investigate: {net_ctx.columns_investigate}")
            console.print(f"  Ready: {net_ctx.columns_ready}")
            console.print(f"Direct signals: {net_ctx.total_direct_signals}")
            console.print(f"Aggregate intents: {len(net_ctx.intents)}")
            console.print(f"Overall readiness: {net_ctx.overall_readiness}")

            if net_ctx.intents:
                intent_table = RichTable(title="Aggregate Intent Readiness")
                intent_table.add_column("Intent")
                intent_table.add_column("Worst P(high)")
                intent_table.add_column("Mean P(high)")
                intent_table.add_column("Blocked")
                intent_table.add_column("Investigate")
                intent_table.add_column("Ready")
                for ai in net_ctx.intents:
                    intent_table.add_row(
                        ai.intent_name,
                        f"{ai.worst_p_high:.3f}",
                        f"{ai.mean_p_high:.3f}",
                        str(ai.columns_blocked),
                        str(ai.columns_investigate),
                        str(ai.columns_ready),
                    )
                console.print(intent_table)

            if net_ctx.top_fix:
                tf = net_ctx.top_fix
                console.print(
                    f"\nTop fix: [cyan]{tf.node_name}[/cyan] across "
                    f"{tf.columns_affected} columns, "
                    f"total delta: {tf.total_intent_delta:.3f}"
                )
                if tf.example_columns:
                    console.print(f"  Worst: {', '.join(tf.example_columns)}")

            # Show a few at-risk columns
            at_risk = [
                (t, c) for t, c in net_ctx.columns.items()
                if c.readiness != "ready"
            ]
            if at_risk:
                at_risk.sort(key=lambda x: x[1].worst_intent_p_high, reverse=True)
                console.print(f"\nAt-risk columns ({len(at_risk)}):")
                for target, col in at_risk[:5]:
                    high_nodes = ", ".join(
                        f"{ne.node_name}={ne.state}({ne.score:.2f})"
                        for ne in col.node_evidence if ne.state != "low"
                    )
                    console.print(
                        f"  {target} ({col.readiness}, "
                        f"P(high)={col.worst_intent_p_high:.3f}) "
                        f"[dim]{high_nodes}[/dim]"
                    )

            console.print()
            formatted = format_network_context(net_ctx)
            console.print("[dim]--- Formatted output ---[/dim]")
            console.print(formatted)

    finally:
        manager.close()

    console.print("\n[bold green]Verification complete.[/bold green]\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: uv run python scripts/verify_network.py /path/to/pipeline_output")
        sys.exit(1)
    main(Path(sys.argv[1]))
